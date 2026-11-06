using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>Per-key tracking state (TrackedKeyInfo, utilities/transactions/lock/point/point_lock_tracker.h).</summary>
internal struct TrackedKeyInfo
{
    /// <summary>Earliest sequence number at which the key is known not to have been modified by others.</summary>
    public ulong Seq;
    public uint NumWrites;
    public uint NumReads;
    public bool Exclusive;
}

/// <summary>Result of untracking a key (UntrackStatus).</summary>
internal enum UntrackStatus : byte
{
    NotTracked,
    Untracked,
    Removed,
}

/// <summary>
/// The keys tracked in one column family. Keys live in buffers recycled by the column family's table, so tracking and
/// clearing keys allocates nothing once the table and its buffer pool have grown to the transaction's size.
/// </summary>
internal sealed class TrackedColumnFamily
{
    private KeyBufferPool _buffers;

    public TrackedColumnFamily(ColumnFamilyData cfd)
    {
        Cfd = cfd;
        Keys = new Dictionary<PooledKey, TrackedKeyInfo>(PooledKeyComparer.Instance);
        Lookup = Keys.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    public ColumnFamilyData Cfd { get; }
    public Dictionary<PooledKey, TrackedKeyInfo> Keys { get; }
    public Dictionary<PooledKey, TrackedKeyInfo>.AlternateLookup<ReadOnlySpan<byte>> Lookup { get; }

    /// <summary>The tracking state of <paramref name="key"/>, added (zeroed, with a recycled key buffer) if absent.</summary>
    public ref TrackedKeyInfo GetOrAdd(ReadOnlySpan<byte> key, out bool exists)
    {
        ref TrackedKeyInfo info = ref CollectionsMarshal.GetValueRefOrNullRef(Lookup, key);
        if (!Unsafe.IsNullRef(ref info))
        {
            exists = true;
            return ref info;
        }
        exists = false;
        return ref CollectionsMarshal.GetValueRefOrAddDefault(Keys, _buffers.Rent(key), out _);
    }

    /// <summary>Untracks <paramref name="key"/> and recycles its buffer.</summary>
    public void Remove(ReadOnlySpan<byte> key)
    {
        if (Lookup.Remove(key, out PooledKey actual, out _)) _buffers.Return(actual);
    }

    /// <summary>Untracks every key (the table and the key buffers are kept for reuse).</summary>
    public void Clear()
    {
        if (Keys.Count == 0) return;
        foreach (PooledKey k in Keys.Keys) _buffers.Return(k);
        Keys.Clear();
    }
}

/// <summary>
/// Port of PointLockTracker (utilities/transactions/lock/point/point_lock_tracker.cc): the keys a transaction read or
/// wrote (optimistic: for validation at commit; pessimistic: the locks to release), with read/write counts so that
/// rolling back to a save point can untrack exactly the keys tracked since then.
/// </summary>
internal sealed class LockTracker
{
    // Most transactions touch a single column family: keep the first one inline.
    private TrackedColumnFamily? _first;
    private List<TrackedColumnFamily>? _more;

    public bool IsEmpty
    {
        get
        {
            if (_first is not null && _first.Keys.Count > 0) return false;
            if (_more is not null)
            {
                foreach (TrackedColumnFamily c in _more)
                {
                    if (c.Keys.Count > 0) return false;
                }
            }
            return true;
        }
    }

    public int ColumnFamilyCount => (_first is null ? 0 : 1) + (_more?.Count ?? 0);

    /// <summary>GetNumPointLocks: the number of tracked keys over all column families.</summary>
    public long NumPointLocks
    {
        get
        {
            long n = _first?.Keys.Count ?? 0;
            if (_more is not null)
            {
                foreach (TrackedColumnFamily c in _more) n += c.Keys.Count;
            }
            return n;
        }
    }

    public TrackedColumnFamily GetColumnFamily(int i) => i == 0 ? _first! : _more![i - 1];

    public TrackedColumnFamily? Find(uint cf)
    {
        if (_first is not null && _first.Cfd.Id == cf) return _first;
        if (_more is not null)
        {
            foreach (TrackedColumnFamily c in _more)
            {
                if (c.Cfd.Id == cf) return c;
            }
        }
        return null;
    }

    private TrackedColumnFamily GetOrAdd(ColumnFamilyData cfd)
    {
        TrackedColumnFamily? c = Find(cfd.Id);
        if (c is not null) return c;
        c = new TrackedColumnFamily(cfd);
        if (_first is null) _first = c;
        else (_more ??= new List<TrackedColumnFamily>()).Add(c);
        return c;
    }

    /// <summary>PointLockTracker::Track: merges the request into the key's tracking state.</summary>
    public void Track(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ulong seq, bool readOnly, bool exclusive)
    {
        TrackedColumnFamily c = GetOrAdd(cfd);
        ref TrackedKeyInfo info = ref c.GetOrAdd(key, out bool exists);
        if (!exists)
        {
            info.Seq = seq;
        }
        else if (seq < info.Seq)
        {
            // Now tracking this key with an earlier sequence number
            info.Seq = seq;
        }
        if (readOnly) info.NumReads++;
        else info.NumWrites++;
        info.Exclusive |= exclusive;
    }

    /// <summary>Tracking state of a key; false if untracked.</summary>
    public bool TryGet(uint cf, ReadOnlySpan<byte> key, out TrackedKeyInfo info)
    {
        TrackedColumnFamily? c = Find(cf);
        if (c is not null && c.Lookup.TryGetValue(key, out info)) return true;
        info = default;
        return false;
    }

    /// <summary>
    /// PointLockTracker::Untrack of a read (UndoGetForUpdate): drops one read of the key.
    /// </summary>
    public UntrackStatus UntrackRead(uint cf, ReadOnlySpan<byte> key)
    {
        TrackedColumnFamily? c = Find(cf);
        if (c is null) return UntrackStatus.NotTracked;
        ref TrackedKeyInfo info = ref CollectionsMarshal.GetValueRefOrNullRef(c.Lookup, key);
        if (Unsafe.IsNullRef(ref info)) return UntrackStatus.NotTracked;
        if (info.NumReads > 0) info.NumReads--;
        if (info.NumReads == 0 && info.NumWrites == 0)
        {
            c.Remove(key);
            return UntrackStatus.Removed;
        }
        return UntrackStatus.Untracked;
    }

    /// <summary>PointLockTracker::Merge: adds the reads and writes tracked by <paramref name="other"/>.</summary>
    public void MergeFrom(LockTracker other)
    {
        for (int i = 0; i < other.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily o = other.GetColumnFamily(i);
            TrackedColumnFamily c = GetOrAdd(o.Cfd);
            foreach (KeyValuePair<PooledKey, TrackedKeyInfo> kv in o.Keys)
            {
                ref TrackedKeyInfo info = ref c.GetOrAdd(kv.Key.Span, out bool exists);
                if (!exists || kv.Value.Seq < info.Seq) info.Seq = kv.Value.Seq;
                info.NumReads += kv.Value.NumReads;
                info.NumWrites += kv.Value.NumWrites;
                info.Exclusive |= kv.Value.Exclusive;
            }
        }
    }

    /// <summary>
    /// PointLockTracker::Subtract: removes the reads and writes recorded in <paramref name="since"/> (the keys tracked
    /// since a save point). Keys left with no reads and writes are untracked and, if <paramref name="untracked"/> is
    /// given, reported there (the locks that can be released).
    /// </summary>
    public void Subtract(LockTracker since, List<(ColumnFamilyData Cfd, byte[] Key)>? untracked)
    {
        for (int i = 0; i < since.ColumnFamilyCount; i++)
        {
            TrackedColumnFamily s = since.GetColumnFamily(i);
            TrackedColumnFamily? c = Find(s.Cfd.Id);
            if (c is null) continue;
            foreach (KeyValuePair<PooledKey, TrackedKeyInfo> kv in s.Keys)
            {
                ReadOnlySpan<byte> key = kv.Key.Span;
                ref TrackedKeyInfo info = ref CollectionsMarshal.GetValueRefOrNullRef(c.Lookup, key);
                if (Unsafe.IsNullRef(ref info)) continue;
                info.NumReads = info.NumReads >= kv.Value.NumReads ? info.NumReads - kv.Value.NumReads : 0;
                info.NumWrites = info.NumWrites >= kv.Value.NumWrites ? info.NumWrites - kv.Value.NumWrites : 0;
                if (info.NumReads == 0 && info.NumWrites == 0)
                {
                    c.Remove(key);
                    untracked?.Add((c.Cfd, key.ToArray()));
                }
            }
        }
    }

    /// <summary>Untracks everything (the per-CF tables and key buffers are kept for reuse).</summary>
    public void Clear()
    {
        _first?.Clear();
        if (_more is not null)
        {
            foreach (TrackedColumnFamily c in _more) c.Clear();
        }
    }
}
