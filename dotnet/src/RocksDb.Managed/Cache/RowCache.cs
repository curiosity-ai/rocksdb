using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Cache;

/// <summary>
/// DBOptions::row_cache: a cache of point lookup results per table file, consulted by Get / MultiGet before the table
/// reader (TableCache::Get / MultiGet, db/table_cache.cc). An entry is keyed by a per column family id, the file number,
/// a snapshot discriminator and the user key, and holds the replay log of the entries the lookup visited in that file
/// (GetContext::appendToReplayLog), which a hit replays without touching the table. Hits and misses are counted in
/// ROW_CACHE_HIT / ROW_CACHE_MISS. One instance may be shared by several DBs.
/// </summary>
/// <remarks>
/// A sharded LRU cache (like the LRUCache upstream's row cache usually is) charged with the key and replay log sizes
/// plus a fixed per-entry overhead. Lookups do not allocate. Thread-safe. Not persisted in OPTIONS files (upstream
/// does not serialize row_cache either).
/// </remarks>
public sealed class RowCache
{
    private const int NumShards = 16; // power of two
    private const int EntryOverhead = 64;
    private readonly Shard[] _shards = new Shard[NumShards];
    private long _nextId;

    /// <summary>Creates a row cache holding up to <paramref name="capacity"/> bytes of entries.</summary>
    public RowCache(long capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        Capacity = capacity;
        long perShard = (capacity + NumShards - 1) / NumShards;
        for (int i = 0; i < NumShards; i++) _shards[i] = new Shard(perShard);
    }

    /// <summary>Capacity in bytes (Cache::GetCapacity).</summary>
    public long Capacity { get; }

    /// <summary>Charge of the entries currently held, in bytes (Cache::GetUsage).</summary>
    public long Usage
    {
        get
        {
            long sum = 0;
            foreach (Shard s in _shards) sum += Volatile.Read(ref s.Usage);
            return sum;
        }
    }

    /// <summary>Number of entries currently held.</summary>
    public int Count
    {
        get
        {
            int n = 0;
            foreach (Shard s in _shards)
            {
                lock (s.Lock) n += s.Map.Count;
            }
            return n;
        }
    }

    /// <summary>Drops every entry.</summary>
    public void Clear()
    {
        foreach (Shard s in _shards)
        {
            lock (s.Lock)
            {
                s.Map.Clear();
                s.Head = s.Tail = null;
                Volatile.Write(ref s.Usage, 0);
            }
        }
    }

    /// <summary>Cache::NewId: a unique prefix for the keys of one table cache (TableCache::row_cache_id_).</summary>
    internal ulong NewId() => (ulong)Interlocked.Increment(ref _nextId);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Shard ShardFor(ReadOnlySpan<byte> key, out int hash)
    {
        hash = KeyComparer.HashOf(key);
        return _shards[(hash >> 7) & (NumShards - 1)];
    }

    /// <summary>The replay log cached for <paramref name="key"/>; the array is immutable and may be held by the caller.</summary>
    internal bool TryGet(ReadOnlySpan<byte> key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out byte[]? value)
    {
        Shard s = ShardFor(key, out _);
        lock (s.Lock)
        {
            if (s.Lookup.TryGetValue(key, out Node? n))
            {
                s.MoveToFront(n);
                value = n.Value;
                return true;
            }
        }
        value = null;
        return false;
    }

    /// <summary>Inserts (or replaces) the entry of <paramref name="key"/>, evicting least recently used entries.</summary>
    internal void Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Shard s = ShardFor(key, out _);
        long charge = key.Length + value.Length + EntryOverhead;
        if (charge > s.Capacity) return; // would not fit (Insert fails; the lookup result is simply not cached)
        var node = new Node(key.ToArray(), value.ToArray(), charge);
        lock (s.Lock)
        {
            if (s.Map.Remove(node.Key, out Node? old)) s.Unlink(old);
            s.Map[node.Key] = node;
            s.PushFront(node);
            while (s.Usage > s.Capacity && s.Tail is { } victim)
            {
                s.Unlink(victim);
                s.Map.Remove(victim.Key);
            }
        }
    }

    private sealed class Node(byte[] key, byte[] value, long charge)
    {
        public readonly byte[] Key = key;
        public readonly byte[] Value = value;
        public readonly long Charge = charge;
        public Node? Prev, Next;
    }

    private sealed class Shard
    {
        public readonly Lock Lock = new();
        public readonly Dictionary<byte[], Node> Map;
        public readonly Dictionary<byte[], Node>.AlternateLookup<ReadOnlySpan<byte>> Lookup;
        public readonly long Capacity;
        public long Usage;
        public Node? Head, Tail;

        public Shard(long capacity)
        {
            Capacity = capacity;
            Map = new Dictionary<byte[], Node>(KeyComparer.Instance);
            Lookup = Map.GetAlternateLookup<ReadOnlySpan<byte>>();
        }

        public void PushFront(Node n)
        {
            n.Prev = null;
            n.Next = Head;
            if (Head is not null) Head.Prev = n;
            Head = n;
            Tail ??= n;
            Usage += n.Charge;
        }

        public void Unlink(Node n)
        {
            if (n.Prev is not null) n.Prev.Next = n.Next;
            else Head = n.Next;
            if (n.Next is not null) n.Next.Prev = n.Prev;
            else Tail = n.Prev;
            n.Prev = n.Next = null;
            Usage -= n.Charge;
        }

        public void MoveToFront(Node n)
        {
            if (Head == n) return;
            Unlink(n);
            PushFront(n);
        }
    }

    private sealed class KeyComparer : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
    {
        public static readonly KeyComparer Instance = new();

        public static int HashOf(ReadOnlySpan<byte> key)
        {
            var h = new HashCode();
            h.AddBytes(key);
            return h.ToHashCode();
        }

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);
        public int GetHashCode(byte[] obj) => HashOf(obj);
        public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
        public int GetHashCode(ReadOnlySpan<byte> alternate) => HashOf(alternate);
        public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
    }
}
