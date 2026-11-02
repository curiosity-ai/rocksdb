using RocksDb.Managed.Db;

namespace RocksDb.Managed
{
    /// <summary>
    /// A consistent point-in-time view of the DB, obtained from <see cref="RocksDatabase.GetSnapshot"/>. Reads with
    /// <see cref="ReadOptions.Snapshot"/> set observe exactly the writes with sequence numbers &lt;= <see cref="SequenceNumber"/>.
    /// Dispose to release it (keeping snapshots alive pins old versions of keys once compaction exists).
    /// Port of rocksdb::Snapshot / SnapshotImpl (include/rocksdb/snapshot.h, db/snapshot_impl.h).
    /// </summary>
    public sealed class Snapshot : IDisposable
    {
        private DbImpl? _db;

        // Intrusive doubly-linked list membership (SnapshotList), guarded by the DB mutex.
        internal Snapshot? Prev;
        internal Snapshot? Next;

        internal Snapshot(DbImpl? db, ulong sequenceNumber, long unixTimeSeconds)
        {
            _db = db;
            SequenceNumber = sequenceNumber;
            UnixTime = unixTimeSeconds;
        }

        /// <summary>The sequence number this snapshot reads at.</summary>
        public ulong SequenceNumber { get; }

        /// <summary>
        /// SnapshotImpl::min_uncommitted_ (WritePrepared / WriteUnprepared transactions): every sequence number below it
        /// is committed. <see cref="ReadCallback.MinUnCommittedSeq"/> by default.
        /// </summary>
        internal ulong MinUncommitted = ReadCallback.MinUnCommittedSeq;

        /// <summary>Creation time (Unix seconds).</summary>
        public long UnixTime { get; }

        internal DbImpl? Owner => _db;

        /// <summary>Releases the snapshot (DB::ReleaseSnapshot). Idempotent.</summary>
        public void Dispose()
        {
            DbImpl? db = Interlocked.Exchange(ref _db, null);
            db?.ReleaseSnapshot(this);
        }
    }
}

namespace RocksDb.Managed.Db
{
    /// <summary>
    /// Sorted (by sequence number, oldest first) list of live snapshots. Port of SnapshotList (db/snapshot_impl.h).
    /// Not thread-safe: guarded by the DB mutex.
    /// </summary>
    internal sealed class SnapshotList
    {
        // Dummy head of a circular doubly-linked list.
        private readonly Snapshot _list = new(null, 0, 0);
        private int _count;

        public SnapshotList()
        {
            _list.Prev = _list;
            _list.Next = _list;
        }

        public bool IsEmpty => _list.Next == _list;
        public int Count => _count;
        public Snapshot Oldest => IsEmpty ? throw new InvalidOperationException("empty") : _list.Next!;
        public Snapshot Newest => IsEmpty ? throw new InvalidOperationException("empty") : _list.Prev!;

        /// <summary>SnapshotList::New: appends a snapshot; sequence numbers must be non-decreasing.</summary>
        public void Add(Snapshot s)
        {
            s.Next = _list;
            s.Prev = _list.Prev;
            s.Prev!.Next = s;
            s.Next.Prev = s;
            _count++;
        }

        /// <summary>SnapshotList::Delete.</summary>
        public void Remove(Snapshot s)
        {
            if (s.Prev is null) return; // not linked
            s.Prev.Next = s.Next;
            s.Next!.Prev = s.Prev;
            s.Prev = s.Next = null;
            _count--;
        }

        /// <summary>SnapshotList::GetAll: sequence numbers of all snapshots &lt;= <paramref name="maxSeq"/>, ascending, deduplicated.</summary>
        public void GetAll(List<ulong> result, ulong maxSeq)
        {
            result.Clear();
            for (Snapshot? s = _list.Next; s != _list; s = s!.Next)
            {
                if (s!.SequenceNumber > maxSeq) break;
                // Avoid duplicates
                if (result.Count == 0 || result[^1] != s.SequenceNumber) result.Add(s.SequenceNumber);
            }
        }

        /// <summary>Oldest snapshot sequence, or <paramref name="defaultValue"/> when there are none (GetOldestSnapshotSequence).</summary>
        public ulong OldestSequence(ulong defaultValue) => IsEmpty ? defaultValue : _list.Next!.SequenceNumber;
    }
}
