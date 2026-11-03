using System.Diagnostics;

namespace RocksDb.Managed.Db;

/// <summary>
/// Parallel memtable insert of one large write batch (<see cref="WriteOptions.MemtableInsertParallelism"/>). Managed
/// extension without an upstream counterpart: upstream parallelizes memtable inserts only across the writers of a write
/// group (allow_concurrent_memtable_write, see LaunchParallelMemTableWriters in DbImpl.Write.cs); a single large batch -
/// e.g. a replica applying its primary's merged WAL batches - is inserted by one thread. Here the batch is cut at record
/// boundaries into contiguous ranges, each range gets the sequence number of its first entry, and the ranges are inserted
/// concurrently (MemTable.AddConcurrently) by the write leader and thread-pool threads. Like a parallel group, the batch
/// becomes visible only when every range is in the memtable: the leader publishes the last sequence afterwards.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>A range is not worth a thread-pool hop below this many entries (about 0.1-0.3 ms of inserts; a hop costs microseconds).</summary>
    internal const int MinEntriesPerInsertRange = 128;

    // Released split-insert states for reuse (a state is released by whichever of its leader and workers finishes last).
    private SplitInsert? _spareSplitInsert0;
    private SplitInsert? _spareSplitInsert1;

    /// <summary>
    /// Inserts the batch of the single writer <paramref name="w"/> of a write group in parallel ranges. Returns false,
    /// having inserted nothing, when the write does not qualify: parallelism below 2, concurrent memtable writes disabled,
    /// sequence-per-batch, a transaction write, blob direct write rewrites, per-key protection, fewer than two ranges of
    /// <see cref="MinEntriesPerInsertRange"/> entries, or a record other than Put / Delete / SingleDelete / DeleteRange /
    /// PutEntity / LogData (merge operands, blob indexes, timed puts and 2PC markers keep the serial insert). REQUIRES:
    /// write leader, sequence numbers assigned, WAL written.
    /// </summary>
    private bool TryInsertBatchInParallel(WriteThread.Writer w)
    {
        int parallelism = w.MemtableInsertParallelism;
        if (parallelism < 2 || !_allowConcurrentMemtableWrite || _seqPerBatch || w.CallbackFailed || w.Txn is not null || _bdwSwapped.Count > 0)
            return false;
        WriteBatch batch = w.Batch!;
        int count = batch.Count;
        if (batch.ProtectionBytesPerKey != 0 || count < 2 * MinEntriesPerInsertRange) return false;
        int ranges = Math.Min(Math.Min(parallelism, count / MinEntriesPerInsertRange), SplitInsert.MaxRanges);
        SplitInsert s = Interlocked.Exchange(ref _spareSplitInsert0, null) ?? Interlocked.Exchange(ref _spareSplitInsert1, null) ?? new SplitInsert(this);
        if (!s.Plan(batch, ranges, w.IgnoreMissingColumnFamilies))
        {
            s.Release();
            return false;
        }
        Exception? error = s.Run();
        if (error is not null) w.Error = error;
        return true;
    }

    private void ReturnSplitInsert(SplitInsert s)
    {
        if (Interlocked.CompareExchange(ref _spareSplitInsert0, s, null) is not null) Interlocked.CompareExchange(ref _spareSplitInsert1, s, null);
    }

    /// <summary>
    /// One parallel insert: the ranges of a batch, claimed one at a time by the leader and the queued workers (the same
    /// object is queued once per worker). A worker that starts after every range was claimed just leaves, so the leader
    /// never waits for a work item that has not started - only for ranges in progress. The state is reused only after
    /// the leader and every queued worker released it.
    /// </summary>
    private sealed class SplitInsert(DbImpl db) : IThreadPoolWorkItem
    {
        public const int MaxRanges = 16;

        private readonly DbImpl _db = db;
        private readonly ManualResetEventSlim _done = new(initialState: false);
        // Range i covers the records in [_starts[i], _starts[i + 1]) of _data; its first entry has sequence _seqs[i].
        private readonly int[] _starts = new int[MaxRanges + 1];
        private readonly ulong[] _seqs = new ulong[MaxRanges];
        private ReadOnlyMemory<byte> _data;
        private bool _ignoreMissingColumnFamilies;
        private int _rangeCount;
        private int _next;
        private int _remaining;
        private int _refs;
        private Exception? _error;

        /// <summary>
        /// Cuts <paramref name="batch"/> into <paramref name="ranges"/> ranges of about equal entry counts. Returns false
        /// when a record cannot be inserted out of order with the others (see TryInsertBatchInParallel) or the batch is
        /// malformed (the serial insert then reports the corruption).
        /// </summary>
        public bool Plan(WriteBatch batch, int ranges, bool ignoreMissingColumnFamilies)
        {
            Debug.Assert(ranges >= 2 && ranges <= MaxRanges);
            _refs = 1;
            ReadOnlyMemory<byte> data = batch.DataMemory;
            ReadOnlySpan<byte> rep = data.Span;
            uint count = (uint)batch.Count;
            uint perRange = (count + (uint)ranges - 1) / (uint)ranges;
            ulong first = batch.Sequence;
            int pos = WriteBatch.HeaderSize;
            uint entries = 0;
            int k = 1;
            _starts[0] = pos;
            _seqs[0] = first;
            while (pos < rep.Length)
            {
                if (k < ranges && entries >= perRange * (uint)k)
                {
                    _starts[k] = pos;
                    _seqs[k] = first + entries;
                    k++;
                }
                if (WriteBatchReader.TryReadRecord(rep, ref pos, out WriteBatchEntry e) is not null) return false;
                switch (e.Type)
                {
                    case EntryType.Value:
                    case EntryType.Deletion:
                    case EntryType.SingleDeletion:
                    case EntryType.RangeDeletion:
                    case EntryType.WideColumnEntity:
                        entries++;
                        break;
                    case EntryType.LogData:
                        break;
                    default:
                        return false;
                }
            }
            if (entries != count || k < 2) return false;
            _starts[k] = rep.Length;
            _rangeCount = k;
            _data = data;
            _ignoreMissingColumnFamilies = ignoreMissingColumnFamilies;
            return true;
        }

        /// <summary>Inserts the planned ranges (with up to one worker per range besides the caller) and releases the caller's reference.</summary>
        public Exception? Run()
        {
            int workers = _rangeCount - 1;
            _error = null;
            _next = 0;
            _done.Reset();
            Volatile.Write(ref _remaining, _rangeCount);
            Volatile.Write(ref _refs, 1 + workers);
            for (int i = 0; i < workers; i++) ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
            InsertRanges();
            // Only ranges claimed by running workers can be outstanding.
            if (Volatile.Read(ref _remaining) != 0) _done.Wait();
            Exception? error = _error;
            Release();
            return error;
        }

        void IThreadPoolWorkItem.Execute()
        {
            InsertRanges();
            Release();
        }

        private void InsertRanges()
        {
            while (true)
            {
                int i = Interlocked.Increment(ref _next) - 1;
                if (i >= _rangeCount) return;
                InsertRange(i);
                if (Interlocked.Decrement(ref _remaining) == 0) _done.Set();
            }
        }

        private void InsertRange(int i)
        {
            var inserter = MemTableInserter.ForConcurrentWriter(_db, _ignoreMissingColumnFamilies);
            inserter.Sequence = _seqs[i];
            try
            {
                TestHooks.Invoke("DBImpl::InsertBatchInParallel:Range");
                inserter.RunRange(_data.Span[_starts[i].._starts[i + 1]]);
            }
            catch (Exception e)
            {
                Interlocked.CompareExchange(ref _error, e, null);
            }
            finally
            {
                inserter.PostProcess();
            }
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0) return;
            _data = default;
            _error = null;
            _db.ReturnSplitInsert(this);
        }
    }
}
