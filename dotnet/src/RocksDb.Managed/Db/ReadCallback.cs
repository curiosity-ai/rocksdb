using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Visibility filter of a read beyond its snapshot sequence number. Port of rocksdb::ReadCallback
/// (db/read_callback.h): used by WritePrepared / WriteUnprepared transactions, whose memtables and table files contain
/// data that is not committed yet. Reads with a callback (Get, MultiGet, iterators, conflict checks) skip the entries
/// the callback reports as invisible and continue with older versions of the key.
/// <para>
/// A callback is per read and not thread-safe. <see cref="MaxVisibleSeq"/> is the largest sequence number that can
/// be visible; entries below <see cref="MinUncommitted"/> are always visible.
/// </para>
/// </summary>
internal abstract class ReadCallback
{
    /// <summary>kMinUnCommittedSeq (include/rocksdb/types.h): 0 is always committed.</summary>
    public const ulong MinUnCommittedSeq = 1;

    private protected ulong _maxVisibleSeq = DbFormat.MaxSequenceNumber;
    private protected ulong _minUncommitted = MinUnCommittedSeq;

    /// <summary>The largest sequence number that may be visible (max_visible_seq).</summary>
    public ulong MaxVisibleSeq => _maxVisibleSeq;

    /// <summary>Entries with a smaller sequence number are committed (min_uncommitted).</summary>
    public ulong MinUncommitted => _minUncommitted;

    /// <summary>The full visibility check for <c>min_uncommitted &lt;= seq &lt;= max_visible_seq</c> (IsVisibleFullCheck).</summary>
    protected abstract bool IsVisibleFullCheck(ulong seq);

    /// <summary>ReadCallback::IsVisible.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsVisible(ulong seq)
    {
        if (seq < _minUncommitted) return true; // handles seq == 0 as well
        if (_maxVisibleSeq < seq) return false;
        return IsVisibleFullCheck(seq);
    }

    /// <summary>
    /// Called by reads without a snapshot once the read sequence is known (after the super version was referenced):
    /// sets the max visible sequence (ReadCallback::Refresh).
    /// </summary>
    public virtual void Refresh(ulong seq) => _maxVisibleSeq = seq;
}

/// <summary>SnapshotCheckerResult (db/snapshot_checker.h).</summary>
internal enum SnapshotCheckerResult
{
    InSnapshot = 0,
    NotInSnapshot = 1,
    /// <summary>The snapshot was released and the checker has no clue whether the sequence is visible to it.</summary>
    SnapshotReleased = 2,
}

/// <summary>
/// Decides whether a sequence number is visible to a snapshot during flush / compaction garbage collection. Port of
/// rocksdb::SnapshotChecker (db/snapshot_checker.h): set by WritePrepared / WriteUnprepared transaction databases,
/// whose data in the memtables and table files may not be committed yet.
/// </summary>
internal abstract class SnapshotChecker
{
    public abstract SnapshotCheckerResult CheckInSnapshot(ulong sequence, ulong snapshotSequence);

    /// <summary>
    /// The sequence number below which every entry is known to be committed (bounded by <paramref name="oldestSnapshot"/>).
    /// Bottommost files are marked for compaction only below it: uncommitted entries are never visible to the
    /// compaction's snapshots, so their sequence numbers cannot be zeroed and the rewrite would be repeated forever.
    /// </summary>
    public virtual ulong BoundOldestSnapshotForBottommostMarking(ulong oldestSnapshot) => oldestSnapshot;
}

/// <summary>
/// DisableGCSnapshotChecker: every value is "not in snapshot", so nothing is garbage collected. Used by DBs with
/// sequence-per-batch writes until their real checker is installed (recovery flushes).
/// </summary>
internal sealed class DisableGcSnapshotChecker : SnapshotChecker
{
    public static readonly DisableGcSnapshotChecker Instance = new();

    private DisableGcSnapshotChecker()
    {
    }

    public override SnapshotCheckerResult CheckInSnapshot(ulong sequence, ulong snapshotSequence) => SnapshotCheckerResult.NotInSnapshot;

    /// <summary>Nothing can be zeroed: no bottommost file is marked.</summary>
    public override ulong BoundOldestSnapshotForBottommostMarking(ulong oldestSnapshot) => 0;
}

/// <summary>
/// Port of rocksdb::PreReleaseCallback (db/pre_release_callback.h): run by the write-group leader for each writer
/// after the WAL write and before the memtable insert / publication of the sequence numbers.
/// </summary>
internal interface IPreReleaseCallback
{
    /// <param name="sequence">The first sequence number assigned to the writer.</param>
    /// <param name="isMemDisabled">The writer's batch is not inserted into the memtables.</param>
    /// <param name="logNumber">The WAL the record went to (0 without WAL).</param>
    /// <param name="index">Index of this callback among the group's callbacks.</param>
    /// <param name="total">Number of callbacks in the group.</param>
    void Callback(ulong sequence, bool isMemDisabled, ulong logNumber, int index, int total);
}

/// <summary>
/// What a sequence-per-batch transaction database (WritePrepared / WriteUnprepared) contributes to the DB layer.
/// Upstream wraps DBImpl in a WritePreparedTxnDB (StackableDB) that overrides reads and snapshots; in the managed port
/// the underlying <see cref="RocksDatabase"/> is used directly for reads, so it consults these hooks instead:
/// <list type="bullet">
/// <item>Reads without an explicit <see cref="ReadCallback"/> get one (WritePreparedTxnDB::Get / MultiGet /
/// NewIterator).</item>
/// <item>Snapshots carry min_uncommitted (WritePreparedTxnDB::GetSnapshot).</item>
/// <item>Writes that do not come from a transaction get their sub-batch count and commit-map update
/// (WritePreparedTxnDB::Write with skip_concurrency_control -&gt; WriteInternal).</item>
/// </list>
/// </summary>
internal abstract class SeqPerBatchHooks
{
    /// <summary>The snapshot checker used by flush and compaction.</summary>
    public abstract SnapshotChecker SnapshotChecker { get; }

    /// <summary>
    /// A read callback for a read through the plain DB API at <paramref name="snapshot"/> (null = latest).
    /// Return it with <see cref="EndRead"/>.
    /// </summary>
    public abstract ReadCallback BeginRead(Snapshot? snapshot);

    /// <summary>Validates a read made with <see cref="BeginRead"/> (TryAgain when the snapshot was invalidated) and recycles the callback.</summary>
    public abstract void EndRead(ReadCallback callback, bool validate);

    /// <summary>The read callback of an iterator reading at <paramref name="snapshot"/> (WritePreparedTxnDB::NewIterator).</summary>
    public abstract ReadCallback NewIteratorCallback(Snapshot snapshot);

    /// <summary>Takes a snapshot with min_uncommitted (WritePreparedTxnDB::GetSnapshot).</summary>
    public abstract Snapshot GetSnapshot();

    /// <summary>
    /// Writes a batch that did not come from a transaction (WritePreparedTxnDB::Write with skip_concurrency_control ->
    /// WriteInternal): the batch gets its sub-batch count and a trailing Noop boundary, and is committed by the
    /// write's pre-release callback, or, with two write queues, by a second (WAL-less) write on the WAL-only queue.
    /// </summary>
    public abstract ValueTask WritePlainAsync(WriteOptions? options, WriteBatch batch, bool synchronous, CancellationToken cancellationToken);
}
