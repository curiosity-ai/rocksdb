using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// Called by <see cref="ColumnFamily.TryGetMergeOperands(ReadOptions?, ReadOnlySpan{byte}, GetMergeOperandsOptions, MergeOperandsResult)"/>
/// with each merge operand found, newest first (base values excluded). Return false to end the lookup at that operand.
/// The span is only valid during the call.
/// </summary>
public delegate bool MergeOperandContinueCallback(ReadOnlySpan<byte> operand);

/// <summary>Options of a merge operand lookup. Port of rocksdb::GetMergeOperandsOptions (include/rocksdb/db.h).</summary>
public sealed class GetMergeOperandsOptions
{
    /// <summary>
    /// Hard limit on the number of operands returned (expected_max_number_of_operands): when the key has more, none are
    /// returned and <see cref="MergeOperandsResult.ExceededExpectedMax"/> is set (upstream: Status::Incomplete), with
    /// <see cref="MergeOperandsResult.NumberOfOperands"/> the number found.
    /// </summary>
    public int ExpectedMaxNumberOfOperands { get; init; }

    /// <summary>
    /// continue_cb: called after reading each merge operand (newest to oldest, excluding a base value); returning false
    /// ends the lookup at that operand, so only a suffix of the operands (the newest ones) is returned. Null = read all.
    /// </summary>
    public MergeOperandContinueCallback? ContinueCallback { get; init; }
}

/// <summary>
/// The merge operands of a key in insertion order (oldest first; a base value found under the operands is the first
/// entry). Reusable: pass the same instance to several lookups to avoid allocations. Dispose to return its pooled
/// buffer.
/// </summary>
public sealed class MergeOperandsResult : IDisposable
{
    private readonly PooledBufferWriter _data = new(256);
    private readonly List<int> _ends = new();

    /// <summary>True if the key has a value or merge operands (false = upstream's NotFound).</summary>
    public bool Found { get; internal set; }

    /// <summary>
    /// True if the key has more than <see cref="GetMergeOperandsOptions.ExpectedMaxNumberOfOperands"/> operands; then
    /// <see cref="Count"/> is 0 (upstream: Status::Incomplete(kMergeOperandsInsufficientCapacity)).
    /// </summary>
    public bool ExceededExpectedMax { get; internal set; }

    /// <summary>The number of operands found (number_of_operands), also when <see cref="ExceededExpectedMax"/>.</summary>
    public int NumberOfOperands { get; internal set; }

    /// <summary>Number of operands held.</summary>
    public int Count => _ends.Count;

    /// <summary>Operand <paramref name="index"/> (0 = oldest).</summary>
    public ReadOnlySpan<byte> this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_ends.Count, nameof(index));
            int start = index == 0 ? 0 : _ends[index - 1];
            return _data.WrittenSpan[start.._ends[index]];
        }
    }

    /// <summary>Copies the operands into new arrays (oldest first).</summary>
    public byte[][] ToArray()
    {
        var r = new byte[_ends.Count][];
        for (int i = 0; i < r.Length; i++) r[i] = this[i].ToArray();
        return r;
    }

    internal void Reset()
    {
        _data.Clear();
        _ends.Clear();
        Found = false;
        ExceededExpectedMax = false;
        NumberOfOperands = 0;
    }

    internal void Add(ReadOnlySpan<byte> operand)
    {
        _data.Write(operand);
        _ends.Add(_data.WrittenCount);
    }

    public void Dispose() => _data.Dispose();
}

/// <summary>A held LockWAL (see <see cref="RocksDatabase.LockWal"/>); disposing it is UnlockWAL. Not reusable.</summary>
public sealed class WalLock : IDisposable
{
    private Db.DbImpl? _db;

    internal WalLock(Db.DbImpl db) => _db = db;

    /// <summary>Releases this lock (DB::UnlockWAL); writes resume when every lock was released. Idempotent.</summary>
    public void Dispose() => Interlocked.Exchange(ref _db, null)?.UnlockWal();
}

public sealed partial class ColumnFamily
{
    // ---- merge operands -------------------------------------------------------------------------------

    /// <summary>
    /// Reads the merge operands of <paramref name="key"/> without merging them (DB::GetMergeOperands) into
    /// <paramref name="result"/>, oldest first; a base value (Put / entity default column) under the operands is the
    /// first entry, a plain value without operands the only one. Returns false if the key doesn't exist. See
    /// <see cref="GetMergeOperandsOptions"/> for the operand limit and the continue callback.
    /// </summary>
    public bool TryGetMergeOperands(ReadOnlySpan<byte> key, GetMergeOperandsOptions options, MergeOperandsResult result) =>
        TryGetMergeOperands(null, key, options, result);

    /// <inheritdoc cref="TryGetMergeOperands(ReadOnlySpan{byte}, GetMergeOperandsOptions, MergeOperandsResult)"/>
    public bool TryGetMergeOperands(ReadOptions? readOptions, ReadOnlySpan<byte> key, GetMergeOperandsOptions options, MergeOperandsResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (_cfd.TimestampSize != 0) ThrowCfHasTimestamp();
        _db.GetMergeOperands(_cfd, readOptions, key, options, result);
        return result.Found;
    }

    /// <summary>Async <see cref="TryGetMergeOperands(ReadOptions, ReadOnlySpan{byte}, GetMergeOperandsOptions, MergeOperandsResult)"/>.</summary>
    public async ValueTask<bool> TryGetMergeOperandsAsync(ReadOptions? readOptions, ReadOnlyMemory<byte> key, GetMergeOperandsOptions options,
        MergeOperandsResult result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (_cfd.TimestampSize != 0) ThrowCfHasTimestamp();
        await _db.GetMergeOperandsAsync(_cfd, readOptions, key, options, result, cancellationToken).ConfigureAwait(false);
        return result.Found;
    }

    // ---- file-level operations ----------------------------------------------------------------------

    /// <summary>
    /// Marks the table files overlapping [<paramref name="begin"/>, <paramref name="end"/>] (null = unbounded) on all
    /// but the last non-empty level for compaction and schedules it (DB::SuggestCompactRange). Returns immediately.
    /// </summary>
    public void SuggestCompactRange(byte[]? begin, byte[]? end) => _db.SuggestCompactRange(_cfd, begin, end);

    /// <summary>
    /// Moves all L0 files to <paramref name="targetLevel"/> without rewriting them (DB::PromoteL0). Requires the L0 files
    /// not to overlap each other or be compacted and levels 1..<paramref name="targetLevel"/> to be empty (else
    /// InvalidArgument).
    /// </summary>
    public void PromoteL0(int targetLevel) => _db.PromoteL0Async(_cfd, targetLevel, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="PromoteL0(int)"/>.</summary>
    public ValueTask PromoteL0Async(int targetLevel, CancellationToken cancellationToken = default) =>
        _db.PromoteL0Async(_cfd, targetLevel, synchronous: false, cancellationToken);

    /// <summary>
    /// Deletes the table files of levels 1+ lying entirely within [<paramref name="begin"/>, <paramref name="end"/>]
    /// (DeleteFilesInRange; null = unbounded; <paramref name="includeEnd"/> keeps files ending at <paramref name="end"/>
    /// out when false). Data of L0, of files overlapping the range boundaries and of the memtables is not deleted.
    /// </summary>
    public void DeleteFilesInRange(byte[]? begin, byte[]? end, bool includeEnd) =>
        DeleteFilesInRanges([new KeyRangeOpt(begin, end)], includeEnd);

    /// <summary><see cref="DeleteFilesInRange(byte[], byte[], bool)"/> for several ranges (DeleteFilesInRanges).</summary>
    public void DeleteFilesInRanges(IReadOnlyList<KeyRangeOpt> ranges, bool includeEnd) =>
        _db.DeleteFilesInRangesAsync(_cfd, ToTuples(ranges), includeEnd, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="DeleteFilesInRanges(IReadOnlyList{KeyRangeOpt}, bool)"/>.</summary>
    public ValueTask DeleteFilesInRangesAsync(IReadOnlyList<KeyRangeOpt> ranges, bool includeEnd, CancellationToken cancellationToken = default) =>
        _db.DeleteFilesInRangesAsync(_cfd, ToTuples(ranges), includeEnd, synchronous: false, cancellationToken);

    private static List<(byte[]? Start, byte[]? Limit)> ToTuples(IReadOnlyList<KeyRangeOpt> ranges)
    {
        ArgumentNullException.ThrowIfNull(ranges);
        var r = new List<(byte[]?, byte[]?)>(ranges.Count);
        foreach (KeyRangeOpt k in ranges) r.Add((k.Start, k.Limit));
        return r;
    }

    /// <summary>
    /// Deletes every key outside [<paramref name="beginKey"/>, <paramref name="endKey"/>) (DB::ClipColumnFamily): flush,
    /// drop whole files outside the range, range-delete the rest outside it and compact the column family to drop the
    /// tombstones.
    /// </summary>
    public void ClipColumnFamily(byte[] beginKey, byte[] endKey) =>
        _db.ClipColumnFamilyAsync(_cfd, beginKey, endKey, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="ClipColumnFamily(byte[], byte[])"/>.</summary>
    public ValueTask ClipColumnFamilyAsync(byte[] beginKey, byte[] endKey, CancellationToken cancellationToken = default) =>
        _db.ClipColumnFamilyAsync(_cfd, beginKey, endKey, synchronous: false, cancellationToken);

    /// <summary>
    /// The table properties of the files overlapping any of <paramref name="ranges"/> (user keys, both bounds inclusive
    /// at file granularity), keyed by file path (DB::GetPropertiesOfTablesInRange).
    /// </summary>
    public Dictionary<string, TableProperties> GetPropertiesOfTablesInRange(IReadOnlyList<KeyRange> ranges) =>
        _db.GetPropertiesOfTablesInRange(_cfd, ranges);

    /// <summary>The table properties of every live file, one collection per level (DB::GetPropertiesOfTablesByLevel).</summary>
    public List<Dictionary<string, TableProperties>> GetPropertiesOfTablesByLevel() => _db.GetPropertiesOfTablesByLevel(_cfd);
}

/// <summary>A user-key range whose bounds may be open (rocksdb::RangeOpt): null = before every key / after every key.</summary>
public readonly record struct KeyRangeOpt(byte[]? Start, byte[]? Limit);

public sealed partial class RocksDatabase
{
    /// <inheritdoc cref="ColumnFamily.TryGetMergeOperands(ReadOnlySpan{byte}, GetMergeOperandsOptions, MergeOperandsResult)"/>
    public bool TryGetMergeOperands(ReadOnlySpan<byte> key, GetMergeOperandsOptions options, MergeOperandsResult result) =>
        _default.TryGetMergeOperands(key, options, result);

    /// <inheritdoc cref="ColumnFamily.TryGetMergeOperands(ReadOptions, ReadOnlySpan{byte}, GetMergeOperandsOptions, MergeOperandsResult)"/>
    public bool TryGetMergeOperands(ReadOptions? readOptions, ReadOnlySpan<byte> key, GetMergeOperandsOptions options, MergeOperandsResult result) =>
        _default.TryGetMergeOperands(readOptions, key, options, result);

    /// <inheritdoc cref="ColumnFamily.TryGetMergeOperandsAsync"/>
    public ValueTask<bool> TryGetMergeOperandsAsync(ReadOptions? readOptions, ReadOnlyMemory<byte> key, GetMergeOperandsOptions options,
        MergeOperandsResult result, CancellationToken cancellationToken = default) =>
        _default.TryGetMergeOperandsAsync(readOptions, key, options, result, cancellationToken);

    /// <inheritdoc cref="ColumnFamily.SuggestCompactRange"/>
    public void SuggestCompactRange(byte[]? begin, byte[]? end) => _default.SuggestCompactRange(begin, end);

    /// <inheritdoc cref="ColumnFamily.PromoteL0"/>
    public void PromoteL0(int targetLevel) => _default.PromoteL0(targetLevel);

    /// <inheritdoc cref="ColumnFamily.ClipColumnFamily"/>
    public void ClipColumnFamily(byte[] beginKey, byte[] endKey) => _default.ClipColumnFamily(beginKey, endKey);

    /// <inheritdoc cref="ColumnFamily.DeleteFilesInRange"/>
    public void DeleteFilesInRange(byte[]? begin, byte[]? end, bool includeEnd) => _default.DeleteFilesInRange(begin, end, includeEnd);

    /// <inheritdoc cref="ColumnFamily.GetPropertiesOfTablesInRange"/>
    public Dictionary<string, TableProperties> GetPropertiesOfTablesInRange(IReadOnlyList<KeyRange> ranges) =>
        _default.GetPropertiesOfTablesInRange(ranges);

    /// <summary>
    /// Freezes the logical state of the DB (DB::LockWAL): stops writes (once no write is in progress) and flushes the WAL
    /// buffer, e.g. to take a checkpoint at a known state (checkpoints skip their memtable flush while the WAL is
    /// locked). Writes wait (or fail with Incomplete under <see cref="WriteOptions.NoSlowdown"/>) until every returned
    /// <see cref="WalLock"/> is disposed. Locks nest and, unlike upstream, may be released from any thread.
    /// </summary>
    public WalLock LockWal()
    {
        _db.LockWalAsync(synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        return new WalLock(_db);
    }

    /// <summary>Async <see cref="LockWal"/>.</summary>
    public async ValueTask<WalLock> LockWalAsync(CancellationToken cancellationToken = default)
    {
        await _db.LockWalAsync(synchronous: false, cancellationToken).ConfigureAwait(false);
        return new WalLock(_db);
    }

    /// <summary>
    /// Pauses manual compactions (DB::DisableManualCompaction): running and waiting CompactRange / CompactFiles calls fail
    /// with Incomplete (subcode ManualCompactionPaused) and this call returns once none is left; new ones fail the same
    /// way until <see cref="EnableManualCompaction"/> was called once per disable. Automatic compactions continue.
    /// </summary>
    public void DisableManualCompaction() => _db.DisableManualCompactionAsync(synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="DisableManualCompaction"/>.</summary>
    public ValueTask DisableManualCompactionAsync(CancellationToken cancellationToken = default) =>
        _db.DisableManualCompactionAsync(synchronous: false, cancellationToken);

    /// <summary>Undoes one <see cref="DisableManualCompaction"/> (DB::EnableManualCompaction).</summary>
    public void EnableManualCompaction() => _db.EnableManualCompaction();

    /// <summary>
    /// The smallest creation time (unix seconds) of the live table files of all column families
    /// (DB::GetCreationTimeOfOldestFile): ulong.MaxValue without files, 0 if a file's creation time is unknown.
    /// Requires <see cref="DbOptions.MaxOpenFiles"/> = -1 (else NotSupported).
    /// </summary>
    public ulong GetCreationTimeOfOldestFile() => _db.GetCreationTimeOfOldestFile();
}
