using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>Outcome of one key of a <see cref="MultiGetBatch"/>.</summary>
public enum MultiGetStatus : byte
{
    /// <summary>The key does not exist (or the batch was not run yet).</summary>
    NotFound,
    /// <summary>The key exists; <see cref="MultiGetBatch.GetValue"/> returns its value.</summary>
    Found,
    /// <summary>The lookup of this key failed; <see cref="MultiGetBatch.GetError"/> describes why.</summary>
    Error,
}

/// <summary>
/// A reusable batch of point lookups for <see cref="ColumnFamily.MultiGet(MultiGetBatch)"/> /
/// <see cref="ColumnFamily.MultiGetAsync(MultiGetBatch, CancellationToken)"/> (DB::MultiGet with a per-key status).
/// Add keys with <see cref="Add"/>, run the batch, then read <see cref="GetStatus"/> and <see cref="GetValue"/> per key
/// (in the order the keys were added).
/// <para>
/// All keys are read from one consistent view (one super version, one snapshot sequence). The lookup is batched like
/// upstream: keys are sorted, the memtables are searched per key, and every table file is visited once per batch, with
/// its filter checked for all keys first and keys that fall into the same data block sharing one block lookup.
/// </para>
/// <para>
/// The batch owns its key and value buffers: after warm-up, adding keys and running the batch again does not allocate.
/// Value spans are valid until the batch is cleared, run again or disposed. Not thread-safe: use one batch per thread
/// (or per concurrent async operation).
/// </para>
/// </summary>
public sealed class MultiGetBatch : IDisposable, ITableMultiGetBatch
{
    private byte[] _keyBytes;
    private int _keyBytesUsed;
    private int[] _keyOffsets;
    private int[] _keyLengths;
    private int _count;

    // Per-run state (sized to the capacity).
    private byte[] _ikeys = [];
    private int[] _ikeyOffsets = [];
    private GetContext?[] _contexts = [];
    private MultiGetStatus[] _status = [];
    private RocksDbException?[]? _errors;
    private int[] _order = [];
    private int[] _sub = [];
    private bool[] _mayMatch = [];
    private bool _hasResults;
    // The last run was a MultiGetEntity: found values hold the serialized entity.
    private bool _entityResults;
    private bool _disposed;

    /// <summary>Creates an empty batch.</summary>
    public MultiGetBatch() : this(16)
    {
    }

    /// <summary>Creates an empty batch with room for <paramref name="capacity"/> keys.</summary>
    public MultiGetBatch(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        capacity = Math.Max(capacity, 1);
        _keyBytes = RocksArrayPool<byte>.Shared.Rent(capacity * 24);
        _keyOffsets = new int[capacity];
        _keyLengths = new int[capacity];
    }

    /// <summary>Number of keys in the batch.</summary>
    public int Count => _count;

    /// <summary>Removes all keys (and results). Buffers are kept for reuse.</summary>
    public void Clear()
    {
        ResetResults();
        _count = 0;
        _keyBytesUsed = 0;
    }

    /// <summary>Appends a key (copied) and returns this batch.</summary>
    public MultiGetBatch Add(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ResetResults();
        if (_count == _keyOffsets.Length)
        {
            int n = _keyOffsets.Length * 2;
            Array.Resize(ref _keyOffsets, n);
            Array.Resize(ref _keyLengths, n);
        }
        if (_keyBytes.Length - _keyBytesUsed < key.Length)
        {
            byte[] bigger = RocksArrayPool<byte>.Shared.Rent(Math.Max(_keyBytes.Length * 2, _keyBytesUsed + key.Length));
            _keyBytes.AsSpan(0, _keyBytesUsed).CopyTo(bigger);
            RocksArrayPool<byte>.Shared.Return(_keyBytes);
            _keyBytes = bigger;
        }
        key.CopyTo(_keyBytes.AsSpan(_keyBytesUsed));
        _keyOffsets[_count] = _keyBytesUsed;
        _keyLengths[_count] = key.Length;
        _keyBytesUsed += key.Length;
        _count++;
        return this;
    }

    /// <summary>The key at <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetKey(int index)
    {
        CheckIndex(index);
        return _keyBytes.AsSpan(_keyOffsets[index], _keyLengths[index]);
    }

    /// <summary>The outcome of the key at <paramref name="index"/> of the last run.</summary>
    public MultiGetStatus GetStatus(int index)
    {
        CheckIndex(index);
        return index < _status.Length ? _status[index] : MultiGetStatus.NotFound;
    }

    /// <summary>
    /// True if the key at <paramref name="index"/> was found; <paramref name="value"/> is then its value (after
    /// <see cref="ColumnFamily.MultiGetEntity(MultiGetBatch)"/>: the value of the entity's default column).
    /// </summary>
    public bool TryGetValue(int index, out ReadOnlySpan<byte> value)
    {
        if (GetStatus(index) == MultiGetStatus.Found)
        {
            value = _entityResults
                ? Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(_contexts[index]!.Value.WrittenSpan)
                : _contexts[index]!.Value.WrittenSpan;
            return true;
        }
        value = default;
        return false;
    }

    /// <summary>
    /// After <see cref="ColumnFamily.MultiGetEntity(MultiGetBatch)"/> (DB::MultiGetEntity): true if the key at
    /// <paramref name="index"/> was found, its columns then copied into <paramref name="columns"/> (a reusable
    /// <see cref="WideColumns"/>; a plain value reads as an entity with only the default column). False (and
    /// <paramref name="columns"/> emptied) if the key was not found or failed (see <see cref="GetError"/>).
    /// Throws InvalidOperationException if the last run was a plain <see cref="ColumnFamily.MultiGet(MultiGetBatch)"/>.
    /// </summary>
    public bool TryGetEntity(int index, WideColumns columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        if (GetStatus(index) == MultiGetStatus.Found)
        {
            if (!_entityResults) ThrowNotEntityRun();
            columns.SetSerialized(_contexts[index]!.Value.WrittenSpan);
            return true;
        }
        columns.Clear();
        return false;
    }

    /// <summary>
    /// After <see cref="ColumnFamily.MultiGetEntity(MultiGetBatch)"/>: the serialized entity of the key at
    /// <paramref name="index"/> (wide-column format version 1; empty if not found). Valid until the batch is cleared, run
    /// again or disposed.
    /// </summary>
    public ReadOnlySpan<byte> GetSerializedEntity(int index)
    {
        if (GetStatus(index) != MultiGetStatus.Found) return default;
        if (!_entityResults) ThrowNotEntityRun();
        return _contexts[index]!.Value.WrittenSpan;
    }

    private static void ThrowNotEntityRun() => throw new InvalidOperationException("The batch was not run with MultiGetEntity.");

    /// <summary>The value of the key at <paramref name="index"/> (empty if it was not found).</summary>
    public ReadOnlySpan<byte> GetValue(int index) => TryGetValue(index, out ReadOnlySpan<byte> v) ? v : default;

    /// <summary>The value of the key at <paramref name="index"/> as memory (empty if it was not found).</summary>
    public ReadOnlyMemory<byte> GetValueMemory(int index)
    {
        if (GetStatus(index) != MultiGetStatus.Found) return default;
        ReadOnlyMemory<byte> all = _contexts[index]!.Value.WrittenMemory;
        if (!_entityResults) return all;
        ReadOnlySpan<byte> v = Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(all.Span);
        if (v.IsEmpty) return default;
        all.Span.Overlaps(v, out int offset);
        return all.Slice(offset, v.Length);
    }

    /// <summary>The error of the key at <paramref name="index"/> (status <see cref="MultiGetStatus.Error"/>), else null.</summary>
    public RocksDbException? GetError(int index)
    {
        CheckIndex(index);
        return _errors is not null && index < _errors.Length ? _errors[index] : null;
    }

    /// <summary>Number of keys found by the last run.</summary>
    public int FoundCount { get; private set; }

    // ---- internal pool (the convenience MultiGet overloads; a user batch is never pooled) ------------------

    // One cached batch per thread. A batch that grew past these bounds is disposed instead of being kept, so a rare
    // huge MultiGet does not pin its buffers on the thread for good.
    [ThreadStatic] private static MultiGetBatch? t_cached;
    private const int MaxPooledKeys = 1024;
    private const int MaxPooledBytes = 1 << 20;

    /// <summary>
    /// A cleared batch for one internal MultiGet call: the calling thread's cached batch, or a new one. Return it with
    /// <see cref="ReturnPooled"/> (from any thread) once its results have been copied out.
    /// </summary>
    internal static MultiGetBatch RentPooled(int capacity)
    {
        MultiGetBatch? b = t_cached;
        if (b is not null)
        {
            // Taken out of the slot while in use, so a nested call on this thread (e.g. from a merge operator) gets
            // its own batch.
            t_cached = null;
            return b;
        }
        return new MultiGetBatch(Math.Min(capacity, MaxPooledKeys)) { _pooled = true };
    }

    /// <summary>Clears <paramref name="batch"/> and caches it on the current thread (or disposes it if too large).</summary>
    internal static void ReturnPooled(MultiGetBatch batch)
    {
        System.Diagnostics.Debug.Assert(batch._pooled);
        batch.Clear();
        if (batch._disposed) return;
        if (t_cached is null && batch.RetainedBytes() <= MaxPooledBytes && batch._keyOffsets.Length <= MaxPooledKeys)
        {
            t_cached = batch;
            return;
        }
        batch.Dispose();
    }

    private bool _pooled;

    private long RetainedBytes()
    {
        long bytes = _keyBytes.Length + _ikeys.Length;
        GetContext?[] ctx = _contexts;
        for (int i = 0; i < ctx.Length; i++)
        {
            if (ctx[i] is { } c) bytes += c.Value.Capacity + c.Scratch.Capacity;
        }
        return bytes;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        RocksArrayPool<byte>.Shared.Return(_keyBytes);
        _keyBytes = [];
        if (_ikeys.Length > 0) RocksArrayPool<byte>.Shared.Return(_ikeys);
        _ikeys = [];
        GetContext?[] ctx = _contexts;
        for (int i = 0; i < ctx.Length; i++) ctx[i]?.Value.Dispose();
        _contexts = [];
        _count = 0;
    }

    private void CheckIndex(int index)
    {
        if ((uint)index >= (uint)_count) ThrowIndex();
    }

    private static void ThrowIndex() => throw new ArgumentOutOfRangeException("index");

    private void ResetResults()
    {
        if (!_hasResults) return;
        _hasResults = false;
        Array.Clear(_status);
        if (_errors is not null) Array.Clear(_errors);
        FoundCount = 0;
    }

    // ---- run (used by DbImpl.MultiGetAsync) ----------------------------------------------------------------

    /// <summary>Set by the caller before a run: whether it is a MultiGetEntity (results are entities).</summary>
    internal bool EntityRun { get; set; }

    internal int[] Order => _order;
    internal int[] Sub => _sub;
    internal bool[] MayMatch => _mayMatch;
    internal GetContext Context(int key) => _contexts[key]!;
    internal ReadOnlySpan<byte> UserKey(int key) => _keyBytes.AsSpan(_keyOffsets[key], _keyLengths[key]);
    internal ReadOnlyMemory<byte> UserKeyMemory(int key) => _keyBytes.AsMemory(_keyOffsets[key], _keyLengths[key]);

    /// <summary>
    /// Prepares a run: resets the per-key lookup state, builds the lookup keys at <paramref name="sequence"/> and sorts
    /// the key order by user key (with the column family's comparator).
    /// </summary>
    /// <summary>Table-level read flags of the current run (fill_cache, rate_limiter_priority).</summary>
    internal Table.TableReadFlags ReadFlags { get; set; }

    internal void BeginRun(MergeOperator? mergeOperator, ulong sequence, Comparator userComparator)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _entityResults = EntityRun;
        int n = _count;
        if (_contexts.Length < n)
        {
            int cap = Math.Max(n, _keyOffsets.Length);
            Array.Resize(ref _contexts, cap);
            _status = new MultiGetStatus[cap];
            if (_errors is not null) _errors = new RocksDbException?[cap];
            _order = new int[cap];
            _sub = new int[cap];
            _mayMatch = new bool[cap];
            _ikeyOffsets = new int[cap + 1];
        }
        else
        {
            ResetResults();
        }
        int ikeyBytes = _keyBytesUsed + n * DbFormat.InternalKeyFooterSize;
        if (_ikeys.Length < ikeyBytes)
        {
            if (_ikeys.Length > 0) RocksArrayPool<byte>.Shared.Return(_ikeys);
            _ikeys = RocksArrayPool<byte>.Shared.Rent(ikeyBytes);
        }
        int off = 0;
        for (int i = 0; i < n; i++)
        {
            (_contexts[i] ??= new GetContext()).Reset(mergeOperator);
            _ikeyOffsets[i] = off;
            off += DbFormat.AppendInternalKey(_ikeys.AsSpan(off), UserKey(i), sequence, DbFormat.ValueTypeForSeek);
            _order[i] = i;
        }
        _ikeyOffsets[n] = off;
        if (n > 1) SortOrder(_order.AsSpan(0, n), new KeyOrder(this, userComparator));
    }

    /// <summary>
    /// Starts a run whose results are computed outside the DB lookup (WriteBatchWithIndex::MultiGetFromBatchAndDB):
    /// resets the per-key state; results are then set with <see cref="SetExternalResult"/> / <see cref="SetError"/>.
    /// </summary>
    internal void BeginExternalRun(Comparator userComparator)
    {
        BeginRun(null, 0, userComparator);
        FoundCount = 0;
        _hasResults = true;
    }

    /// <summary>Records the outcome of <paramref name="key"/> for an external run (value copied into the batch).</summary>
    internal void SetExternalResult(int key, bool found, ReadOnlySpan<byte> value)
    {
        GetContext ctx = _contexts[key]!;
        ctx.Value.Clear();
        if (found)
        {
            ctx.Value.Write(value);
            _status[key] = MultiGetStatus.Found;
            FoundCount++;
        }
        else
        {
            _status[key] = MultiGetStatus.NotFound;
        }
    }

    /// <summary>Records a per-key failure.</summary>
    internal void SetError(int key, RocksDbException error)
    {
        _errors ??= new RocksDbException?[_status.Length];
        _errors[key] = error;
        _status[key] = MultiGetStatus.Error;
        _hasResults = true;
    }

    /// <summary>Ends a run: applies merge operands and records the final status of every key.</summary>
    internal void FinishRun()
    {
        int found = 0;
        for (int i = 0; i < _count; i++)
        {
            if (_status[i] == MultiGetStatus.Error) continue;
            GetContext ctx = _contexts[i]!;
            try
            {
                if (_entityResults)
                {
                    // MultiGetEntity: the entity (a plain value as a default-column entity, merges applied).
                    ctx.Scratch.Clear();
                    if (ctx.TryResolveEntity(UserKey(i), ctx.Scratch))
                    {
                        ctx.Value.Clear();
                        ctx.Value.Write(ctx.Scratch.WrittenSpan);
                        _status[i] = MultiGetStatus.Found;
                        found++;
                    }
                    else
                    {
                        _status[i] = MultiGetStatus.NotFound;
                    }
                }
                else if (ctx.TryResolve(UserKey(i), out _))
                {
                    // MultiGet returns the default column of an entity (GetContext::SaveValue, kTypeWideColumnEntity).
                    ctx.ReplaceEntityWithDefaultColumn();
                    _status[i] = MultiGetStatus.Found;
                    found++;
                }
                else
                {
                    _status[i] = MultiGetStatus.NotFound;
                }
            }
            catch (RocksDbException e)
            {
                SetError(i, e);
            }
        }
        FoundCount = found;
        _hasResults = true;
    }

    ReadOnlySpan<byte> ITableMultiGetBatch.InternalKey(int key) => InternalKey(key);

    ReadOnlyMemory<byte> ITableMultiGetBatch.InternalKeyMemory(int key) =>
        new(_ikeys, _ikeyOffsets[key], _ikeyOffsets[key + 1] - _ikeyOffsets[key]);

    internal ReadOnlySpan<byte> InternalKey(int key) => _ikeys.AsSpan(_ikeyOffsets[key], _ikeyOffsets[key + 1] - _ikeyOffsets[key]);

    bool ITableMultiGetBatch.Visit(int key, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value) => _contexts[key]!.SaveValue(internalKey, value);

    void ITableMultiGetBatch.SetIncomplete(int key) => SetIncomplete(key);

    /// <summary>Fails <paramref name="key"/> with TimedOut (ReadOptions::deadline passed before its lookup).</summary>
    internal void SetTimedOut(int key)
    {
        _contexts[key]!.State = GetContext.GetState.Found;
        SetError(key, new RocksDbException(StatusCode.TimedOut, "Operation timed out: "));
    }

    /// <summary>Ends the lookup of <paramref name="key"/> with Aborted (value_size_soft_limit exceeded).</summary>
    internal void SetAborted(int key)
    {
        // Status::Aborted() of MultiGetImpl / Version::MultiGet once ReadOptions::value_size_soft_limit is exceeded.
        _contexts[key]!.State = GetContext.GetState.Found;
        SetError(key, new RocksDbException(StatusCode.Aborted, "Operation aborted: "));
    }

    /// <summary>
    /// Ends the lookup of <paramref name="key"/> with Incomplete (read_tier = kBlockCacheTier and data that is not in
    /// the block cache): GetContext::MarkKeyMayExist ends the search (no older level is consulted).
    /// </summary>
    internal void SetIncomplete(int key)
    {
        _contexts[key]!.State = GetContext.GetState.Found;
        SetError(key, new RocksDbException(StatusCode.Incomplete, "Result incomplete: no blocking io"));
    }

    /// <summary>
    /// Sorts key indices by user key (ties by index, so the order is deterministic). Quicksort with insertion sort for
    /// small ranges; unlike <see cref="MemoryExtensions.Sort{T, TComparer}(Span{T}, TComparer)"/> it does not box the
    /// comparer into a delegate.
    /// </summary>
    private static void SortOrder(Span<int> a, KeyOrder cmp)
    {
        while (a.Length > 16)
        {
            // Median of three pivot moved to the end.
            int mid = a.Length / 2, last = a.Length - 1;
            if (cmp.Compare(a[mid], a[0]) < 0) (a[mid], a[0]) = (a[0], a[mid]);
            if (cmp.Compare(a[last], a[0]) < 0) (a[last], a[0]) = (a[0], a[last]);
            if (cmp.Compare(a[last], a[mid]) < 0) (a[last], a[mid]) = (a[mid], a[last]);
            (a[mid], a[last - 1]) = (a[last - 1], a[mid]);
            int pivot = a[last - 1];
            int i = 0, j = last - 1;
            while (true)
            {
                while (cmp.Compare(a[++i], pivot) < 0) { }
                while (j > 0 && cmp.Compare(pivot, a[--j]) < 0) { }
                if (i >= j) break;
                (a[i], a[j]) = (a[j], a[i]);
            }
            (a[i], a[last - 1]) = (a[last - 1], a[i]);
            // Recurse into the smaller part, loop on the larger one.
            if (i < a.Length - i - 1)
            {
                SortOrder(a[..i], cmp);
                a = a[(i + 1)..];
            }
            else
            {
                SortOrder(a[(i + 1)..], cmp);
                a = a[..i];
            }
        }
        for (int i = 1; i < a.Length; i++)
        {
            int v = a[i];
            int j = i - 1;
            while (j >= 0 && cmp.Compare(a[j], v) > 0)
            {
                a[j + 1] = a[j];
                j--;
            }
            a[j + 1] = v;
        }
    }

    private readonly struct KeyOrder(MultiGetBatch batch, Comparator comparator)
    {
        public int Compare(int x, int y)
        {
            int c = comparator.Compare(batch.UserKey(x), batch.UserKey(y));
            return c != 0 ? c : x.CompareTo(y);
        }
    }
}
