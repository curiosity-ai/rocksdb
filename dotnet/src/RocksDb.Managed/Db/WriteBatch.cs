using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// A batch of updates applied atomically. Port of <c>WriteBatch</c> / <c>WriteBatchInternal</c>
/// (db/write_batch.cc, db/write_batch_internal.h, include/rocksdb/write_batch.h).
/// <para>The rep (<see cref="Data"/>) is byte-identical to RocksDB's and is also the WAL record payload:</para>
/// <code>
/// rep      := sequence: fixed64, count: fixed32, data: record[count]
/// record   := kTypeValue varstring varstring
///           | kTypeDeletion varstring | kTypeSingleDeletion varstring
///           | kTypeRangeDeletion varstring varstring | kTypeMerge varstring varstring
///           | kTypeColumnFamilyValue varint32 varstring varstring     (and the other ColumnFamily* variants)
///           | kTypeBeginPrepareXID | kTypeEndPrepareXID varstring | kTypeCommitXID varstring
///           | kTypeCommitXIDAndTimestamp varstring varstring | kTypeRollbackXID varstring
///           | kTypeBeginPersistedPrepareXID | kTypeBeginUnprepareXID
///           | kTypeWideColumnEntity varstring varstring | kTypeColumnFamilyWideColumnEntity varint32 varstring varstring
///           | kTypeLogData varstring | kTypeNoop
/// varstring := len: varint32, data: uint8[len]
/// </code>
/// <para>
/// The rep lives in a pooled buffer; building a batch does not allocate once the buffer has grown to its working
/// size. Batches are reusable (<see cref="Clear"/>) and can be pooled with <see cref="Rent"/> / <see cref="Dispose"/>.
/// Not thread-safe. Operations write to column family ids directly; the public DB API will layer column family
/// objects on top.
/// </para>
/// </summary>
public sealed class WriteBatch : IDisposable
{
    /// <summary>WriteBatchInternal::kHeader: 8-byte sequence number followed by a 4-byte count.</summary>
    public const int HeaderSize = 12;

    private PooledBufferWriter _rep;
    private ContentFlags _contentFlags;
    private SavePoint[]? _savePoints;
    private int _savePointCount;
    private readonly int _maxBytes;
    private bool _pooled;

    [Flags]
    private enum ContentFlags : uint
    {
        None = 0,
        Deferred = 1 << 0,
        HasPut = 1 << 1,
        HasDelete = 1 << 2,
        HasSingleDelete = 1 << 3,
        HasMerge = 1 << 4,
        HasBeginPrepare = 1 << 5,
        HasEndPrepare = 1 << 6,
        HasCommit = 1 << 7,
        HasRollback = 1 << 8,
        HasDeleteRange = 1 << 9,
        HasBlobIndex = 1 << 10,
        HasBeginUnprepare = 1 << 11,
        HasPutEntity = 1 << 12,
        HasTimedPut = 1 << 13,
    }

    private readonly record struct SavePoint(int Size, uint Count, ContentFlags Flags);

    /// <summary>Creates an empty batch.</summary>
    public WriteBatch() : this(0, 0)
    {
    }

    /// <summary>Creates an empty batch. Port of <c>WriteBatch(size_t reserved_bytes, size_t max_bytes, ...)</c>.</summary>
    /// <param name="reservedBytes">Initial capacity of the rep.</param>
    /// <param name="maxBytes">Maximum rep size; 0 for unlimited. Operations that would exceed it throw an Aborted
    /// (MemoryLimit) <see cref="RocksDbException"/> and leave the batch unchanged.</param>
    public WriteBatch(int reservedBytes, int maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(reservedBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(maxBytes);
        _maxBytes = maxBytes;
        _rep = new PooledBufferWriter(Math.Max(reservedBytes, HeaderSize));
        _rep.GetSpan(HeaderSize)[..HeaderSize].Clear();
        _rep.Advance(HeaderSize);
    }

    /// <summary>Creates a batch from an existing rep (e.g. a WAL record). The bytes are copied.</summary>
    public WriteBatch(ReadOnlySpan<byte> rep)
    {
        _rep = new PooledBufferWriter(Math.Max(rep.Length, HeaderSize));
        SetContents(rep);
    }

    // ---- Pooling ----------------------------------------------------------------------------------------------

    private static readonly WriteBatch?[] s_pool = new WriteBatch?[Math.Min(64, Environment.ProcessorCount * 4)];
    private const int MaxPooledCapacity = 1 << 20;

    /// <summary>
    /// Gets an empty batch from a process-wide pool (or a new one). <see cref="Dispose"/> returns it to the pool, so the
    /// batch must not be used after disposal.
    /// </summary>
    public static WriteBatch Rent()
    {
        WriteBatch?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            WriteBatch? b = pool[i];
            if (b is not null && Interlocked.CompareExchange(ref pool[i], null, b) == b)
            {
                b._pooled = true;
                return b;
            }
        }
        return new WriteBatch { _pooled = true };
    }

    /// <summary>
    /// Releases the batch. A batch obtained from <see cref="Rent"/> is cleared and returned to the pool; otherwise
    /// its buffer is returned to the array pool. The batch must not be used afterwards.
    /// </summary>
    public void Dispose()
    {
        if (_pooled && _rep.Capacity <= MaxPooledCapacity && _maxBytes == 0)
        {
            _pooled = false;
            Clear();
            WriteBatch?[] pool = s_pool;
            for (int i = 0; i < pool.Length; i++)
            {
                if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], this, null) is null) return;
            }
        }
        _pooled = false;
        _rep.Dispose();
    }

    // ---- Accessors --------------------------------------------------------------------------------------------

    /// <summary>The rep: header followed by records. Valid until the batch is modified.</summary>
    public ReadOnlySpan<byte> Data => _rep.WrittenSpan;
    /// <summary>The rep as memory (for async I/O). Valid until the batch is modified.</summary>
    public ReadOnlyMemory<byte> DataMemory => _rep.WrittenMemory;
    /// <summary>Size of the rep in bytes (<c>GetDataSize</c>).</summary>
    public int DataSize => _rep.WrittenCount;
    /// <summary>Maximum rep size (0 = unlimited).</summary>
    public int MaxBytes => _maxBytes;

    /// <summary>Number of key-value operations in the batch (<c>WriteBatchInternal::Count</c>).</summary>
    public int Count => (int)Coding.DecodeFixed32(_rep.WrittenSpan[8..]);

    /// <summary>The sequence number of the first operation (<c>WriteBatchInternal::Sequence/SetSequence</c>).</summary>
    public ulong Sequence
    {
        get => Coding.DecodeFixed64(_rep.WrittenSpan);
        set => Coding.EncodeFixed64(_rep.WrittenSpanMutable, value);
    }

    internal void SetCount(uint n) => Coding.EncodeFixed32(_rep.WrittenSpanMutable[8..], n);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void IncrementCount()
    {
        Span<byte> c = _rep.WrittenSpanMutable.Slice(8, 4);
        Coding.EncodeFixed32(c, Coding.DecodeFixed32(c) + 1);
    }

    public bool HasPut => (ComputeContentFlags() & ContentFlags.HasPut) != 0;
    public bool HasTimedPut => (ComputeContentFlags() & ContentFlags.HasTimedPut) != 0;
    public bool HasPutEntity => (ComputeContentFlags() & ContentFlags.HasPutEntity) != 0;
    public bool HasDelete => (ComputeContentFlags() & ContentFlags.HasDelete) != 0;
    public bool HasSingleDelete => (ComputeContentFlags() & ContentFlags.HasSingleDelete) != 0;
    public bool HasDeleteRange => (ComputeContentFlags() & ContentFlags.HasDeleteRange) != 0;
    public bool HasMerge => (ComputeContentFlags() & ContentFlags.HasMerge) != 0;
    public bool HasBeginPrepare => (ComputeContentFlags() & ContentFlags.HasBeginPrepare) != 0;
    public bool HasEndPrepare => (ComputeContentFlags() & ContentFlags.HasEndPrepare) != 0;
    public bool HasCommit => (ComputeContentFlags() & ContentFlags.HasCommit) != 0;
    public bool HasRollback => (ComputeContentFlags() & ContentFlags.HasRollback) != 0;

    private ContentFlags ComputeContentFlags()
    {
        ContentFlags rv = _contentFlags;
        if ((rv & ContentFlags.Deferred) != 0)
        {
            // BatchContentClassifier
            rv = ContentFlags.None;
            var reader = new WriteBatchReader(_rep.WrittenSpan[HeaderSize..], wholeBatch: false, count: 0);
            while (reader.TryReadNext(out WriteBatchEntry e))
            {
                rv |= e.Type switch
                {
                    EntryType.Value => ContentFlags.HasPut,
                    EntryType.ValuePreferredSeqno => ContentFlags.HasTimedPut,
                    EntryType.WideColumnEntity => ContentFlags.HasPutEntity,
                    EntryType.Deletion => ContentFlags.HasDelete,
                    EntryType.SingleDeletion => ContentFlags.HasSingleDelete,
                    EntryType.RangeDeletion => ContentFlags.HasDeleteRange,
                    EntryType.Merge => ContentFlags.HasMerge,
                    EntryType.BlobIndex => ContentFlags.HasBlobIndex,
                    EntryType.BeginPrepareXID or EntryType.BeginPersistedPrepareXID => ContentFlags.HasBeginPrepare,
                    EntryType.BeginUnprepareXID => ContentFlags.HasBeginPrepare | ContentFlags.HasBeginUnprepare,
                    EntryType.EndPrepareXID => ContentFlags.HasEndPrepare,
                    EntryType.CommitXID or EntryType.CommitXIDAndTimestamp => ContentFlags.HasCommit,
                    EntryType.RollbackXID => ContentFlags.HasRollback,
                    _ => ContentFlags.None,
                };
            }
            _contentFlags = rv;
        }
        return rv;
    }

    // ---- Mutation ---------------------------------------------------------------------------------------------

    /// <summary>Removes all records and save points (keeps the buffer). Port of <c>WriteBatch::Clear</c>.</summary>
    public void Clear()
    {
        _rep.Clear();
        _rep.GetSpan(HeaderSize)[..HeaderSize].Clear();
        _rep.Advance(HeaderSize);
        _contentFlags = ContentFlags.None;
        _savePointCount = 0;
        HasKeyWithTimestamp = false;
        IsLatestPersistentState = false;
        _cfTimestampSizes?.Clear();
    }

    /// <summary>
    /// is_latest_persistent_state_ (WriteBatchInternal::SetAsLatestPersistentState): the batch is a commit-time batch
    /// of a WritePrepared / WriteUnprepared transaction with use_only_the_last_commit_time_batch_for_recovery. Only its
    /// last occurrence matters for recovery, so the DB caches the last such WAL record and writes it to the memtables
    /// before a memtable switch (the WAL holding it may be deleted after the flush). Not persisted; reset by
    /// <see cref="Clear"/>.
    /// </summary>
    internal bool IsLatestPersistentState { get; set; }

    private Dictionary<uint, int>? _cfTimestampSizes;

    /// <summary>
    /// WriteBatch::SetTrackTimestampSize (write_batch_track_timestamp_size): record the timestamp size of every column
    /// family a timestamped write of this batch targets, so that a transaction commit assigning timestamps
    /// (UpdateTimestamps) uses them for writes made directly to <see cref="Utilities.Transactions.Transaction.WriteBatch"/>.
    /// </summary>
    internal bool TrackTimestampSize { get; set; }

    /// <summary>
    /// True once a key was added with a timestamp by the timestamped overloads (has_key_with_ts_ of
    /// WriteBatchInternal::HasKeyWithTimestamp); reset by <see cref="Clear"/>.
    /// </summary>
    internal bool HasKeyWithTimestamp { get; private set; }

    /// <summary>WriteBatch::MaybeTrackTimestampSize: the first timestamp size seen per column family is kept.</summary>
    private void MaybeTrackTimestampSize(uint columnFamilyId, int timestampSize)
    {
        HasKeyWithTimestamp = true;
        if (!TrackTimestampSize) return;
        _cfTimestampSizes ??= new Dictionary<uint, int>();
        _cfTimestampSizes.TryAdd(columnFamilyId, timestampSize);
    }

    /// <summary>WriteBatch::GetColumnFamilyToTimestampSize lookup: the tracked timestamp size of a column family.</summary>
    internal bool TryGetTrackedTimestampSize(uint columnFamilyId, out int timestampSize)
    {
        timestampSize = 0;
        return _cfTimestampSizes is { } m && m.TryGetValue(columnFamilyId, out timestampSize);
    }

    /// <summary>Replaces the contents with a copy of <paramref name="rep"/>. Port of <c>WriteBatchInternal::SetContents</c>.</summary>
    public void SetContents(ReadOnlySpan<byte> rep)
    {
        if (rep.Length < HeaderSize) ThrowHelper.InvalidArgument("WriteBatch rep is smaller than the header");
        _rep.Clear();
        _rep.Write(rep);
        _contentFlags = ContentFlags.Deferred;
        _savePointCount = 0;
    }

    /// <summary>Stores <paramref name="key"/> -> <paramref name="value"/> in the default column family.</summary>
    public WriteBatch Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Put(0, key, value);

    /// <summary>Stores <paramref name="key"/> -> <paramref name="value"/>. Port of <c>WriteBatchInternal::Put</c>.</summary>
    public WriteBatch Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        AppendKeyValueRecord(EntryType.Value, EntryType.ColumnFamilyValue, columnFamilyId, key, value, ContentFlags.HasPut);
        return this;
    }

    /// <summary>Removes the mapping for <paramref name="key"/> in the default column family, if any.</summary>
    public WriteBatch Delete(ReadOnlySpan<byte> key) => Delete(0, key);

    /// <summary>Removes the mapping for <paramref name="key"/>, if any. Port of <c>WriteBatchInternal::Delete</c>.</summary>
    public WriteBatch Delete(uint columnFamilyId, ReadOnlySpan<byte> key)
    {
        AppendKeyRecord(EntryType.Deletion, EntryType.ColumnFamilyDeletion, columnFamilyId, key, ContentFlags.HasDelete);
        return this;
    }

    /// <summary>SingleDelete in the default column family.</summary>
    public WriteBatch SingleDelete(ReadOnlySpan<byte> key) => SingleDelete(0, key);

    /// <summary>
    /// Removes the mapping for <paramref name="key"/>; only valid if the key was put at most once since the previous
    /// SingleDelete. Port of <c>WriteBatchInternal::SingleDelete</c>.
    /// </summary>
    public WriteBatch SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key)
    {
        AppendKeyRecord(EntryType.SingleDeletion, EntryType.ColumnFamilySingleDeletion, columnFamilyId, key, ContentFlags.HasSingleDelete);
        return this;
    }

    /// <summary>Removes keys in [<paramref name="beginKey"/>, <paramref name="endKey"/>) in the default column family.</summary>
    public WriteBatch DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => DeleteRange(0, beginKey, endKey);

    /// <summary>Removes keys in [<paramref name="beginKey"/>, <paramref name="endKey"/>). Port of <c>WriteBatchInternal::DeleteRange</c>.</summary>
    public WriteBatch DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
    {
        AppendKeyValueRecord(EntryType.RangeDeletion, EntryType.ColumnFamilyRangeDeletion, columnFamilyId, beginKey, endKey, ContentFlags.HasDeleteRange);
        return this;
    }

    /// <summary>Merges <paramref name="value"/> into <paramref name="key"/> in the default column family.</summary>
    public WriteBatch Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Merge(0, key, value);

    /// <summary>Merges <paramref name="value"/> into <paramref name="key"/>. Port of <c>WriteBatchInternal::Merge</c>.</summary>
    public WriteBatch Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        AppendKeyValueRecord(EntryType.Merge, EntryType.ColumnFamilyMerge, columnFamilyId, key, value, ContentFlags.HasMerge);
        return this;
    }

    // ---- User-defined timestamps (WriteBatch::Put/Delete/SingleDelete/DeleteRange/Merge with ts) ---------------

    /// <summary>
    /// Stores <paramref name="key"/> at <paramref name="timestamp"/> -> <paramref name="value"/>: the record key is
    /// <c>key | timestamp</c> (WriteBatch::Put(cf, key, ts, value)). The column family must use a timestamp-aware
    /// comparator whose timestamp size equals <paramref name="timestamp"/>.Length (checked when written to a DB).
    /// </summary>
    public WriteBatch Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value)
    {
        AppendTimestampedRecord(EntryType.Value, EntryType.ColumnFamilyValue, columnFamilyId, key, timestamp, value, true, ContentFlags.HasPut);
        return this;
    }

    /// <summary>Deletes <paramref name="key"/> at <paramref name="timestamp"/> (WriteBatch::Delete(cf, key, ts)).</summary>
    public WriteBatch Delete(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp)
    {
        AppendTimestampedRecord(EntryType.Deletion, EntryType.ColumnFamilyDeletion, columnFamilyId, key, timestamp, default, false, ContentFlags.HasDelete);
        return this;
    }

    /// <summary>SingleDelete of <paramref name="key"/> at <paramref name="timestamp"/> (WriteBatch::SingleDelete(cf, key, ts)).</summary>
    public WriteBatch SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp)
    {
        AppendTimestampedRecord(EntryType.SingleDeletion, EntryType.ColumnFamilySingleDeletion, columnFamilyId, key, timestamp, default, false,
            ContentFlags.HasSingleDelete);
        return this;
    }

    /// <summary>Merge operand for <paramref name="key"/> at <paramref name="timestamp"/> (WriteBatch::Merge(cf, key, ts, value)).</summary>
    public WriteBatch Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value)
    {
        AppendTimestampedRecord(EntryType.Merge, EntryType.ColumnFamilyMerge, columnFamilyId, key, timestamp, value, true, ContentFlags.HasMerge);
        return this;
    }

    /// <summary>
    /// Range deletion at <paramref name="timestamp"/>: both bounds carry the timestamp (WriteBatch::DeleteRange(cf, begin,
    /// end, ts)).
    /// </summary>
    public WriteBatch DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey, ReadOnlySpan<byte> timestamp)
    {
        MaybeTrackTimestampSize(columnFamilyId, timestamp.Length);
        SaveState(out SavePoint save);
        int keyLen = beginKey.Length + timestamp.Length;
        int endLen = endKey.Length + timestamp.Length;
        Span<byte> dst = BeginRecord(EntryType.RangeDeletion, EntryType.ColumnFamilyRangeDeletion, columnFamilyId, keyLen, endLen, out int p);
        p += Coding.EncodeVarint32(dst[p..], (uint)keyLen);
        beginKey.CopyTo(dst[p..]);
        timestamp.CopyTo(dst[(p + beginKey.Length)..]);
        p += keyLen;
        p += Coding.EncodeVarint32(dst[p..], (uint)endLen);
        endKey.CopyTo(dst[p..]);
        timestamp.CopyTo(dst[(p + endKey.Length)..]);
        p += endLen;
        CommitRecord(p, ContentFlags.HasDeleteRange, counted: true, save);
        return this;
    }

    private void AppendTimestampedRecord(EntryType defaultCfTag, EntryType cfTag, uint columnFamilyId, ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value, bool hasValue, ContentFlags flag)
    {
        MaybeTrackTimestampSize(columnFamilyId, timestamp.Length);
        SaveState(out SavePoint save);
        int keyLen = key.Length + timestamp.Length;
        Span<byte> dst = BeginRecord(defaultCfTag, cfTag, columnFamilyId, keyLen, value.Length, out int p);
        p += Coding.EncodeVarint32(dst[p..], (uint)keyLen);
        key.CopyTo(dst[p..]);
        timestamp.CopyTo(dst[(p + key.Length)..]);
        p += keyLen;
        if (hasValue)
        {
            p += Coding.EncodeVarint32(dst[p..], (uint)value.Length);
            value.CopyTo(dst[p..]);
            p += value.Length;
        }
        CommitRecord(p, flag, counted: true, save);
    }

    /// <summary>
    /// Port of WriteBatch::UpdateTimestamps: overwrites, in place, the trailing timestamp of every key (and range
    /// deletion bound) of the column families for which <paramref name="timestampSizeOf"/> returns a non-zero size with
    /// <paramref name="timestamp"/>. Used for batches built with placeholder timestamps that are assigned at commit time.
    /// Throws InvalidArgument if a size differs from <paramref name="timestamp"/>.Length or a key is shorter than it.
    /// </summary>
    public void UpdateTimestamps(ReadOnlySpan<byte> timestamp, Func<uint, int> timestampSizeOf)
    {
        ArgumentNullException.ThrowIfNull(timestampSizeOf);
        Span<byte> rep = _rep.WrittenSpanMutable;
        ref byte origin = ref System.Runtime.InteropServices.MemoryMarshal.GetReference(rep);
        var reader = new WriteBatchReader(rep);
        while (reader.TryReadNext(out WriteBatchEntry e))
        {
            bool hasKey = e.Type is EntryType.Value or EntryType.Deletion or EntryType.SingleDeletion or EntryType.Merge
                or EntryType.RangeDeletion or EntryType.WideColumnEntity or EntryType.BlobIndex or EntryType.ValuePreferredSeqno;
            if (!hasKey) continue;
            int tsSize = timestampSizeOf(e.ColumnFamilyId);
            if (tsSize == 0) continue;
            if (tsSize != timestamp.Length) ThrowHelper.InvalidArgument("timestamp size mismatch");
            OverwriteTimestamp(rep, ref origin, e.Key, timestamp);
            if (e.Type == EntryType.RangeDeletion) OverwriteTimestamp(rep, ref origin, e.Value, timestamp);
        }
    }

    private static void OverwriteTimestamp(Span<byte> rep, ref byte origin, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp)
    {
        if (key.Length < timestamp.Length) ThrowHelper.InvalidArgument("key is shorter than the timestamp");
        int offset = (int)Unsafe.ByteOffset(ref origin, ref System.Runtime.InteropServices.MemoryMarshal.GetReference(key));
        timestamp.CopyTo(rep[(offset + key.Length - timestamp.Length)..]);
    }

    /// <summary>
    /// Stores a wide-column entity whose serialized form (<c>WideColumnSerialization</c>, columns sorted by name) is
    /// <paramref name="entity"/>. Port of <c>WriteBatchInternal::PutEntitySerialized</c>.
    /// </summary>
    public WriteBatch PutEntitySerialized(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
    {
        AppendKeyValueRecord(EntryType.WideColumnEntity, EntryType.ColumnFamilyWideColumnEntity, columnFamilyId, key, entity, ContentFlags.HasPutEntity);
        return this;
    }

    /// <summary>Stores a wide-column entity in the default column family (see <see cref="PutEntity(uint, ReadOnlySpan{byte}, ReadOnlySpan{WideColumn})"/>).</summary>
    public WriteBatch PutEntity(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => PutEntity(0, key, columns);

    /// <summary>
    /// Stores the attribute groups of <paramref name="key"/>: one entity per group, in the group's column family
    /// (WriteBatch::PutEntity(key, attribute_groups), db/write_batch.cc). InvalidArgument when
    /// <paramref name="attributeGroups"/> is empty.
    /// </summary>
    public WriteBatch PutAttributeGroups(ReadOnlySpan<byte> key, ReadOnlySpan<AttributeGroup> attributeGroups)
    {
        if (attributeGroups.IsEmpty) ThrowHelper.InvalidArgument("Cannot call this method with empty attribute groups");
        for (int gi = 0; gi < attributeGroups.Length; gi++)
        {
            AttributeGroup ag = attributeGroups[gi];
            if (ag.ColumnFamily is null) ThrowHelper.InvalidArgument("Cannot call this method without a column family handle");
            PutEntity(ag.ColumnFamily.Id, key, ag.Columns.Span);
        }
        return this;
    }

    /// <summary>
    /// Stores a wide-column entity (WriteBatch::PutEntity): the columns are sorted by name and serialized
    /// (wide-column format version 1). Duplicate column names fail with Corruption ("Wide columns out of order") like
    /// upstream. Port of WriteBatchInternal::PutEntity (db/write_batch.cc).
    /// </summary>
    public WriteBatch PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns)
    {
        WideColumn[] sorted = System.Buffers.ArrayPool<WideColumn>.Shared.Rent(Math.Max(1, columns.Length));
        PooledBufferWriter entity = ObjectPools.RentWriter();
        try
        {
            columns.CopyTo(sorted);
            Span<WideColumn> s = sorted.AsSpan(0, columns.Length);
            Wide.WideColumnSerialization.SortColumns(s);
            Wide.WideColumnSerialization.Serialize(s, entity);
            return PutEntitySerialized(columnFamilyId, key, entity.WrittenSpan);
        }
        finally
        {
            ObjectPools.ReturnWriter(entity);
            System.Buffers.ArrayPool<WideColumn>.Shared.Return(sorted, clearArray: true);
        }
    }

    /// <summary>
    /// Put with an associated write time (<c>kTypeValuePreferredSeqno</c>). A write time of <see cref="ulong.MaxValue"/>
    /// means "unknown" and degrades to a plain Put. Port of <c>WriteBatchInternal::TimedPut</c>.
    /// </summary>
    public WriteBatch TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime)
    {
        if (writeUnixTime == ulong.MaxValue) return Put(columnFamilyId, key, value);
        SaveState(out SavePoint save);
        // PackValueAndWriteTime: value | fixed64(write_unix_time)
        long packedLength = (long)value.Length + 8;
        Span<byte> dst = BeginRecord(EntryType.ValuePreferredSeqno, EntryType.ColumnFamilyValuePreferredSeqno, columnFamilyId,
            key.Length, packedLength, out int p);
        p += Coding.EncodeVarint32(dst[p..], (uint)key.Length);
        key.CopyTo(dst[p..]);
        p += key.Length;
        p += Coding.EncodeVarint32(dst[p..], (uint)packedLength);
        value.CopyTo(dst[p..]);
        p += value.Length;
        Coding.EncodeFixed64(dst[p..], writeUnixTime);
        p += 8;
        CommitRecord(p, ContentFlags.HasTimedPut, counted: true, save);
        return this;
    }

    /// <summary>Stores a blob index (BlobDB). Port of <c>WriteBatchInternal::PutBlobIndex</c>.</summary>
    internal WriteBatch PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex)
    {
        AppendKeyValueRecord(EntryType.BlobIndex, EntryType.ColumnFamilyBlobIndex, columnFamilyId, key, blobIndex, ContentFlags.HasBlobIndex);
        return this;
    }

    /// <summary>
    /// Appends a blob that is written to the WAL but not applied to the DB (it does not count towards
    /// <see cref="Count"/>). Port of <c>WriteBatch::PutLogData</c>.
    /// </summary>
    public WriteBatch PutLogData(ReadOnlySpan<byte> blob)
    {
        SaveState(out SavePoint save);
        Span<byte> dst = _rep.GetSpan(1 + Coding.MaxVarint32Length + blob.Length);
        dst[0] = (byte)EntryType.LogData;
        int p = 1 + Coding.EncodeVarint32(dst[1..], (uint)blob.Length);
        blob.CopyTo(dst[p..]);
        CommitRecord(p + blob.Length, ContentFlags.None, counted: false, save);
        return this;
    }

    // ---- Transaction markers (WriteBatchInternal) -----------------------------------------------------------

    /// <summary>WriteBatchInternal::InsertNoop.</summary>
    internal void InsertNoop() => _rep.WriteByte((byte)EntryType.Noop);

    /// <summary>WriteBatchInternal::GetBeginPrepareType.</summary>
    internal static EntryType GetBeginPrepareType(bool writeAfterCommit, bool unpreparedBatch) =>
        writeAfterCommit ? EntryType.BeginPrepareXID
            : (unpreparedBatch ? EntryType.BeginUnprepareXID : EntryType.BeginPersistedPrepareXID);

    /// <summary>WriteBatchInternal::InsertBeginPrepare.</summary>
    internal void InsertBeginPrepare(bool writeAfterCommit, bool unpreparedBatch)
    {
        _rep.WriteByte((byte)GetBeginPrepareType(writeAfterCommit, unpreparedBatch));
        _contentFlags |= ContentFlags.HasBeginPrepare;
    }

    /// <summary>WriteBatchInternal::InsertEndPrepare.</summary>
    internal void InsertEndPrepare(ReadOnlySpan<byte> xid)
    {
        _rep.WriteByte((byte)EntryType.EndPrepareXID);
        Coding.PutLengthPrefixedSlice(_rep, xid);
        _contentFlags |= ContentFlags.HasEndPrepare;
    }

    /// <summary>
    /// Rewrites the leading Noop as a begin-prepare marker and appends an end-prepare marker.
    /// Port of <c>WriteBatchInternal::MarkEndPrepare</c>.
    /// </summary>
    internal void MarkEndPrepare(ReadOnlySpan<byte> xid, bool writeAfterCommit, bool unpreparedBatch)
    {
        // a manually constructed batch can only contain one prepare section
        if (_rep.WrittenCount <= HeaderSize || _rep.WrittenSpan[HeaderSize] != (byte)EntryType.Noop)
            ThrowHelper.InvalidArgument("MarkEndPrepare requires a batch that starts with a Noop");
        // all savepoints up to this point are cleared
        _savePointCount = 0;
        // rewrite noop as begin marker
        _rep.WrittenSpanMutable[HeaderSize] = (byte)GetBeginPrepareType(writeAfterCommit, unpreparedBatch);
        _contentFlags |= ContentFlags.HasBeginPrepare;
        if (unpreparedBatch) _contentFlags |= ContentFlags.HasBeginUnprepare;
        InsertEndPrepare(xid);
    }

    /// <summary>WriteBatchInternal::MarkCommit.</summary>
    internal void MarkCommit(ReadOnlySpan<byte> xid)
    {
        _rep.WriteByte((byte)EntryType.CommitXID);
        Coding.PutLengthPrefixedSlice(_rep, xid);
        _contentFlags |= ContentFlags.HasCommit;
    }

    /// <summary>WriteBatchInternal::MarkCommitWithTimestamp.</summary>
    internal void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp)
    {
        if (commitTimestamp.IsEmpty) ThrowHelper.InvalidArgument("commit timestamp must not be empty");
        _rep.WriteByte((byte)EntryType.CommitXIDAndTimestamp);
        Coding.PutLengthPrefixedSlice(_rep, commitTimestamp);
        Coding.PutLengthPrefixedSlice(_rep, xid);
        _contentFlags |= ContentFlags.HasCommit;
    }

    /// <summary>WriteBatchInternal::MarkRollback.</summary>
    internal void MarkRollback(ReadOnlySpan<byte> xid)
    {
        _rep.WriteByte((byte)EntryType.RollbackXID);
        Coding.PutLengthPrefixedSlice(_rep, xid);
        _contentFlags |= ContentFlags.HasRollback;
    }

    /// <summary>
    /// Appends the records of <paramref name="src"/> to <paramref name="dst"/> (used to merge batches for group
    /// commit). Port of <c>WriteBatchInternal::Append</c> (without WAL termination points).
    /// </summary>
    internal static void Append(WriteBatch dst, WriteBatch src)
    {
        ContentFlags srcFlags = src._contentFlags;
        int srcCount = src.Count;
        dst.SetCount((uint)(dst.Count + srcCount));
        dst._rep.Write(src._rep.WrittenSpan[HeaderSize..]);
        dst._contentFlags |= srcFlags;
    }

    /// <summary>WriteBatchInternal::AppendedByteSize.</summary>
    internal static int AppendedByteSize(int leftByteSize, int rightByteSize) =>
        leftByteSize == 0 || rightByteSize == 0 ? leftByteSize + rightByteSize : leftByteSize + rightByteSize - HeaderSize;

    // ---- Record encoding helpers --------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SaveState(out SavePoint save) =>
        save = _maxBytes == 0 ? default : new SavePoint(_rep.WrittenCount, (uint)Count, _contentFlags);

    /// <summary>
    /// Reserves room for a record of the given payload sizes and writes the tag (+ CF id). Returns the span to write
    /// into and the number of bytes written so far.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<byte> BeginRecord(EntryType defaultCfTag, EntryType cfTag, uint columnFamilyId, int keyLength, long valueLength, out int p)
    {
        long max = 1 + Coding.MaxVarint32Length + (2L * Coding.MaxVarint32Length) + keyLength + valueLength;
        if (max > Array.MaxLength) ThrowTooLarge();
        Span<byte> dst = _rep.GetSpan((int)max);
        if (columnFamilyId == 0)
        {
            dst[0] = (byte)defaultCfTag;
            p = 1;
        }
        else
        {
            dst[0] = (byte)cfTag;
            p = 1 + Coding.EncodeVarint32(dst[1..], columnFamilyId);
        }
        return dst;
    }

    private void AppendKeyValueRecord(EntryType defaultCfTag, EntryType cfTag, uint columnFamilyId,
        ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ContentFlags flag)
    {
        SaveState(out SavePoint save);
        Span<byte> dst = BeginRecord(defaultCfTag, cfTag, columnFamilyId, key.Length, value.Length, out int p);
        p += Coding.EncodeVarint32(dst[p..], (uint)key.Length);
        key.CopyTo(dst[p..]);
        p += key.Length;
        p += Coding.EncodeVarint32(dst[p..], (uint)value.Length);
        value.CopyTo(dst[p..]);
        p += value.Length;
        CommitRecord(p, flag, counted: true, save);
    }

    private void AppendKeyRecord(EntryType defaultCfTag, EntryType cfTag, uint columnFamilyId, ReadOnlySpan<byte> key, ContentFlags flag)
    {
        SaveState(out SavePoint save);
        Span<byte> dst = BeginRecord(defaultCfTag, cfTag, columnFamilyId, key.Length, 0, out int p);
        p += Coding.EncodeVarint32(dst[p..], (uint)key.Length);
        key.CopyTo(dst[p..]);
        p += key.Length;
        CommitRecord(p, flag, counted: true, save);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CommitRecord(int length, ContentFlags flag, bool counted, in SavePoint save)
    {
        _rep.Advance(length);
        if (counted) IncrementCount();
        _contentFlags |= flag;
        // LocalSavePoint::commit
        if (_maxBytes != 0 && _rep.WrittenCount > _maxBytes) RollbackAndThrowMemoryLimit(save);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void RollbackAndThrowMemoryLimit(in SavePoint save)
    {
        _rep.Truncate(save.Size);
        SetCount(save.Count);
        _contentFlags = save.Flags;
        throw new RocksDbException(StatusCode.Aborted, "Operation aborted: Memory limit reached: Write batch is too big",
            StatusSubCode.MemoryLimit);
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowTooLarge() => ThrowHelper.InvalidArgument("key or value is too large");

    // ---- Save points ------------------------------------------------------------------------------------------

    /// <summary>Records the current state so that it can be restored by <see cref="RollbackToSavePoint"/>.</summary>
    public void SetSavePoint()
    {
        _savePoints ??= new SavePoint[4];
        if (_savePointCount == _savePoints.Length) Array.Resize(ref _savePoints, _savePoints.Length * 2);
        // Record length and count of current batch of writes.
        _savePoints[_savePointCount++] = new SavePoint(_rep.WrittenCount, (uint)Count, _contentFlags);
    }

    /// <summary>
    /// Removes all entries added since the most recent save point and pops it. Returns false (C++: NotFound) if there
    /// is no save point. Port of <c>WriteBatch::RollbackToSavePoint</c>.
    /// </summary>
    public bool RollbackToSavePoint()
    {
        if (_savePointCount == 0) return false;
        // Pop the most recent savepoint off the stack
        SavePoint sp = _savePoints![--_savePointCount];
        if (sp.Size == _rep.WrittenCount)
        {
            // No changes to rollback
        }
        else
        {
            _rep.Truncate(sp.Size);
            SetCount(sp.Count);
            _contentFlags = sp.Flags;
        }
        return true;
    }

    /// <summary>Pops the most recent save point without rolling back. Returns false if there is none.</summary>
    public bool PopSavePoint()
    {
        if (_savePointCount == 0) return false;
        _savePointCount--;
        return true;
    }

    // ---- Iteration --------------------------------------------------------------------------------------------

    /// <summary>Returns an allocation-free reader over the records (usable with <c>foreach</c>).</summary>
    public WriteBatchReader GetEnumerator() => new(_rep.WrittenSpan);

    /// <summary>Invokes <paramref name="handler"/> for every record. Port of <c>WriteBatch::Iterate</c>.</summary>
    public void Iterate<THandler>(ref THandler handler) where THandler : IWriteBatchHandler =>
        Iterate(_rep.WrittenSpan, ref handler);

    /// <summary>
    /// Invokes <paramref name="handler"/> for every record of <paramref name="rep"/> and validates the record count.
    /// Throws a Corruption <see cref="RocksDbException"/> on malformed input. Port of
    /// <c>WriteBatchInternal::Iterate</c>.
    /// </summary>
    public static void Iterate<THandler>(ReadOnlySpan<byte> rep, ref THandler handler) where THandler : IWriteBatchHandler
    {
        if (rep.Length < HeaderSize) ThrowHelper.Corruption("malformed WriteBatch (too small)");
        IterateRecords(rep[HeaderSize..], wholeBatch: true, Coding.DecodeFixed32(rep[8..]), ref handler);
    }

    /// <summary>
    /// The 2PC marker types a handler accepts (WriteBatch::Handler::WriteAfterCommit / WriteBeforePrepare option
    /// states). <see cref="TxnMarkerPolicy.Any"/> = kUnknown (no check).
    /// </summary>
    internal enum TxnMarkerPolicy : byte
    {
        Any = 0,
        /// <summary>write_after_commit enabled, write_before_prepare disabled (WriteCommitted / plain DB).</summary>
        WriteCommitted = 1,
        /// <summary>write_after_commit and write_before_prepare disabled (WritePrepared).</summary>
        WritePrepared = 2,
        /// <summary>write_after_commit disabled, write_before_prepare enabled (WriteUnprepared).</summary>
        WriteUnprepared = 3,
    }

    /// <summary>
    /// <see cref="Iterate{THandler}(ReadOnlySpan{byte}, ref THandler)"/> that refuses begin-prepare markers of another
    /// write policy with NotSupported, like WriteBatchInternal::Iterate for a handler with known option states (the
    /// memtable inserter of a DB opened with a given write policy).
    /// </summary>
    internal static void Iterate<THandler>(ReadOnlySpan<byte> rep, ref THandler handler, TxnMarkerPolicy policy) where THandler : IWriteBatchHandler
    {
        if (rep.Length < HeaderSize) ThrowHelper.Corruption("malformed WriteBatch (too small)");
        IterateRecords(rep[HeaderSize..], wholeBatch: true, Coding.DecodeFixed32(rep[8..]), ref handler, policy);
    }

    /// <summary>The NotSupported status of WriteBatchInternal::Iterate for a begin-prepare marker the handler's policy refuses, or null.</summary>
    private static string? CheckBeginPrepareMarker(EntryType type, TxnMarkerPolicy policy)
    {
        bool writeAfterCommit = policy == TxnMarkerPolicy.WriteCommitted;
        bool writeBeforePrepare = policy == TxnMarkerPolicy.WriteUnprepared;
        string? s = null;
        switch (type)
        {
            case EntryType.BeginPrepareXID:
                if (!writeAfterCommit)
                {
                    s = "WriteCommitted txn tag when write_after_commit_ is disabled (in WritePrepared/WriteUnprepared mode). If it is not due to "
                        + "corruption, the WAL must be emptied before changing the WritePolicy.";
                }
                if (writeBeforePrepare)
                {
                    s = "WriteCommitted txn tag when write_before_prepare_ is enabled (in WriteUnprepared mode). If it is not due to corruption, the "
                        + "WAL must be emptied before changing the WritePolicy.";
                }
                break;
            case EntryType.BeginPersistedPrepareXID:
                if (writeAfterCommit)
                {
                    s = "WritePrepared/WriteUnprepared txn tag when write_after_commit_ is enabled (in default WriteCommitted mode). If it is not due "
                        + "to corruption, the WAL must be emptied before changing the WritePolicy.";
                }
                break;
            case EntryType.BeginUnprepareXID:
                if (writeAfterCommit)
                {
                    s = "WriteUnprepared txn tag when write_after_commit_ is enabled (in default WriteCommitted mode). If it is not due to corruption, "
                        + "the WAL must be emptied before changing the WritePolicy.";
                }
                if (!writeBeforePrepare)
                {
                    s = "WriteUnprepared txn tag when write_before_prepare_ is disabled (in WriteCommitted/WritePrepared mode). If it is not due to "
                        + "corruption, the WAL must be emptied before changing the WritePolicy.";
                }
                break;
        }
        return s;
    }

    /// <summary>Iterates a sub-range [<paramref name="begin"/>, <paramref name="end"/>) of the rep without count validation.</summary>
    internal void Iterate<THandler>(int begin, int end, ref THandler handler) where THandler : IWriteBatchHandler
    {
        int size = _rep.WrittenCount;
        if (begin > size || end > size || end < begin) ThrowHelper.Corruption("Invalid start/end bounds for Iterate");
        bool wholeBatch = begin == HeaderSize && end == size;
        IterateRecords(_rep.WrittenSpan[begin..end], wholeBatch, (uint)Count, ref handler);
    }

    private static void IterateRecords<THandler>(ReadOnlySpan<byte> input, bool wholeBatch, uint count, ref THandler handler)
        where THandler : IWriteBatchHandler =>
        IterateRecords(input, wholeBatch, count, ref handler, TxnMarkerPolicy.Any);

    private static void IterateRecords<THandler>(ReadOnlySpan<byte> input, bool wholeBatch, uint count, ref THandler handler, TxnMarkerPolicy policy)
        where THandler : IWriteBatchHandler
    {
        // Sometimes a sub-batch starts with a Noop. We want to exclude such Noops as the batch boundary symbols
        // otherwise we would mis-count the number of batches. We do that by checking whether the accumulated batch is
        // empty before seeing the next Noop.
        bool emptyBatch = true;
        uint found = 0;
        bool handlerContinue = true;
        int pos = 0;
        while (pos < input.Length)
        {
            handlerContinue = handler.Continue();
            if (!handlerContinue) break;

            string? error = WriteBatchReader.TryReadRecord(input, ref pos, out WriteBatchEntry e);
            if (error is not null) ThrowHelper.Corruption(error);

            switch (e.Type)
            {
                case EntryType.Value:
                    handler.Put(e.ColumnFamilyId, e.Key, e.Value);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.Deletion:
                    handler.Delete(e.ColumnFamilyId, e.Key);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.SingleDeletion:
                    handler.SingleDelete(e.ColumnFamilyId, e.Key);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.RangeDeletion:
                    handler.DeleteRange(e.ColumnFamilyId, e.Key, e.Value);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.Merge:
                    handler.Merge(e.ColumnFamilyId, e.Key, e.Value);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.BlobIndex:
                    handler.PutBlobIndex(e.ColumnFamilyId, e.Key, e.Value);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.LogData:
                    handler.LogData(e.Value);
                    // A batch might have nothing but LogData. It is still a batch.
                    emptyBatch = false;
                    break;
                case EntryType.BeginPrepareXID:
                case EntryType.BeginPersistedPrepareXID:
                    if (policy != TxnMarkerPolicy.Any && CheckBeginPrepareMarker(e.Type, policy) is { } bad) ThrowHelper.NotSupported(bad);
                    handler.MarkBeginPrepare(false);
                    emptyBatch = false;
                    break;
                case EntryType.BeginUnprepareXID:
                    if (policy != TxnMarkerPolicy.Any && CheckBeginPrepareMarker(e.Type, policy) is { } badU) ThrowHelper.NotSupported(badU);
                    handler.MarkBeginPrepare(true);
                    emptyBatch = false;
                    break;
                case EntryType.EndPrepareXID:
                    handler.MarkEndPrepare(e.Value);
                    emptyBatch = true;
                    break;
                case EntryType.CommitXID:
                    handler.MarkCommit(e.Value);
                    emptyBatch = true;
                    break;
                case EntryType.CommitXIDAndTimestamp:
                    // key stores the commit timestamp.
                    handler.MarkCommitWithTimestamp(e.Value, e.Key);
                    emptyBatch = true;
                    break;
                case EntryType.RollbackXID:
                    handler.MarkRollback(e.Value);
                    emptyBatch = true;
                    break;
                case EntryType.Noop:
                    handler.MarkNoop(emptyBatch);
                    emptyBatch = true;
                    break;
                case EntryType.WideColumnEntity:
                    handler.PutEntity(e.ColumnFamilyId, e.Key, e.Value);
                    emptyBatch = false;
                    found++;
                    break;
                case EntryType.ValuePreferredSeqno:
                    handler.TimedPut(e.ColumnFamilyId, e.Key, e.Value, e.WriteUnixTime);
                    emptyBatch = false;
                    found++;
                    break;
                default:
                    ThrowHelper.Corruption("unknown WriteBatch tag");
                    break;
            }
        }
        if (handlerContinue && wholeBatch && found != count)
        {
            ThrowHelper.Corruption("WriteBatch has wrong count");
        }
    }
}
