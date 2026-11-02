using System.Diagnostics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Memory;
using RocksDb.Managed.Memtable;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// In-memory write buffer: a concurrent skiplist of entries allocated in a <see cref="ConcurrentArena"/>.
/// Port of the DB-independent parts of rocksdb::MemTable (db/memtable.h, db/memtable.cc) with the default
/// SkipListRep (memtable/skiplistrep.cc).
/// </summary>
/// <remarks>
/// <para><b>Entry format</b> (as upstream, without per-key protection bytes):
/// <c>varint32(internal_key_len) | user_key | fixed64(seq &lt;&lt; 8 | type) | varint32(value_len) | value</c>.
/// Point entries live in one skiplist; <see cref="EntryType.RangeDeletion"/> entries (value = end key) live in a
/// separate skiplist like upstream's <c>range_del_table_</c>.</para>
/// <para><b>Ordering.</b> Entries are ordered by <see cref="InternalKeyComparator.CompareKeySeq"/>; a second entry with
/// the same user key and sequence number is rejected (<see cref="Add"/> returns false, upstream
/// <c>Status::TryAgain("key+seq exists")</c>).</para>
/// <para><b>Threading.</b> <see cref="Add"/> calls must be serialized by the caller (the write thread);
/// <see cref="AddConcurrently"/> may run concurrently with each other (followed by one
/// <see cref="BatchPostProcess"/> per writer). Reads (<see cref="Get"/>, iterators) are lock-free and may run
/// concurrently with writes.</para>
/// <para><b>Lifetime.</b> Spans returned by <see cref="Get"/> and iterators point into the arena and are valid until the
/// memtable is disposed. Like upstream, the memtable is reference counted: <see cref="Ref"/> / <see cref="Unref"/>; the
/// owner disposes it when <see cref="Unref"/> returns true.</para>
/// </remarks>
public sealed unsafe class MemTable : IDisposable
{
    private const int FlushNotRequested = 0;
    private const int FlushRequested = 1;
    private const int FlushScheduled = 2;

    private readonly InternalKeyComparator _comparator;
    private readonly Comparator _ucmp;
    private readonly bool _bytewise;
    private readonly int _arenaBlockSize;
    private readonly ConcurrentArena _arena;
    private readonly InlineSkipList<MemTableKeyComparer> _table;
    private readonly InlineSkipList<MemTableKeyComparer> _rangeDelTable;
    private readonly long _maxRangeDeletions;
    private readonly ulong _creationSeq;
    // newest_udt_ (persist_user_defined_timestamps = false only): the newest timestamp added, guarded by _newestUdtLock.
    private readonly int _newestUdtSize;
    private readonly byte[]? _newestUdt;
    private bool _hasNewestUdt;
    private readonly Lock? _newestUdtLock;

    private int _refs;
    private long _writeBufferSize;
    private long _dataSize;
    private long _numEntries;
    private long _numDeletes;
    private long _numRangeDeletes;
    private long _approximateMemoryUsage;
    private ulong _firstSeqno;
    private ulong _earliestSeqno;
    private int _flushState;
    private int _isRangeDelTableEmpty = 1;
    private bool _disposed;

    // Range tombstone cache: rebuilt lazily when _rangeDelVersion moves (see GetFragmentedRangeTombstones).
    private int _rangeDelVersion;
    // (version, list) published together as one immutable object: reading the list and its version separately could
    // pair a stale list with a newer version under a concurrent rebuild and miss a visible tombstone.
    private RangeDelCacheEntry? _rangeDelCache;
    private readonly Lock _rangeDelLock = new();

    /// <param name="comparator">Internal key comparator (wrapping the column family's user comparator).</param>
    /// <param name="options">Sizing options.</param>
    /// <param name="latestSequence">Last sequence number at creation; initial <see cref="EarliestSequenceNumber"/>.</param>
    public MemTable(InternalKeyComparator comparator, MemTableOptions options, ulong latestSequence)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        ArgumentNullException.ThrowIfNull(options);
        _comparator = comparator;
        _ucmp = comparator.UserComparator;
        _bytewise = comparator.IsBytewise;
        int blockSize = options.GetEffectiveArenaBlockSize();
        _arenaBlockSize = Arena.OptimizeBlockSize(blockSize);
        _arena = new ConcurrentArena(blockSize);
        var keyCmp = new MemTableKeyComparer(comparator);
        _table = new InlineSkipList<MemTableKeyComparer>(keyCmp, _arena, options.SkipListMaxHeight, options.SkipListBranchingFactor);
        // range del table must support concurrent inserts
        _rangeDelTable = new InlineSkipList<MemTableKeyComparer>(keyCmp, _arena, options.SkipListMaxHeight, options.SkipListBranchingFactor);
        _writeBufferSize = options.WriteBufferSize;
        _maxRangeDeletions = options.MaxRangeDeletions;
        _earliestSeqno = latestSequence;
        _creationSeq = latestSequence;
        _newestUdtSize = options.TrackNewestTimestampSize;
        if (_newestUdtSize > 0)
        {
            _newestUdt = new byte[_newestUdtSize];
            _newestUdtLock = new Lock();
        }
        UpdateFlushState();
        // something went wrong if we need to flush before inserting anything
        Debug.Assert(!ShouldScheduleFlush);
    }

    public InternalKeyComparator Comparator => _comparator;

    // ------------------------------------------------------------------ reference counting

    /// <summary>Increments the reference count.</summary>
    public void Ref() => Interlocked.Increment(ref _refs);

    /// <summary>Decrements the reference count; returns true when it dropped to zero (caller should dispose).</summary>
    public bool Unref()
    {
        int r = Interlocked.Decrement(ref _refs);
        Debug.Assert(r >= 0);
        return r == 0;
    }

    public int RefCount => Volatile.Read(ref _refs);

    // ------------------------------------------------------------------ 2PC

    private long _minPrepLog; // 0 = no committed 2PC data in this memtable

    /// <summary>
    /// Records that this memtable holds committed data whose prepared section is in WAL <paramref name="log"/>
    /// (MemTable::RefLogContainingPrepSection): that WAL must stay until the memtable is flushed.
    /// </summary>
    public void RefLogContainingPrepSection(ulong log)
    {
        long cur = Volatile.Read(ref _minPrepLog);
        while (cur == 0 || (ulong)cur > log)
        {
            long o = Interlocked.CompareExchange(ref _minPrepLog, (long)log, cur);
            if (o == cur) return;
            cur = o;
        }
    }

    /// <summary>MemTable::GetMinLogContainingPrepSection: 0 if none.</summary>
    public ulong MinLogContainingPrepSection => (ulong)Volatile.Read(ref _minPrepLog);

    // ------------------------------------------------------------------ writes

    /// <summary>
    /// Adds an entry. Must be serialized with other <see cref="Add"/> calls. Returns false if an entry with the same
    /// user key and sequence number already exists (upstream Status::TryAgain).
    /// </summary>
    public bool Add(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (!Insert(sequence, type, key, value, concurrent: false, out int encodedLen)) return false;
        if (_newestUdtSize != 0) MaybeUpdateNewestUdt(key);

        Interlocked.Increment(ref _numEntries);
        Interlocked.Add(ref _dataSize, encodedLen);
        if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp)
        {
            Interlocked.Increment(ref _numDeletes);
        }
        else if (type == EntryType.RangeDeletion)
        {
            Interlocked.Increment(ref _numRangeDeletes);
        }
        UpdateFlushState();
        AfterInsert(sequence, type);
        return true;
    }

    /// <summary>
    /// Adds an entry; may run concurrently with other <see cref="AddConcurrently"/> calls. Counters are accumulated in
    /// <paramref name="postProcessInfo"/> and must be applied with <see cref="BatchPostProcess"/>.
    /// </summary>
    public bool AddConcurrently(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref MemTablePostProcessInfo postProcessInfo)
    {
        if (!Insert(sequence, type, key, value, concurrent: true, out int encodedLen)) return false;
        if (_newestUdtSize != 0) MaybeUpdateNewestUdt(key);
        postProcessInfo.NumEntries++;
        postProcessInfo.DataSize += encodedLen;
        if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp)
        {
            postProcessInfo.NumDeletes++;
        }
        else if (type == EntryType.RangeDeletion)
        {
            postProcessInfo.NumRangeDeletes++;
        }
        AfterInsert(sequence, type);
        return true;
    }

    /// <summary>Applies counters accumulated by <see cref="AddConcurrently"/>. Port of MemTable::BatchPostProcess.</summary>
    public void BatchPostProcess(in MemTablePostProcessInfo info)
    {
        Interlocked.Add(ref _numEntries, info.NumEntries);
        Interlocked.Add(ref _dataSize, info.DataSize);
        if (info.NumDeletes != 0) Interlocked.Add(ref _numDeletes, info.NumDeletes);
        if (info.NumRangeDeletes > 0) Interlocked.Add(ref _numRangeDeletes, info.NumRangeDeletes);
        UpdateFlushState();
    }

    [SkipLocalsInit]
    private bool Insert(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool concurrent, out int encodedLen)
    {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        uint internalKeySize = (uint)key.Length + DbFormat.InternalKeyFooterSize;
        uint valSize = (uint)value.Length;
        long len = Coding.VarintLength(internalKeySize) + (long)internalKeySize + Coding.VarintLength(valSize) + valSize;
        if (len > int.MaxValue / 2) ThrowHelper.InvalidArgument("memtable entry too large");
        encodedLen = (int)len;
        InlineSkipList<MemTableKeyComparer> table = type == EntryType.RangeDeletion ? _rangeDelTable : _table;

        byte* buf = table.AllocateKey(encodedLen);
        var dst = new Span<byte>(buf, encodedLen);
        int p = Coding.EncodeVarint32(dst, internalKeySize);
        key.CopyTo(dst[p..]);
        p += key.Length;
        Coding.EncodeFixed64(dst[p..], DbFormat.PackSequenceAndType(sequence, type));
        p += DbFormat.InternalKeyFooterSize;
        p += Coding.EncodeVarint32(dst[p..], valSize);
        value.CopyTo(dst[p..]);
        Debug.Assert(p + value.Length == encodedLen);

        // Range deletions always insert concurrently-safe (they may race with other range-deletion writers).
        return concurrent || type == EntryType.RangeDeletion ? table.InsertConcurrently(buf) : table.Insert(buf);
    }

    /// <summary>Port of MemTable::MaybeUpdateNewestUDT: tracks the newest timestamp of the user keys added.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MaybeUpdateNewestUdt(ReadOnlySpan<byte> userKey)
    {
        if (userKey.Length < _newestUdtSize) return;
        ReadOnlySpan<byte> udt = userKey[^_newestUdtSize..];
        lock (_newestUdtLock!)
        {
            if (_hasNewestUdt && _ucmp.CompareTimestamp(udt, _newestUdt) <= 0) return;
            udt.CopyTo(_newestUdt);
            _hasNewestUdt = true;
        }
    }

    /// <summary>
    /// Port of MemTable::GetNewestUDT: a copy of the newest timestamp added, or null when the memtable holds no entry
    /// (or timestamps are persisted: only tracked with persist_user_defined_timestamps = false).
    /// </summary>
    public byte[]? GetNewestUdt()
    {
        if (_newestUdtSize == 0) return null;
        lock (_newestUdtLock!)
        {
            return _hasNewestUdt ? (byte[])_newestUdt!.Clone() : null;
        }
    }

    private void AfterInsert(ulong s, EntryType type)
    {
        // Atomically update first_seqno_ and earliest_seqno_.
        ulong cur = Volatile.Read(ref _firstSeqno);
        while (cur == 0 || s < cur)
        {
            ulong observed = Interlocked.CompareExchange(ref _firstSeqno, s, cur);
            if (observed == cur) break;
            cur = observed;
        }
        ulong curEarliest = Volatile.Read(ref _earliestSeqno);
        while (curEarliest == DbFormat.MaxSequenceNumber || s < curEarliest)
        {
            ulong observed = Interlocked.CompareExchange(ref _earliestSeqno, s, curEarliest);
            if (observed == curEarliest) break;
            curEarliest = observed;
        }
        if (type == EntryType.RangeDeletion)
        {
            // Invalidate the fragmented tombstone cache. Readers that loaded the old cache are fine: the new
            // sequence number is not published yet.
            Interlocked.Increment(ref _rangeDelVersion);
            Volatile.Write(ref _isRangeDelTableEmpty, 0);
        }
    }

    // ------------------------------------------------------------------ reads

    /// <summary>
    /// Point lookup of <paramref name="key"/> at its sequence number. Port of MemTable::Get + SaveValue without merge
    /// operator / read callback / blob support: merge operands are pushed (newest first) to
    /// <paramref name="mergeContext"/> and the merge itself is left to the caller.
    /// </summary>
    /// <param name="key">Lookup key (user key + snapshot sequence).</param>
    /// <param name="value">For <see cref="MemTableGetResult.Found"/>: the value bytes (arena memory; valid while the memtable is alive).</param>
    /// <param name="valueType">Type of the entry that produced the result (Value, BlobIndex, WideColumnEntity, a deletion type, ...).</param>
    /// <param name="sequence">Sequence number of the newest visible entry for the key (or of the covering range tombstone), else <see cref="DbFormat.MaxSequenceNumber"/>.</param>
    /// <param name="mergeContext">Receives merge operands; when null, a merge entry yields <see cref="MemTableGetResult.MergeInProgress"/> immediately.</param>
    /// <param name="maxCoveringTombstoneSeq">In/out: the largest covering range-tombstone seqno seen so far (newer sources first).</param>
    public MemTableGetResult Get(scoped in LookupKey key, out ReadOnlySpan<byte> value, out EntryType valueType, out ulong sequence,
        MergeContext? mergeContext, ref ulong maxCoveringTombstoneSeq) =>
        Get(key, out value, out valueType, out sequence, mergeContext, ref maxCoveringTombstoneSeq, null);

    /// <inheritdoc cref="Get(in LookupKey, out ReadOnlySpan{byte}, out EntryType, out ulong, MergeContext?, ref ulong)"/>
    /// <param name="timestampSink">When non-null with a non-zero <see cref="GetContext.TimestampSize"/>, receives the
    /// timestamp of the first entry of the key that is matched (SaveValue, s->timestamp).</param>
    internal MemTableGetResult Get(scoped in LookupKey key, out ReadOnlySpan<byte> value, out EntryType valueType, out ulong sequence,
        MergeContext? mergeContext, ref ulong maxCoveringTombstoneSeq, GetContext? timestampSink)
    {
        value = default;
        valueType = EntryType.Value;
        sequence = DbFormat.MaxSequenceNumber;
        bool mergeInProgress = mergeContext is not null && mergeContext.Count > 0;
        if (IsEmpty) return mergeInProgress ? MemTableGetResult.MergeInProgress : MemTableGetResult.NotFound;

        ReadOnlySpan<byte> lookupUserKey = key.UserKey;
        ulong lookupSeq = key.Sequence;
        int tsSize = _ucmp.TimestampSize;
        if (Volatile.Read(ref _isRangeDelTableEmpty) == 0)
        {
            // With user-defined timestamps the lookup key carries the read timestamp, which bounds the visible
            // tombstones (NewRangeTombstoneIterator(read_opts, ...) with read_opts.timestamp).
            ReadOnlySpan<byte> readTs = tsSize == 0 ? default : lookupUserKey[^tsSize..];
            ulong covering = GetFragmentedRangeTombstones().MaxCoveringTombstoneSeqnum(lookupUserKey, lookupSeq, readTs, out ReadOnlySpan<byte> tombstoneTs);
            if (covering > maxCoveringTombstoneSeq)
            {
                maxCoveringTombstoneSeq = covering;
                // Overwritten in SaveValue if there is a point key with a higher seqno.
                if (timestampSink is { TimestampSize: not 0 }) timestampSink.SetTimestampFromRangeTombstone(tombstoneTs);
            }
        }

        ReadOnlySpan<byte> ikey = key.InternalKey;
        var it = new InlineSkipList<MemTableKeyComparer>.Iterator(_table);
        fixed (byte* pk = ikey)
        {
            it.Seek(new SkipListKey(pk, ikey.Length));
        }
        for (; it.Valid; it.Next())
        {
            byte* entry = it.Key;
            SkipListKey ek = MemTableKeyComparer.DecodeEntryKey(entry);
            int userKeyLen = ek.Length - DbFormat.InternalKeyFooterSize;
            var entryUserKey = new ReadOnlySpan<byte>(ek.Ptr, userKeyLen);
            bool same = _bytewise ? entryUserKey.SequenceEqual(lookupUserKey) : _ucmp.EqualWithoutTimestamp(entryUserKey, lookupUserKey);
            if (!same) break;

            ulong tag = Coding.DecodeFixed64(new ReadOnlySpan<byte>(ek.Ptr + userKeyLen, DbFormat.InternalKeyFooterSize));
            ulong seq = tag >> 8;
            // With user-defined timestamps older timestamps sort after the lookup key whatever their seqno: entries
            // newer than the read snapshot are skipped (native returns NotFound for keys written after the snapshot).
            if (tsSize != 0 && seq > lookupSeq) continue;
            if (timestampSink is { TimestampSize: not 0 }) timestampSink.RecordTimestampOfUserKey(entryUserKey, seq);
            var type = (EntryType)(byte)tag;
            if (sequence == DbFormat.MaxSequenceNumber)
            {
                sequence = seq > maxCoveringTombstoneSeq ? seq : maxCoveringTombstoneSeq;
            }
            if (maxCoveringTombstoneSeq > seq &&
                type is EntryType.Value or EntryType.Merge or EntryType.BlobIndex or EntryType.WideColumnEntity or EntryType.Deletion
                    or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp or EntryType.ValuePreferredSeqno)
            {
                type = EntryType.RangeDeletion;
            }
            switch (type)
            {
                case EntryType.Value:
                case EntryType.BlobIndex:
                case EntryType.WideColumnEntity:
                    value = EntryValue(ek);
                    valueType = type;
                    return MemTableGetResult.Found;
                case EntryType.ValuePreferredSeqno:
                    // ParsePackedValueForValue: strip the trailing packed preferred seqno.
                    value = EntryValue(ek);
                    value = value[..^sizeof(ulong)];
                    valueType = EntryType.Value;
                    return MemTableGetResult.Found;
                case EntryType.Deletion:
                case EntryType.DeletionWithTimestamp:
                case EntryType.SingleDeletion:
                case EntryType.RangeDeletion:
                    valueType = type;
                    return MemTableGetResult.Deleted;
                case EntryType.Merge:
                    valueType = type;
                    if (mergeContext is null) return MemTableGetResult.MergeInProgress;
                    mergeContext.PushOperand(EntryValue(ek));
                    mergeInProgress = true;
                    continue;
                default:
                    valueType = type;
                    return MemTableGetResult.Corruption;
            }
        }
        // A covering range tombstone without a point entry here is reported through maxCoveringTombstoneSeq; the
        // caller applies it to older sources (as upstream).
        return mergeInProgress ? MemTableGetResult.MergeInProgress : MemTableGetResult.NotFound;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlySpan<byte> EntryValue(SkipListKey ikey)
    {
        byte* p = ikey.Ptr + ikey.Length;
        uint len = *p;
        if (len < 0x80) return new ReadOnlySpan<byte>(p + 1, (int)len);
        int n = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out len);
        return new ReadOnlySpan<byte>(p + n, (int)len);
    }

    /// <summary>
    /// Largest seqno &lt;= <paramref name="readSequence"/> of a range tombstone in this memtable covering
    /// <paramref name="userKey"/>, or 0.
    /// </summary>
    public ulong MaxCoveringTombstoneSeqnum(ReadOnlySpan<byte> userKey, ulong readSequence) =>
        Volatile.Read(ref _isRangeDelTableEmpty) != 0 ? 0 : GetFragmentedRangeTombstones().MaxCoveringTombstoneSeqnum(userKey, readSequence);

    /// <summary>
    /// The fragmented view of all range tombstones in this memtable (cached; rebuilt after new range deletions).
    /// Port of the cached_range_tombstone_ logic of MemTable::NewRangeTombstoneIteratorInternal.
    /// </summary>
    public FragmentedRangeTombstoneList GetFragmentedRangeTombstones()
    {
        int version = Volatile.Read(ref _rangeDelVersion);
        RangeDelCacheEntry? cache = Volatile.Read(ref _rangeDelCache);
        if (cache is not null && cache.Version == version) return cache.List;
        lock (_rangeDelLock)
        {
            version = Volatile.Read(ref _rangeDelVersion);
            cache = _rangeDelCache;
            if (cache is not null && cache.Version == version) return cache.List;
            // Every insert that incremented the version up to `version` is visible to this scan (the version is
            // bumped after the skiplist insert).
            var list = new List<FragmentedRangeTombstoneList.Tombstone>();
            MemTableIterator it = NewRangeTombstoneEntryIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                list.Add(new FragmentedRangeTombstoneList.Tombstone(it.UserKey.ToArray(), it.Value.ToArray(), it.Sequence));
            }
            FragmentedRangeTombstoneList built = FragmentedRangeTombstoneList.Build(_ucmp, list);
            Volatile.Write(ref _rangeDelCache, new RangeDelCacheEntry(version, built));
            return built;
        }
    }

    private sealed record RangeDelCacheEntry(int Version, FragmentedRangeTombstoneList List);

    /// <summary>Iterator over point entries (internal keys). Allocation-free; valid while the memtable is alive.</summary>
    public MemTableIterator NewIterator() => new(_table);

    /// <summary>Iterator over raw range-deletion entries: key = start internal key, value = end user key.</summary>
    public MemTableIterator NewRangeTombstoneEntryIterator() => new(_rangeDelTable);

    /// <summary>Estimated number of point entries in [startInternalKey, endInternalKey).</summary>
    public ulong ApproximateNumEntries(ReadOnlySpan<byte> startInternalKey, ReadOnlySpan<byte> endInternalKey)
    {
        fixed (byte* s = startInternalKey)
        fixed (byte* e = endInternalKey)
        {
            return _table.ApproximateNumEntries(new SkipListKey(s, startInternalKey.Length), new SkipListKey(e, endInternalKey.Length));
        }
    }

    // ------------------------------------------------------------------ stats / flush policy

    /// <summary>Approximate bytes used by the memtable (arena usage).</summary>
    public long ApproximateMemoryUsage
    {
        get
        {
            long usage = _arena.ApproximateMemoryUsage;
            Volatile.Write(ref _approximateMemoryUsage, usage);
            return usage;
        }
    }

    /// <summary>Last value computed by <see cref="ApproximateMemoryUsage"/> / <see cref="ShouldFlushNow"/> (cheap).</summary>
    public long ApproximateMemoryUsageFast => Volatile.Read(ref _approximateMemoryUsage);

    public long NumEntries => Volatile.Read(ref _numEntries);
    public long NumDeletes => Volatile.Read(ref _numDeletes);
    public long NumRangeDeletes => Volatile.Read(ref _numRangeDeletes);
    /// <summary>Total encoded bytes of all entries.</summary>
    public long DataSize => Volatile.Read(ref _dataSize);

    /// <summary>Sequence number of the first element added (0 if empty). In concurrent mode, the smallest.</summary>
    public ulong FirstSequenceNumber => Volatile.Read(ref _firstSeqno);

    /// <summary>Lower bound on the sequence numbers that are or will be in this memtable.</summary>
    public ulong EarliestSequenceNumber => Volatile.Read(ref _earliestSeqno);

    /// <summary>Sets the earliest seqno (used when a memtable is created before its first write's seqno is known).</summary>
    public void SetEarliestSequenceNumber(ulong value) => Volatile.Write(ref _earliestSeqno, value);

    public ulong CreationSequence => _creationSeq;

    public bool IsEmpty => Volatile.Read(ref _firstSeqno) == 0;

    /// <summary>Dynamically changes write_buffer_size (SetOptions).</summary>
    public void UpdateWriteBufferSize(long newWriteBufferSize)
    {
        if (Volatile.Read(ref _flushState) == FlushNotRequested) Volatile.Write(ref _writeBufferSize, newWriteBufferSize);
    }

    /// <summary>True once the memtable became full and nobody scheduled its flush yet.</summary>
    public bool ShouldScheduleFlush => Volatile.Read(ref _flushState) == FlushRequested;

    /// <summary>Claims the flush (REQUESTED -&gt; SCHEDULED); returns true for exactly one caller.</summary>
    public bool MarkFlushScheduled() => Interlocked.CompareExchange(ref _flushState, FlushScheduled, FlushRequested) == FlushRequested;

    private volatile bool _markedForFlush;

    /// <summary>Port of MemTable::ShouldFlushNow: decides whether the memtable is full.</summary>
    public bool ShouldFlushNow()
    {
        if (_markedForFlush) return true;
        // This is set if memtable_max_range_deletions is > 0, and that many range deletions are done.
        if (_maxRangeDeletions > 0 && Volatile.Read(ref _numRangeDeletes) >= _maxRangeDeletions) return true;

        long writeBufferSize = Volatile.Read(ref _writeBufferSize);
        // In a lot of times, we cannot allocate arena blocks that exactly matches the buffer size. Thus we have to
        // decide if we should over-allocate or under-allocate. This constant can be interpreted as: if we still have
        // more than "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over allocate one more block.
        const double kAllowOverAllocationRatio = 0.6;

        // If arena still have room for new block allocation, we can safely say it shouldn't flush.
        long allocatedMemory = _arena.MemoryAllocatedBytes;
        Volatile.Write(ref _approximateMemoryUsage, allocatedMemory);

        // if we can still allocate one more block without exceeding the over-allocation ratio, then we should not flush.
        if (allocatedMemory + _arenaBlockSize < writeBufferSize + _arenaBlockSize * kAllowOverAllocationRatio) return false;

        // if user keeps adding entries that exceeds write_buffer_size, we need to flush earlier even though we still
        // have much available memory left.
        if (allocatedMemory > writeBufferSize + _arenaBlockSize * kAllowOverAllocationRatio) return true;

        // In this code path, Arena has already allocated its "last block": stop when it is 3/4 full (see upstream).
        return _arena.AllocatedAndUnused < _arenaBlockSize / 4;
    }

    private void UpdateFlushState()
    {
        if (Volatile.Read(ref _flushState) == FlushNotRequested && ShouldFlushNow())
        {
            // ignore CAS failure, because that means somebody else requested a flush
            Interlocked.CompareExchange(ref _flushState, FlushRequested, FlushNotRequested);
        }
    }

    /// <summary>Marks the memtable to be flushed at the next opportunity, independent of its size (MarkForFlush).</summary>
    public void MarkForFlush()
    {
        _markedForFlush = true;
        UpdateFlushState();
    }

    /// <summary>Releases the arena. Callers must guarantee no reader still uses memtable memory (see <see cref="Unref"/>).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _arena.Dispose();
        Interlocked.Exchange(ref OnDisposed, null)?.Invoke();
    }

    /// <summary>
    /// Called once when the memtable is disposed (the last reference is gone): releases the blob direct write files it
    /// references (memtable-held protection of BlobFilePartitionManager).
    /// </summary>
    internal Action? OnDisposed;
}

/// <summary>
/// Allocation-free iterator over memtable entries, exposing internal keys and values as spans into arena memory.
/// Port of MemTableIterator (db/memtable.cc) for the skiplist rep. Valid while the memtable is alive. Not thread-safe
/// itself, but may run concurrently with memtable writes (it observes a consistent, growing set of entries).
/// </summary>
public unsafe struct MemTableIterator
{
    private InlineSkipList<MemTableKeyComparer>.Iterator _it;

    internal MemTableIterator(InlineSkipList<MemTableKeyComparer> list) => _it = new InlineSkipList<MemTableKeyComparer>.Iterator(list);

    public readonly bool Valid => _it.Valid;

    public void SeekToFirst() => _it.SeekToFirst();
    public void SeekToLast() => _it.SeekToLast();
    public void Next() => _it.Next();
    public void Prev() => _it.Prev();

    /// <summary>Positions at the first entry with internal key &gt;= <paramref name="internalKey"/>.</summary>
    public void Seek(ReadOnlySpan<byte> internalKey)
    {
        fixed (byte* p = internalKey) _it.Seek(new SkipListKey(p, internalKey.Length));
    }

    /// <summary>Positions at the last entry with internal key &lt;= <paramref name="internalKey"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> internalKey)
    {
        fixed (byte* p = internalKey) _it.SeekForPrev(new SkipListKey(p, internalKey.Length));
    }

    /// <summary>The current internal key. REQUIRES: Valid.</summary>
    public readonly ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => MemTableKeyComparer.DecodeEntryKey(_it.Key).Span;
    }

    /// <summary>The current user key. REQUIRES: Valid.</summary>
    public readonly ReadOnlySpan<byte> UserKey
    {
        get
        {
            SkipListKey k = MemTableKeyComparer.DecodeEntryKey(_it.Key);
            return new ReadOnlySpan<byte>(k.Ptr, k.Length - DbFormat.InternalKeyFooterSize);
        }
    }

    /// <summary>The packed (seq &lt;&lt; 8 | type) footer of the current key.</summary>
    public readonly ulong Footer
    {
        get
        {
            SkipListKey k = MemTableKeyComparer.DecodeEntryKey(_it.Key);
            return Coding.DecodeFixed64(new ReadOnlySpan<byte>(k.Ptr + k.Length - DbFormat.InternalKeyFooterSize, DbFormat.InternalKeyFooterSize));
        }
    }

    public readonly ulong Sequence => Footer >> 8;
    public readonly EntryType Type => (EntryType)(byte)Footer;

    /// <summary>The current value. REQUIRES: Valid.</summary>
    public readonly ReadOnlySpan<byte> Value
    {
        get
        {
            SkipListKey k = MemTableKeyComparer.DecodeEntryKey(_it.Key);
            byte* p = k.Ptr + k.Length;
            int n = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out uint len);
            return new ReadOnlySpan<byte>(p + n, (int)len);
        }
    }
}
