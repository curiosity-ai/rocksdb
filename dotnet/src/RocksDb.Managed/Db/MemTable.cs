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
/// <para><b>Entry format</b> (as upstream):
/// <c>varint32(internal_key_len) | user_key | fixed64(seq &lt;&lt; 8 | type) | varint32(value_len) | value | checksum</c>,
/// where checksum is memtable_protection_bytes_per_key (0, 1, 2, 4 or 8) bytes of the entry's ProtectionInfoKVOS64
/// (<see cref="KvChecksum"/>), verified by point lookups, iterators (and so flush) like upstream.
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
    private readonly AllocTracker? _memTracker;
    private readonly InlineSkipList<MemTableKeyComparer> _table;
    private readonly InlineSkipList<MemTableKeyComparer> _rangeDelTable;
    private readonly long _maxRangeDeletions;
    private readonly ulong _creationSeq;
    // newest_udt_ (persist_user_defined_timestamps = false only): the newest timestamp added, guarded by _newestUdtLock.
    private readonly int _newestUdtSize;
    private readonly byte[]? _newestUdt;
    private bool _hasNewestUdt;
    private readonly Lock? _newestUdtLock;
    // Memtable bloom (bloom_filter_): prefixes of prefix_extractor_ and/or whole user keys (without timestamp).
    private readonly DynamicBloom? _bloom;
    private readonly SliceTransform? _prefixExtractor;
    private readonly bool _wholeKeyFiltering;
    private readonly int _tsSize;
    // inplace_update_support: striped reader/writer spin locks (locks_), 0 = free, > 0 = readers, -1 = writer.
    private readonly int[]? _locks;
    private readonly InplaceUpdateCallback? _inplaceCallback;
    // memtable_insert_with_hint_prefix_extractor: per-prefix skiplist splice (insert_hints_), serial inserts only.
    private readonly SliceTransform? _insertHintExtractor;
    // moptions_.protection_bytes_per_key (memtable_protection_bytes_per_key): 0 = no per-entry checksum.
    private readonly int _protectionBytes;
    private Dictionary<ArenaSlice, nint>? _insertHints;
    [ThreadStatic] private static byte[]? t_inplaceValue;

    private int _refs;
    private long _writeBufferSize;
    private long _dataSize;
    private long _numEntries;
    private long _numDeletes;
    private long _numRangeDeletes;
    private long _approximateMemoryUsage;
    private ulong _firstSeqno;
    private ulong _earliestSeqno;
    // Unix seconds of the first insert (MemTable::oldest_key_time_); 0 = no insert yet (upstream: uint64 max).
    private long _oldestKeyTime;
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
        // Memtable memory is reported to the write buffer manager only when it limits memory or charges a cache.
        if (options.WriteBufferManager is { } wbm && (wbm.Enabled || wbm.CostToCache)) _memTracker = new AllocTracker(wbm);
        _arena = new ConcurrentArena(blockSize, _memTracker);
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

        _tsSize = _ucmp.TimestampSize;
        _prefixExtractor = options.PrefixExtractor;
        _wholeKeyFiltering = options.WholeKeyFiltering;
        // use bloom_filter_ for both whole key and prefix bloom filter
        if ((_prefixExtractor is not null || _wholeKeyFiltering) && options.PrefixBloomBits > 0)
            _bloom = new DynamicBloom(_arena, options.PrefixBloomBits, 6 /* hard coded 6 probes */);
        if (options.InplaceUpdateSupport)
        {
            _locks = new int[Math.Max(1, options.InplaceUpdateNumLocks)];
            _inplaceCallback = options.InplaceCallback;
        }
        _insertHintExtractor = options.InsertWithHintPrefixExtractor;
        _protectionBytes = (int)options.ProtectionBytesPerKey;
    }

    /// <summary>memtable_protection_bytes_per_key of this memtable (bytes of checksum after each entry).</summary>
    public int ProtectionBytesPerKey => _protectionBytes;

    /// <summary>The memtable bloom filter (memtable_prefix_bloom_size_ratio), or null.</summary>
    public DynamicBloom? BloomFilter => _bloom;

    /// <summary>Whether entries may be overwritten in place (inplace_update_support): values must be copied under a lock.</summary>
    public bool InplaceUpdateSupport => _locks is not null;

    /// <summary>inplace_callback captured when the memtable was created (null: plain in-place Update).</summary>
    public InplaceUpdateCallback? InplaceCallback => _inplaceCallback;

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
    public bool Add(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        Add(sequence, type, key, value, hasKvProtInfo: false, kvProtInfo: 0);

    /// <summary>
    /// <see cref="Add(ulong, EntryType, ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> with the entry's ProtectionInfoKVOS64
    /// from the write batch (MemTable::Add's kv_prot_info): it becomes the entry checksum and the encoded entry is
    /// verified against it (Corruption "ProtectionInfo mismatch" when the bytes written to the memtable differ).
    /// </summary>
    internal bool Add(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool hasKvProtInfo, ulong kvProtInfo)
    {
        if (!Insert(sequence, type, key, value, concurrent: false, out int encodedLen, hasKvProtInfo, kvProtInfo)) return false;
        if (_newestUdtSize != 0) MaybeUpdateNewestUdt(key);
        // Range deletions take the concurrent insert path upstream (AddConcurrently on the bloom too).
        if (_bloom is not null) AddToBloom(key, concurrent: type == EntryType.RangeDeletion);

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
    public bool AddConcurrently(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref MemTablePostProcessInfo postProcessInfo) =>
        AddConcurrently(sequence, type, key, value, ref postProcessInfo, hasKvProtInfo: false, kvProtInfo: 0);

    /// <summary>
    /// <see cref="AddConcurrently(ulong, EntryType, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ref MemTablePostProcessInfo)"/>
    /// with the entry's ProtectionInfoKVOS64 from the write batch: it becomes the entry checksum and the encoded entry
    /// is verified against it, exactly like the serial <see cref="Add(ulong, EntryType, ReadOnlySpan{byte}, ReadOnlySpan{byte}, bool, ulong)"/>.
    /// </summary>
    internal bool AddConcurrently(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref MemTablePostProcessInfo postProcessInfo,
        bool hasKvProtInfo, ulong kvProtInfo)
    {
        if (!Insert(sequence, type, key, value, concurrent: true, out int encodedLen, hasKvProtInfo, kvProtInfo)) return false;
        if (_newestUdtSize != 0) MaybeUpdateNewestUdt(key);
        if (_bloom is not null) AddToBloom(key, concurrent: true);
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
    private bool Insert(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool concurrent, out int encodedLen,
        bool hasKvProtInfo, ulong kvProtInfo)
    {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        //  checksum     : char[moptions_.protection_bytes_per_key]
        uint internalKeySize = (uint)key.Length + DbFormat.InternalKeyFooterSize;
        uint valSize = (uint)value.Length;
        long len = Coding.VarintLength(internalKeySize) + (long)internalKeySize + Coding.VarintLength(valSize) + valSize + _protectionBytes;
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
        Debug.Assert(p + value.Length + _protectionBytes == encodedLen);
        if (_protectionBytes != 0 || hasKvProtInfo) ProtectEncodedEntry(dst, p + value.Length, sequence, type, key, value, hasKvProtInfo, kvProtInfo);

        // Range deletions always insert concurrently-safe (they may race with other range-deletion writers).
        if (concurrent || type == EntryType.RangeDeletion) return table.InsertConcurrently(buf);
        // Extract prefix for insert with hint. Hints are for point key table (`table_`) only.
        if (_insertHintExtractor is { } hx)
        {
            int keyOffset = Coding.VarintLength(internalKeySize);
            var keySlice = new ReadOnlySpan<byte>(buf + keyOffset, key.Length);
            if (hx.InDomain(keySlice))
            {
                ReadOnlySpan<byte> prefix = hx.Transform(keySlice);
                // The prefix bytes live in the arena (the inserted entry), so they stay valid as the dictionary key.
                var slice = new ArenaSlice((byte*)Unsafe.AsPointer(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(prefix)), prefix.Length);
                ref nint hint = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(_insertHints ??= new Dictionary<ArenaSlice, nint>(), slice, out _);
                return table.InsertWithHint(buf, ref hint);
            }
        }
        return table.Insert(buf);
    }

    /// <summary>Length of the encoded entry at <paramref name="entry"/> including its checksum.</summary>
    private static int EncodedEntryLength(byte* entry, int protectionBytes)
    {
        SkipListKey ek = MemTableKeyComparer.DecodeEntryKey(entry);
        byte* p = ek.Ptr + ek.Length;
        int n = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out uint valueLen);
        return (int)(p - entry) + n + (int)valueLen + protectionBytes;
    }

    /// <summary>
    /// UpdateEntryChecksum + (with the batch's protection info) VerifyEncodedEntry of MemTable::Add: writes the checksum
    /// at <paramref name="checksumOffset"/> of the encoded entry <paramref name="dst"/> and verifies the encoded bytes.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ProtectEncodedEntry(Span<byte> dst, int checksumOffset, ulong sequence, EntryType type, ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value, bool hasKvProtInfo, ulong kvProtInfo)
    {
        if (_protectionBytes != 0)
        {
            ulong info = hasKvProtInfo ? kvProtInfo : KvChecksum.ProtectS(KvChecksum.ProtectKvo(key, value, type), sequence);
            KvChecksum.Encode(info, _protectionBytes, dst[checksumOffset..]);
        }
        if (hasKvProtInfo)
        {
            TestHooks.InvokeSpan("MemTable::Add:Encoded", dst[..checksumOffset]);
            VerifyEncodedEntry(dst[..checksumOffset], kvProtInfo);
        }
        TestHooks.InvokeSpan("MemTable::Add:BeforeReturn:Encoded", dst[..checksumOffset]);
    }

    /// <summary>Port of MemTable::VerifyEncodedEntry: strips the encoded entry's fields from its ProtectionInfoKVOS64.</summary>
    private void VerifyEncodedEntry(ReadOnlySpan<byte> encoded, ulong kvProtInfo)
    {
        if (!Coding.GetVarint32(ref encoded, out uint ikeyLen)) ThrowHelper.Corruption("Unable to parse internal key length");
        if (ikeyLen < DbFormat.InternalKeyFooterSize + (uint)_tsSize) ThrowHelper.Corruption("Internal key length too short");
        if (ikeyLen > (uint)encoded.Length) ThrowHelper.Corruption("Internal key length too long");
        int userKeyLen = (int)ikeyLen - DbFormat.InternalKeyFooterSize;
        ReadOnlySpan<byte> userKey = encoded[..userKeyLen];
        ulong packed = Coding.DecodeFixed64(encoded[userKeyLen..]);
        encoded = encoded[(int)ikeyLen..];
        if (!Coding.GetVarint32(ref encoded, out uint valueLen)) ThrowHelper.Corruption("Unable to parse value length");
        if (valueLen < (uint)encoded.Length) ThrowHelper.Corruption("Value length too short");
        if (valueLen > (uint)encoded.Length) ThrowHelper.Corruption("Value length too long");
        ulong info = KvChecksum.StripKvo(KvChecksum.StripS(kvProtInfo, packed >> 8), userKey, encoded, (EntryType)(byte)packed);
        KvChecksum.ThrowIfMismatch(info);
    }

    /// <summary>
    /// Port of MemTable::VerifyEntryChecksum: recomputes the checksum of the entry at <paramref name="entry"/> (as stored
    /// in the skiplist) and throws Corruption when it does not match the stored one.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void VerifyEntryChecksum(byte* entry, int protectionBytes)
    {
        SkipListKey ek = MemTableKeyComparer.DecodeEntryKey(entry);
        if (ek.Length < DbFormat.InternalKeyFooterSize) ThrowHelper.Corruption("Memtable entry internal key length too short.");
        int userKeyLen = ek.Length - DbFormat.InternalKeyFooterSize;
        var userKey = new ReadOnlySpan<byte>(ek.Ptr, userKeyLen);
        ulong packed = Coding.DecodeFixed64(new ReadOnlySpan<byte>(ek.Ptr + userKeyLen, DbFormat.InternalKeyFooterSize));
        byte* p = ek.Ptr + ek.Length;
        int n = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out uint valueLen);
        if (n <= 0) ThrowHelper.Corruption("Unable to parse internal key value");
        var value = new ReadOnlySpan<byte>(p + n, (int)valueLen);
        var checksum = new ReadOnlySpan<byte>(p + n + valueLen, protectionBytes);
        ulong info = KvChecksum.ProtectS(KvChecksum.ProtectKvo(userKey, value, (EntryType)(byte)packed), packed >> 8);
        if (!KvChecksum.Verify(info, protectionBytes, checksum)) ThrowHelper.Corruption(KvChecksum.MemtableEntryCorruptedMessage);
    }

    /// <summary>MemTable::Add's bloom update: the prefix (if in the extractor's domain) and/or the whole key, without timestamp.</summary>
    private void AddToBloom(ReadOnlySpan<byte> userKey, bool concurrent)
    {
        DynamicBloom bloom = _bloom!;
        ReadOnlySpan<byte> keyWithoutTs = _tsSize == 0 ? userKey : userKey[..^_tsSize];
        if (_prefixExtractor is { } pe && pe.InDomain(keyWithoutTs))
        {
            if (concurrent) bloom.AddConcurrently(pe.Transform(keyWithoutTs));
            else bloom.Add(pe.Transform(keyWithoutTs));
        }
        if (_wholeKeyFiltering)
        {
            if (concurrent) bloom.AddConcurrently(keyWithoutTs);
            else bloom.Add(keyWithoutTs);
        }
    }

    /// <summary>
    /// The memtable bloom check of MemTable::Get: false when the key (whole-key filtering) or its prefix is definitely
    /// absent. When both whole-key filtering and a prefix extractor are set, only the whole key is checked for Get.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool BloomMayContain(ReadOnlySpan<byte> userKey)
    {
        ReadOnlySpan<byte> keyWithoutTs = _tsSize == 0 ? userKey : userKey[..^_tsSize];
        if (_wholeKeyFiltering) return _bloom!.MayContain(keyWithoutTs);
        SliceTransform pe = _prefixExtractor!;
        return !pe.InDomain(keyWithoutTs) || _bloom!.MayContain(pe.Transform(keyWithoutTs));
    }

    // ------------------------------------------------------------------ in-place updates

    /// <summary>Result of <see cref="UpdateCallback"/>.</summary>
    internal enum UpdateCallbackResult : byte
    {
        /// <summary>The memtable handled the write (updated in place, added a merged value, or no update).</summary>
        Done,
        /// <summary>The newest entry of the key is not a value in this memtable (Status::NotFound).</summary>
        NotFound,
        /// <summary>The merged value could not be added (key+seq exists).</summary>
        AddFailed,
    }

    private int LockIndex(ReadOnlySpan<byte> userKey) => (int)Hash.FastRange32(Hash.Hash32(userKey, 0), (uint)_locks!.Length);

    private void ReadLock(int i)
    {
        var sw = new SpinWait();
        while (true)
        {
            int s = Volatile.Read(ref _locks![i]);
            if (s >= 0 && Interlocked.CompareExchange(ref _locks[i], s + 1, s) == s) return;
            sw.SpinOnce();
        }
    }

    private void ReadUnlock(int i) => Interlocked.Decrement(ref _locks![i]);

    private void WriteLock(int i)
    {
        var sw = new SpinWait();
        while (Interlocked.CompareExchange(ref _locks![i], -1, 0) != 0) sw.SpinOnce();
    }

    private void WriteUnlock(int i) => Volatile.Write(ref _locks![i], 0);

    /// <summary>
    /// Finds the newest entry of <paramref name="userKey"/> visible at <paramref name="sequence"/> (iter-&gt;Seek of a
    /// LookupKey): the entry's internal key and the pointer to its value-length varint, or false when the newest entry has
    /// another user key.
    /// </summary>
    [SkipLocalsInit]
    private bool FindLatestEntry(ReadOnlySpan<byte> userKey, ulong sequence, out SkipListKey entryKey)
    {
        int n = userKey.Length + DbFormat.InternalKeyFooterSize;
        byte[]? rented = null;
        Span<byte> ikey = n <= 256 ? stackalloc byte[n] : (rented = RocksArrayPool<byte>.Shared.Rent(n)).AsSpan(0, n);
        try
        {
            userKey.CopyTo(ikey);
            Coding.EncodeFixed64(ikey[userKey.Length..], DbFormat.PackSequenceAndType(sequence, DbFormat.ValueTypeForSeek));
            var it = new InlineSkipList<MemTableKeyComparer>.Iterator(_table);
            fixed (byte* pk = ikey) it.Seek(new SkipListKey(pk, n));
            entryKey = default;
            if (!it.Valid) return false;
            entryKey = MemTableKeyComparer.DecodeEntryKey(it.Key);
            // Check that it belongs to same user key. We do not check the sequence number since the Seek() call above
            // should have skipped all entries with overly large sequence numbers.
            return _ucmp.Equal(new ReadOnlySpan<byte>(entryKey.Ptr, entryKey.Length - DbFormat.InternalKeyFooterSize), userKey);
        }
        finally
        {
            if (rented is not null) RocksArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Port of MemTable::Update (inplace_update_support): if the newest entry of the key is of the same
    /// <paramref name="type"/> and its value is at least as large as <paramref name="value"/>, the value is overwritten in
    /// place under the key's write lock (the existing sequence number prevails); otherwise the entry is added. Returns
    /// false like <see cref="Add"/> when an entry with the same key and sequence number exists. Serial writers only.
    /// </summary>
    public bool Update(ulong sequence, EntryType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_locks is not null && FindLatestEntry(key, sequence, out SkipListKey ek))
        {
            ulong tag = Coding.DecodeFixed64(new ReadOnlySpan<byte>(ek.Ptr + ek.Length - DbFormat.InternalKeyFooterSize, DbFormat.InternalKeyFooterSize));
            if ((EntryType)(byte)tag == type)
            {
                byte* p = ek.Ptr + ek.Length;
                int lenBytes = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out uint prevSize);
                // Update value, if new value size <= previous value size
                if ((uint)value.Length <= prevSize)
                {
                    int lockIndex = LockIndex(key);
                    WriteLock(lockIndex);
                    try
                    {
                        int w = Coding.EncodeVarint32(new Span<byte>(p, lenBytes), (uint)value.Length);
                        value.CopyTo(new Span<byte>(p + w, value.Length));
                        // UpdateEntryChecksum(nullptr, key, value, type, existing_seq, p + value.size()).
                        if (_protectionBytes != 0)
                            KvChecksum.Encode(KvChecksum.ProtectS(KvChecksum.ProtectKvo(key, value, type), tag >> 8), _protectionBytes,
                                new Span<byte>(p + w + value.Length, _protectionBytes));
                    }
                    finally
                    {
                        WriteUnlock(lockIndex);
                    }
                    return true;
                }
            }
        }
        // The latest value is not value_type or key doesn't exist
        return Add(sequence, type, key, value);
    }

    /// <summary>
    /// Port of MemTable::UpdateCallback (inplace_update_support with inplace_callback): when the newest entry of the key
    /// is a value, the callback runs on it under the key's write lock and either updates it in place, produces a merged
    /// value (added as a new entry) or does nothing. <see cref="UpdateCallbackResult.NotFound"/> when the key has no
    /// value entry here (the caller then reads the DB). Serial writers only.
    /// </summary>
    internal UpdateCallbackResult UpdateCallback(ulong sequence, ReadOnlySpan<byte> key, ReadOnlySpan<byte> delta, InplaceUpdateCallback callback)
    {
        if (_locks is null || !FindLatestEntry(key, sequence, out SkipListKey ek)) return UpdateCallbackResult.NotFound;
        ulong tag = Coding.DecodeFixed64(new ReadOnlySpan<byte>(ek.Ptr + ek.Length - DbFormat.InternalKeyFooterSize, DbFormat.InternalKeyFooterSize));
        if ((EntryType)(byte)tag != EntryType.Value) return UpdateCallbackResult.NotFound;
        byte* p = ek.Ptr + ek.Length;
        int lenBytes = Coding.DecodeVarint32(new ReadOnlySpan<byte>(p, Coding.MaxVarint32Length), out uint prevSizeU);
        int prevSize = (int)prevSizeU;
        byte* prevBuffer = p + lenBytes;
        int newPrevSize = prevSize;
        int lockIndex = LockIndex(key);
        using var merged = new PooledBufferWriter(0);
        WriteLock(lockIndex);
        try
        {
            InplaceUpdateStatus status = callback(new Span<byte>(prevBuffer, prevSize), true, ref newPrevSize, delta, merged);
            if (status == InplaceUpdateStatus.UpdatedInplace)
            {
                // Value already updated by callback.
                if ((uint)newPrevSize > (uint)prevSize) ThrowHelper.InvalidArgument("inplace_callback grew the value in place");
                if (newPrevSize < prevSize)
                {
                    // overwrite the new prev_size
                    int w = Coding.EncodeVarint32(new Span<byte>(p, lenBytes), (uint)newPrevSize);
                    // shift the value buffer as well.
                    if (w < lenBytes) Buffer.MemoryCopy(prevBuffer, p + w, newPrevSize, newPrevSize);
                    prevBuffer = p + w;
                }
                // UpdateEntryChecksum(nullptr, key, new_value, type, existing_seq, prev_buffer + new_prev_size).
                if (_protectionBytes != 0)
                    KvChecksum.Encode(KvChecksum.ProtectS(KvChecksum.ProtectKvo(key, new ReadOnlySpan<byte>(prevBuffer, newPrevSize), EntryType.Value), tag >> 8),
                        _protectionBytes, new Span<byte>(prevBuffer + newPrevSize, _protectionBytes));
                UpdateFlushState();
                return UpdateCallbackResult.Done;
            }
            if (status == InplaceUpdateStatus.Updated)
            {
                bool ok = Add(sequence, EntryType.Value, key, merged.WrittenSpan);
                UpdateFlushState();
                return ok ? UpdateCallbackResult.Done : UpdateCallbackResult.AddFailed;
            }
            // `UPDATE_FAILED` is named incorrectly. It indicates no update happened. It does not indicate a failure.
            UpdateFlushState();
            return UpdateCallbackResult.Done;
        }
        finally
        {
            WriteUnlock(lockIndex);
        }
    }

    /// <summary>
    /// SaveValue's read lock with inplace_update_support: copies the value of <paramref name="ek"/> under the key's read
    /// lock into a per-thread buffer (valid until the next memtable read on this thread).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ReadOnlySpan<byte> CopyValueLocked(SkipListKey ek, ReadOnlySpan<byte> userKey)
    {
        int lockIndex = LockIndex(userKey);
        ReadLock(lockIndex);
        try
        {
            ReadOnlySpan<byte> v = EntryValue(ek);
            byte[] buf = t_inplaceValue is { } b && b.Length >= v.Length ? b : (t_inplaceValue = new byte[Math.Max(64, (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)v.Length))]);
            v.CopyTo(buf);
            return new ReadOnlySpan<byte>(buf, 0, v.Length);
        }
        finally
        {
            ReadUnlock(lockIndex);
        }
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
        // MemTable::UpdateOldestKeyTime: set once, by the first insert. One relaxed load per insert afterwards.
        if (Volatile.Read(ref _oldestKeyTime) == 0) UpdateOldestKeyTime();
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
        MergeContext? mergeContext, ref ulong maxCoveringTombstoneSeq, GetContext? timestampSink) =>
        Get(key, out value, out valueType, out sequence, mergeContext, ref maxCoveringTombstoneSeq, timestampSink, null);

    /// <inheritdoc cref="Get(in LookupKey, out ReadOnlySpan{byte}, out EntryType, out ulong, MergeContext?, ref ulong)"/>
    /// <param name="callback">Read callback (SaveValue's CheckCallback): entries it reports as invisible are skipped.</param>
    internal MemTableGetResult Get(scoped in LookupKey key, out ReadOnlySpan<byte> value, out EntryType valueType, out ulong sequence,
        MergeContext? mergeContext, ref ulong maxCoveringTombstoneSeq, GetContext? timestampSink, ReadCallback? callback) =>
        Get(key, out value, out valueType, out sequence, mergeContext, ref maxCoveringTombstoneSeq, timestampSink, callback, ignoreRangeDeletions: false);

    /// <inheritdoc cref="Get(in LookupKey, out ReadOnlySpan{byte}, out EntryType, out ulong, MergeContext?, ref ulong)"/>
    /// <param name="ignoreRangeDeletions">ReadOptions::ignore_range_deletions: this memtable's range tombstones are not consulted (MemTable::Get).</param>
    internal MemTableGetResult Get(scoped in LookupKey key, out ReadOnlySpan<byte> value, out EntryType valueType, out ulong sequence,
        MergeContext? mergeContext, ref ulong maxCoveringTombstoneSeq, GetContext? timestampSink, ReadCallback? callback, bool ignoreRangeDeletions)
    {
        value = default;
        valueType = EntryType.Value;
        sequence = DbFormat.MaxSequenceNumber;
        bool mergeInProgress = mergeContext is not null && mergeContext.Count > 0;
        if (IsEmpty) return mergeInProgress ? MemTableGetResult.MergeInProgress : MemTableGetResult.NotFound;

        ReadOnlySpan<byte> lookupUserKey = key.UserKey;
        ulong lookupSeq = key.Sequence;
        int tsSize = _ucmp.TimestampSize;
        if (!ignoreRangeDeletions && Volatile.Read(ref _isRangeDelTableEmpty) == 0)
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

        // iter is null if prefix bloom says the key does not exist (sequence stays kMaxSequenceNumber).
        if (_bloom is not null && !BloomMayContain(lookupUserKey))
            return mergeInProgress ? MemTableGetResult.MergeInProgress : MemTableGetResult.NotFound;

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
            // SaveValue (db/memtable.cc) verifies the entry before the visibility check: a corrupted entry of the key
            // fails the lookup even when the read would skip it.
            if (_protectionBytes != 0)
            {
                TestHooks.InvokeSpan("Memtable::SaveValue:Found:entry", new Span<byte>(entry, EncodedEntryLength(entry, _protectionBytes)));
                // Memtable entry corrupted: Corruption ends the lookup (found_final_value).
                VerifyEntryChecksum(entry, _protectionBytes);
            }
            if (callback is not null && !callback.IsVisible(seq)) continue; // CheckCallback: to continue to the next seq
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
                    value = _locks is null ? EntryValue(ek) : CopyValueLocked(ek, entryUserKey);
                    valueType = type;
                    return MemTableGetResult.Found;
                case EntryType.ValuePreferredSeqno:
                    // ParsePackedValueForValue: strip the trailing packed preferred seqno. TimedPut entries are updated in
                    // place too (MemTable::Update with kTypeValuePreferredSeqno), so they are read under the same lock.
                    value = _locks is null ? EntryValue(ek) : CopyValueLocked(ek, entryUserKey);
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
            FragmentedRangeTombstoneList built;
            RangeTombstoneFragmenter frag = RangeTombstoneFragmenter.Rent(_ucmp, endIncludesTimestamp: true);
            try
            {
                MemTableIterator it = NewRangeTombstoneEntryIterator();
                for (it.SeekToFirst(); it.Valid; it.Next()) frag.Add(it.UserKey, it.Value, it.Sequence);
                built = frag.Finish();
            }
            finally
            {
                RangeTombstoneFragmenter.Return(frag);
            }
            Volatile.Write(ref _rangeDelCache, new RangeDelCacheEntry(version, built));
            return built;
        }
    }

    private sealed record RangeDelCacheEntry(int Version, FragmentedRangeTombstoneList List);

    /// <summary>Iterator over point entries (internal keys). Allocation-free; valid while the memtable is alive.</summary>
    public MemTableIterator NewIterator() => new(_table, _protectionBytes);

    /// <summary>
    /// Iterator over point entries that, when <paramref name="usePrefixBloom"/> and the memtable has a bloom filter, skips
    /// Seek / SeekForPrev targets whose prefix the bloom excludes (MemTableIterator in prefix mode: prefix_same_as_start,
    /// or neither total_order_seek nor auto_prefix_mode, with the column family's own prefix extractor).
    /// </summary>
    public MemTableIterator NewIterator(bool usePrefixBloom) =>
        usePrefixBloom && _bloom is not null && _prefixExtractor is not null
            ? new(_table, _bloom, _prefixExtractor, _tsSize, _protectionBytes)
            : new(_table, _protectionBytes);

    /// <summary>
    /// Whether a DB iterator with <paramref name="readOptions"/> and the column family extractor
    /// <paramref name="cfPrefixExtractor"/> uses the prefix bloom (the MemTableIterator constructor's condition).
    /// </summary>
    public bool UsesPrefixBloom(ReadOptions? readOptions, SliceTransform? cfPrefixExtractor) =>
        _bloom is not null && _prefixExtractor is not null && ReferenceEquals(_prefixExtractor, cfPrefixExtractor)
        && (readOptions?.PrefixSameAsStart == true || (readOptions?.TotalOrderSeek != true && readOptions?.AutoPrefixMode != true));

    /// <summary>Iterator over raw range-deletion entries: key = start internal key, value = end user key.</summary>
    public MemTableIterator NewRangeTombstoneEntryIterator() => new(_rangeDelTable, _protectionBytes);

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

    /// <summary>
    /// Port of MemTable::ApproximateOldestKeyTime: unix seconds of the first insert into this memtable, or
    /// <see cref="ulong.MaxValue"/> if nothing was inserted yet (upstream's initial value).
    /// </summary>
    public ulong ApproximateOldestKeyTime
    {
        get
        {
            long t = Volatile.Read(ref _oldestKeyTime);
            return t == 0 ? ulong.MaxValue : (ulong)t;
        }
    }

    /// <summary>Port of MemTable::UpdateOldestKeyTime (kept out of line: runs once per memtable).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void UpdateOldestKeyTime()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        // If the exchange fails, a concurrent writer already set the time.
        if (now > 0) Interlocked.CompareExchange(ref _oldestKeyTime, now, 0);
    }

    /// <summary>Lower bound on the sequence numbers that are or will be in this memtable.</summary>
    public ulong EarliestSequenceNumber => Volatile.Read(ref _earliestSeqno);

    /// <summary>Sets the earliest seqno (used when a memtable is created before its first write's seqno is known).</summary>
    public void SetEarliestSequenceNumber(ulong value) => Volatile.Write(ref _earliestSeqno, value);

    public ulong CreationSequence => _creationSeq;

    public bool IsEmpty => Volatile.Read(ref _firstSeqno) == 0;

    /// <summary>
    /// Dynamically changes write_buffer_size (SetOptions). Port of MemTable::UpdateWriteBufferSize: a memtable with a
    /// bloom filter (sized for the write buffer size it was created with) only ever shrinks.
    /// </summary>
    public void UpdateWriteBufferSize(long newWriteBufferSize)
    {
        if (_bloom is null || newWriteBufferSize < Volatile.Read(ref _writeBufferSize)) Volatile.Write(ref _writeBufferSize, newWriteBufferSize);
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

    /// <summary>
    /// The memtable no longer accepts writes (MemTable::MarkImmutable): its memory is scheduled to be freed in the write
    /// buffer manager (no longer counted as mutable).
    /// </summary>
    public void MarkImmutable() => _memTracker?.DoneAllocating();

    /// <summary>Memory reported to the write buffer manager (WBMTrackedMemoryUsage); 0 when not tracked.</summary>
    internal long WriteBufferManagerTrackedMemory => _memTracker?.BytesAllocated ?? 0;

    /// <summary>Releases the arena. Callers must guarantee no reader still uses memtable memory (see <see cref="Unref"/>).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _arena.Dispose();
        _memTracker?.FreeMem();
        Interlocked.Exchange(ref OnDisposed, null)?.Invoke();
        FlushedPinnedSet?.Remove(this);
    }

    /// <summary>
    /// The column family's set of flushed memtables still referenced (by iterators' super versions) that this memtable
    /// was added to after its flush; it leaves the set when disposed. Guarded by the DB mutex (Dispose of a referenced
    /// flushed memtable runs in CleanupSuperVersion, under the mutex).
    /// </summary>
    internal HashSet<MemTable>? FlushedPinnedSet;

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
    // Prefix mode (bloom_): Seek/SeekForPrev targets whose prefix the memtable bloom excludes leave the iterator invalid.
    private readonly DynamicBloom? _bloom;
    private readonly SliceTransform? _prefixExtractor;
    private readonly int _tsSize;
    // memtable_protection_bytes_per_key: every position is verified (MemTableIterator::VerifyEntryChecksum), a corrupted
    // entry throws Corruption (upstream: invalid iterator with a Corruption status).
    private readonly int _protectionBytes;

    internal MemTableIterator(InlineSkipList<MemTableKeyComparer> list, int protectionBytes)
    {
        _it = new InlineSkipList<MemTableKeyComparer>.Iterator(list);
        _bloom = null;
        _prefixExtractor = null;
        _tsSize = 0;
        _protectionBytes = protectionBytes;
    }

    internal MemTableIterator(InlineSkipList<MemTableKeyComparer> list, DynamicBloom bloom, SliceTransform prefixExtractor, int tsSize, int protectionBytes)
    {
        _it = new InlineSkipList<MemTableKeyComparer>.Iterator(list);
        _bloom = bloom;
        _prefixExtractor = prefixExtractor;
        _tsSize = tsSize;
        _protectionBytes = protectionBytes;
    }

    public readonly bool Valid => _it.Valid;

    public void SeekToFirst()
    {
        _it.SeekToFirst();
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    public void SeekToLast()
    {
        _it.SeekToLast();
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    public void Next()
    {
        _it.Next();
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    public void Prev()
    {
        _it.Prev();
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    private readonly void VerifyEntryChecksum()
    {
        if (_it.Valid) MemTable.VerifyEntryChecksum(_it.Key, _protectionBytes);
    }

    /// <summary>Positions at the first entry with internal key &gt;= <paramref name="internalKey"/>.</summary>
    public void Seek(ReadOnlySpan<byte> internalKey)
    {
        if (_bloom is not null && PrefixExcluded(internalKey))
        {
            _it.Invalidate();
            return;
        }
        fixed (byte* p = internalKey) _it.Seek(new SkipListKey(p, internalKey.Length));
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    /// <summary>Positions at the last entry with internal key &lt;= <paramref name="internalKey"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> internalKey)
    {
        if (_bloom is not null && PrefixExcluded(internalKey))
        {
            _it.Invalidate();
            return;
        }
        fixed (byte* p = internalKey) _it.SeekForPrev(new SkipListKey(p, internalKey.Length));
        if (_protectionBytes != 0) VerifyEntryChecksum();
    }

    /// <summary>The iterator only uses the prefix bloom filter (never the whole key).</summary>
    private readonly bool PrefixExcluded(ReadOnlySpan<byte> internalKey)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        ReadOnlySpan<byte> keyWithoutTs = _tsSize == 0 ? userKey : userKey[..^_tsSize];
        return _prefixExtractor!.InDomain(keyWithoutTs) && !_bloom!.MayContain(_prefixExtractor.Transform(keyWithoutTs));
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
