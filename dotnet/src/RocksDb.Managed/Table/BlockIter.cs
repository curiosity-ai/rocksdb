using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Table;

/// <summary>Which flavour of block the iterator decodes (DataBlockIter / IndexBlockIter / MetaBlockIter).</summary>
public enum BlockIterKind : byte
{
    /// <summary>Data / range-deletion blocks: internal keys, raw values.</summary>
    Data,
    /// <summary>Index blocks: internal or user keys, values decoded as <see cref="BlockHandle"/> (+ first key).</summary>
    Index,
    /// <summary>Metaindex / properties blocks: raw byte-ordered keys, raw values.</summary>
    Meta,
}

/// <summary>
/// Reusable, allocation-free iterator over a <see cref="Block"/>. Port of BlockIter / DataBlockIter /
/// IndexBlockIter / MetaBlockIter (table/block_based/block.{h,cc}, block_util.h).
/// <para>
/// The iterator does not own the block: callers keep the block alive (reference) while iterating. Keys that are
/// stored whole in the block (restart points) are exposed directly from the block buffer; delta-encoded keys are
/// materialized into an internal pooled key buffer. <see cref="Key"/>/<see cref="Value"/> spans are only valid until
/// the next positioning call. Not thread-safe.
/// </para>
/// <para>Corrupted block contents surface as <see cref="RocksDbException"/> with <see cref="StatusCode.Corruption"/>.</para>
/// </summary>
[SkipLocalsInit]
public sealed class BlockIter : IDisposable
{
    /// <summary>kDisableGlobalSequenceNumber.</summary>
    public const ulong DisableGlobalSequenceNumber = ulong.MaxValue;

    private byte[] _data = [];
    private int _restarts;      // offset of restart array
    private int _keysEnd;       // end of the entries (keys) section: == _restarts unless key/value are separated
    private int _valuesSection = -1; // separated key/value storage: start of the values section (values end at _restarts)
    private int _numRestarts;
    private int _current;       // offset of the current entry; == _keysEnd when invalid
    private int _next;          // offset just past the current entry (incl. value)
    private int _restartIndex;  // restart block containing _current
    private int _hashMapOffset = -1;
    private int _hashNumBuckets;
    private int _prefixLength;     // common user-key prefix stored at [0, _prefixLength) (format_version >= 8)
    private byte[] _restartKeyBuf = [];
    private byte[] _restartPadBuf = [];

    private byte[] _keyBuf = [];
    private int _keyLen;

    // Prev cache (data / meta blocks): entries of the restart interval scanned by the last uncached Prev.
    private PrevEntry[] _prevEntries = [];
    private int _prevCount;
    private int _prevIdx = -1;
    private byte[] _prevKeyBuf = [];
    private int _prevKeyBytes;
    private bool _keyInBuf;
    private int _keyBlockOffset;

    private int _valueOffset;
    private int _valueLength;

    // Index-specific
    private BlockHandle _handle;
    private int _firstKeyOffset;
    private int _firstKeyLength;

    private BlockIterKind _kind;
    private InternalKeyComparator _icmp = InternalKeyComparator.Bytewise;
    private bool _keysAreInternal;
    private bool _valueDeltaEncoded;
    private bool _valueDeltaEscape;
    private bool _haveFirstKey;
    private ulong _globalSeqno = DisableGlobalSequenceNumber;

    // persist_user_defined_timestamps = false (pad_min_timestamp_): keys are stored without their user-defined timestamp
    // of _padTsSize bytes; the stored ("raw") key is kept in _rawKeyBuf as the delta base and the exposed key in _keyBuf
    // carries the minimum timestamp (UpdateRawKeyAndMaybePadMinTimestamp / TrimAppendWithTimestamp).
    private int _padTsSize;
    private byte[] _rawKeyBuf = [];
    private int _rawKeyLen;
    private byte[] _firstKeyPadBuf = [];
    private int _firstKeyPadLen;
    // Entries need the generic decoder (value delta encoding or timestamp padding).
    private bool _slowParse;
    // Restart keys need rewriting before comparisons (global seqno or timestamp padding).
    private bool _restartKeyFixup;
    // block_protection_bytes_per_key of the block (0: no per key-value verification) and the block itself.
    private int _kvProtectionBytes;
    private Block? _block;
    // Index blocks: Seek finds the restart point by interpolation search (index_search_type_ == kInterpolation).
    private bool _interpolationSearch;

    /// <summary>Iterator for a data or range-deletion block (internal keys ordered by <paramref name="comparator"/>).</summary>
    public void InitializeData(Block block, InternalKeyComparator comparator, ulong globalSeqno)
    {
        Reset(block, BlockIterKind.Data, comparator);
        _keysAreInternal = true;
        _globalSeqno = globalSeqno;
        _hashMapOffset = block.HashMapOffset;
        _hashNumBuckets = block.HashNumBuckets;
        UpdateModes();
    }

    /// <summary>
    /// <see cref="InitializeData(Block, InternalKeyComparator, ulong)"/> for a block whose keys were stored without their
    /// user-defined timestamp of <paramref name="padTimestampSize"/> bytes (user_defined_timestamps_persisted = false):
    /// every key is exposed with the minimum timestamp. 0 behaves like the overload without it.
    /// </summary>
    public void InitializeData(Block block, InternalKeyComparator comparator, ulong globalSeqno, int padTimestampSize)
    {
        InitializeData(block, comparator, globalSeqno);
        if (padTimestampSize != 0) EnablePadding(padTimestampSize);
    }

    /// <summary>
    /// Iterator for an index block. <paramref name="keyIncludesSeq"/> = !index_key_is_user_key;
    /// <paramref name="valueIsFull"/> = !index_value_is_delta_encoded; <paramref name="haveFirstKey"/> for
    /// kBinarySearchWithFirstKey; <paramref name="valueDeltaEscape"/> for format_version &gt;= 8
    /// (FormatVersionUsesValueDeltaEscape).
    /// </summary>
    public void InitializeIndex(Block block, InternalKeyComparator comparator, bool keyIncludesSeq, bool valueIsFull, bool haveFirstKey,
        bool valueDeltaEscape, ulong globalSeqno)
    {
        Reset(block, BlockIterKind.Index, comparator);
        _keysAreInternal = keyIncludesSeq;
        _valueDeltaEncoded = !valueIsFull;
        _valueDeltaEscape = valueDeltaEscape;
        _haveFirstKey = haveFirstKey;
        _globalSeqno = globalSeqno;
        UpdateModes();
    }

    /// <summary>
    /// <see cref="InitializeIndex(Block, InternalKeyComparator, bool, bool, bool, bool, ulong)"/> for an index block whose
    /// keys (and first keys) were stored without their user-defined timestamp of <paramref name="padTimestampSize"/>
    /// bytes; they are exposed with the minimum timestamp. 0 behaves like the overload without it.
    /// </summary>
    public void InitializeIndex(Block block, InternalKeyComparator comparator, bool keyIncludesSeq, bool valueIsFull, bool haveFirstKey,
        bool valueDeltaEscape, ulong globalSeqno, int padTimestampSize)
    {
        InitializeIndex(block, comparator, keyIncludesSeq, valueIsFull, haveFirstKey, valueDeltaEscape, globalSeqno);
        if (padTimestampSize != 0) EnablePadding(padTimestampSize);
    }

    private void EnablePadding(int padTimestampSize)
    {
        // The common-prefix feature is never combined with stripped timestamps (BlockBuilder ctor assert).
        if (_prefixLength != 0) ThrowHelper.Corruption("block with a common key prefix cannot have stripped timestamps");
        _padTsSize = padTimestampSize;
        // The hash index hashes user keys with their timestamp (upstream TODO: make UDT work with the hash index).
        _hashMapOffset = -1;
        UpdateModes();
    }

    private void UpdateModes()
    {
        _slowParse = _valueDeltaEncoded || _padTsSize != 0 || _valuesSection >= 0;
        _restartKeyFixup = _padTsSize != 0 || (_globalSeqno != DisableGlobalSequenceNumber && _keysAreInternal);
    }

    /// <summary>
    /// Makes <see cref="Seek"/> on this index block use interpolation search (IndexBlockIter with index_search_type_ =
    /// kInterpolation, i.e. kInterpolation or a resolved kAuto). The caller guarantees the bytewise comparator. Like
    /// upstream, blocks with a stripped common key prefix (format_version &gt;= 8) keep binary search; so do blocks whose
    /// restart keys need rewriting before comparison (global seqno, stripped timestamps), which upstream would also
    /// interpolate (same seek result, only the probe sequence differs).
    /// </summary>
    public void UseInterpolationSearch()
    {
        _interpolationSearch = _kind == BlockIterKind.Index && _prefixLength == 0 && !_restartKeyFixup;
    }

    /// <summary>Whether <see cref="Seek"/> uses interpolation search (tests).</summary>
    internal bool UsesInterpolationSearch => _interpolationSearch;

    /// <summary>Iterator for a metaindex or properties block (bytewise ordered keys).</summary>
    public void InitializeMeta(Block block)
    {
        Reset(block, BlockIterKind.Meta, InternalKeyComparator.Bytewise);
        _keysAreInternal = false;
        UpdateModes();
    }

    private void Reset(Block block, BlockIterKind kind, InternalKeyComparator icmp)
    {
        _kind = kind;
        _block = block;
        _readAmp = kind == BlockIterKind.Data ? block.ReadAmp : null;
        _lastBitmapOffset = -1;
        _kvProtectionBytes = block.KvProtectionBytes;
        _data = block.Buffer;
        _keysEnd = block.KeysEndOffset;
        _valuesSection = block.ValuesSectionOffset;
        _restarts = block.RestartOffset;
        _numRestarts = block.NumRestarts;
        _prefixLength = block.CommonPrefixLength;
        _icmp = icmp;
        _hashMapOffset = -1;
        _hashNumBuckets = 0;
        _valueDeltaEncoded = false;
        _valueDeltaEscape = false;
        _haveFirstKey = false;
        _globalSeqno = DisableGlobalSequenceNumber;
        _padTsSize = 0;
        _rawKeyLen = 0;
        _slowParse = false;
        _restartKeyFixup = false;
        _interpolationSearch = false;
        _handle = default;
        _keyLen = 0;
        _keyInBuf = true;
        _prevIdx = -1;
        _prevCount = 0;
        Invalidate();
    }

    /// <summary>Detaches from the current block (does not release it).</summary>
    public void Clear()
    {
        _block = null;
        _kvProtectionBytes = 0;
        _data = [];
        _restarts = 0;
        _keysEnd = 0;
        _valuesSection = -1;
        _numRestarts = 0;
        _current = 0;
        _next = 0;
        _prevIdx = -1;
        _prevCount = 0;
    }

    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current < _keysEnd;
    }

    /// <summary>Current key (internal key for data blocks / internal-key index blocks).</summary>
    public ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _keyInBuf ? new ReadOnlySpan<byte>(_keyBuf, 0, _keyLen) : new ReadOnlySpan<byte>(_data, _keyBlockOffset, _keyLen);
    }

    /// <summary>Raw value (for index blocks: the encoded index value bytes).</summary>
    public ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        get
        {
            if (_readAmp is not null) MarkReadAmp();
            return new(_data, _valueOffset, _valueLength);
        }
    }

    // read_amp_bytes_per_bit: the data block's bitmap (null when off) and the entry last marked (last_bitmap_offset_).
    private BlockReadAmpBitmap? _readAmp;
    private int _lastBitmapOffset;

    /// <summary>
    /// DataBlockIter::value(): the first read of each entry's value marks [entry offset, NextEntryOffset() - 1] in
    /// the block's read-amp bitmap. Not tracked for blocks with separated key/value storage.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MarkReadAmp()
    {
        if (_current < _keysEnd && _current != _lastBitmapOffset && _valuesSection < 0)
        {
            _readAmp!.Mark((uint)_current, (uint)(_valueOffset + _valueLength - 1));
            _lastBitmapOffset = _current;
        }
    }

    /// <summary>Raw value as memory over the block buffer (valid while the block is alive).</summary>
    public ReadOnlyMemory<byte> ValueMemory => new(_data, _valueOffset, _valueLength);

    /// <summary>The value as a slice of the block buffer (valid while the block is alive).</summary>
    internal Db.Iterators.PinnedSlice PinnedValue => new(_data, _valueOffset, _valueLength);

    /// <summary>Index blocks: the decoded block handle of the current entry.</summary>
    public BlockHandle BlockHandle => _handle;

    /// <summary>Index blocks with kBinarySearchWithFirstKey: first internal key of the referenced data block.</summary>
    public ReadOnlySpan<byte> FirstInternalKey =>
        _padTsSize == 0 ? new(_data, _firstKeyOffset, _firstKeyLength) : new(_firstKeyPadBuf, 0, _firstKeyPadLen);

    /// <summary>Offset of the current entry's value within the block (port of ValueOffset, used for global seqno patching).</summary>
    public int ValueOffsetInBlock => _valueOffset;

    // ---------------------------------------------------------------------------------------------
    // Positioning

    public void SeekToFirst()
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        SeekToRestartPoint(0);
        ParseNextKey();
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    public void SeekToLast()
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        SeekToRestartPoint(_numRestarts - 1);
        while (ParseNextKey() && _next < _keysEnd)
        {
            // Keep skipping
        }
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Next()
    {
        System.Diagnostics.Debug.Assert(Valid);
        ParseNextKey();
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    /// <summary>
    /// Port of BlockIter::PrevImpl / DataBlockIter::PrevImpl: scans forward from the preceding restart point. For data
    /// and meta blocks the entries decoded by that scan are cached (DataBlockIter's prev_entries_), so a run of Prev
    /// calls decodes each restart interval once instead of once per entry.
    /// </summary>
    public void Prev()
    {
        PrevImpl();
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    private void PrevImpl()
    {
        System.Diagnostics.Debug.Assert(Valid);
        // The prev cache holds exposed keys; with timestamp padding the delta base differs, so it is not used.
        bool useCache = _kind != BlockIterKind.Index && _padTsSize == 0;
        if (useCache && _prevIdx > 0 && _prevEntries[_prevIdx].Offset == _current)
        {
            PrevFromCache();
            return;
        }
        _prevIdx = -1;
        _prevCount = 0;
        _prevKeyBytes = 0;
        int original = _current;
        while (GetRestartPoint(_restartIndex) >= (uint)original)
        {
            if (_restartIndex == 0)
            {
                // No more entries
                Invalidate();
                return;
            }
            _restartIndex--;
        }
        SeekToRestartPoint(_restartIndex);
        if (!useCache)
        {
            // Loop until end of current entry hits the start of original entry
            while (ParseNextKey() && _next < original)
            {
            }
            return;
        }
        while (ParseNextKey())
        {
            CachePrevEntry();
            if (_next >= original) break;
        }
        _prevIdx = _prevCount - 1;
    }

    private void CachePrevEntry()
    {
        if (_prevCount == _prevEntries.Length) Array.Resize(ref _prevEntries, Math.Max(16, _prevCount * 2));
        ref PrevEntry e = ref _prevEntries[_prevCount++];
        e.Offset = _current;
        e.Next = _next;
        e.ValueOffset = _valueOffset;
        e.ValueLength = _valueLength;
        e.KeyLength = _keyLen;
        e.KeyInBlock = !_keyInBuf;
        if (!_keyInBuf)
        {
            e.KeyOffset = _keyBlockOffset;
            return;
        }
        // Delta-encoded key: keep a copy (the key buffer is overwritten by the next entry).
        if (_prevKeyBuf.Length - _prevKeyBytes < _keyLen)
        {
            byte[] bigger = RocksArrayPool<byte>.Shared.Rent(Math.Max(256, (_prevKeyBytes + _keyLen) * 2));
            _prevKeyBuf.AsSpan(0, _prevKeyBytes).CopyTo(bigger);
            if (_prevKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_prevKeyBuf);
            _prevKeyBuf = bigger;
        }
        _keyBuf.AsSpan(0, _keyLen).CopyTo(_prevKeyBuf.AsSpan(_prevKeyBytes));
        e.KeyOffset = _prevKeyBytes;
        _prevKeyBytes += _keyLen;
    }

    private void PrevFromCache()
    {
        ref PrevEntry e = ref _prevEntries[--_prevIdx];
        _current = e.Offset;
        _next = e.Next;
        _valueOffset = e.ValueOffset;
        _valueLength = e.ValueLength;
        _keyLen = e.KeyLength;
        if (e.KeyInBlock)
        {
            _keyInBuf = false;
            _keyBlockOffset = e.KeyOffset;
        }
        else
        {
            // Copied back so that a following Next can delta-decode against it.
            if (_keyBuf.Length < e.KeyLength) EnsureKeyCapacity(e.KeyLength);
            _prevKeyBuf.AsSpan(e.KeyOffset, e.KeyLength).CopyTo(_keyBuf);
            _keyInBuf = true;
        }
    }

    /// <summary>An entry decoded by the restart-interval scan of <see cref="Prev"/> (CachedPrevEntry).</summary>
    private struct PrevEntry
    {
        public int Offset;
        public int Next;
        public int KeyOffset;   // block offset (KeyInBlock) or offset in _prevKeyBuf
        public int KeyLength;
        public int ValueOffset;
        public int ValueLength;
        public bool KeyInBlock;
    }

    /// <summary>Positions at the first entry with key &gt;= target.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        if (_kind == BlockIterKind.Index && !_keysAreInternal) target = DbFormat.ExtractUserKey(target);
        int index;
        bool skipLinearScan;
        bool ok = _interpolationSearch
            ? InterpolationSeekRestartPointIndex(target, out index, out skipLinearScan)
            : BinarySeekRestartPointIndex(target, out index, out skipLinearScan);
        if (!ok) { Invalidate(); return; }
        FindKeyAfterBinarySeek(target, index, skipLinearScan);
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    /// <summary>
    /// Port of IndexBlockIter::Seek with a prefix index (PrefixSeek + BinaryBlockIndexSeek, table/block_based/block.cc)
    /// for kHashSearch index blocks (restart interval 1, so a data block index is a restart index).
    /// <paramref name="blockIds"/> are the candidate blocks of the target's prefix (<see cref="BlockPrefixIndex.GetBlocks"/>).
    /// Leaves the iterator invalid when no key with the prefix can be &gt;= target (prefix absent); otherwise positions
    /// at the first index entry &gt;= target (possibly past the prefix, like a total-order seek).
    /// </summary>
    public void PrefixSeek(ReadOnlySpan<byte> target, ReadOnlySpan<uint> blockIds)
    {
        if (_numRestarts == 0 || blockIds.IsEmpty) { Invalidate(); return; }
        ReadOnlySpan<byte> seekKey = _kind == BlockIterKind.Index && !_keysAreInternal ? DbFormat.ExtractUserKey(target) : target;
        int left = 0, right = blockIds.Length - 1;
        int leftBound = left;
        while (left <= right)
        {
            int mid = (right + left) / 2;
            int cmp = CompareBlockKey(blockIds[mid], seekKey);
            if (cmp < 0)
            {
                // Key at "target" is larger than "mid". Therefore all blocks before or at "mid" are uninteresting.
                left = mid + 1;
            }
            else
            {
                // Key at "target" is <= "mid". Therefore all blocks after "mid" are uninteresting.
                // If there is only one block left, we found it.
                if (left == right) break;
                right = mid;
            }
        }
        int index;
        if (left == right)
        {
            // Either left is the first of block_ids or there is a gap of blocks between left and left - 1: the key does
            // not exist if the block to the left of the one found is already larger than the target.
            if (blockIds[left] > 0 && (left == leftBound || blockIds[left - 1] != blockIds[left] - 1)
                && CompareBlockKey(blockIds[left] - 1, seekKey) > 0)
            {
                Invalidate();
                return;
            }
            index = (int)blockIds[left];
        }
        else
        {
            // If the next block key is larger than the seek key, either no key shares the target's prefix or all of them
            // are smaller than the target; then position like a total-order seek on the next block (or stay invalid).
            uint rightIndex = blockIds[right];
            if (rightIndex + 1 < (uint)_numRestarts && CompareBlockKey(rightIndex + 1, seekKey) >= 0)
            {
                index = (int)rightIndex + 1;
            }
            else
            {
                Invalidate();
                return;
            }
        }
        // restart interval must be one when hash search is enabled so the binary search simply lands at the right place.
        FindKeyAfterBinarySeek(seekKey, index, skipLinearScan: true);
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    /// <summary>Port of IndexBlockIter::CompareBlockKey: compares the key of restart entry <paramref name="blockIndex"/> with the target.</summary>
    private int CompareBlockKey(uint blockIndex, ReadOnlySpan<byte> target)
    {
        if (blockIndex >= (uint)_numRestarts) ThrowHelper.Corruption("bad hash index block id");
        return CompareRestartKey(GetRestartKey((int)blockIndex), target);
    }

    /// <summary>Positions at the last entry with key &lt;= target (data and meta blocks).</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        if (!BinarySeekRestartPointIndex(target, out int index, out bool skipLinearScan)) { Invalidate(); return; }
        FindKeyAfterBinarySeek(target, index, skipLinearScan);
        if (!Valid)
        {
            SeekToLast();
        }
        else
        {
            while (Valid && CompareKey(Key, target) > 0) Prev();
        }
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
    }

    /// <summary>
    /// Port of DataBlockIter::SeekForGet: point-lookup seek for an internal <paramref name="target"/>, using the
    /// data block hash index when present. Returns false when the user key is definitely absent from this block
    /// AND cannot be in the next block; otherwise true with the iterator positioned as by <see cref="Seek"/>
    /// (possibly invalid, meaning "continue with the next block").
    /// </summary>
    public bool SeekForGet(ReadOnlySpan<byte> target)
    {
        if (_hashMapOffset < 0 || _globalSeqno != DisableGlobalSequenceNumber)
        {
            Seek(target);
            return true;
        }
        ReadOnlySpan<byte> targetUserKey = DbFormat.ExtractUserKey(target);
        byte entry = DataBlockHashIndex.Lookup(_data, _hashMapOffset, _hashNumBuckets, targetUserKey);
        if (entry == DataBlockHashIndex.Collision)
        {
            Seek(target);
            return true;
        }
        if (entry == DataBlockHashIndex.NoEntry)
        {
            // The key may still be in the next block; pretend it is in the last restart interval.
            entry = (byte)(_numRestarts - 1);
        }
        int restartIndex = entry;
        if (restartIndex >= _numRestarts) ThrowHelper.Corruption("bad data block hash index entry");
        SeekToRestartPoint(restartIndex);
        int cur = (int)GetRestartPoint(restartIndex);
        int limit = restartIndex + 1 < _numRestarts ? (int)GetRestartPoint(restartIndex + 1) : _keysEnd;
        while (cur < limit)
        {
            // Only linear-seek inside the restart interval; stop at the first potential match.
            if (!ParseNextKey() || CompareKey(Key, target) >= 0) break;
            cur = _current;
        }
        if (_current == _keysEnd) return true;
        if (_kvProtectionBytes != 0) VerifyKvChecksum();
        if (!UserKeyEquals(DbFormat.ExtractUserKey(Key), targetUserKey)) return false;
        EntryType t = (EntryType)(byte)DbFormat.ExtractFooter(Key);
        if (t != EntryType.Value && t != EntryType.Deletion && t != EntryType.Merge && t != EntryType.SingleDeletion
            && t != EntryType.BlobIndex && t != EntryType.WideColumnEntity && t != EntryType.ValuePreferredSeqno)
        {
            Seek(target);
        }
        return true;
    }

    // ---------------------------------------------------------------------------------------------
    // Per key-value checksum (block_protection_bytes_per_key)

    /// <summary>
    /// Port of BlockIter::UpdateKey's verification: the current entry's ProtectKV(key, value) must match the block's
    /// checksum of the entry; otherwise Corruption (upstream: invalid iterator with a Corruption status).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void VerifyKvChecksum()
    {
        if (!Valid) return;
        BlockKvProtection p = _block!.KvProtection ?? _block.PublishKvProtection(ComputeKvProtection());
        TestHooks.InvokeSpan("BlockIter::UpdateKey::value", _data.AsSpan(_valueOffset, _valueLength));
        int idx = Array.BinarySearch(p.Offsets, _current);
        if (idx < 0 || !KvChecksum.Verify(KvChecksum.ProtectKv(Key, Value), p.BytesPerKey, p.Checksums.AsSpan(idx * p.BytesPerKey, p.BytesPerKey)))
        {
            ThrowHelper.Corruption("Corrupted block entry: per key-value checksum verification failed. Offset: " + _current.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + ". Entry index: " + Math.Max(idx, -1).ToString(System.Globalization.CultureInfo.InvariantCulture) + ".");
        }
    }

    /// <summary>
    /// Port of Block::Initialize*ProtectionInfo: a second iterator configured like this one (unverified) scans the block
    /// and records ProtectKV(key, value) of every entry.
    /// </summary>
    private BlockKvProtection ComputeKvProtection()
    {
        int bytes = _kvProtectionBytes;
        using var it = new BlockIter();
        it.Reset(_block!, _kind, _icmp);
        it._kvProtectionBytes = 0;
        it._keysAreInternal = _keysAreInternal;
        it._valueDeltaEncoded = _valueDeltaEncoded;
        it._valueDeltaEscape = _valueDeltaEscape;
        it._haveFirstKey = _haveFirstKey;
        it._globalSeqno = _globalSeqno;
        it._hashMapOffset = _hashMapOffset;
        it._hashNumBuckets = _hashNumBuckets;
        it._padTsSize = _padTsSize;
        it.UpdateModes();
        var offsets = new List<int>(64);
        var sums = new PooledBufferWriter(64 * bytes);
        try
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                offsets.Add(it._current);
                KvChecksum.Encode(KvChecksum.ProtectKv(it.Key, it.Value), bytes, sums.GetSpan(bytes));
                sums.Advance(bytes);
            }
            return new BlockKvProtection(bytes, offsets.ToArray(), sums.WrittenSpan.ToArray());
        }
        finally
        {
            sums.Dispose();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Internals

    private void Invalidate()
    {
        _current = _keysEnd;
        _next = _keysEnd;
        _restartIndex = _numRestarts;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint GetRestartPoint(int index) => Coding.DecodeFixed32(new ReadOnlySpan<byte>(_data, _restarts + index * 4, 4));

    private void SeekToRestartPoint(int index)
    {
        _keyLen = 0;
        _rawKeyLen = 0;
        _keyInBuf = true;
        _restartIndex = index;
        // _current is fixed by ParseNextKey; set _next so the next parse starts at the restart point.
        _next = (int)GetRestartPoint(index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKey(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        // InternalKeyComparator devirtualizes the built-in (reverse) bytewise user comparators.
        return _keysAreInternal ? _icmp.Compare(a, b) : _icmp.CompareUserKey(a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool UserKeyEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => _icmp.EqualUserKey(a, b);

    /// <summary>Decodes the entry header at <paramref name="p"/>; returns the key-delta offset or -1 on error. Port of DecodeEntry / DecodeEntryV4.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int DecodeEntry(int p, int limit, out uint shared, out uint nonShared, out uint valueLength)
    {
        // Invariant: 0 <= p and limit <= _restarts <= _data.Length (Block.Parse validated the restart array), so after
        // the length check the three header bytes are in bounds; read them without per-byte bounds checks.
        ref byte d = ref MemoryMarshal.GetArrayDataReference(_data);
        if (_valueDeltaEncoded)
        {
            valueLength = 0;
            if (limit - p < 3) { shared = nonShared = 0; return -1; }
            shared = Unsafe.Add(ref d, (uint)p);
            nonShared = Unsafe.Add(ref d, (uint)p + 1);
            if ((shared | nonShared) < 128) return p + 2;
            return DecodeVarints(p, limit, 2, out shared, out nonShared, out _);
        }
        if (limit - p < 3) { shared = nonShared = valueLength = 0; return -1; }
        shared = Unsafe.Add(ref d, (uint)p);
        nonShared = Unsafe.Add(ref d, (uint)p + 1);
        valueLength = Unsafe.Add(ref d, (uint)p + 2);
        if ((shared | nonShared | valueLength) < 128) return p + 3;
        return DecodeVarints(p, limit, 3, out shared, out nonShared, out valueLength);
    }

    /// <summary>
    /// Copies <paramref name="n"/> bytes between non-overlapping buffers; short copies (the usual key delta) are done
    /// with two possibly-overlapping word moves instead of a memmove call.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyKeyBytes(ref byte dst, ref byte src, uint n)
    {
        if (n >= 8 && n <= 16)
        {
            ulong a = Unsafe.ReadUnaligned<ulong>(ref src);
            ulong b = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, n - 8));
            Unsafe.WriteUnaligned(ref dst, a);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, n - 8), b);
        }
        else if (n >= 4 && n < 8)
        {
            uint a = Unsafe.ReadUnaligned<uint>(ref src);
            uint b = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref src, n - 4));
            Unsafe.WriteUnaligned(ref dst, a);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, n - 4), b);
        }
        else
        {
            Unsafe.CopyBlockUnaligned(ref dst, ref src, n);
        }
    }

    private int DecodeVarints(int p, int limit, int count, out uint a, out uint b, out uint c)
    {
        ReadOnlySpan<byte> s = new(_data, p, limit - p);
        int start = s.Length;
        a = b = c = 0;
        if (!Coding.GetVarint32(ref s, out a) || !Coding.GetVarint32(ref s, out b)) return -1;
        if (count == 3 && !Coding.GetVarint32(ref s, out c)) return -1;
        return p + (start - s.Length);
    }

    /// <summary>Port of BlockIter::ParseNextKey (+ IndexBlockIter::DecodeCurrentValue / global seqno handling).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool ParseNextKey()
    {
        int cur = _next;
        _current = cur;
        int limit = _keysEnd;
        if (cur >= limit)
        {
            Invalidate();
            return false;
        }
        // Fast path: data/meta entry whose three header varints are single bytes (keeps the header in registers;
        // the out-parameter decoder below forces them to the stack).
        if (!_slowParse && limit - cur >= 3)
        {
            ref byte d = ref MemoryMarshal.GetArrayDataReference(_data);
            uint shared = Unsafe.Add(ref d, (uint)cur);
            uint nonShared = Unsafe.Add(ref d, (uint)cur + 1);
            uint valueLength = Unsafe.Add(ref d, (uint)cur + 2);
            if ((shared | nonShared | valueLength) < 128) return ParseEntry(cur + 3, limit, shared, nonShared, valueLength);
        }
        return ParseNextKeySlow(cur, limit);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool ParseNextKeySlow(int cur, int limit)
    {
        if (_valuesSection >= 0) return ParseNextKeySeparated(cur, limit);
        int p = DecodeEntry(cur, limit, out uint shared, out uint nonShared, out uint valueLength);
        if (p < 0) return CorruptionError();
        if (_padTsSize != 0) return ParseEntryPadded(p, limit, shared, nonShared, valueLength);
        return ParseEntry(p, limit, shared, nonShared, valueLength);
    }

    /// <summary>
    /// Port of the separated key/value branch of BlockIter::ParseNextKey (separate_key_value_in_data_block): entries in
    /// the keys section are <c>shared | non_shared | value_size | [value_offset at restart points] | key_delta</c>; a
    /// restart entry's value is at values_section + value_offset, every other value immediately follows the previous
    /// entry's value. Upstream identifies restart entries by entry index modulo block_restart_interval; the port compares
    /// the entry offset with the restart array, which is equivalent for well-formed blocks and needs no table property.
    /// </summary>
    private bool ParseNextKeySeparated(int cur, int limit)
    {
        while (_restartIndex + 1 < _numRestarts && GetRestartPoint(_restartIndex + 1) <= (uint)cur) _restartIndex++;
        bool restartEntry = _restartIndex < _numRestarts && GetRestartPoint(_restartIndex) == (uint)cur;
        int p = DecodeEntry(cur, limit, out uint shared, out uint nonShared, out uint valueLength);
        uint valueOffset = 0;
        if (p >= 0 && restartEntry)
        {
            ReadOnlySpan<byte> s = new(_data, p, limit - p);
            int before = s.Length;
            p = Coding.GetVarint32(ref s, out valueOffset) ? p + (before - s.Length) : -1;
            if (shared != 0) return CorruptionError();
        }
        if (p < 0) return CorruptionError();
        int previousValueEnd = _valueOffset + _valueLength;
        bool ok = _padTsSize != 0 ? ParseEntryPadded(p, limit, shared, nonShared, 0) : ParseEntry(p, limit, shared, nonShared, 0);
        if (!ok) return false;
        int valuesEnd = _restarts;
        int valueStart;
        if (restartEntry)
        {
            uint valuesSize = (uint)(valuesEnd - _valuesSection);
            if (valueOffset > valuesSize || valueLength > valuesSize - valueOffset) return CorruptionError();
            valueStart = _valuesSection + (int)valueOffset;
        }
        else
        {
            valueStart = previousValueEnd;
            if (valueStart < _valuesSection || valueStart > valuesEnd || valueLength > (uint)(valuesEnd - valueStart)) return CorruptionError();
        }
        _valueOffset = valueStart;
        _valueLength = (int)valueLength;
        return true;
    }

    /// <summary>Returns the offset just past the varint32 at <paramref name="p"/>, or -1 when malformed.</summary>
    private int SkipVarint32(int p, int limit)
    {
        ReadOnlySpan<byte> s = new(_data, p, limit - p);
        int before = s.Length;
        return Coding.GetVarint32(ref s, out _) ? p + (before - s.Length) : -1;
    }

    /// <summary>
    /// ParseEntry for blocks with stripped timestamps: the stored key is rebuilt in <see cref="_rawKeyBuf"/> (delta
    /// base) and exposed with the minimum timestamp inserted before the footer (internal keys) or appended (user keys).
    /// Port of the pad_min_timestamp_ paths of BlockIter::ParseNextKey (UpdateRawKeyAndMaybePadMinTimestamp,
    /// IterKey::TrimAppendWithTimestamp) and IndexBlockIter::DecodeCurrentValue (first_internal_key padding).
    /// </summary>
    private bool ParseEntryPadded(int p, int limit, uint shared, uint nonShared, uint valueLength)
    {
        if ((uint)_rawKeyLen < shared) return CorruptionError();
        ulong entryLength = (ulong)nonShared + valueLength;
        if (entryLength > (ulong)(limit - p)) return CorruptionError();
        int rawLen = (int)(shared + nonShared);
        if (_rawKeyBuf.Length < rawLen)
        {
            byte[] n = RocksArrayPool<byte>.Shared.Rent(Math.Max(rawLen, 64));
            _rawKeyBuf.AsSpan(0, _rawKeyLen).CopyTo(n);
            if (_rawKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_rawKeyBuf);
            _rawKeyBuf = n;
        }
        Buffer.BlockCopy(_data, p, _rawKeyBuf, (int)shared, (int)nonShared);
        _rawKeyLen = rawLen;
        if (shared == 0)
        {
            while (_restartIndex + 1 < _numRestarts && GetRestartPoint(_restartIndex + 1) < (uint)_current) _restartIndex++;
        }
        int padded = PadMinTimestamp(new ReadOnlySpan<byte>(_rawKeyBuf, 0, rawLen), ref _keyBuf);
        if (padded < 0) return CorruptionError();
        _keyLen = padded;
        _keyInBuf = true;

        int valueStart = p + (int)nonShared;
        if (_kind == BlockIterKind.Index)
        {
            DecodeIndexValue(valueStart, shared != 0, valueLength);
            _firstKeyPadLen = 0;
            if (_haveFirstKey && _firstKeyLength > 0)
            {
                int n = PadMinTimestampInternal(new ReadOnlySpan<byte>(_data, _firstKeyOffset, _firstKeyLength), ref _firstKeyPadBuf);
                if (n < 0) return CorruptionError();
                _firstKeyPadLen = n;
            }
        }
        else
        {
            _valueOffset = valueStart;
            _valueLength = (int)valueLength;
            _next = valueStart + (int)valueLength;
        }
        if (_globalSeqno != DisableGlobalSequenceNumber && _keysAreInternal) ApplyGlobalSeqno();
        return true;
    }

    /// <summary>Writes <paramref name="stored"/> padded with the minimum timestamp into <paramref name="dst"/>; returns its length (-1 if malformed).</summary>
    private int PadMinTimestamp(ReadOnlySpan<byte> stored, ref byte[] dst) =>
        _keysAreInternal ? PadMinTimestampInternal(stored, ref dst) : PadMinTimestampUser(stored, ref dst);

    /// <summary>PadInternalKeyWithMinTimestamp: user key | min ts | footer. The minimum (u64) timestamp is all zeros.</summary>
    private int PadMinTimestampInternal(ReadOnlySpan<byte> stored, ref byte[] dst)
    {
        if (stored.Length < DbFormat.InternalKeyFooterSize) return -1;
        int len = stored.Length + _padTsSize;
        GrowScratch(ref dst, len);
        int userLen = stored.Length - DbFormat.InternalKeyFooterSize;
        stored[..userLen].CopyTo(dst);
        dst.AsSpan(userLen, _padTsSize).Clear();
        stored[userLen..].CopyTo(dst.AsSpan(userLen + _padTsSize));
        return len;
    }

    /// <summary>AppendKeyWithMinTimestamp: user key | min ts.</summary>
    private int PadMinTimestampUser(ReadOnlySpan<byte> stored, ref byte[] dst)
    {
        int len = stored.Length + _padTsSize;
        GrowScratch(ref dst, len);
        stored.CopyTo(dst);
        dst.AsSpan(stored.Length, _padTsSize).Clear();
        return len;
    }

    /// <summary>Grows a pooled scratch buffer whose contents are rewritten whole (nothing is preserved).</summary>
    private static void GrowScratch(ref byte[] buf, int len)
    {
        if (buf.Length >= len) return;
        if (buf.Length > 0) RocksArrayPool<byte>.Shared.Return(buf);
        buf = RocksArrayPool<byte>.Shared.Rent(Math.Max(len, 64));
    }

    /// <summary>Applies a decoded entry header at key-delta offset <paramref name="p"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ParseEntry(int p, int limit, uint shared, uint nonShared, uint valueLength)
    {
        if ((uint)_keyLen < shared) return CorruptionError();
        ulong entryLength = (ulong)nonShared + valueLength;
        if (entryLength > (ulong)(limit - p)) return CorruptionError();

        if (shared == 0)
        {
            if (_prefixLength == 0)
            {
                // Key stored whole: point into the block (no copy).
                _keyInBuf = false;
                _keyBlockOffset = p;
                _keyLen = (int)nonShared;
            }
            else
            {
                // Restart key stored without the block's common prefix: full key = prefix + stored bytes.
                int newLen = _prefixLength + (int)nonShared;
                EnsureKeyCapacity(newLen);
                ref byte kb = ref MemoryMarshal.GetArrayDataReference(_keyBuf);
                ref byte db = ref MemoryMarshal.GetArrayDataReference(_data);
                CopyKeyBytes(ref kb, ref db, (uint)_prefixLength);
                CopyKeyBytes(ref Unsafe.Add(ref kb, (uint)_prefixLength), ref Unsafe.Add(ref db, (uint)p), nonShared);
                _keyInBuf = true;
                _keyLen = newLen;
            }
            // Keep restart index in sync.
            while (_restartIndex + 1 < _numRestarts && GetRestartPoint(_restartIndex + 1) < (uint)_current) _restartIndex++;
        }
        else
        {
            int newLen = (int)(shared + nonShared);
            if (_keyBuf.Length < newLen) EnsureKeyCapacity(newLen);
            // Bounds: shared <= _keyLen (checked above) and p + nonShared <= limit (entry length check).
            ref byte kb = ref MemoryMarshal.GetArrayDataReference(_keyBuf);
            ref byte db = ref MemoryMarshal.GetArrayDataReference(_data);
            if (!_keyInBuf)
            {
                CopyKeyBytes(ref kb, ref Unsafe.Add(ref db, (uint)_keyBlockOffset), shared);
                _keyInBuf = true;
            }
            CopyKeyBytes(ref Unsafe.Add(ref kb, shared), ref Unsafe.Add(ref db, (uint)p), nonShared);
            _keyLen = newLen;
        }

        int valueStart = p + (int)nonShared;
        if (_kind == BlockIterKind.Index)
        {
            DecodeIndexValue(valueStart, shared != 0, valueLength);
        }
        else
        {
            _valueOffset = valueStart;
            _valueLength = (int)valueLength;
            _next = valueStart + (int)valueLength;
        }

        if (_globalSeqno != DisableGlobalSequenceNumber && _keysAreInternal) ApplyGlobalSeqno();
        return true;
    }

    private void DecodeIndexValue(int valueStart, bool isShared, uint valueLength)
    {
        int limit = _valueDeltaEncoded ? _restarts : valueStart + (int)valueLength;
        ReadOnlySpan<byte> v = new(_data, valueStart, limit - valueStart);
        int startLen = v.Length;
        bool hasPrevious = _valueDeltaEncoded && isShared;
        if (!IndexValue.TryDecode(ref v, _haveFirstKey, hasPrevious, _handle, _valueDeltaEscape, out BlockHandle h, out int fko, out int fkl))
        {
            CorruptionError();
            return;
        }
        _handle = h;
        _firstKeyOffset = valueStart + fko;
        _firstKeyLength = fkl;
        int consumed = startLen - v.Length;
        _valueOffset = valueStart;
        _valueLength = _valueDeltaEncoded ? consumed : (int)valueLength;
        _next = valueStart + _valueLength;
    }

    private void ApplyGlobalSeqno()
    {
        // Port of the global seqno overwrite in IterKey::UpdateInternalKey: keys in external SST files carry
        // seqno 0; the effective sequence number is substituted here.
        if (!_keyInBuf)
        {
            EnsureKeyCapacity(_keyLen);
            Buffer.BlockCopy(_data, _keyBlockOffset, _keyBuf, 0, _keyLen);
            _keyInBuf = true;
        }
        if (_keyLen < DbFormat.InternalKeyFooterSize) { CorruptionError(); return; }
        Span<byte> footer = _keyBuf.AsSpan(_keyLen - DbFormat.InternalKeyFooterSize, DbFormat.InternalKeyFooterSize);
        EntryType t = (EntryType)footer[0];
        Coding.EncodeFixed64(footer, DbFormat.PackSequenceAndType(_globalSeqno, t));
    }

    private void EnsureKeyCapacity(int len)
    {
        if (_keyBuf.Length >= len) return;
        byte[] n = RocksArrayPool<byte>.Shared.Rent(Math.Max(len, 64));
        if (_keyInBuf && _keyLen > 0) Buffer.BlockCopy(_keyBuf, 0, n, 0, Math.Min(_keyLen, _keyBuf.Length));
        if (_keyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_keyBuf);
        _keyBuf = n;
    }

    private bool CorruptionError()
    {
        Invalidate();
        ThrowHelper.Corruption("bad entry in block");
        return false;
    }

    /// <summary>
    /// Port of BlockIter::BinarySeekRestartPointIndex: finds the last restart point whose key is &lt;= target
    /// (index 0 with skipLinearScan when all keys are &gt; target).
    /// </summary>
    private bool BinarySeekRestartPointIndex(ReadOnlySpan<byte> target, out int index, out bool skipLinearScan)
    {
        index = 0;
        skipLinearScan = false;
        if (_keysEnd == 0)
        {
            // Blocks with no keys (e.g. index of a range-tombstone-only file) have num_restarts == 1.
            return false;
        }
        long left = -1;
        long right = _numRestarts - 1;
        while (left != right)
        {
            long mid = left + (right - left + 1) / 2;
            ReadOnlySpan<byte> midKey = GetRestartKey((int)mid);
            int cmp = CompareRestartKey(midKey, target);
            if (cmp < 0) left = mid;
            else if (cmp > 0) right = mid - 1;
            else
            {
                skipLinearScan = true;
                left = right = mid;
            }
        }
        if (left == -1)
        {
            skipLinearScan = true;
            index = 0;
        }
        else
        {
            index = (int)left;
        }
        return true;
    }

    /// <summary>
    /// Port of BlockIter::InterpolationSeekRestartPointIndex (table/block_based/block.cc): same contract as
    /// <see cref="BinarySeekRestartPointIndex"/>, but probes the restart point estimated from the first 8 bytes (big
    /// endian) of the keys after the prefix shared by the boundary keys (ReadBe64FromKey). Falls back to binary search
    /// for small windows (kGuardLen) and after kMaxPoorSearches probes that did not halve the window. Only used with
    /// the bytewise comparator on blocks without key rewriting (<see cref="UseInterpolationSearch"/>), so restart keys are
    /// spans over the block buffer and comparing whole keys is equivalent to upstream's suffix comparisons.
    /// </summary>
    private bool InterpolationSeekRestartPointIndex(ReadOnlySpan<byte> target, out int index, out bool skipLinearScan)
    {
        const long GuardLen = 8;
        const ulong MaxPoorSearches = 8;
        index = 0;
        skipLinearScan = false;
        if (_keysEnd == 0) return false;

        bool isUserKey = !_keysAreInternal;
        long left = -1;
        long right = _numRestarts - 1;
        int sharedUserPrefixLen = 0;
        ReadOnlySpan<byte> leftKey = default;
        ReadOnlySpan<byte> rightKey = default;
        bool firstIter = true;
        ulong leftVal = 0, rightVal = 0, targetVal = 0;
        // A poor search is when less than half the search space is reduced, because binary search would do better.
        ulong continuousPoorSearches = 0;
        ReadOnlySpan<byte> targetUserKey = isUserKey ? target : DbFormat.ExtractUserKey(target);
        while (left != right)
        {
            long mid = 0;
            // If either search window is small or we've had numerous bad guesses, fall back to binary search.
            bool seekFailed = right - left <= GuardLen || continuousPoorSearches >= MaxPoorSearches;
            if (!seekFailed)
            {
                // Interpolation reads the left and right boundaries anyway, so left can be 0 (we exit early on the
                // first iteration when target <= left).
                int usableLeft = (int)Math.Max(left, 0);
                if (firstIter)
                {
                    leftKey = GetRestartKey(usableLeft);
                    rightKey = GetRestartKey((int)right);
                    sharedUserPrefixLen = leftKey.CommonPrefixLength(rightKey);
                    // Only the user key part may be shared (bytewise: the shorter user key is the left one).
                    if (!isUserKey) sharedUserPrefixLen = Math.Min(sharedUserPrefixLen, leftKey.Length - DbFormat.InternalKeyFooterSize);
                    leftVal = BlockBuilder.ReadBe64FromKey(leftKey, isUserKey, sharedUserPrefixLen);
                    rightVal = BlockBuilder.ReadBe64FromKey(rightKey, isUserKey, sharedUserPrefixLen);
                    targetVal = BlockBuilder.ReadBe64FromKey(target, isUserKey, sharedUserPrefixLen);
                    if (sharedUserPrefixLen > 0)
                    {
                        // The boundaries' shared prefix is not necessarily a prefix of the target: exit early if not.
                        int cmpLen = Math.Min(targetUserKey.Length, sharedUserPrefixLen);
                        int c = targetUserKey[..cmpLen].SequenceCompareTo(leftKey[..cmpLen]);
                        if (c < 0 || (c == 0 && cmpLen < sharedUserPrefixLen))
                        {
                            skipLinearScan = true;
                            index = usableLeft;
                            return true;
                        }
                        if (c > 0)
                        {
                            index = (int)right;
                            return true;
                        }
                    }
                }
                if (leftVal > rightVal) return CorruptionError();

                bool lteLeft = false;
                bool gtRight = false;
                if (targetVal < leftVal)
                {
                    lteLeft = true;
                }
                else if (targetVal == leftVal)
                {
                    // Equal values don't imply equal keys (only 8 bytes are read and the footer is skipped).
                    if (CompareKey(leftKey, target) >= 0) lteLeft = true;
                }
                if (!lteLeft)
                {
                    // Only arr[target] < arr[right + 1] is guaranteed, so this can happen after the first iteration.
                    if (targetVal > rightVal) gtRight = true;
                    else if (rightVal == leftVal) seekFailed = true; // cannot divide by 0
                }
                if (lteLeft)
                {
                    skipLinearScan = true;
                    index = usableLeft;
                    return true;
                }
                if (gtRight)
                {
                    index = (int)right;
                    return true;
                }
                if (!seekFailed)
                {
                    // HAVE_UINT128_EXTENSION branch: range * target_delta / range_delta in 128 bits.
                    UInt128 range = (ulong)(right - usableLeft);
                    UInt128 targetDelta = targetVal - leftVal;
                    ulong rangeDelta = rightVal - leftVal;
                    long offset = (long)(ulong)(range * targetDelta / rangeDelta);
                    left = usableLeft; // can reduce search space by 1
                    mid = usableLeft + offset;
                    if (mid == usableLeft) ++mid; // guarantee progress
                }
            }
            if (seekFailed) mid = left + (right - left + 1) / 2;

            ReadOnlySpan<byte> midKey = GetRestartKey((int)mid);
            int cmp = CompareKey(midKey, target);
            long previousSearchSpace = right - left;
            if (cmp < 0)
            {
                left = mid;
                leftKey = midKey;
                leftVal = BlockBuilder.ReadBe64FromKey(leftKey, isUserKey, sharedUserPrefixLen);
            }
            else if (cmp > 0)
            {
                right = mid - 1;
                if (!seekFailed && left != right)
                {
                    rightKey = GetRestartKey((int)right);
                    rightVal = BlockBuilder.ReadBe64FromKey(rightKey, isUserKey, sharedUserPrefixLen);
                }
            }
            else
            {
                skipLinearScan = true;
                left = right = mid;
            }
            // If the search space was not at least halved, the data is likely not uniform.
            if (right - left > previousSearchSpace / 2) ++continuousPoorSearches;
            else continuousPoorSearches = 0;
            firstIter = false;
        }
        if (left == -1)
        {
            // All keys in the block were strictly greater than target: the first key is the result.
            skipLinearScan = true;
            index = 0;
        }
        else
        {
            index = (int)left;
        }
        return true;
    }

    private int CompareRestartKey(ReadOnlySpan<byte> restartKey, ReadOnlySpan<byte> target)
    {
        if (!_restartKeyFixup) return CompareKey(restartKey, target);
        if (_padTsSize != 0)
        {
            int n = PadMinTimestamp(restartKey, ref _restartPadBuf);
            if (n < 0)
            {
                CorruptionError();
                return 0;
            }
            restartKey = new ReadOnlySpan<byte>(_restartPadBuf, 0, n);
        }
        if (_globalSeqno != DisableGlobalSequenceNumber && _keysAreInternal && restartKey.Length >= DbFormat.InternalKeyFooterSize)
        {
            // Compare as if the footer carried the global seqno.
            Span<byte> tmp = restartKey.Length <= 256 ? stackalloc byte[restartKey.Length] : new byte[restartKey.Length];
            restartKey.CopyTo(tmp);
            Span<byte> footer = tmp[^DbFormat.InternalKeyFooterSize..];
            Coding.EncodeFixed64(footer, DbFormat.PackSequenceAndType(_globalSeqno, (EntryType)footer[0]));
            return CompareKey(tmp, target);
        }
        return CompareKey(restartKey, target);
    }

    private ReadOnlySpan<byte> GetRestartKey(int index)
    {
        int offset = (int)GetRestartPoint(index);
        int p = DecodeEntry(offset, _keysEnd, out uint shared, out uint nonShared, out _);
        // Separated key/value storage: restart entries carry the value offset after the header (DecodeKey with value_offset).
        if (p >= 0 && _valuesSection >= 0) p = SkipVarint32(p, _keysEnd);
        if (p < 0 || shared != 0 || nonShared > (uint)(_keysEnd - p))
        {
            CorruptionError();
            return default;
        }
        if (_prefixLength == 0) return new ReadOnlySpan<byte>(_data, p, (int)nonShared);
        // Reconstruct prefix + stored suffix (format_version >= 8 common-prefix blocks).
        int len = _prefixLength + (int)nonShared;
        if (_restartKeyBuf.Length < len)
        {
            if (_restartKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_restartKeyBuf);
            _restartKeyBuf = RocksArrayPool<byte>.Shared.Rent(Math.Max(len, 64));
        }
        Buffer.BlockCopy(_data, 0, _restartKeyBuf, 0, _prefixLength);
        Buffer.BlockCopy(_data, p, _restartKeyBuf, _prefixLength, (int)nonShared);
        return new ReadOnlySpan<byte>(_restartKeyBuf, 0, len);
    }

    private void FindKeyAfterBinarySeek(ReadOnlySpan<byte> target, int index, bool skipLinearScan)
    {
        SeekToRestartPoint(index);
        ParseNextKey();
        if (!skipLinearScan)
        {
            // Linear search (within restart block) for first key >= target
            int maxOffset = index + 1 < _numRestarts ? (int)GetRestartPoint(index + 1) : int.MaxValue;
            while (true)
            {
                ParseNextKey();
                if (!Valid) break;
                if (_current == maxOffset) break;
                if (CompareKey(Key, target) >= 0) break;
            }
        }
    }

    public void Dispose()
    {
        Clear();
        if (_keyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_keyBuf);
        _keyBuf = [];
        if (_restartKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_restartKeyBuf);
        _restartKeyBuf = [];
        if (_prevKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_prevKeyBuf);
        _prevKeyBuf = [];
        if (_rawKeyBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_rawKeyBuf);
        _rawKeyBuf = [];
        if (_firstKeyPadBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_firstKeyPadBuf);
        _firstKeyPadBuf = [];
        if (_restartPadBuf.Length > 0) RocksArrayPool<byte>.Shared.Return(_restartPadBuf);
        _restartPadBuf = [];
        _keyLen = 0;
        _keyInBuf = true;
    }
}
