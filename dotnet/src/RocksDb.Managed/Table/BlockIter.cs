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
    private int _restarts;      // offset of restart array == end of entries
    private int _numRestarts;
    private int _current;       // offset of the current entry; == _restarts when invalid
    private int _next;          // offset just past the current entry (incl. value)
    private int _restartIndex;  // restart block containing _current
    private int _hashMapOffset = -1;
    private int _hashNumBuckets;
    private int _prefixLength;     // common user-key prefix stored at [0, _prefixLength) (format_version >= 8)
    private byte[] _restartKeyBuf = [];

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

    /// <summary>Iterator for a data or range-deletion block (internal keys ordered by <paramref name="comparator"/>).</summary>
    public void InitializeData(Block block, InternalKeyComparator comparator, ulong globalSeqno)
    {
        Reset(block, BlockIterKind.Data, comparator);
        _keysAreInternal = true;
        _globalSeqno = globalSeqno;
        _hashMapOffset = block.HashMapOffset;
        _hashNumBuckets = block.HashNumBuckets;
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
    }

    /// <summary>Iterator for a metaindex or properties block (bytewise ordered keys).</summary>
    public void InitializeMeta(Block block)
    {
        Reset(block, BlockIterKind.Meta, InternalKeyComparator.Bytewise);
        _keysAreInternal = false;
    }

    private void Reset(Block block, BlockIterKind kind, InternalKeyComparator icmp)
    {
        _kind = kind;
        _data = block.Buffer;
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
        _data = [];
        _restarts = 0;
        _numRestarts = 0;
        _current = 0;
        _next = 0;
        _prevIdx = -1;
        _prevCount = 0;
    }

    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current < _restarts;
    }

    /// <summary>Current key (internal key for data blocks / internal-key index blocks).</summary>
    public ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _keyInBuf ? new ReadOnlySpan<byte>(_keyBuf, 0, _keyLen) : new ReadOnlySpan<byte>(_data, _keyBlockOffset, _keyLen);
    }

    /// <summary>Raw value (for index blocks: the encoded index value bytes).</summary>
    public ReadOnlySpan<byte> Value => new(_data, _valueOffset, _valueLength);

    /// <summary>Raw value as memory over the block buffer (valid while the block is alive).</summary>
    public ReadOnlyMemory<byte> ValueMemory => new(_data, _valueOffset, _valueLength);

    /// <summary>Index blocks: the decoded block handle of the current entry.</summary>
    public BlockHandle BlockHandle => _handle;

    /// <summary>Index blocks with kBinarySearchWithFirstKey: first internal key of the referenced data block.</summary>
    public ReadOnlySpan<byte> FirstInternalKey => new(_data, _firstKeyOffset, _firstKeyLength);

    /// <summary>Offset of the current entry's value within the block (port of ValueOffset, used for global seqno patching).</summary>
    public int ValueOffsetInBlock => _valueOffset;

    // ---------------------------------------------------------------------------------------------
    // Positioning

    public void SeekToFirst()
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        SeekToRestartPoint(0);
        ParseNextKey();
    }

    public void SeekToLast()
    {
        if (_numRestarts == 0) { Invalidate(); return; }
        SeekToRestartPoint(_numRestarts - 1);
        while (ParseNextKey() && _next < _restarts)
        {
            // Keep skipping
        }
    }

    public void Next()
    {
        System.Diagnostics.Debug.Assert(Valid);
        ParseNextKey();
    }

    /// <summary>
    /// Port of BlockIter::PrevImpl / DataBlockIter::PrevImpl: scans forward from the preceding restart point. For data
    /// and meta blocks the entries decoded by that scan are cached (DataBlockIter's prev_entries_), so a run of Prev
    /// calls decodes each restart interval once instead of once per entry.
    /// </summary>
    public void Prev()
    {
        System.Diagnostics.Debug.Assert(Valid);
        if (_kind != BlockIterKind.Index && _prevIdx > 0 && _prevEntries[_prevIdx].Offset == _current)
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
        if (_kind == BlockIterKind.Index)
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
            byte[] bigger = ArrayPool<byte>.Shared.Rent(Math.Max(256, (_prevKeyBytes + _keyLen) * 2));
            _prevKeyBuf.AsSpan(0, _prevKeyBytes).CopyTo(bigger);
            if (_prevKeyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_prevKeyBuf);
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
        if (!BinarySeekRestartPointIndex(target, out int index, out bool skipLinearScan)) { Invalidate(); return; }
        FindKeyAfterBinarySeek(target, index, skipLinearScan);
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
        int limit = restartIndex + 1 < _numRestarts ? (int)GetRestartPoint(restartIndex + 1) : _restarts;
        while (cur < limit)
        {
            // Only linear-seek inside the restart interval; stop at the first potential match.
            if (!ParseNextKey() || CompareKey(Key, target) >= 0) break;
            cur = _current;
        }
        if (_current == _restarts) return true;
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
    // Internals

    private void Invalidate()
    {
        _current = _restarts;
        _next = _restarts;
        _restartIndex = _numRestarts;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint GetRestartPoint(int index) => Coding.DecodeFixed32(new ReadOnlySpan<byte>(_data, _restarts + index * 4, 4));

    private void SeekToRestartPoint(int index)
    {
        _keyLen = 0;
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
    private bool ParseNextKey()
    {
        int cur = _next;
        _current = cur;
        int limit = _restarts;
        if (cur >= limit)
        {
            Invalidate();
            return false;
        }
        // Fast path: data/meta entry whose three header varints are single bytes (keeps the header in registers;
        // the out-parameter decoder below forces them to the stack).
        if (!_valueDeltaEncoded && limit - cur >= 3)
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
        int p = DecodeEntry(cur, limit, out uint shared, out uint nonShared, out uint valueLength);
        if (p < 0) return CorruptionError();
        return ParseEntry(p, limit, shared, nonShared, valueLength);
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
        byte[] n = ArrayPool<byte>.Shared.Rent(Math.Max(len, 64));
        if (_keyInBuf && _keyLen > 0) Buffer.BlockCopy(_keyBuf, 0, n, 0, Math.Min(_keyLen, _keyBuf.Length));
        if (_keyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_keyBuf);
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
        if (_restarts == 0)
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

    private int CompareRestartKey(ReadOnlySpan<byte> restartKey, ReadOnlySpan<byte> target)
    {
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
        int p = DecodeEntry(offset, _restarts, out uint shared, out uint nonShared, out _);
        if (p < 0 || shared != 0 || nonShared > (uint)(_restarts - p))
        {
            CorruptionError();
            return default;
        }
        if (_prefixLength == 0) return new ReadOnlySpan<byte>(_data, p, (int)nonShared);
        // Reconstruct prefix + stored suffix (format_version >= 8 common-prefix blocks).
        int len = _prefixLength + (int)nonShared;
        if (_restartKeyBuf.Length < len)
        {
            if (_restartKeyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_restartKeyBuf);
            _restartKeyBuf = ArrayPool<byte>.Shared.Rent(Math.Max(len, 64));
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
        if (_keyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_keyBuf);
        _keyBuf = [];
        if (_restartKeyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_restartKeyBuf);
        _restartKeyBuf = [];
        if (_prevKeyBuf.Length > 0) ArrayPool<byte>.Shared.Return(_prevKeyBuf);
        _prevKeyBuf = [];
        _keyLen = 0;
        _keyInBuf = true;
    }
}
