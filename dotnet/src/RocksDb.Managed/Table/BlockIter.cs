using System.Buffers;
using System.Runtime.CompilerServices;
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

    private byte[] _keyBuf = [];
    private int _keyLen;
    private bool _keyInBuf;
    private int _keyBlockOffset;

    private int _valueOffset;
    private int _valueLength;

    // Index-specific
    private BlockHandle _handle;
    private int _firstKeyOffset;
    private int _firstKeyLength;

    private BlockIterKind _kind;
    private Comparator _ucmp = Comparator.Bytewise;
    private bool _bytewise = true;
    private bool _keysAreInternal;
    private bool _valueDeltaEncoded;
    private bool _haveFirstKey;
    private ulong _globalSeqno = DisableGlobalSequenceNumber;

    /// <summary>Iterator for a data or range-deletion block (internal keys ordered by <paramref name="userComparator"/>).</summary>
    public void InitializeData(Block block, Comparator userComparator, ulong globalSeqno)
    {
        Reset(block, BlockIterKind.Data, userComparator);
        _keysAreInternal = true;
        _globalSeqno = globalSeqno;
        _hashMapOffset = block.HashMapOffset;
        _hashNumBuckets = block.HashNumBuckets;
    }

    /// <summary>
    /// Iterator for an index block. <paramref name="keyIncludesSeq"/> = !index_key_is_user_key;
    /// <paramref name="valueIsFull"/> = !index_value_is_delta_encoded; <paramref name="haveFirstKey"/> for
    /// kBinarySearchWithFirstKey.
    /// </summary>
    public void InitializeIndex(Block block, Comparator userComparator, bool keyIncludesSeq, bool valueIsFull, bool haveFirstKey, ulong globalSeqno)
    {
        Reset(block, BlockIterKind.Index, userComparator);
        _keysAreInternal = keyIncludesSeq;
        _valueDeltaEncoded = !valueIsFull;
        _haveFirstKey = haveFirstKey;
        _globalSeqno = globalSeqno;
    }

    /// <summary>Iterator for a metaindex or properties block (bytewise ordered keys).</summary>
    public void InitializeMeta(Block block)
    {
        Reset(block, BlockIterKind.Meta, Comparator.Bytewise);
        _keysAreInternal = false;
    }

    private void Reset(Block block, BlockIterKind kind, Comparator ucmp)
    {
        _kind = kind;
        _data = block.Buffer;
        _restarts = block.RestartOffset;
        _numRestarts = block.NumRestarts;
        _ucmp = ucmp;
        _bytewise = ReferenceEquals(ucmp, Comparator.Bytewise);
        _hashMapOffset = -1;
        _hashNumBuckets = 0;
        _valueDeltaEncoded = false;
        _haveFirstKey = false;
        _globalSeqno = DisableGlobalSequenceNumber;
        _handle = default;
        _keyLen = 0;
        _keyInBuf = true;
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

    /// <summary>Port of BlockIter::PrevImpl (scan forward from the preceding restart point).</summary>
    public void Prev()
    {
        System.Diagnostics.Debug.Assert(Valid);
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
        // Loop until end of current entry hits the start of original entry
        while (ParseNextKey() && _next < original)
        {
        }
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
        if (_keysAreInternal) return InternalKeyOrdering.Compare(_ucmp, _bytewise, a, b);
        return _bytewise ? a.SequenceCompareTo(b) : _ucmp.Compare(a, b);
    }

    private bool UserKeyEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => _bytewise ? a.SequenceEqual(b) : _ucmp.Equal(a, b);

    /// <summary>Decodes the entry header at <paramref name="p"/>; returns the key-delta offset or -1 on error. Port of DecodeEntry / DecodeEntryV4.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int DecodeEntry(int p, int limit, out uint shared, out uint nonShared, out uint valueLength)
    {
        byte[] d = _data;
        if (_valueDeltaEncoded)
        {
            valueLength = 0;
            if (limit - p < 3) { shared = nonShared = 0; return -1; }
            shared = d[p];
            nonShared = d[p + 1];
            if ((shared | nonShared) < 128) return p + 2;
            return DecodeVarints(p, limit, 2, out shared, out nonShared, out _);
        }
        if (limit - p < 3) { shared = nonShared = valueLength = 0; return -1; }
        shared = d[p];
        nonShared = d[p + 1];
        valueLength = d[p + 2];
        if ((shared | nonShared | valueLength) < 128) return p + 3;
        return DecodeVarints(p, limit, 3, out shared, out nonShared, out valueLength);
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
        _current = _next;
        int limit = _restarts;
        if (_current >= limit)
        {
            Invalidate();
            return false;
        }
        int p = DecodeEntry(_current, limit, out uint shared, out uint nonShared, out uint valueLength);
        if (p < 0 || (uint)_keyLen < shared) return CorruptionError();
        ulong entryLength = (ulong)nonShared + valueLength;
        if (entryLength > (ulong)(limit - p)) return CorruptionError();

        if (shared == 0)
        {
            // Key stored whole: point into the block (no copy).
            _keyInBuf = false;
            _keyBlockOffset = p;
            _keyLen = (int)nonShared;
            // Keep restart index in sync.
            while (_restartIndex + 1 < _numRestarts && GetRestartPoint(_restartIndex + 1) < (uint)_current) _restartIndex++;
        }
        else
        {
            int newLen = (int)(shared + nonShared);
            EnsureKeyCapacity(newLen);
            if (!_keyInBuf)
            {
                Buffer.BlockCopy(_data, _keyBlockOffset, _keyBuf, 0, (int)shared);
                _keyInBuf = true;
            }
            Buffer.BlockCopy(_data, p, _keyBuf, (int)shared, (int)nonShared);
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
        if (!IndexValue.TryDecode(ref v, _haveFirstKey, hasPrevious, _handle, out BlockHandle h, out int fko, out int fkl))
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
        return new ReadOnlySpan<byte>(_data, p, (int)nonShared);
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
        _keyLen = 0;
        _keyInBuf = true;
    }
}
