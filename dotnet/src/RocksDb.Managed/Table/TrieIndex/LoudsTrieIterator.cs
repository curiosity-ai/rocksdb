using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table.TrieIndex;

/// <summary>
/// Port of LoudsTrieIterator (utilities/trie_index/louds_trie.{h,cc}): bidirectional iteration over the trie's leaves
/// (Seek / SeekToFirst / Next, SeekToLast / Prev) with key reconstruction from the root-to-leaf path. The SuRF-style
/// Select-free sparse traversal and the path-compression chain skip are ported; SeekImpl's kHasChains template is a
/// generic struct parameter so the JIT drops the chain code for tries without chains. Not thread-safe; reusable across
/// tries via <see cref="Bind"/> (allocation-free once its buffers are large enough).
/// </summary>
internal sealed class LoudsTrieIterator
{
    private const ulong DenseFlag = 1UL << 63;

    private LoudsTrie _t = null!;
    private bool _hasChains;
    private bool _valid;
    private ulong _leafIndex;
    private byte[] _keyBuf = [];
    private int _keyLen;
    private ulong[] _path = [];
    private int _pathLen;
    private bool _isAtPrefixKey;

    private struct WithChains;
    private struct NoChains;

    public LoudsTrieIterator(LoudsTrie trie) => Bind(trie);

    /// <summary>Re-targets the iterator at <paramref name="trie"/> (unpositioned).</summary>
    public void Bind(LoudsTrie trie)
    {
        _t = trie;
        _hasChains = trie.HasChains;
        int cap = (int)trie.MaxDepth + 1;
        if (_keyBuf.Length < cap) _keyBuf = new byte[cap];
        if (_path.Length < cap) _path = new ulong[cap];
        Reset();
    }

    public bool Valid => _valid;
    public ReadOnlySpan<byte> Key => _keyBuf.AsSpan(0, _keyLen);
    public ulong LeafIndex => _leafIndex;
    public TrieBlockHandle Value => _t.GetHandle(_leafIndex);

    private void Reset()
    {
        _valid = false;
        _leafIndex = 0;
        _keyLen = 0;
        _pathLen = 0;
        _isAtPrefixKey = false;
    }

    // ---- path / key helpers ----

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Push(ulong posAndFlag, byte label)
    {
        if ((uint)_keyLen >= (uint)_keyBuf.Length) ThrowDepth();
        _path[_pathLen++] = posAndFlag;
        _keyBuf[_keyLen++] = label;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowDepth() => ThrowHelper.Corruption("Trie index: trie depth exceeds max_depth");

    private void Pop()
    {
        _pathLen--;
        if (_keyLen > 0) _keyLen--;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PushDense(ulong pos) => Push(pos | DenseFlag, (byte)(pos % 256));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PushSparse(ulong pos) => Push(pos, _t.SLabels[(int)pos]);

    // ---- dense helpers ----

    private bool DenseSeekLabel(ulong nodeNum, byte target, out ulong outPos)
    {
        Bitvector dl = _t.DLabels;
        ulong pos = nodeNum * 256 + target;
        if (pos < dl.NumBits && dl.GetBit(pos))
        {
            outPos = pos;
            return true;
        }
        ulong nodeEnd = (nodeNum + 1) * 256;
        ulong next = dl.NextSetBit(pos + 1);
        outPos = next < nodeEnd && next < dl.NumBits ? next : nodeEnd;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong DenseChildNodeNumFromRank(ulong labelRank) => _t.DHasChild.Rank1(labelRank + 1);

    private ulong DenseLeafIndexFromRank(ulong pos, ulong labelRank) =>
        DenseLeafIndexFromRankAndHasChildRank(pos, labelRank, _t.DHasChild.Rank1(labelRank + 1));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong DenseLeafIndexFromRankAndHasChildRank(ulong pos, ulong labelRank, ulong hasChildRank)
    {
        ulong leafLabels = labelRank + 1 - hasChildRank;
        if (_t.DIsPrefixKey.NumOnes == 0) return leafLabels - 1;
        return _t.DIsPrefixKey.Rank1(pos / 256 + 1) + leafLabels - 1;
    }

    private ulong DensePrefixKeyLeafIndex(ulong nodeNum)
    {
        ulong labelsBefore = _t.DLabels.Rank1(nodeNum * 256);
        ulong internalBefore = _t.DHasChild.Rank1(labelsBefore);
        return _t.DIsPrefixKey.Rank1(nodeNum) + labelsBefore - internalBefore;
    }

    // ---- sparse helpers ----

    private bool SparseSeekLabel(ulong start, ulong end, byte target, out ulong outPos)
    {
        ReadOnlySpan<byte> labels = _t.SLabels.AsSpan((int)start, (int)(end - start));
        int i;
        if (labels.Length <= 16)
        {
            i = 0;
            while (i < labels.Length && labels[i] < target) i++;
        }
        else
        {
            int lo = 0, hi = labels.Length;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (labels[mid] < target) lo = mid + 1;
                else hi = mid;
            }
            i = lo;
        }
        if (i == labels.Length)
        {
            outPos = end;
            return false;
        }
        outPos = start + (ulong)i;
        return labels[i] == target;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong SparseChildNodeNum(ulong pos) => _t.DenseChildCount + _t.SHasChild.Rank1(pos + 1) - 1;

    private ulong SparseLeafIndex(ulong pos) => SparseLeafIndexFromHasChildRank(pos, _t.SHasChild.Rank1(pos + 1));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong SparseLeafIndexFromHasChildRank(ulong pos, ulong hasChildRank)
    {
        ulong leafLabels = pos + 1 - hasChildRank;
        if (_t.SIsPrefixKey.NumOnes == 0) return _t.DenseLeafCount + leafLabels - 1;
        ulong node = SparseNodeNum(pos);
        return _t.DenseLeafCount + _t.SIsPrefixKey.Rank1(node + 1) + leafLabels - 1;
    }

    private ulong SparsePrefixKeyLeafIndex(ulong sparseNode)
    {
        ulong start = SparseNodeStartPos(sparseNode);
        ulong prefixBefore = _t.SIsPrefixKey.Rank1(sparseNode);
        ulong internalBefore = _t.SHasChild.Rank1(start);
        return _t.DenseLeafCount + prefixBefore + start - internalBefore;
    }

    private ulong SparseNodeNum(ulong pos) => _t.SLouds.Rank1(pos + 1) - 1;

    private ulong SparseNodeStartPos(ulong sparseNode)
    {
        if (sparseNode == 0) return 0;
        uint[] starts = _t.SChildStartPos;
        if (sparseNode >= _t.DenseChildCount && starts.Length > 0)
        {
            ulong idx = sparseNode - _t.DenseChildCount;
            if (idx < (ulong)starts.Length) return starts[(int)idx];
        }
        return _t.SLouds.FindNthOneBit(sparseNode);
    }

    private ulong SparseNodeEndPos(ulong start)
    {
        ulong next = _t.SLouds.NextSetBit(start + 1);
        return next >= _t.SLouds.NumBits ? _t.SLabelsSize : next;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsDensePrefixKey(ulong nodeNum)
    {
        Bitvector b = _t.DIsPrefixKey;
        return b.NumBits > 0 && nodeNum < b.NumBits && b.GetBit(nodeNum);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsSparsePrefixKey(ulong nodeNum)
    {
        Bitvector b = _t.SIsPrefixKey;
        return b.NumBits > 0 && nodeNum < b.NumBits && b.GetBit(nodeNum);
    }

    private bool SetLeaf(ulong leafIndex)
    {
        _leafIndex = leafIndex;
        _valid = true;
        return true;
    }

    private bool Invalid()
    {
        _valid = false;
        return false;
    }

    // ---- traversal ----

    private bool DescendToLeftmostLeaf(bool inDense, ulong nodeNum)
    {
        LoudsTrie t = _t;
        while (true)
        {
            if (inDense)
            {
                if (IsDensePrefixKey(nodeNum))
                {
                    _isAtPrefixKey = true;
                    return SetLeaf(DensePrefixKeyLeafIndex(nodeNum));
                }
                ulong b = nodeNum * 256;
                if (b >= t.DLabels.NumBits) return Invalid();
                ulong first = t.DLabels.NextSetBit(b);
                if (first >= b + 256 || first >= t.DLabels.NumBits) return Invalid();
                PushDense(first);
                ulong labelRank = t.DLabels.Rank1(first + 1) - 1;
                if (!t.DHasChild.GetBit(labelRank)) return SetLeaf(DenseLeafIndexFromRank(first, labelRank));
                ulong child = DenseChildNodeNumFromRank(labelRank);
                if (child < t.DenseNodeCount)
                {
                    nodeNum = child;
                }
                else
                {
                    nodeNum = child - t.DenseNodeCount;
                    inDense = false;
                }
            }
            else
            {
                if (IsSparsePrefixKey(nodeNum))
                {
                    _isAtPrefixKey = true;
                    return SetLeaf(SparsePrefixKeyLeafIndex(nodeNum));
                }
                ulong start = SparseNodeStartPos(nodeNum);
                if (start >= t.SLabelsSize) return Invalid();
                PushSparse(start);
                if (!t.SHasChild.GetBit(start)) return SetLeaf(SparseLeafIndex(start));
                nodeNum = SparseChildNodeNum(start);
            }
        }
    }

    /// <summary>SeekToFirst: the leftmost leaf.</summary>
    public bool SeekToFirst()
    {
        Reset();
        if (_t.NumKeys == 0) return false;
        return DescendToLeftmostLeaf(_t.CutoffLevel > 0, 0);
    }

    /// <summary>Seek: the first leaf whose key is &gt;= <paramref name="target"/>.</summary>
    public bool Seek(ReadOnlySpan<byte> target) => _hasChains ? SeekImpl<WithChains>(target) : SeekImpl<NoChains>(target);

    private bool SeekImpl<TChains>(ReadOnlySpan<byte> target) where TChains : struct
    {
        Reset();
        LoudsTrie t = _t;
        if (t.NumKeys == 0) return false;
        bool inDense = t.CutoffLevel > 0;
        ulong nodeNum = 0;
        ulong sparseStart = 0, sparseEnd = 0;
        bool haveSparseBounds = false;
        int targetLen = target.Length;
        for (int depth = 0; depth < targetLen; depth++)
        {
            byte targetByte = target[depth];
            if (inDense)
            {
                bool exact = DenseSeekLabel(nodeNum, targetByte, out ulong pos);
                ulong nodeEnd = (nodeNum + 1) * 256;
                if (pos >= nodeEnd) return Advance();
                PushDense(pos);
                ulong labelRank = t.DLabels.Rank1(pos + 1) - 1;
                bool isInternal = t.DHasChild.GetBit(labelRank);
                ulong hasChildRank = t.DHasChild.Rank1(labelRank + 1);
                if (!exact)
                {
                    if (!isInternal) return SetLeaf(DenseLeafIndexFromRankAndHasChildRank(pos, labelRank, hasChildRank));
                    ulong c = hasChildRank;
                    bool cd = c < t.DenseNodeCount;
                    return DescendToLeftmostLeaf(cd, cd ? c : c - t.DenseNodeCount);
                }
                if (!isInternal)
                {
                    SetLeaf(DenseLeafIndexFromRankAndHasChildRank(pos, labelRank, hasChildRank));
                    if (depth == targetLen - 1) return true;
                    return Advance();
                }
                ulong child = hasChildRank;
                if (child < t.DenseNodeCount)
                {
                    nodeNum = child;
                }
                else
                {
                    nodeNum = child - t.DenseNodeCount;
                    inDense = false;
                    haveSparseBounds = false;
                }
            }
            else
            {
                ulong start, end;
                if (haveSparseBounds)
                {
                    start = sparseStart;
                    end = sparseEnd;
                }
                else
                {
                    start = SparseNodeStartPos(nodeNum);
                    end = SparseNodeEndPos(start);
                }

                ulong pos;
                bool exact;
                if (start + 1 == end)
                {
                    // Fanout-1 fast path.
                    byte label = t.SLabels[(int)start];
                    PushSparse(start);
                    if (label != targetByte)
                    {
                        if (label > targetByte)
                        {
                            if (!t.SHasChild.GetBit(start)) return SetLeaf(SparseLeafIndexFromHasChildRank(start, t.SHasChild.Rank1(start + 1)));
                            return DescendToLeftmostLeaf(false, SparseChildNodeNum(start));
                        }
                        return Advance();
                    }
                    pos = start;
                    exact = true;
                }
                else
                {
                    exact = SparseSeekLabel(start, end, targetByte, out pos);
                    if (pos >= end) return Advance();
                    PushSparse(pos);
                }

                bool internalLabel = t.SHasChild.GetBit(pos);
                ulong hcr = t.SHasChild.Rank1(pos + 1);
                if (!exact)
                {
                    if (!internalLabel) return SetLeaf(SparseLeafIndexFromHasChildRank(pos, hcr));
                    return DescendToLeftmostLeaf(false, SparseChildNodeNum(pos));
                }
                if (!internalLabel)
                {
                    SetLeaf(SparseLeafIndexFromHasChildRank(pos, hcr));
                    if (depth == targetLen - 1) return true;
                    return Advance();
                }

                ulong childIdx = hcr - 1;
                uint[] starts = t.SChildStartPos;
                if (starts.Length > 0 && childIdx < (ulong)starts.Length)
                {
                    if (typeof(TChains) == typeof(WithChains))
                    {
                        Bitvector cb = t.SChainBitmap!;
                        if (childIdx < cb.NumBits && cb.GetBit(childIdx))
                        {
                            int r = ChainSeek(target, ref depth, childIdx, cb, out bool cont, ref sparseStart, ref sparseEnd);
                            if (!cont) return r != 0;
                            haveSparseBounds = true;
                            inDense = false;
                            continue;
                        }
                    }
                    sparseStart = starts[(int)childIdx];
                    sparseEnd = t.SChildEndPos[(int)childIdx];
                    haveSparseBounds = true;
                }
                else
                {
                    nodeNum = SparseChildNodeNum(pos);
                    haveSparseBounds = false;
                }
                inDense = false;
            }
        }

        // Target consumed: prefix key at the current node, else its leftmost leaf.
        if (inDense)
        {
            if (IsDensePrefixKey(nodeNum))
            {
                _isAtPrefixKey = true;
                return SetLeaf(DensePrefixKeyLeafIndex(nodeNum));
            }
        }
        else
        {
            if (haveSparseBounds) nodeNum = t.SLouds.Rank1(sparseStart + 1) - 1;
            if (IsSparsePrefixKey(nodeNum))
            {
                _isAtPrefixKey = true;
                return SetLeaf(SparsePrefixKeyLeafIndex(nodeNum));
            }
        }
        return DescendToLeftmostLeaf(inDense, nodeNum);
    }

    /// <summary>
    /// The path-compression chain skip of SeekImpl (both the fanout-1 and the general path, which are identical
    /// upstream): <paramref name="childIdx"/> is the internal-label index whose child starts a chain. Returns with
    /// <paramref name="cont"/> true when the chain fully matched and ended at an internal node (the seek continues at
    /// <paramref name="sparseStart"/>..<paramref name="sparseEnd"/>); otherwise the seek's result (1 = positioned).
    /// </summary>
    private int ChainSeek(ReadOnlySpan<byte> target, ref int depth, ulong childIdx, Bitvector chainBitmap, out bool cont, ref ulong sparseStart,
        ref ulong sparseEnd)
    {
        LoudsTrie t = _t;
        cont = false;
        uint[] starts = t.SChildStartPos;
        int chainIdx = (int)(chainBitmap.Rank1(childIdx + 1) - 1);
        int chainLen = t.SChainLens[chainIdx];
        ReadOnlySpan<byte> suffix = t.SChainSuffix.AsSpan((int)t.SChainSuffixOffsets[chainIdx], chainLen);
        int targetRemaining = target.Length - depth - 1;
        ReadOnlySpan<byte> tb = target[(depth + 1)..];
        if (targetRemaining >= chainLen)
        {
            int mismatch = tb[..chainLen].CommonPrefixLength(suffix);
            if (mismatch == chainLen)
            {
                ulong cur = childIdx;
                for (int ci = 0; ci < chainLen; ci++)
                {
                    uint cs = starts[(int)cur];
                    PushSparse(cs);
                    if (ci + 1 < chainLen) cur = t.SHasChild.Rank1((ulong)cs + 1) - 1;
                }
                depth += chainLen;
                uint endChild = t.SChainEndChildIdx[chainIdx];
                if (endChild == uint.MaxValue)
                {
                    uint lastCs = starts[(int)cur];
                    SetLeaf(SparseLeafIndexFromHasChildRank(lastCs, t.SHasChild.Rank1((ulong)lastCs + 1)));
                    if (depth == target.Length - 1) return 1;
                    return Advance() ? 1 : 0;
                }
                sparseStart = starts[(int)endChild];
                sparseEnd = t.SChildEndPos[(int)endChild];
                cont = true;
                return 0;
            }
            ulong c2 = PushChainPrefix(childIdx, mismatch);
            uint misCs = starts[(int)c2];
            PushSparse(misCs);
            if (tb[mismatch] < suffix[mismatch])
            {
                if (!t.SHasChild.GetBit(misCs)) return SetLeaf(SparseLeafIndex(misCs)) ? 1 : 0;
                return DescendToLeftmostLeaf(false, SparseChildNodeNum(misCs)) ? 1 : 0;
            }
            return Advance() ? 1 : 0;
        }
        if (targetRemaining > 0)
        {
            int cmp = tb[..targetRemaining].SequenceCompareTo(suffix[..targetRemaining]);
            if (cmp < 0)
            {
                uint cs = starts[(int)childIdx];
                PushSparse(cs);
                if (!t.SHasChild.GetBit(cs)) return SetLeaf(SparseLeafIndex(cs)) ? 1 : 0;
                return DescendToLeftmostLeaf(false, SparseChildNodeNum(cs)) ? 1 : 0;
            }
            if (cmp > 0)
            {
                int mp = tb[..targetRemaining].CommonPrefixLength(suffix);
                ulong c3 = PushChainPrefix(childIdx, mp);
                PushSparse(starts[(int)c3]);
                return Advance() ? 1 : 0;
            }
            // Target consumed mid-chain.
            ulong cur = childIdx;
            for (int ci = 0; ci < targetRemaining; ci++)
            {
                uint cs = starts[(int)cur];
                PushSparse(cs);
                if (ci + 1 < targetRemaining) cur = t.SHasChild.Rank1((ulong)cs + 1) - 1;
            }
            uint lastCs2 = starts[(int)cur];
            return DescendToLeftmostLeaf(false, SparseChildNodeNum(lastCs2)) ? 1 : 0;
        }
        // Target ends at the parent node: the first chain node is > target.
        uint first = starts[(int)childIdx];
        PushSparse(first);
        if (!t.SHasChild.GetBit(first)) return SetLeaf(SparseLeafIndex(first)) ? 1 : 0;
        return DescendToLeftmostLeaf(false, SparseChildNodeNum(first)) ? 1 : 0;
    }

    /// <summary>Pushes the first <paramref name="count"/> chain nodes; returns the internal-label index of the next one.</summary>
    private ulong PushChainPrefix(ulong childIdx, int count)
    {
        uint[] starts = _t.SChildStartPos;
        ulong cur = childIdx;
        for (int ci = 0; ci < count; ci++)
        {
            uint cs = starts[(int)cur];
            PushSparse(cs);
            cur = _t.SHasChild.Rank1((ulong)cs + 1) - 1;
        }
        return cur;
    }

    /// <summary>Next: the next leaf in key order.</summary>
    public bool Next()
    {
        if (!_valid) return false;
        if (_isAtPrefixKey)
        {
            _isAtPrefixKey = false;
            LoudsTrie t = _t;
            bool inDense;
            ulong nodeNum;
            if (_pathLen == 0)
            {
                inDense = t.CutoffLevel > 0;
                nodeNum = 0;
            }
            else
            {
                ulong last = _path[_pathLen - 1];
                if ((last & DenseFlag) != 0)
                {
                    ulong lr = t.DLabels.Rank1((last & ~DenseFlag) + 1) - 1;
                    ulong child = DenseChildNodeNumFromRank(lr);
                    inDense = child < t.DenseNodeCount;
                    nodeNum = inDense ? child : child - t.DenseNodeCount;
                }
                else
                {
                    nodeNum = SparseChildNodeNum(last);
                    inDense = false;
                }
            }
            if (inDense)
            {
                ulong b = nodeNum * 256;
                if (b >= t.DLabels.NumBits) return Advance();
                ulong first = t.DLabels.NextSetBit(b);
                if (first >= b + 256 || first >= t.DLabels.NumBits) return Advance();
                PushDense(first);
                ulong labelRank = t.DLabels.Rank1(first + 1) - 1;
                if (!t.DHasChild.GetBit(labelRank)) return SetLeaf(DenseLeafIndexFromRank(first, labelRank));
                ulong child = DenseChildNodeNumFromRank(labelRank);
                bool cd = child < t.DenseNodeCount;
                return DescendToLeftmostLeaf(cd, cd ? child : child - t.DenseNodeCount);
            }
            ulong start = SparseNodeStartPos(nodeNum);
            if (start >= t.SLabelsSize) return Advance();
            PushSparse(start);
            if (!t.SHasChild.GetBit(start)) return SetLeaf(SparseLeafIndex(start));
            return DescendToLeftmostLeaf(false, SparseChildNodeNum(start));
        }
        return Advance();
    }

    private bool Advance()
    {
        LoudsTrie t = _t;
        while (_pathLen > 0)
        {
            ulong cur = _path[_pathLen - 1];
            if ((cur & DenseFlag) != 0)
            {
                ulong curPos = cur & ~DenseFlag;
                ulong nodeEnd = (curPos / 256 + 1) * 256;
                ulong next = t.DLabels.NextSetBit(curPos + 1);
                if (next < nodeEnd && next < t.DLabels.NumBits)
                {
                    _path[_pathLen - 1] = next | DenseFlag;
                    _keyBuf[_keyLen - 1] = (byte)(next % 256);
                    ulong labelRank = t.DLabels.Rank1(next + 1) - 1;
                    if (!t.DHasChild.GetBit(labelRank)) return SetLeaf(DenseLeafIndexFromRank(next, labelRank));
                    ulong child = DenseChildNodeNumFromRank(labelRank);
                    bool cd = child < t.DenseNodeCount;
                    return DescendToLeftmostLeaf(cd, cd ? child : child - t.DenseNodeCount);
                }
            }
            else
            {
                ulong nextPos = cur + 1;
                if (nextPos < t.SLabelsSize && !t.SLouds.GetBit(nextPos))
                {
                    _path[_pathLen - 1] = nextPos;
                    _keyBuf[_keyLen - 1] = t.SLabels[(int)nextPos];
                    if (!t.SHasChild.GetBit(nextPos)) return SetLeaf(SparseLeafIndex(nextPos));
                    return DescendToLeftmostLeaf(false, SparseChildNodeNum(nextPos));
                }
            }
            Pop();
        }
        return Invalid();
    }

    private bool DescendToRightmostLeaf(bool inDense, ulong nodeNum)
    {
        LoudsTrie t = _t;
        ulong sparseEnd = 0;
        bool haveSparseEnd = false;
        while (true)
        {
            if (inDense)
            {
                ulong nodeEnd = Math.Min((nodeNum + 1) * 256, t.DLabels.NumBits);
                ulong b = nodeNum * 256;
                if (b >= t.DLabels.NumBits) return Invalid();
                ulong last = t.DLabels.PrevSetBit(nodeEnd);
                if (last >= t.DLabels.NumBits || last < b) return Invalid();
                PushDense(last);
                ulong labelRank = t.DLabels.Rank1(last + 1) - 1;
                if (!t.DHasChild.GetBit(labelRank)) return SetLeaf(DenseLeafIndexFromRank(last, labelRank));
                ulong child = DenseChildNodeNumFromRank(labelRank);
                if (child < t.DenseNodeCount)
                {
                    nodeNum = child;
                }
                else
                {
                    nodeNum = child - t.DenseNodeCount;
                    inDense = false;
                }
            }
            else
            {
                ulong end;
                if (haveSparseEnd)
                {
                    end = sparseEnd;
                    haveSparseEnd = false;
                }
                else
                {
                    ulong start = SparseNodeStartPos(nodeNum);
                    if (start >= t.SLabelsSize)
                    {
                        // A trie holding only the empty key (the label-less root's prefix key, see LoudsTrieBuilder).
                        if (nodeNum == 0 && t.SLabelsSize == 0 && _pathLen == 0 && IsSparsePrefixKey(0))
                        {
                            _isAtPrefixKey = true;
                            return SetLeaf(SparsePrefixKeyLeafIndex(0));
                        }
                        return Invalid();
                    }
                    end = SparseNodeEndPos(start);
                }
                ulong lastPos = end - 1;
                PushSparse(lastPos);
                if (!t.SHasChild.GetBit(lastPos)) return SetLeaf(SparseLeafIndex(lastPos));
                ulong childIdx = t.SHasChild.Rank1(lastPos + 1) - 1;
                if (childIdx < (ulong)t.SChildEndPos.Length)
                {
                    sparseEnd = t.SChildEndPos[(int)childIdx];
                    haveSparseEnd = true;
                }
                nodeNum = t.DenseChildCount + childIdx;
            }
        }
    }

    /// <summary>SeekToLast: the rightmost leaf.</summary>
    public bool SeekToLast()
    {
        Reset();
        if (_t.NumKeys == 0) return false;
        return DescendToRightmostLeaf(_t.CutoffLevel > 0, 0);
    }

    private bool Retreat()
    {
        LoudsTrie t = _t;
        while (_pathLen > 0)
        {
            ulong cur = _path[_pathLen - 1];
            if ((cur & DenseFlag) != 0)
            {
                ulong curPos = cur & ~DenseFlag;
                ulong nodeNum = curPos / 256;
                ulong nodeBase = nodeNum * 256;
                ulong prev = t.DLabels.PrevSetBit(curPos);
                if (prev < t.DLabels.NumBits && prev >= nodeBase)
                {
                    _path[_pathLen - 1] = prev | DenseFlag;
                    _keyBuf[_keyLen - 1] = (byte)(prev % 256);
                    ulong labelRank = t.DLabels.Rank1(prev + 1) - 1;
                    if (!t.DHasChild.GetBit(labelRank)) return SetLeaf(DenseLeafIndexFromRank(prev, labelRank));
                    ulong child = DenseChildNodeNumFromRank(labelRank);
                    bool cd = child < t.DenseNodeCount;
                    return DescendToRightmostLeaf(cd, cd ? child : child - t.DenseNodeCount);
                }
                if (IsDensePrefixKey(nodeNum))
                {
                    Pop();
                    _isAtPrefixKey = true;
                    return SetLeaf(DensePrefixKeyLeafIndex(nodeNum));
                }
            }
            else
            {
                if (!t.SLouds.GetBit(cur))
                {
                    ulong prevPos = cur - 1;
                    _path[_pathLen - 1] = prevPos;
                    _keyBuf[_keyLen - 1] = t.SLabels[(int)prevPos];
                    if (!t.SHasChild.GetBit(prevPos)) return SetLeaf(SparseLeafIndex(prevPos));
                    return DescendToRightmostLeaf(false, SparseChildNodeNum(prevPos));
                }
                ulong sparseNode = SparseNodeNum(cur);
                if (IsSparsePrefixKey(sparseNode))
                {
                    Pop();
                    _isAtPrefixKey = true;
                    return SetLeaf(SparsePrefixKeyLeafIndex(sparseNode));
                }
            }
            Pop();
        }
        return Invalid();
    }

    /// <summary>Prev: the previous leaf in key order.</summary>
    public bool Prev()
    {
        if (!_valid) return false;
        _isAtPrefixKey = false;
        return Retreat();
    }
}
