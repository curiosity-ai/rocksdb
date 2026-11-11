using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.TrieIndex;

/// <summary>TrieBlockHandle (utilities/trie_index/louds_trie.h): offset and size of a data block.</summary>
internal readonly record struct TrieBlockHandle(ulong Offset, ulong Size);

/// <summary>Constants of the serialized trie (utilities/trie_index/louds_trie.cc).</summary>
internal static class TrieFormat
{
    /// <summary>kTrieFormatVersion.</summary>
    public const uint FormatVersion = 1;
    /// <summary>kTrieMagic: "TRIE" in ASCII.</summary>
    public const uint Magic = 0x54524945;
    /// <summary>kFlagSeqnoEncoding.</summary>
    public const uint FlagSeqnoEncoding = 1u << 0;
    /// <summary>Header size: magic(4) + version(4) + num_keys(8) + cutoff(4) + max_depth(4) + 3 x u64 + flags(4) + reserved(4).</summary>
    public const int HeaderSize = 56;
    /// <summary>kMinChainLength of SerializeAll.</summary>
    public const int MinChainLength = 8;
    /// <summary>kPerChainMetaBytes (offset u32 + len u16 + end_child_idx u32).</summary>
    public const ulong PerChainMetaBytes = 10;
    /// <summary>kChainBudgetPct.</summary>
    public const double ChainBudgetPct = 0.10;
}

/// <summary>
/// Port of LoudsTrieBuilder (utilities/trie_index/louds_trie.{h,cc}): builds a LOUDS-Dense / LOUDS-Sparse trie from
/// sorted separator keys and serializes it, byte for byte like the C++ builder: header, dense bitvectors, sparse labels
/// and bitvectors, child position tables, path-compression chains, the BFS-ordered handle arrays and (with seqno
/// encoding) the seqno side-table.
/// </summary>
internal sealed class LoudsTrieBuilder : IDisposable
{
    // Input keys (concatenated) and their handles / side-table data, in key order.
    private readonly PooledBufferWriter _keyBytes = new(1024);
    private readonly List<int> _keyOffsets = new() { 0 };
    private List<TrieBlockHandle> _handles = new();
    private List<ulong> _seqnos = new();
    private List<uint> _blockCounts = new();
    private List<TrieBlockHandle> _overflowHandles = new();
    private List<ulong> _overflowSeqnos = new();

    private uint _cutoffLevel;
    private uint _maxDepth;
    private BitvectorBuilder _dLabels = new(), _dHasChild = new(), _dIsPrefixKey = new();
    private readonly List<byte> _sLabels = new();
    private BitvectorBuilder _sHasChild = new(), _sLouds = new(), _sIsPrefixKey = new();
    private ulong _denseLeafCount, _denseNodeCount, _denseChildCount;
    private bool _hasSeqnoEncoding;
    private readonly PooledBufferWriter _out = new(1024);

    public int NumKeys => _keyOffsets.Count - 1;

    private ReadOnlySpan<byte> Key(int i) => _keyBytes.WrittenSpan[_keyOffsets[i].._keyOffsets[i + 1]];

    /// <summary>AddKey: a separator (ascending order) and its block handle.</summary>
    public void AddKey(ReadOnlySpan<byte> key, TrieBlockHandle handle)
    {
        _keyBytes.Write(key);
        _keyOffsets.Add(_keyBytes.WrittenCount);
        _handles.Add(handle);
    }

    /// <summary>AddKeyWithSeqno: a separator with its side-table seqno (packed tag) and run length.</summary>
    public void AddKeyWithSeqno(ReadOnlySpan<byte> key, TrieBlockHandle handle, ulong seqno, uint blockCount)
    {
        AddKey(key, handle);
        _seqnos.Add(seqno);
        _blockCounts.Add(blockCount);
    }

    /// <summary>AddOverflowBlock: the next block of the most recently added key's run.</summary>
    public void AddOverflowBlock(TrieBlockHandle handle, ulong seqno)
    {
        _overflowHandles.Add(handle);
        _overflowSeqnos.Add(seqno);
    }

    public void SetHasSeqnoEncoding(bool value) => _hasSeqnoEncoding = value;

    /// <summary>GetSerializedData (valid after <see cref="Finish"/> until disposal).</summary>
    public ReadOnlySpan<byte> SerializedData => _out.WrittenSpan;

    private static uint Lcp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => (uint)a.CommonPrefixLength(b);

    /// <summary>ComputeCutoffLevel: the first level where sparse encoding is cheaper than dense.</summary>
    private uint ComputeCutoffLevel()
    {
        int n = NumKeys;
        if (n == 0) return 0;
        var nodesPerLevel = new ulong[_maxDepth + 1];
        var labelsPerLevel = new ulong[_maxDepth + 1];
        nodesPerLevel[0] = 1;
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<byte> cur = Key(i);
            uint lcp = i > 0 ? Lcp(Key(i - 1), cur) : 0;
            uint keyLen = (uint)cur.Length;
            for (uint l = lcp + 1; l <= keyLen && l <= _maxDepth; l++) nodesPerLevel[l]++;
            uint labelStart = i == 0 ? 0 : lcp;
            for (uint l = labelStart; l < keyLen && l <= _maxDepth; l++) labelsPerLevel[l]++;
        }
        for (uint l = 0; l <= _maxDepth; l++)
        {
            ulong nodes = nodesPerLevel[l], labels = labelsPerLevel[l];
            if (nodes == 0) return l;
            ulong dense = nodes * 257 + labels;
            ulong sparse = labels * 10 + nodes;
            if (sparse < dense) return l;
        }
        return _maxDepth + 1;
    }

    /// <summary>PerLevelData of LoudsTrieBuilder::Finish: flat per-level label and node arrays.</summary>
    private sealed class PerLevelData
    {
        public readonly List<byte> Labels = new();
        public readonly List<bool> HasChild = new();
        public readonly List<long> LeafHandle = new();
        public readonly List<bool> IsPrefix = new();
        public readonly List<long> PrefixHandle = new();
        public readonly List<int> NodeLabelStart = new();

        public int NodeCount => IsPrefix.Count;

        public void StartNode()
        {
            NodeLabelStart.Add(Labels.Count);
            IsPrefix.Add(false);
            PrefixHandle.Add(-1);
        }

        public void AddLabel(byte label, bool isInternal, long handle)
        {
            Labels.Add(label);
            HasChild.Add(isInternal);
            LeafHandle.Add(isInternal ? -1 : handle);
        }
    }

    /// <summary>Finish: builds the trie from the added keys and serializes it.</summary>
    public void Finish()
    {
        int n = NumKeys;
        if (n == 0)
        {
            _cutoffLevel = 0;
            _maxDepth = 0;
            _denseLeafCount = _denseNodeCount = _denseChildCount = 0;
            SerializeAll();
            return;
        }
        _maxDepth = 0;
        for (int i = 0; i < n; i++) _maxDepth = Math.Max(_maxDepth, (uint)Key(i).Length);
        _cutoffLevel = Math.Min(ComputeCutoffLevel(), _maxDepth + 1);

        var levels = new PerLevelData[_maxDepth + 1];
        for (int i = 0; i < levels.Length; i++) levels[i] = new PerLevelData();
        levels[0].StartNode();
        for (int ki = 0; ki < n; ki++)
        {
            ReadOnlySpan<byte> key = Key(ki);
            uint k = (uint)key.Length;
            uint lcp = ki > 0 ? Lcp(Key(ki - 1), key) : 0;
            if (k == 0 && ki == 0)
            {
                // Deviation from upstream (which adds no label for a zero-length key and so loses its handle, see
                // LoudsTrieBuilder::Finish): the empty key (a data block ending with the empty user key) is the
                // root's prefix key. The format and the iterators support a prefix key on any node, the root included.
                levels[0].IsPrefix[0] = true;
                levels[0].PrefixHandle[0] = ki;
                continue;
            }
            // Step 1: retroactive internal marking (leaf -> internal) with handle migration to a prefix-key child.
            if (lcp > 0)
            {
                PerLevelData pl = levels[lcp - 1];
                if (pl.HasChild.Count > 0 && !pl.HasChild[^1])
                {
                    pl.HasChild[^1] = true;
                    long h = pl.LeafHandle[^1];
                    pl.LeafHandle[^1] = -1;
                    if (lcp <= _maxDepth)
                    {
                        levels[lcp].StartNode();
                        levels[lcp].IsPrefix[^1] = true;
                        levels[lcp].PrefixHandle[^1] = h;
                    }
                }
            }
            // Step 2: labels for levels lcp..K-1.
            for (uint l = lcp; l < k; l++)
            {
                bool isInternal = l + 1 < k;
                if (l != lcp) levels[l].StartNode();
                levels[l].AddLabel(key[(int)l], isInternal, isInternal ? -1 : ki);
            }
        }

        _dLabels = new BitvectorBuilder();
        _dHasChild = new BitvectorBuilder();
        _dIsPrefixKey = new BitvectorBuilder();
        _sLabels.Clear();
        _sHasChild = new BitvectorBuilder();
        _sLouds = new BitvectorBuilder();
        _sIsPrefixKey = new BitvectorBuilder();
        _denseNodeCount = _denseChildCount = _denseLeafCount = 0;

        var bfsHandles = new List<TrieBlockHandle>(n);
        var bfsSeqnos = new List<ulong>(_hasSeqnoEncoding ? n : 0);
        var bfsBlockCounts = new List<uint>(_hasSeqnoEncoding ? n : 0);
        uint[]? keySortedOverflowBase = null;
        var bfsOverflowHandles = new List<TrieBlockHandle>();
        var bfsOverflowSeqnos = new List<ulong>();
        if (_hasSeqnoEncoding && _overflowHandles.Count > 0)
        {
            keySortedOverflowBase = new uint[n];
            uint sum = 0;
            for (int i = 0; i < n; i++)
            {
                keySortedOverflowBase[i] = sum;
                sum += _blockCounts[i] - 1;
            }
        }

        void EmitLeaf(int ki)
        {
            bfsHandles.Add(_handles[ki]);
            if (!_hasSeqnoEncoding) return;
            bfsSeqnos.Add(_seqnos[ki]);
            bfsBlockCounts.Add(_blockCounts[ki]);
            if (keySortedOverflowBase is not null && _blockCounts[ki] > 1)
            {
                uint b = keySortedOverflowBase[ki];
                for (uint j = 0; j < _blockCounts[ki] - 1; j++)
                {
                    bfsOverflowHandles.Add(_overflowHandles[(int)(b + j)]);
                    bfsOverflowSeqnos.Add(_overflowSeqnos[(int)(b + j)]);
                }
            }
        }

        Span<ulong> bitmap = stackalloc ulong[4];
        for (uint level = 0; level <= _maxDepth; level++)
        {
            PerLevelData ld = levels[level];
            if (ld.NodeCount == 0) continue;
            bool isDense = level < _cutoffLevel;
            for (int ni = 0; ni < ld.NodeCount; ni++)
            {
                int labelStart = ld.NodeLabelStart[ni];
                int labelEnd = ni + 1 < ld.NodeCount ? ld.NodeLabelStart[ni + 1] : ld.Labels.Count;
                if (ld.IsPrefix[ni] && ld.PrefixHandle[ni] >= 0) EmitLeaf((int)ld.PrefixHandle[ni]);
                if (labelStart == labelEnd)
                {
                    if (ld.IsPrefix[ni] && isDense) _denseLeafCount++;
                    // A trie holding only the empty key: the label-less root still records its prefix bit (always
                    // sparse: ComputeCutoffLevel returns 0 for a level without labels).
                    if (level == 0 && ld.IsPrefix[ni] && !isDense) _sIsPrefixKey.Append(true);
                    continue;
                }
                if (isDense)
                {
                    _denseNodeCount++;
                    _dIsPrefixKey.Append(ld.IsPrefix[ni]);
                    bitmap.Clear();
                    for (int li = labelStart; li < labelEnd; li++)
                    {
                        byte label = ld.Labels[li];
                        bitmap[label / 64] |= 1UL << (label % 64);
                    }
                    for (int w = 0; w < 4; w++) _dLabels.AppendWord(bitmap[w], 64);
                    for (int li = labelStart; li < labelEnd; li++)
                    {
                        bool isInternal = ld.HasChild[li];
                        _dHasChild.Append(isInternal);
                        if (!isInternal)
                        {
                            if (ld.LeafHandle[li] >= 0) EmitLeaf((int)ld.LeafHandle[li]);
                            _denseLeafCount++;
                        }
                        else if (level == _cutoffLevel - 1)
                        {
                            _denseChildCount++;
                        }
                    }
                    if (ld.IsPrefix[ni]) _denseLeafCount++;
                }
                else
                {
                    _sIsPrefixKey.Append(ld.IsPrefix[ni]);
                    bool first = true;
                    for (int li = labelStart; li < labelEnd; li++)
                    {
                        bool isInternal = ld.HasChild[li];
                        _sLabels.Add(ld.Labels[li]);
                        _sHasChild.Append(isInternal);
                        _sLouds.Append(first);
                        first = false;
                        if (!isInternal && ld.LeafHandle[li] >= 0) EmitLeaf((int)ld.LeafHandle[li]);
                    }
                }
            }
        }
        if (_cutoffLevel == 0) _denseChildCount = 1;
        if (bfsHandles.Count != n) ThrowHelper.InvalidArgument("Trie index: keys must be distinct and sorted");
        _handles = bfsHandles;
        if (_hasSeqnoEncoding)
        {
            _seqnos = bfsSeqnos;
            _blockCounts = bfsBlockCounts;
            if (_overflowHandles.Count > 0)
            {
                _overflowHandles = bfsOverflowHandles;
                _overflowSeqnos = bfsOverflowSeqnos;
            }
        }
        SerializeAll();
    }

    private void SerializeAll()
    {
        PooledBufferWriter o = _out;
        o.Clear();
        Coding.PutFixed32(o, TrieFormat.Magic);
        Coding.PutFixed32(o, TrieFormat.FormatVersion);
        Coding.PutFixed64(o, (ulong)NumKeys);
        Coding.PutFixed32(o, _cutoffLevel);
        Coding.PutFixed32(o, _maxDepth);
        Coding.PutFixed64(o, _denseLeafCount);
        Coding.PutFixed64(o, _denseNodeCount);
        Coding.PutFixed64(o, _denseChildCount);
        Coding.PutFixed32(o, _hasSeqnoEncoding ? TrieFormat.FlagSeqnoEncoding : 0);
        Coding.PutFixed32(o, 0);

        Bitvector.BuildFrom(_dLabels).EncodeTo(o);
        Bitvector.BuildFrom(_dHasChild).EncodeTo(o);
        Bitvector.BuildFrom(_dIsPrefixKey).EncodeTo(o);

        ulong slSize = (ulong)_sLabels.Count;
        Coding.PutFixed64(o, slSize);
        if (slSize > 0)
        {
            Span<byte> dst = o.GetSpan(_sLabels.Count);
            for (int i = 0; i < _sLabels.Count; i++) dst[i] = _sLabels[i];
            o.Advance(_sLabels.Count);
            TrieBits.WritePadding(o, slSize);
        }
        Bitvector sHasChild = Bitvector.BuildFrom(_sHasChild);
        sHasChild.EncodeTo(o);
        Bitvector sLouds = Bitvector.BuildFrom(_sLouds);
        sLouds.EncodeTo(o);
        Bitvector sIsPrefixKey = Bitvector.BuildFrom(_sIsPrefixKey);
        sIsPrefixKey.EncodeTo(o);

        // Child position lookup tables.
        ulong numInternal = sHasChild.NumOnes;
        Coding.PutFixed64(o, numInternal);
        var childStarts = new uint[numInternal];
        var childEnds = new uint[numInternal];
        bool tables = numInternal > 0 && slSize <= uint.MaxValue;
        if (tables)
        {
            for (ulong k = 0; k < numInternal; k++)
            {
                ulong cs = sLouds.FindNthOneBit(_denseChildCount + k);
                ulong ce = sLouds.NextSetBit(cs + 1);
                if (ce > slSize) ce = slSize;
                childStarts[k] = (uint)cs;
                childEnds[k] = (uint)ce;
            }
            TrieBits.WriteUInt32sPadded(o, childStarts);
            TrieBits.WriteUInt32sPadded(o, childEnds);
        }

        // Path compression: fanout-1 chains.
        var suffixData = new List<byte>();
        var chainOffsets = new uint[numInternal];
        var chainLens = new ushort[numInternal];
        var chainEndChildIdx = new uint[numInternal];
        Array.Fill(chainOffsets, uint.MaxValue);
        Array.Fill(chainEndChildIdx, uint.MaxValue);
        var suffix = new List<byte>();
        for (ulong k = 0; k < numInternal; k++)
        {
            uint cs = childStarts[k], ce = childEnds[k];
            if (ce - cs != 1) continue;
            if (!sHasChild.GetBit(cs)) continue;
            ulong childNode = sLouds.Rank1((ulong)cs + 1) - 1;
            if (childNode < sIsPrefixKey.NumBits && sIsPrefixKey.GetBit(childNode)) continue;
            suffix.Clear();
            suffix.Add(_sLabels[(int)cs]);
            ulong cur = sHasChild.Rank1((ulong)cs + 1) - 1;
            uint lastChildIdx = uint.MaxValue;
            while (true)
            {
                if (cur >= numInternal) break;
                uint ncs = childStarts[cur], nce = childEnds[cur];
                if (nce - ncs != 1)
                {
                    lastChildIdx = (uint)cur;
                    break;
                }
                ulong nextNode = sLouds.Rank1((ulong)ncs + 1) - 1;
                if (nextNode < sIsPrefixKey.NumBits && sIsPrefixKey.GetBit(nextNode))
                {
                    lastChildIdx = (uint)cur;
                    break;
                }
                suffix.Add(_sLabels[(int)ncs]);
                if (!sHasChild.GetBit(ncs))
                {
                    lastChildIdx = uint.MaxValue;
                    break;
                }
                cur = sHasChild.Rank1((ulong)ncs + 1) - 1;
            }
            if (suffix.Count >= TrieFormat.MinChainLength && suffix.Count <= ushort.MaxValue)
            {
                chainOffsets[k] = (uint)suffixData.Count;
                chainLens[k] = (ushort)suffix.Count;
                chainEndChildIdx[k] = lastChildIdx;
                suffixData.AddRange(suffix);
            }
        }

        // Chain cost/benefit filter.
        ulong candidateCount = 0;
        for (ulong k = 0; k < numInternal; k++) if (chainLens[k] > 0) candidateCount++;
        if (candidateCount == 0 || candidateCount > (ulong)_handles.Count)
        {
            Array.Clear(chainLens);
        }
        else
        {
            ulong baseSize = (ulong)o.WrittenCount;
            ulong budget = (ulong)(baseSize * TrieFormat.ChainBudgetPct);
            ulong bitmapFixedCost = (numInternal + 7) / 8 + (numInternal / 512 + 1) * 8;
            if (budget <= bitmapFixedCost)
            {
                Array.Clear(chainLens);
            }
            else
            {
                ulong available = budget - bitmapFixedCost;
                ulong total = 0;
                for (ulong k = 0; k < numInternal; k++) if (chainLens[k] > 0) total += TrieFormat.PerChainMetaBytes + chainLens[k];
                if (total > available) ApplyChainBudget(available, candidateCount, chainOffsets, chainLens, chainEndChildIdx, ref suffixData);
            }
        }

        ulong numChains = 0;
        var chainBitmap = new BitvectorBuilder();
        for (ulong k = 0; k < numInternal; k++)
        {
            bool has = chainLens[k] > 0;
            chainBitmap.Append(has);
            if (has) numChains++;
        }
        Coding.PutFixed64(o, numChains);
        if (numChains > 0)
        {
            Bitvector.BuildFrom(chainBitmap).EncodeTo(o);
            for (ulong k = 0; k < numInternal; k++) if (chainLens[k] > 0) Coding.PutFixed32(o, chainOffsets[k]);
            TrieBits.WritePadding(o, numChains * 4);
            for (ulong k = 0; k < numInternal; k++) if (chainLens[k] > 0) Coding.PutFixed16(o, chainLens[k]);
            TrieBits.WritePadding(o, numChains * 2);
            for (ulong k = 0; k < numInternal; k++) if (chainLens[k] > 0) Coding.PutFixed32(o, chainEndChildIdx[k]);
            TrieBits.WritePadding(o, numChains * 4);
            Coding.PutFixed64(o, (ulong)suffixData.Count);
            Span<byte> dst = o.GetSpan(suffixData.Count);
            for (int i = 0; i < suffixData.Count; i++) dst[i] = suffixData[i];
            o.Advance(suffixData.Count);
            TrieBits.WritePadding(o, (ulong)suffixData.Count);
        }

        // Block handles (BFS leaf order): packed u32 offsets, then sizes.
        if (_handles.Count > 0)
        {
            foreach (TrieBlockHandle h in _handles) Coding.PutFixed32(o, (uint)h.Offset);
            TrieBits.WritePadding(o, (ulong)_handles.Count * 4);
            foreach (TrieBlockHandle h in _handles) Coding.PutFixed32(o, (uint)h.Size);
            TrieBits.WritePadding(o, (ulong)_handles.Count * 4);
        }

        // Seqno side-table.
        if (_hasSeqnoEncoding)
        {
            uint numOverflow = (uint)_overflowHandles.Count;
            Coding.PutFixed32(o, numOverflow);
            Coding.PutFixed32(o, 0);
            for (int i = 0; i < _handles.Count; i++) Coding.PutFixed64(o, _seqnos[i]);
            for (int i = 0; i < _handles.Count; i++) Coding.PutFixed32(o, _blockCounts[i]);
            TrieBits.WritePadding(o, (ulong)_handles.Count * 4);
            foreach (TrieBlockHandle h in _overflowHandles) Coding.PutFixed32(o, (uint)h.Offset);
            if (numOverflow > 0) TrieBits.WritePadding(o, (ulong)numOverflow * 4);
            foreach (TrieBlockHandle h in _overflowHandles) Coding.PutFixed32(o, (uint)h.Size);
            if (numOverflow > 0) TrieBits.WritePadding(o, (ulong)numOverflow * 4);
            foreach (ulong s in _overflowSeqnos) Coding.PutFixed64(o, s);
        }
    }

    /// <summary>
    /// The over-budget branch of the chain filter: keeps the longest chains first while they fit. Upstream sorts with
    /// std::sort (unstable); ties are kept here in internal-label order (a stable sort), so a trie whose chain metadata
    /// exceeds the budget with equal-length chains straddling the cut can differ from native (both remain valid tries).
    /// </summary>
    private static void ApplyChainBudget(ulong available, ulong candidateCount, uint[] chainOffsets, ushort[] chainLens, uint[] chainEndChildIdx,
        ref List<byte> suffixData)
    {
        var candidates = new List<int>((int)candidateCount);
        for (int k = 0; k < chainLens.Length; k++) if (chainLens[k] > 0) candidates.Add(k);
        int[] order = candidates.ToArray();
        ushort[] lens = chainLens;
        // Stable descending by length.
        var keys = new int[order.Length];
        for (int i = 0; i < order.Length; i++) keys[i] = -lens[order[i]];
        var idx = new int[order.Length];
        for (int i = 0; i < idx.Length; i++) idx[i] = i;
        Array.Sort(idx, (a, b) => keys[a] != keys[b] ? keys[a].CompareTo(keys[b]) : a.CompareTo(b));
        var keep = new bool[chainLens.Length];
        ulong used = 0;
        foreach (int i in idx)
        {
            int k = order[i];
            ulong cost = TrieFormat.PerChainMetaBytes + chainLens[k];
            if (used + cost <= available)
            {
                keep[k] = true;
                used += cost;
            }
        }
        var newSuffix = new List<byte>();
        for (int k = 0; k < chainLens.Length; k++)
        {
            if (chainLens[k] > 0 && !keep[k])
            {
                chainLens[k] = 0;
                chainOffsets[k] = uint.MaxValue;
                chainEndChildIdx[k] = uint.MaxValue;
            }
            else if (chainLens[k] > 0)
            {
                uint old = chainOffsets[k];
                chainOffsets[k] = (uint)newSuffix.Count;
                for (int j = 0; j < chainLens[k]; j++) newSuffix.Add(suffixData[(int)old + j]);
            }
        }
        suffixData = newSuffix;
    }

    public void Dispose()
    {
        _keyBytes.Dispose();
        _out.Dispose();
    }
}
