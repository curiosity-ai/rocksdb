using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.TrieIndex;

/// <summary>
/// Port of LoudsTrie (utilities/trie_index/louds_trie.{h,cc}): the deserialized, immutable trie. InitFromData validates
/// the serialized form with upstream's Corruption messages. The arrays are decoded into typed managed arrays (the C++
/// reader aliases the block bytes, re-aligning them when needed), so lookups are endian-neutral and bounds-checked.
/// Thread-safe for concurrent iterators.
/// </summary>
internal sealed class LoudsTrie
{
    private const ulong MaxReasonableKeys = 1UL << 30;
    private const uint MaxReasonableDepth = 65536;

    internal ulong NumKeysValue;
    internal uint CutoffLevelValue;
    internal uint MaxDepthValue;
    internal bool HasSeqno;
    internal ulong DenseLeafCount;
    internal ulong DenseNodeCount;
    internal ulong DenseChildCount;

    internal Bitvector DLabels = null!, DHasChild = null!, DIsPrefixKey = null!;
    internal byte[] SLabels = [];
    internal ulong SLabelsSize;
    internal Bitvector SHasChild = null!, SLouds = null!, SIsPrefixKey = null!;
    internal uint[] SChildStartPos = [];
    internal uint[] SChildEndPos = [];

    internal Bitvector? SChainBitmap;
    internal uint[] SChainSuffixOffsets = [];
    internal ushort[] SChainLens = [];
    internal uint[] SChainEndChildIdx = [];
    internal byte[] SChainSuffix = [];

    private uint[] _handleOffsets = [];
    private uint[] _handleSizes = [];
    private ulong[] _leafSeqnos = [];
    private uint[] _leafBlockCounts = [];
    private uint[] _overflowOffsets = [];
    private uint[] _overflowSizes = [];
    private ulong[] _overflowSeqnos = [];
    private uint[] _overflowBase = [];
    private uint _numOverflowBlocks;

    public ulong NumKeys => NumKeysValue;
    public uint CutoffLevel => CutoffLevelValue;
    public uint MaxDepth => MaxDepthValue;
    public bool HasSeqnoEncoding => HasSeqno;
    public bool HasChains => SChainLens.Length > 0;
    public uint NumOverflowBlocks => _numOverflowBlocks;

    /// <summary>
    /// Heap bytes of the decoded structures. Upstream's ApproximateAuxMemoryUsage counts only the child position tables
    /// because its other arrays alias the block; here every array is a decoded copy.
    /// </summary>
    public ulong ApproximateMemoryUsage
    {
        get
        {
            ulong m = DLabels.MemoryUsage + DHasChild.MemoryUsage + DIsPrefixKey.MemoryUsage + SHasChild.MemoryUsage + SLouds.MemoryUsage
                      + SIsPrefixKey.MemoryUsage + (SChainBitmap?.MemoryUsage ?? 0) + (ulong)SLabels.Length + (ulong)SChainSuffix.Length;
            m += ((ulong)SChildStartPos.Length + (ulong)SChildEndPos.Length + (ulong)SChainSuffixOffsets.Length + (ulong)SChainEndChildIdx.Length
                  + (ulong)_handleOffsets.Length + (ulong)_handleSizes.Length + (ulong)_leafBlockCounts.Length + (ulong)_overflowOffsets.Length
                  + (ulong)_overflowSizes.Length + (ulong)_overflowBase.Length) * 4;
            m += (ulong)SChainLens.Length * 2 + ((ulong)_leafSeqnos.Length + (ulong)_overflowSeqnos.Length) * 8;
            return m;
        }
    }

    /// <summary>InitFromData: parses the serialized trie (throws Corruption on malformed data).</summary>
    public static LoudsTrie Read(ReadOnlySpan<byte> data)
    {
        var t = new LoudsTrie();
        if (data.Length < TrieFormat.HeaderSize) ThrowHelper.Corruption("Trie index: data too short for header");
        if (Coding.DecodeFixed32(data) != TrieFormat.Magic) ThrowHelper.Corruption("Trie index: bad magic number");
        if (Coding.DecodeFixed32(data[4..]) != TrieFormat.FormatVersion) ThrowHelper.Corruption("Trie index: unsupported format version");
        t.NumKeysValue = Coding.DecodeFixed64(data[8..]);
        if (t.NumKeysValue > MaxReasonableKeys) ThrowHelper.Corruption("Trie index: num_keys exceeds reasonable limit");
        t.CutoffLevelValue = Coding.DecodeFixed32(data[16..]);
        t.MaxDepthValue = Coding.DecodeFixed32(data[20..]);
        if (t.MaxDepthValue > MaxReasonableDepth) ThrowHelper.Corruption("Trie index: max_depth exceeds reasonable limit");
        t.DenseLeafCount = Coding.DecodeFixed64(data[24..]);
        t.DenseNodeCount = Coding.DecodeFixed64(data[32..]);
        t.DenseChildCount = Coding.DecodeFixed64(data[40..]);
        uint flags = Coding.DecodeFixed32(data[48..]);
        t.HasSeqno = (flags & TrieFormat.FlagSeqnoEncoding) != 0;
        int p = TrieFormat.HeaderSize;

        t.DLabels = Bitvector.Read(data, ref p);
        t.DHasChild = Bitvector.Read(data, ref p);
        t.DIsPrefixKey = Bitvector.Read(data, ref p);
        if (t.DenseNodeCount > 0)
        {
            if (t.DLabels.NumBits != t.DenseNodeCount * 256) ThrowHelper.Corruption("Trie index: d_labels size inconsistent with dense_node_count");
            if (t.DHasChild.NumBits != t.DLabels.NumOnes) ThrowHelper.Corruption("Trie index: d_has_child size inconsistent with d_labels");
            if (t.DIsPrefixKey.NumBits != t.DenseNodeCount) ThrowHelper.Corruption("Trie index: d_is_prefix_key size inconsistent with dense_node_count");
        }
        if (t.DenseLeafCount > t.NumKeysValue) ThrowHelper.Corruption("Trie index: dense_leaf_count exceeds num_keys");

        if ((ulong)(data.Length - p) < 8) ThrowHelper.Corruption("Trie index: truncated sparse labels size");
        t.SLabelsSize = Coding.DecodeFixed64(data[p..]);
        p += 8;
        if (t.SLabelsSize > 0)
        {
            if ((ulong)(data.Length - p) < t.SLabelsSize) ThrowHelper.Corruption("Trie index: truncated sparse labels");
            t.SLabels = data.Slice(p, (int)t.SLabelsSize).ToArray();
            p += (int)t.SLabelsSize;
            ulong padding = (8 - t.SLabelsSize % 8) % 8;
            if ((ulong)(data.Length - p) < padding) ThrowHelper.Corruption("Trie index: truncated sparse label padding");
            p += (int)padding;
        }
        t.SHasChild = Bitvector.Read(data, ref p);
        t.SLouds = Bitvector.Read(data, ref p);
        t.SIsPrefixKey = Bitvector.Read(data, ref p);
        if (t.SLabelsSize > 0)
        {
            if (t.SHasChild.NumBits != t.SLabelsSize) ThrowHelper.Corruption("Trie index: s_has_child size inconsistent with s_labels_size");
            if (t.SLouds.NumBits != t.SLabelsSize) ThrowHelper.Corruption("Trie index: s_louds size inconsistent with s_labels_size");
        }

        if ((ulong)(data.Length - p) < 8) ThrowHelper.Corruption("Trie index: truncated child position table count");
        ulong numInternal = Coding.DecodeFixed64(data[p..]);
        p += 8;
        if (numInternal > 1UL << 30) ThrowHelper.Corruption("Trie index: num_internal exceeds reasonable limit");
        if (numInternal > 0)
        {
            ulong padded = TrieBits.Pad8(numInternal * 4);
            if ((ulong)(data.Length - p) < padded) ThrowHelper.Corruption("Trie index: truncated child start position table");
            t.SChildStartPos = TrieBits.ReadUInt32s(data[p..], (int)numInternal);
            p += (int)padded;
            if ((ulong)(data.Length - p) < padded) ThrowHelper.Corruption("Trie index: truncated child end position table");
            t.SChildEndPos = TrieBits.ReadUInt32s(data[p..], (int)numInternal);
            p += (int)padded;
            for (int k = 0; k < (int)numInternal; k++)
            {
                if (t.SChildStartPos[k] >= t.SLabelsSize || t.SChildEndPos[k] > t.SLabelsSize || t.SChildEndPos[k] < t.SChildStartPos[k])
                    ThrowHelper.Corruption("Trie index: child position out of range for sparse labels");
            }
        }

        if ((ulong)(data.Length - p) < 8) ThrowHelper.Corruption("Trie index: truncated chain count");
        ulong numChains = Coding.DecodeFixed64(data[p..]);
        p += 8;
        if (numChains > 0)
        {
            t.SChainBitmap = Bitvector.Read(data, ref p);
            if (numChains > 1UL << 30) ThrowHelper.Corruption("Trie index: truncated chain offsets");
            ulong offPadded = TrieBits.Pad8(numChains * 4);
            if ((ulong)(data.Length - p) < offPadded) ThrowHelper.Corruption("Trie index: truncated chain offsets");
            t.SChainSuffixOffsets = TrieBits.ReadUInt32s(data[p..], (int)numChains);
            p += (int)offPadded;
            ulong lensPadded = TrieBits.Pad8(numChains * 2);
            if ((ulong)(data.Length - p) < lensPadded) ThrowHelper.Corruption("Trie index: truncated chain lengths");
            t.SChainLens = TrieBits.ReadUInt16s(data[p..], (int)numChains);
            p += (int)lensPadded;
            if ((ulong)(data.Length - p) < offPadded) ThrowHelper.Corruption("Trie index: truncated chain end indices");
            t.SChainEndChildIdx = TrieBits.ReadUInt32s(data[p..], (int)numChains);
            p += (int)offPadded;
            foreach (uint idx in t.SChainEndChildIdx)
                if (idx != uint.MaxValue && idx >= numInternal) ThrowHelper.Corruption("Trie index: chain end child index out of range");
            if ((ulong)(data.Length - p) < 8) ThrowHelper.Corruption("Trie index: truncated chain suffix size");
            ulong suffixSize = Coding.DecodeFixed64(data[p..]);
            p += 8;
            ulong suffixPadded = TrieBits.Pad8(suffixSize);
            if (suffixSize > int.MaxValue || (ulong)(data.Length - p) < suffixPadded) ThrowHelper.Corruption("Trie index: truncated chain suffix data");
            t.SChainSuffix = data.Slice(p, (int)suffixSize).ToArray();
            p += (int)suffixPadded;
            for (int ci = 0; ci < (int)numChains; ci++)
                if ((ulong)t.SChainSuffixOffsets[ci] + t.SChainLens[ci] > suffixSize)
                    ThrowHelper.Corruption("Trie index: chain suffix offset + length exceeds suffix data");
        }

        int n = (int)t.NumKeysValue;
        if (n > 0)
        {
            ulong arrPadded = TrieBits.Pad8((ulong)n * 4);
            if ((ulong)(data.Length - p) < arrPadded) ThrowHelper.Corruption("Trie index: truncated handle offsets");
            t._handleOffsets = TrieBits.ReadUInt32s(data[p..], n);
            p += (int)arrPadded;
            if ((ulong)(data.Length - p) < arrPadded) ThrowHelper.Corruption("Trie index: truncated handle sizes");
            t._handleSizes = TrieBits.ReadUInt32s(data[p..], n);
            p += (int)arrPadded;
        }

        if (t.HasSeqno && n > 0)
        {
            if ((ulong)(data.Length - p) < 8) ThrowHelper.Corruption("Trie index: truncated seqno side-table header");
            t._numOverflowBlocks = Coding.DecodeFixed32(data[p..]);
            p += 8;
            if ((ulong)(data.Length - p) < (ulong)n * 8) ThrowHelper.Corruption("Trie index: truncated leaf seqnos");
            t._leafSeqnos = TrieBits.ReadUInt64s(data[p..], n);
            p += n * 8;
            ulong countsPadded = TrieBits.Pad8((ulong)n * 4);
            if ((ulong)(data.Length - p) < countsPadded) ThrowHelper.Corruption("Trie index: truncated leaf block counts");
            t._leafBlockCounts = TrieBits.ReadUInt32s(data[p..], n);
            p += (int)countsPadded;
            uint no = t._numOverflowBlocks;
            if (no > 0)
            {
                if (no > MaxReasonableKeys) ThrowHelper.Corruption("Trie index: num_overflow_blocks exceeds reasonable limit");
                ulong opadded = TrieBits.Pad8((ulong)no * 4);
                if ((ulong)(data.Length - p) < opadded) ThrowHelper.Corruption("Trie index: truncated overflow offsets");
                t._overflowOffsets = TrieBits.ReadUInt32s(data[p..], (int)no);
                p += (int)opadded;
                if ((ulong)(data.Length - p) < opadded) ThrowHelper.Corruption("Trie index: truncated overflow sizes");
                t._overflowSizes = TrieBits.ReadUInt32s(data[p..], (int)no);
                p += (int)opadded;
                if ((ulong)(data.Length - p) < (ulong)no * 8) ThrowHelper.Corruption("Trie index: truncated overflow seqnos");
                t._overflowSeqnos = TrieBits.ReadUInt64s(data[p..], (int)no);
            }
            t._overflowBase = new uint[n];
            uint running = 0;
            for (int i = 0; i < n; i++)
            {
                if (t._leafBlockCounts[i] == 0) ThrowHelper.Corruption("Trie index: leaf block count is zero for leaf " + i.ToString(System.Globalization.CultureInfo.InvariantCulture));
                t._overflowBase[i] = running;
                running += t._leafBlockCounts[i] - 1;
            }
            if (running != no) ThrowHelper.Corruption("Trie index: overflow count mismatch with leaf block counts");
        }
        return t;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TrieBlockHandle GetHandle(ulong leafIndex) => new(_handleOffsets[(int)leafIndex], _handleSizes[(int)leafIndex]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong GetLeafSeqno(ulong leafIndex) => _leafSeqnos[(int)leafIndex];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint GetLeafBlockCount(ulong leafIndex) => _leafBlockCounts[(int)leafIndex];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint GetOverflowBase(ulong leafIndex) => _overflowBase[(int)leafIndex];

    public TrieBlockHandle GetOverflowHandle(uint i) => new(_overflowOffsets[i], _overflowSizes[i]);

    public ulong GetOverflowSeqno(uint i) => _overflowSeqnos[i];
}
