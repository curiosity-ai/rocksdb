using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.TrieIndex;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// The built-in trie user-defined index ("trie_index"): a Fast Succinct Trie (LOUDS-Dense / LOUDS-Sparse, after SuRF)
/// over the data-block separators of block-based tables, stored in the meta block
/// <c>rocksdb.user_defined_index.trie_index</c>. Port of TrieIndexFactory (utilities/trie_index/trie_index_factory.{h,cc});
/// files are interchangeable with native RocksDB's trie index (the same factory name and serialized format). EXPERIMENTAL
/// upstream.
/// <para>
/// Usage: set <see cref="BlockBasedTableOptions.UserDefinedIndexFactory"/> to a <see cref="TrieIndexFactory"/> and pick
/// <see cref="BlockBasedTableOptions.IndexMode"/>: <see cref="BlockBasedIndexMode.StandardDefault"/> builds both indexes
/// and reads through the trie only with <see cref="ReadIndex.PreferCustom"/>; <see cref="BlockBasedIndexMode.CustomDefault"/>
/// and <see cref="BlockBasedIndexMode.CustomOnly"/> read through the trie by default.
/// </para>
/// <para>
/// Limitations, as upstream: the trie requires the bytewise comparator (NotSupported otherwise) and stores data-block
/// offsets and sizes as 32-bit integers, so building a table whose blocks lie beyond 4 GiB fails with NotSupported.
/// </para>
/// </summary>
public sealed class TrieIndexFactory : UserDefinedIndexFactory
{
    /// <summary>kClassName: "trie_index".</summary>
    public const string ClassName = "trie_index";

    /// <summary>A shared instance (the factory is stateless).</summary>
    public static TrieIndexFactory Instance { get; } = new();

    public override string Name => ClassName;

    /// <summary>NewBuilder: NotSupported unless the comparator is bytewise.</summary>
    public override UserDefinedIndexBuilder NewBuilder(UserDefinedIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        CheckComparator(options.Comparator);
        return new TrieIndexBuilder();
    }

    /// <summary>NewReader: parses the serialized trie (Corruption when malformed).</summary>
    public override UserDefinedIndexReader NewReader(UserDefinedIndexOptions options, ReadOnlyMemory<byte> indexContents)
    {
        ArgumentNullException.ThrowIfNull(options);
        CheckComparator(options.Comparator);
        return new TrieIndexReader(LoudsTrie.Read(indexContents.Span));
    }

    private static void CheckComparator(Comparator? comparator)
    {
        if (comparator is not null && !ReferenceEquals(comparator, Comparator.Bytewise))
            ThrowHelper.NotSupported("TrieIndexFactory requires BytewiseComparator; got: " + comparator.Name);
    }
}

/// <summary>
/// Port of TrieIndexBuilder (trie_index_factory.cc): buffers (separator, tag, handle) per data block, then de-duplicates
/// same-separator runs into trie leaves plus overflow blocks and serializes the trie with its seqno side-table.
/// Separators are the bytewise FindShortestSeparator of the boundary user keys; the last block keeps its last key.
/// The parallel-compression protocol (PrepareAddEntry / FinishAddEntry) is not ported: tables with a custom index are
/// built single-threaded (see <see cref="UserDefinedIndexBuilder"/>).
/// </summary>
internal sealed class TrieIndexBuilder : UserDefinedIndexBuilder
{
    private readonly struct Entry(int offset, int length, ulong tag, TrieBlockHandle handle)
    {
        public readonly int Offset = offset;
        public readonly int Length = length;
        public readonly ulong Tag = tag;
        public readonly TrieBlockHandle Handle = handle;
    }

    private readonly PooledBufferWriter _separators = new(1024);
    private readonly List<Entry> _entries = new();
    private readonly LoudsTrieBuilder _trie = new();
    private bool _finished;
    private bool _hasUnsupportedHandle;
    private ulong _estimatedTrieEdges;
    private ulong _estimatedNumEntries;

    private ReadOnlySpan<byte> Separator(in Entry e) => _separators.WrittenSpan.Slice(e.Offset, e.Length);

    public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNextBlock,
        BlockHandle blockHandle, in UserDefinedIndexEntryContext context)
    {
        bool sameUserKey = false;
        int offset = _separators.WrittenCount;
        bool havePrev = _entries.Count > 0;
        if (hasNextBlock)
        {
            sameUserKey = lastKeyInCurrentBlock.SequenceEqual(firstKeyInNextBlock);
            Comparator.Bytewise.FindShortestSeparator(lastKeyInCurrentBlock, firstKeyInNextBlock, _separators);
            // FindShortestSeparator may fail to shorten: a separator equal to the previous one joins its run.
            if (!sameUserKey && havePrev && Separator(_entries[^1]).SequenceEqual(_separators.WrittenSpan[offset..])) sameUserKey = true;
        }
        else
        {
            // Last block: the last key itself (not a short successor, which could widen the block's key range).
            _separators.Write(lastKeyInCurrentBlock);
            if (havePrev && Separator(_entries[^1]).SequenceEqual(_separators.WrittenSpan[offset..])) sameUserKey = true;
        }
        int length = _separators.WrittenCount - offset;
        // Same-user-key boundaries and the last block carry the real tag; other separators the 0 sentinel.
        ulong tag = sameUserKey || !hasNextBlock ? context.LastKeyTag : 0;
        var handle = new TrieBlockHandle(blockHandle.Offset, blockHandle.Size);
        _hasUnsupportedHandle |= handle.Offset > uint.MaxValue || handle.Size > uint.MaxValue;
        ReadOnlySpan<byte> sep = _separators.WrittenSpan.Slice(offset, length);
        // UpdateSizeEstimate: adjacent separators charge their common prefix once.
        int shared = havePrev ? Separator(_entries[^1]).CommonPrefixLength(sep) : 0;
        _estimatedTrieEdges += (ulong)(length - shared);
        _estimatedNumEntries++;
        _entries.Add(new Entry(offset, length, tag, handle));
        return sep;
    }

    public override ReadOnlySpan<byte> Finish()
    {
        if (_finished) ThrowHelper.InvalidArgument("TrieIndexBuilder::Finish called twice");
        _finished = true;
        if (_hasUnsupportedHandle) ThrowHelper.NotSupported("Trie index block offset or size exceeds the 32-bit encoding limit");
        bool useSeqno = _entries.Count > 0;
        _trie.SetHasSeqnoEncoding(useSeqno);
        int i = 0;
        while (i < _entries.Count)
        {
            Entry e = _entries[i];
            ReadOnlySpan<byte> sep = Separator(e);
            int runEnd = i + 1;
            while (runEnd < _entries.Count && Separator(_entries[runEnd]).SequenceEqual(sep)) runEnd++;
            _trie.AddKeyWithSeqno(sep, e.Handle, e.Tag, (uint)(runEnd - i));
            for (int j = i + 1; j < runEnd; j++) _trie.AddOverflowBlock(_entries[j].Handle, _entries[j].Tag);
            i = runEnd;
        }
        _entries.Clear();
        _entries.TrimExcess();
        _trie.Finish();
        return _trie.SerializedData;
    }

    /// <summary>
    /// EstimatedSize: an upper bound of the serialized size (1 KiB + 12 bytes per distinct trie edge + 20 per entry), or
    /// the actual size once finished.
    /// </summary>
    public override ulong EstimatedSize =>
        _finished ? (ulong)_trie.SerializedData.Length : 1024 + _estimatedTrieEdges * 12 + _estimatedNumEntries * 20;

    public override void Dispose()
    {
        _separators.Dispose();
        _trie.Dispose();
    }
}

/// <summary>Port of TrieIndexReader (trie_index_factory.cc): the deserialized trie; iterators are pooled per thread.</summary>
internal sealed class TrieIndexReader : UserDefinedIndexReader
{
    internal LoudsTrie Trie { get; }

    public TrieIndexReader(LoudsTrie trie) => Trie = trie;

    public override UserDefinedIndexIterator NewIterator(ReadOptions? readOptions) => TrieIndexIterator.Rent(Trie);

    /// <summary>The decoded trie's heap usage (the serialized block is owned by the table reader).</summary>
    public override ulong ApproximateMemoryUsage => Trie.ApproximateMemoryUsage;

    public override void Dispose() { }
}

/// <summary>
/// Port of TrieIndexIterator (trie_index_factory.cc): wraps <see cref="LoudsTrieIterator"/> and walks same-user-key
/// overflow runs with the seqno side-table (post-seek correction by the target's packed tag). Scan bounds (Prepare /
/// CheckBounds) are not ported, so every position is in bound. Instances are recycled through a per-thread slot when
/// disposed, so creating one per lookup does not allocate in steady state.
/// </summary>
internal sealed class TrieIndexIterator : UserDefinedIndexIterator
{
    [ThreadStatic] private static TrieIndexIterator? t_cached;

    private LoudsTrie _trie;
    private readonly LoudsTrieIterator _iter;
    private bool _hasSeqno;
    private bool _rented;
    private uint _overflowRunIndex;
    private uint _overflowRunSize = 1;
    private uint _overflowBaseIdx;

    private TrieIndexIterator(LoudsTrie trie)
    {
        _trie = trie;
        _iter = new LoudsTrieIterator(trie);
        _hasSeqno = trie.HasSeqnoEncoding;
    }

    internal static TrieIndexIterator Rent(LoudsTrie trie)
    {
        TrieIndexIterator? it = t_cached;
        if (it is null)
        {
            it = new TrieIndexIterator(trie);
        }
        else
        {
            t_cached = null;
            it._trie = trie;
            it._iter.Bind(trie);
            it._hasSeqno = trie.HasSeqnoEncoding;
            it.ResetOverflowState();
        }
        it._rented = true;
        return it;
    }

    public override void Dispose()
    {
        if (!_rented) return;
        _rented = false;
        t_cached = this;
    }

    private void ResetOverflowState()
    {
        _overflowRunIndex = 0;
        _overflowRunSize = 1;
        _overflowBaseIdx = 0;
    }

    private void SetupOverflowForCurrentLeaf(bool positionAtLast)
    {
        if (!_hasSeqno || !_iter.Valid) return;
        ulong leaf = _iter.LeafIndex;
        uint count = _trie.GetLeafBlockCount(leaf);
        _overflowRunSize = count;
        _overflowBaseIdx = _trie.GetOverflowBase(leaf);
        if (positionAtLast) _overflowRunIndex = count - 1;
    }

    public override bool SeekToFirst()
    {
        ResetOverflowState();
        if (!_iter.SeekToFirst()) return false;
        SetupOverflowForCurrentLeaf(false);
        return true;
    }

    public override bool SeekToLast()
    {
        ResetOverflowState();
        if (!_iter.SeekToLast()) return false;
        SetupOverflowForCurrentLeaf(true);
        return true;
    }

    public override bool Prev()
    {
        if (_overflowRunIndex > 0)
        {
            _overflowRunIndex--;
            return true;
        }
        ResetOverflowState();
        if (!_iter.Prev()) return false;
        SetupOverflowForCurrentLeaf(true);
        return true;
    }

    public override bool Seek(ReadOnlySpan<byte> targetUserKey, ulong targetTag)
    {
        ResetOverflowState();
        if (!_iter.Seek(targetUserKey)) return false;
        if (_hasSeqno)
        {
            LoudsTrie t = _trie;
            ulong leaf = _iter.LeafIndex;
            ulong leafSeqno = t.GetLeafSeqno(leaf);
            uint blockCount = t.GetLeafBlockCount(leaf);
            uint baseIdx = t.GetOverflowBase(leaf);
            if (targetTag < leafSeqno && _iter.Key.SequenceEqual(targetUserKey))
            {
                // The target's internal key sorts after the separator: advance through the overflow blocks.
                for (uint oi = 0; oi + 1 < blockCount; oi++)
                {
                    ulong ov = t.GetOverflowSeqno(baseIdx + oi);
                    if (ov == 0 || targetTag >= ov)
                    {
                        _overflowRunIndex = oi + 1;
                        _overflowRunSize = blockCount;
                        _overflowBaseIdx = baseIdx;
                        return true;
                    }
                }
                // Below every tag of the run: the block after the run.
                if (!_iter.Next()) return false;
                ulong next = _iter.LeafIndex;
                _overflowRunIndex = 0;
                _overflowRunSize = t.GetLeafBlockCount(next);
                _overflowBaseIdx = t.GetOverflowBase(next);
            }
            else
            {
                _overflowRunIndex = 0;
                _overflowRunSize = blockCount;
                _overflowBaseIdx = baseIdx;
            }
        }
        return true;
    }

    public override bool Next()
    {
        if (_overflowRunIndex + 1 < _overflowRunSize)
        {
            _overflowRunIndex++;
            return true;
        }
        ResetOverflowState();
        if (!_iter.Next()) return false;
        SetupOverflowForCurrentLeaf(false);
        return true;
    }

    /// <summary>The current separator (a user key); unchanged while walking an overflow run.</summary>
    public override ReadOnlySpan<byte> Key => _iter.Key;

    public override BlockHandle Value
    {
        get
        {
            TrieBlockHandle h = _overflowRunIndex == 0 ? _iter.Value : _trie.GetOverflowHandle(_overflowBaseIdx + _overflowRunIndex - 1);
            return new BlockHandle(h.Offset, h.Size);
        }
    }
}
