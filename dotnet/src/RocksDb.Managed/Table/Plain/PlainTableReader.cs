using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Plain;

/// <summary>User-collected property names of plain tables (PlainTablePropertyNames, table/plain/plain_table_factory.cc).</summary>
public static class PlainTablePropertyNames
{
    /// <summary>kEncodingType: fixed32 EncodingType.</summary>
    public const string EncodingType = "rocksdb.plain.table.encoding.type";
    /// <summary>kBloomVersion: "1" when the index is stored in the file.</summary>
    public const string BloomVersion = "rocksdb.plain.table.bloom.version";
    /// <summary>kNumBloomBlocks: varint32 number of cache-line blocks of the stored bloom filter.</summary>
    public const string NumBloomBlocks = "rocksdb.plain.table.bloom.numblocks";
    /// <summary>BloomBlockBuilder::kBloomBlock: name of the bloom filter meta block.</summary>
    public const string BloomBlock = "kBloomBlock";
}

/// <summary>
/// Port of PlainTableReader (table/plain/plain_table_reader.{h,cc}). The whole file is read into memory at open
/// (the managed equivalent of upstream's mmap mode, which plain tables are designed for: there are no blocks); the
/// prefix hash index (and bloom filter) is loaded from the file (store_index_in_file) or rebuilt by scanning all keys.
/// </summary>
/// <remarks>
/// Lookups are allocation-free apart from a pooled decoder per call. Without a prefix extractor the table is in total
/// order mode: one hash bucket, binary search over the index records, whole-key bloom filter.
/// </remarks>
public sealed class PlainTableReader : TableReader
{
    private readonly byte[] _data;
    private readonly uint _dataEnd;      // file_info_.data_end_offset (= data_size)
    private const uint DataStartOffset = 0;
    private readonly InternalKeyComparator _icmp;
    private readonly PlainTableEncodingType _encodingType;
    private readonly uint _userKeyLen;
    private readonly SliceTransform? _prefixExtractor;
    private readonly PlainTableIndex _index = new();
    private bool _fullScanMode;
    private bool _enableBloom;
    private readonly PlainTableBloom _bloom = new(6);
    private readonly TableProperties _props;
    private readonly long _fileSize;
    private readonly uint _bloomLocality;
    private int _disposed;

    private PlainTableReader(byte[] data, InternalKeyComparator icmp, PlainTableEncodingType encodingType, TableProperties props,
        SliceTransform? prefixExtractor, uint bloomLocality)
    {
        _data = data;
        _fileSize = data.Length;
        _icmp = icmp;
        _encodingType = encodingType;
        _userKeyLen = (uint)props.FixedKeyLen;
        _prefixExtractor = prefixExtractor;
        _dataEnd = (uint)props.DataSize;
        _props = props;
        _bloomLocality = bloomLocality;
    }

    public override TableProperties Properties => _props;
    public override long FileSize => _fileSize;
    internal override InternalKeyComparator InternalComparator => _icmp;

    /// <summary>The key encoding of this file.</summary>
    public PlainTableEncodingType EncodingType => _encodingType;

    /// <summary>GetIndexSize: number of hash buckets of the prefix index.</summary>
    public uint IndexSize => _index.IndexSize;

    /// <summary>IsTotalOrderMode: no prefix extractor.</summary>
    internal bool IsTotalOrderMode => _prefixExtractor is null;

    /// <summary>
    /// SliceTransform::Transform as used by plain tables. Upstream transforms without checking InDomain (out of domain
    /// keys are undefined behaviour there, e.g. FixedPrefix reads past the key); the port uses the whole key instead.
    /// </summary>
    internal static ReadOnlySpan<byte> Transform(SliceTransform pe, ReadOnlySpan<byte> userKey) => pe.InDomain(userKey) ? pe.Transform(userKey) : userKey;

    private ReadOnlySpan<byte> GetPrefixFromUserKey(ReadOnlySpan<byte> userKey) =>
        // Use empty slice as prefix if prefix_extractor is not set: it falls back to pure binary search and total
        // iterator seek is supported.
        _prefixExtractor is null ? default : Transform(_prefixExtractor, userKey);

    private ReadOnlySpan<byte> GetPrefix(ReadOnlySpan<byte> internalKey) => GetPrefixFromUserKey(DbFormat.ExtractUserKey(internalKey));

    /// <summary>
    /// Port of PlainTableReader::Open (with PlainTableFactory::NewTableReader's options). Takes ownership of
    /// <paramref name="file"/> (read fully, then closed).
    /// </summary>
    public static PlainTableReader Open(IRandomAccessFile file, TableReaderOptions options, PlainTableOptions tableOptions)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(tableOptions);
        byte[] data;
        Footer footer;
        try
        {
            long fileSize = file.Length;
            if ((ulong)fileSize > PlainTableIndex.MaxFileSize)
                ThrowHelper.NotSupported("File size " + fileSize + " exceeds PlainTableReader max file size " + PlainTableIndex.MaxFileSize);
            footer = TableMetaReader.ReadFooter(file, TableMagicNumbers.PlainTable);
            data = TableMetaReader.ReadAll(file);
        }
        finally
        {
            file.Dispose();
        }
        Dictionary<string, BlockHandle> metaBlocks = TableMetaReader.ReadMetaIndex(data, footer);
        TableProperties props = TableMetaReader.ReadProperties(data, metaBlocks);
        if (props.DataSize > (ulong)data.Length) ThrowHelper.Corruption("plain table data size " + props.DataSize + " beyond the file size");

        SliceTransform? prefixExtractor = options.PrefixExtractor;
        string prefixExtractorInFile = props.PrefixExtractorName;
        bool fullScanMode = tableOptions.FullScanMode;
        if (!fullScanMode && prefixExtractorInFile.Length > 0 /* old version sst file*/ && prefixExtractorInFile != "nullptr")
        {
            if (prefixExtractor is null)
                ThrowHelper.InvalidArgument("Prefix extractor is missing when opening a PlainTable built using a prefix extractor");
            else if (prefixExtractorInFile != prefixExtractor.AsString())
                ThrowHelper.InvalidArgument("Prefix extractor given doesn't match the one used to build PlainTable");
        }

        var encodingType = PlainTableEncodingType.Plain;
        if (props.UserCollectedProperties.TryGetValue(PlainTablePropertyNames.EncodingType, out byte[]? enc) && enc.Length >= 4)
            encodingType = (PlainTableEncodingType)Coding.DecodeFixed32(enc);
        if (encodingType > PlainTableEncodingType.Prefix) ThrowHelper.Corruption("Unknown plain table encoding type " + (uint)encodingType);

        Comparator ucmp = options.Comparator;
        InternalKeyComparator icmp = ucmp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(ucmp);
        var reader = new PlainTableReader(data, icmp, encodingType, props, prefixExtractor, options.BloomLocality);
        if (!fullScanMode)
        {
            reader.PopulateIndex(metaBlocks, tableOptions.BloomBitsPerKey, tableOptions.HashTableRatio, tableOptions.IndexSparseness);
        }
        else
        {
            // Flag to indicate it is a full scan mode so that none of the indexes can be used.
            reader._fullScanMode = true;
        }
        return reader;
    }

    /// <summary>Port of PlainTableReader::PopulateIndex.</summary>
    private void PopulateIndex(Dictionary<string, BlockHandle> metaBlocks, int bloomBitsPerKey, double hashTableRatio, ulong indexSparseness)
    {
        bool indexInFile = metaBlocks.TryGetValue(PlainTableIndex.PlainTableIndexBlock, out BlockHandle indexHandle);
        bool bloomInFile = false;
        BlockHandle bloomHandle = default;
        // We only need to read the bloom block if index block is in file.
        if (indexInFile) bloomInFile = metaBlocks.TryGetValue(PlainTablePropertyNames.BloomBlock, out bloomHandle) && bloomHandle.Size > 0;

        if (_prefixExtractor is null && hashTableRatio != 0)
        {
            // moptions.prefix_extractor is requried for a hash-based look-up.
            ThrowHelper.NotSupported("PlainTable requires a prefix extractor enable prefix hash mode.");
        }

        // First, read the whole file, for every kIndexIntervalForSamePrefixKeys rows for a prefix (starting from the
        // first one), generate a record of (hash, offset) and append it to IndexRecordList.
        if (!indexInFile)
        {
            // Allocate bloom filter here for total order mode.
            if (IsTotalOrderMode) AllocateBloom(bloomBitsPerKey, (uint)_props.NumEntries);
        }
        else if (bloomInFile)
        {
            _enableBloom = true;
            uint numBlocks = 0;
            if (_props.UserCollectedProperties.TryGetValue(PlainTablePropertyNames.NumBloomBlocks, out byte[]? nb))
            {
                ReadOnlySpan<byte> s = nb;
                if (!Coding.GetVarint32(ref s, out numBlocks)) numBlocks = 0;
            }
            TableMetaReader.Slice(_data, bloomHandle);
            // num_blocks addresses cache lines of the bloom block (upstream reads out of bounds for a larger value).
            if ((ulong)numBlocks * 64 /* CACHE_LINE_SIZE */ > bloomHandle.Size)
                ThrowHelper.Corruption("plain table bloom filter: " + numBlocks + " blocks do not fit in a bloom block of " + bloomHandle.Size + " bytes");
            _bloom.SetRawData(_data, (int)bloomHandle.Offset, (uint)bloomHandle.Size * 8, numBlocks);
        }
        else
        {
            // Index in file but no bloom in file. Disable bloom filter in this case.
            _enableBloom = false;
        }

        var indexBuilder = new PlainTableIndexBuilder(_prefixExtractor is not null, indexSparseness, hashTableRatio);
        List<uint>? prefixHashes = null;
        if (!indexInFile)
        {
            // Populates _bloom if enabled (total order mode)
            prefixHashes = PopulateIndexRecordList(indexBuilder);
        }
        else
        {
            TableMetaReader.Slice(_data, indexHandle);
            _index.InitFromRawData(_data, (int)indexHandle.Offset, (int)indexHandle.Size);
        }

        if (!indexInFile && !IsTotalOrderMode)
        {
            // Calculated bloom filter size and allocate memory for bloom filter based on the number of prefixes, then fill it.
            AllocateBloom(bloomBitsPerKey, _index.NumPrefixes);
            if (_enableBloom)
            {
                foreach (uint h in prefixHashes!) _bloom.AddHash(h);
            }
        }

        // Fill two table properties.
        _props.UserCollectedProperties["plain_table_hash_table_size"] = Utf8(!indexInFile ? (ulong)_index.IndexSize * PlainTableIndex.OffsetLen : 0);
        _props.UserCollectedProperties["plain_table_sub_index_size"] = Utf8(!indexInFile ? _index.SubIndexSize : 0);
    }

    private static byte[] Utf8(ulong v) => System.Text.Encoding.ASCII.GetBytes(v.ToString(CultureInfo.InvariantCulture));

    private void AllocateBloom(int bloomBitsPerKey, uint numKeys)
    {
        uint bloomTotalBits = unchecked(numKeys * (uint)bloomBitsPerKey);
        if (bloomTotalBits > 0)
        {
            _enableBloom = true;
            _bloom.SetTotalBits(bloomTotalBits, _bloomLocality);
        }
    }

    /// <summary>Port of PlainTableReader::PopulateIndexRecordList: scans every entry. Returns the prefix hashes.</summary>
    private List<uint> PopulateIndexRecordList(PlainTableIndexBuilder indexBuilder)
    {
        var prefixHashes = new List<uint>();
        uint pos = DataStartOffset;
        bool isFirstRecord = true;
        byte[] prevPrefix = [];
        int prevPrefixLength = 0;
        var decoder = new PlainTableKeyDecoder(_data, _dataEnd, _encodingType, _userKeyLen);
        while (pos < _dataEnd)
        {
            uint keyOffset = pos;
            pos += decoder.NextKey(pos, out bool seekable);
            ReadOnlySpan<byte> userKey = decoder.UserKey;
            ReadOnlySpan<byte> keyPrefix = GetPrefixFromUserKey(userKey);
            if (_enableBloom)
            {
                _bloom.AddHash(Hash.GetSliceHash(userKey));
            }
            else if (isFirstRecord || !keyPrefix.SequenceEqual(prevPrefix.AsSpan(0, prevPrefixLength)))
            {
                if (!isFirstRecord) prefixHashes.Add(Hash.GetSliceHash(prevPrefix.AsSpan(0, prevPrefixLength)));
                if (prevPrefix.Length < keyPrefix.Length) prevPrefix = new byte[keyPrefix.Length * 2];
                keyPrefix.CopyTo(prevPrefix);
                prevPrefixLength = keyPrefix.Length;
            }
            indexBuilder.AddKeyPrefix(keyPrefix, keyOffset);
            if (!seekable && isFirstRecord) ThrowHelper.Corruption("Key for a prefix is not seekable");
            isFirstRecord = false;
            if (pos == _dataEnd)
            {
                // prefix_hashes->push_back(GetSliceHash(key_prefix_slice)) after the loop: the last key's prefix.
                prefixHashes.Add(Hash.GetSliceHash(keyPrefix));
            }
        }
        if (isFirstRecord) prefixHashes.Add(Hash.GetSliceHash(default));
        byte[] raw = indexBuilder.Finish();
        _index.InitFromRawData(raw, 0, raw.Length);
        return prefixHashes;
    }

    /// <summary>MatchBloom: whether the bloom filter (if any) may contain <paramref name="hash"/>.</summary>
    internal bool MatchBloom(uint hash) => !_enableBloom || _bloom.MayContainHash(hash);

    /// <summary>Next: decodes the entry at <paramref name="offset"/> (no-op at the end of the data) and advances it.</summary>
    private void Next(PlainTableKeyDecoder decoder, ref uint offset)
    {
        if (offset == _dataEnd) return;
        if (offset > _dataEnd) ThrowHelper.Corruption("Offset is out of file size");
        offset += decoder.NextKey(offset, out _);
    }

    /// <summary>
    /// Port of PlainTableReader::GetOffset: the file offset to start a linear search for <paramref name="target"/> at;
    /// <paramref name="prefixMatched"/> when the key there is known to have the target's prefix.
    /// </summary>
    internal uint GetOffset(PlainTableKeyDecoder decoder, ReadOnlySpan<byte> target, ReadOnlySpan<byte> prefix, uint prefixHash, out bool prefixMatched)
    {
        prefixMatched = false;
        PlainTableIndex.SearchResult res = _index.GetOffset(prefixHash, out uint prefixIndexOffset);
        if (res == PlainTableIndex.SearchResult.NoPrefixForBucket) return _dataEnd;
        if (res == PlainTableIndex.SearchResult.DirectToFile) return prefixIndexOffset;

        // point to sub-index, need to do a binary search
        ReadOnlySpan<byte> basePtr = _index.GetSubIndex(prefixIndexOffset, out uint upperBound);
        uint low = 0;
        uint high = upperBound;
        if (!DbFormat.TryParseInternalKey(target, out _)) ThrowHelper.Corruption("Corrupted target key");
        // The key is between [low, high). Do a binary search between it.
        while (high - low > 1)
        {
            uint mid = (high + low) / 2;
            uint fileOffset = Coding.DecodeFixed32(basePtr[(int)(mid * 4)..]);
            decoder.NextKeyNoValue(fileOffset, out _);
            int cmp = _icmp.Compare(decoder.InternalKey, target);
            if (cmp < 0)
            {
                low = mid;
            }
            else if (cmp == 0)
            {
                // Happen to have found the exact key or target is smaller than the first key after base_offset.
                prefixMatched = true;
                return fileOffset;
            }
            else
            {
                high = mid;
            }
        }
        // Both of the key at the position low or low+1 could share the same prefix as target. We need to rule out one
        // of them to avoid to go to the wrong prefix.
        uint lowKeyOffset = Coding.DecodeFixed32(basePtr[(int)(low * 4)..]);
        decoder.NextKeyNoValue(lowKeyOffset, out _);
        if (GetPrefixFromUserKey(decoder.UserKey).SequenceEqual(prefix))
        {
            prefixMatched = true;
            return lowKeyOffset;
        }
        // There is possible a next prefix, return it; otherwise target is larger than a key of the last prefix in
        // this bucket but with a different prefix. Key does not exist.
        return low + 1 < upperBound ? Coding.DecodeFixed32(basePtr[(int)((low + 1) * 4)..]) : _dataEnd;
    }

    internal PlainTableKeyDecoder NewDecoder() => new(_data, _dataEnd, _encodingType, _userKeyLen);

    [ThreadStatic] private static PlainTableKeyDecoder? t_decoder;

    /// <summary>Port of PlainTableReader::Get.</summary>
    public override void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags)
    {
        // Check bloom filter first.
        ReadOnlySpan<byte> prefix;
        uint prefixHash;
        if (IsTotalOrderMode)
        {
            // Upstream records InvalidArgument in a member status nobody reads and continues without an index.
            if (_fullScanMode) ThrowHelper.InvalidArgument("Get() is not allowed in full scan mode.");
            // Match whole user key for bloom filter check.
            if (!MatchBloom(Hash.GetSliceHash(DbFormat.ExtractUserKey(internalKey)))) return;
            // in total order mode, there is only one bucket 0, and we always use empty prefix.
            prefix = default;
            prefixHash = 0;
        }
        else
        {
            if (_fullScanMode) ThrowHelper.InvalidArgument("Get() is not allowed in full scan mode.");
            prefix = GetPrefix(internalKey);
            prefixHash = Hash.GetSliceHash(prefix);
            if (!MatchBloom(prefixHash)) return;
        }
        PlainTableKeyDecoder decoder = RentDecoder();
        try
        {
            uint offset = GetOffset(decoder, internalKey, prefix, prefixHash, out bool prefixMatch);
            ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
            Comparator ucmp = _icmp.UserComparator;
            while (offset < _dataEnd)
            {
                Next(decoder, ref offset);
                ReadOnlySpan<byte> foundKey = decoder.InternalKey;
                if (!prefixMatch)
                {
                    // Need to verify prefix for the first key found if it is not yet checked.
                    if (!GetPrefix(foundKey).SequenceEqual(prefix)) return;
                    prefixMatch = true;
                }
                if (_icmp.Compare(foundKey, internalKey) >= 0)
                {
                    // GetContext::SaveValue stops at the first key with a different user key.
                    if (!ucmp.Equal(DbFormat.ExtractUserKey(foundKey), userKey)) return;
                    if (!visitor(ref state, foundKey, decoder.Value)) return;
                }
            }
        }
        finally
        {
            ReturnDecoder(decoder);
        }
    }

    private PlainTableKeyDecoder RentDecoder()
    {
        // One pooled decoder per thread (lookups do not nest).
        PlainTableKeyDecoder? d = t_decoder;
        if (d is null) return NewDecoder();
        t_decoder = null;
        d.Reset(_data, _dataEnd, _encodingType, _userKeyLen);
        return d;
    }

    private static void ReturnDecoder(PlainTableKeyDecoder d) => t_decoder = d;

    public override InternalIterator NewInternalIterator(TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        // Auto prefix mode is not implemented in PlainTable.
        bool usePrefixSeek = !IsTotalOrderMode
            && (readOptions?.PrefixSameAsStart == true || (readOptions?.TotalOrderSeek != true && readOptions?.AutoPrefixMode != true));
        return new PlainTableIterator(this, usePrefixSeek);
    }

    public override void Dispose() => Interlocked.Exchange(ref _disposed, 1);

    internal uint DataEnd => _dataEnd;
    internal bool FullScanMode => _fullScanMode;

    /// <summary>The iterator: port of PlainTableIterator (table/plain/plain_table_reader.cc).</summary>
    private sealed class PlainTableIterator : InternalIterator
    {
        private readonly PlainTableReader _table;
        private readonly PlainTableKeyDecoder _decoder;
        private readonly bool _usePrefixSeek;
        private uint _offset;
        private uint _nextOffset;

        public PlainTableIterator(PlainTableReader table, bool usePrefixSeek)
        {
            _table = table;
            _decoder = table.NewDecoder();
            _usePrefixSeek = usePrefixSeek;
            _nextOffset = _offset = table._dataEnd;
        }

        public override bool Valid => _offset < _table._dataEnd && _offset >= DataStartOffset;
        public override ReadOnlySpan<byte> Key => _decoder.InternalKey;
        public override ReadOnlySpan<byte> Value => _decoder.Value;

        private void Invalidate() => _offset = _nextOffset = _table._dataEnd;

        public override void SeekToFirst()
        {
            _nextOffset = DataStartOffset;
            if (_nextOffset >= _table._dataEnd) Invalidate();
            else Next();
        }

        public override void SeekToLast()
        {
            Invalidate();
            ThrowHelper.NotSupported("SeekToLast() is not supported in PlainTable");
        }

        public override void Seek(ReadOnlySpan<byte> target)
        {
            PlainTableReader t = _table;
            if (_usePrefixSeek != !t.IsTotalOrderMode)
            {
                // This check is done here instead of NewIterator() to permit creating an iterator with total_order_seek =
                // true even if we won't be able to Seek() it. This is needed for compaction: it creates iterator with
                // total_order_seek = true but usually never does Seek() on it, only SeekToFirst().
                Invalidate();
                ThrowHelper.InvalidArgument("total_order_seek not implemented for PlainTable.");
            }
            // If the user doesn't set prefix seek option and we are not able to do a total Seek(). assert failure.
            if (t.IsTotalOrderMode)
            {
                if (t._fullScanMode)
                {
                    Invalidate();
                    ThrowHelper.InvalidArgument("Seek() is not allowed in full scan mode.");
                }
                else if (t.IndexSize > 1)
                {
                    Invalidate();
                    ThrowHelper.NotSupported("PlainTable cannot issue non-prefix seek unless in total order mode.");
                }
            }
            else if (t._fullScanMode)
            {
                // Upstream would use the unpopulated index (undefined behaviour); full scan mode allows no seeks.
                Invalidate();
                ThrowHelper.InvalidArgument("Seek() is not allowed in full scan mode.");
            }

            ReadOnlySpan<byte> prefix = t.GetPrefix(target);
            uint prefixHash = 0;
            // Bloom filter is ignored in total-order mode.
            if (!t.IsTotalOrderMode)
            {
                prefixHash = Hash.GetSliceHash(prefix);
                if (!t.MatchBloom(prefixHash))
                {
                    Invalidate();
                    return;
                }
            }
            // prefix is a slice of the caller's target, not of the decoder's buffers.
            uint next;
            bool prefixMatch;
            try
            {
                next = t.GetOffset(_decoder, target, prefix, prefixHash, out prefixMatch);
            }
            catch
            {
                Invalidate();
                throw;
            }
            _nextOffset = next;
            if (_nextOffset < t._dataEnd)
            {
                for (Next(); Valid; Next())
                {
                    if (!prefixMatch)
                    {
                        // Need to verify the first key's prefix
                        if (!t.GetPrefix(Key).SequenceEqual(prefix))
                        {
                            Invalidate();
                            break;
                        }
                        prefixMatch = true;
                    }
                    if (t._icmp.Compare(Key, target) >= 0) break;
                }
            }
            else
            {
                _offset = t._dataEnd;
            }
        }

        public override void SeekForPrev(ReadOnlySpan<byte> target)
        {
            Invalidate();
            ThrowHelper.NotSupported("SeekForPrev() is not supported in PlainTable");
        }

        public override void Next()
        {
            _offset = _nextOffset;
            if (_offset < _table._dataEnd)
            {
                try
                {
                    _table.Next(_decoder, ref _nextOffset);
                }
                catch
                {
                    Invalidate();
                    throw;
                }
            }
        }

        /// <summary>Upstream's Prev() only asserts (a no-op in release builds); the port reports NotSupported.</summary>
        public override void Prev()
        {
            Invalidate();
            ThrowHelper.NotSupported("Prev() is not supported in PlainTable");
        }
    }
}
