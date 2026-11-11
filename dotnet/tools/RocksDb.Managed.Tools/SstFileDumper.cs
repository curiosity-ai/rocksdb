using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Tools;

/// <summary>
/// Port of SstFileDumper (table/sst_file_dumper.cc) for block-based tables: opens a table file, scans it, verifies
/// its checksums, prints its properties and writes the raw dump (BlockBasedTable::DumpTable).
/// </summary>
internal sealed class SstFileDumper : IDisposable
{
    private readonly TextWriter _out;
    private readonly bool _outputHex;
    private readonly BlockBasedTableReader? _reader;

    public Exception? InitError { get; }
    public ulong ReadNumber { get; private set; }
    public TableProperties? Properties => _reader?.Properties;
    public IReadOnlyDictionary<string, BlockHandle> MetaBlocks => Reader.MetaBlocks;

    public SstFileDumper(IFileSystem fs, string path, bool verifyChecksum, bool outputHex, bool silent, TextWriter output)
    {
        _out = output;
        _outputHex = outputHex;
        if (!silent) _out.Write("Process " + path + "\n");
        try
        {
            _reader = Open(fs, path, verifyChecksum, Comparator.Bytewise);
            // SetTableOptionsByMagicNumber.
            if (!silent) _out.Write("Sst file format: block-based\n");
            if (_reader.Properties.ComparatorName == Comparator.ReverseBytewise.Name)
            {
                _reader.Dispose();
                _reader = Open(fs, path, verifyChecksum, Comparator.ReverseBytewise);
            }
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            InitError = e;
            _reader?.Dispose();
            _reader = null;
        }
    }

    private static BlockBasedTableReader Open(IFileSystem fs, string path, bool verifyChecksum, Comparator comparator)
    {
        IRandomAccessFile file = fs.OpenRandomAccess(path);
        try
        {
            var cf = new ColumnFamilyOptions();
            TableReaderOptions ro = TableFactory.CreateReaderOptions(cf, TableFactory.CreateFormatOptions(cf.TableOptions)) with
            {
                Comparator = comparator,
                VerifyChecksums = verifyChecksum,
            };
            return BlockBasedTableReader.Open(file, ro);
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    private BlockBasedTableReader Reader => _reader ?? throw InitError!;

    /// <summary>SstFileDumper::ReadSequential: prints "key => value" lines (when <paramref name="printKv"/>), returns the error, if any.</summary>
    public string? ReadSequential(bool printKv, ulong readNumLimit, bool hasFrom, byte[] from, bool hasTo, byte[] to, bool useFromAsPrefix)
    {
        BlockBasedTableReader reader = Reader;
        Comparator ucmp = reader.InternalComparator.UserComparator;
        ulong i = 0;
        using (BlockBasedTableIterator it = reader.NewIterator(TableReadFlags.DontFillCache))
        {
            if (hasFrom)
            {
                byte[] seek = new byte[from.Length + 8];
                from.CopyTo(seek, 0);
                Util.Coding.EncodeFixed64(seek.AsSpan(from.Length), DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
                it.Seek(seek);
            }
            else
            {
                it.SeekToFirst();
            }
            for (; it.Valid; it.Next())
            {
                if (readNumLimit > 0 && i >= readNumLimit) break;
                ReadOnlySpan<byte> key = it.Key;
                ++i;
                if (!DbFormat.TryParseInternalKey(key, out ParsedInternalKey p))
                {
                    Console.Error.Write("Corrupted Key: " + ToolText.EscapeString(key) + "\n");
                    continue;
                }
                ReadOnlySpan<byte> userKey = key[..p.UserKeyLength];
                if (useFromAsPrefix && !userKey.StartsWith(from)) break;
                if (hasTo && ucmp.Compare(userKey, to) >= 0) break;
                if (printKv)
                {
                    string v;
                    if (DecodeBlobIndex && p.Type == EntryType.BlobIndex)
                    {
                        if (Db.Blob.BlobIndex.TryDecode(it.Value, out Db.Blob.BlobIndex blobIndex) is not null)
                        {
                            Console.Error.Write(ToolText.InternalKeyDebugString(key, _outputHex) + " => error decoding blob index\n");
                            continue;
                        }
                        v = BlobIndexDebugString(blobIndex, _outputHex);
                    }
                    else if (p.Type == EntryType.WideColumnEntity)
                    {
                        if (!TryWideColumnsToString(it.Value, _outputHex, out v))
                        {
                            // SstFileDumper::ReadSequential: the error goes to stderr and the entry is skipped.
                            Console.Error.Write(ToolText.InternalKeyDebugString(key, _outputHex) + " => error deserializing wide columns\n");
                            continue;
                        }
                    }
                    else if (p.Type == EntryType.ValuePreferredSeqno && it.Value.Length >= sizeof(ulong))
                    {
                        // ParsePackedValueWithSeqno: "value, preferred_seqno".
                        ReadOnlySpan<byte> packed = it.Value;
                        v = ToolText.ToString(packed[..^sizeof(ulong)], _outputHex) + ", "
                            + Util.Coding.DecodeFixed64(packed[^sizeof(ulong)..]).ToString(CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        v = ToolText.ToString(it.Value, _outputHex);
                    }
                    _out.Write(ToolText.InternalKeyDebugString(key, _outputHex) + " => " + v + "\n");
                }
            }
        }
        ReadNumber += i;
        TableProperties props = reader.Properties;
        bool verifyEntries = (readNumLimit == 0 || readNumLimit == ulong.MaxValue) && !hasFrom && !hasTo;
        if (verifyEntries && i != props.NumEntries - props.NumRangeDeletions)
        {
            return "Corruption: Table property expects " + (props.NumEntries - props.NumRangeDeletions).ToString(CultureInfo.InvariantCulture)
                + " entries when excluding range deletions, but scanning the table returned " + i.ToString(CultureInfo.InvariantCulture) + " entries";
        }
        return null;
    }

    /// <summary>--decode_blob_index: blob references are printed as BlobIndex::DebugString instead of their raw bytes.</summary>
    public bool DecodeBlobIndex { get; init; }

    /// <summary>BlobIndex::DebugString(output_hex) (db/blob/blob_index.h).</summary>
    private static string BlobIndexDebugString(in Db.Blob.BlobIndex index, bool hex)
    {
        var sb = new System.Text.StringBuilder();
        if (index.IsInlined)
        {
            sb.Append("[inlined blob] value:").Append(ToolText.ToString(index.InlinedValue, hex));
        }
        else
        {
            sb.Append("[blob ref] file:").Append(index.IsSameFile ? "same" : index.FileNumber.ToString(CultureInfo.InvariantCulture))
                .Append(" offset:").Append(index.Offset.ToString(CultureInfo.InvariantCulture))
                .Append(" size:").Append(index.Size.ToString(CultureInfo.InvariantCulture))
                .Append(" compression: ").Append(index.Compression switch
                {
                    // CompressionTypeToString (util/compression.cc).
                    Compression.CompressionType.None => "NoCompression",
                    Compression.CompressionType.Snappy => "Snappy",
                    Compression.CompressionType.Zlib => "Zlib",
                    Compression.CompressionType.BZip2 => "BZip2",
                    Compression.CompressionType.LZ4 => "LZ4",
                    Compression.CompressionType.LZ4HC => "LZ4HC",
                    Compression.CompressionType.Xpress => "Xpress",
                    Compression.CompressionType.Zstd => "ZSTD",
                    _ => "",
                });
        }
        if (index.HasTtl) sb.Append(" exp:").Append(index.Expiration.ToString(CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    /// <summary>WideColumnsHelper::DumpSliceAsWideColumns: "name:value name:value" (the default column name is empty).</summary>
    internal static string WideColumnsToString(ReadOnlySpan<byte> entity, bool hex) =>
        TryWideColumnsToString(entity, hex, out string s) ? s : "error deserializing wide columns";

    /// <summary>WideColumnsHelper::DumpSliceAsWideColumns: false when the entity does not deserialize.</summary>
    internal static bool TryWideColumnsToString(ReadOnlySpan<byte> entity, bool hex, out string text)
    {
        text = "";
        var slots = new List<Db.Wide.WideColumnSlot>();
        // DeserializeSimple: a blob reference (version 2 kTypeBlobIndex column) is Corruption for this caller too.
        if (Db.Wide.WideColumnSerialization.TryDeserialize(entity, slots, out bool hasBlobColumns) is not null || hasBlobColumns) return false;
        var sb = new System.Text.StringBuilder();
        for (int c = 0; c < slots.Count; c++)
        {
            Db.Wide.WideColumnSlot sl = slots[c];
            ReadOnlySpan<byte> name = entity.Slice(sl.NameOffset, sl.NameLength), value = entity.Slice(sl.ValueOffset, sl.ValueLength);
            if (c > 0) sb.Append(' ');
            sb.Append(Col(name, hex)).Append(':').Append(Col(value, hex));
        }
        text = sb.ToString();
        return true;
    }

    private static string Col(ReadOnlySpan<byte> s, bool hex) => s.IsEmpty ? "" : hex ? ToolText.StringToHex(s) : ToolText.Raw(s);

    /// <summary>
    /// TableReader::VerifyChecksum: reads every data, index, filter and meta block with checksum verification.
    /// Returns the error message, if any.
    /// </summary>
    public string? VerifyChecksum(IFileSystem fs, string path)
    {
        BlockBasedTableReader reader = Reader;
        try
        {
            using (BlockBasedTableIterator it = reader.NewIterator(TableReadFlags.DontFillCache))
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) { }
            }
            using IRandomAccessFile file = fs.OpenRandomAccess(path);
            foreach (BlockHandle h in reader.MetaBlocks.Values)
            {
                byte[] buf = BlockFetcher.ReadRawPayload(file, reader.Footer, h, verifyChecksums: true, out _);
                RocksArrayPool<byte>.Shared.Return(buf);
            }
            byte[] ib = BlockFetcher.ReadRawPayload(file, reader.Footer, reader.IndexHandle, verifyChecksums: true, out _);
            RocksArrayPool<byte>.Shared.Return(ib);
            return null;
        }
        catch (RocksDbException e)
        {
            return e.Message;
        }
    }

    /// <summary>BlockBasedTable::DumpTable (raw dump), without the per-block checksum lines.</summary>
    public void DumpTable(TextWriter o)
    {
        BlockBasedTableReader reader = Reader;
        Footer f = reader.Footer;
        o.Write("Footer Details:\n--------------------------------------\n");
        o.Write("  metaindex handle: " + HandleHex(f.MetaindexHandle) + " offset: " + f.MetaindexHandle.Offset.ToString(CultureInfo.InvariantCulture)
            + " size: " + f.MetaindexHandle.Size.ToString(CultureInfo.InvariantCulture) + "\n  ");
        o.Write("index handle: " + HandleHex(f.IndexHandle) + " offset: " + f.IndexHandle.Offset.ToString(CultureInfo.InvariantCulture)
            + " size: " + f.IndexHandle.Size.ToString(CultureInfo.InvariantCulture) + "\n  ");
        o.Write("table_magic_number: " + f.TableMagicNumber.ToString(CultureInfo.InvariantCulture) + "\n  ");
        if (f.TableMagicNumber != BlockBasedTableConstants.LegacyBlockBasedTableMagicNumber)
        {
            o.Write("format version: " + f.FormatVersion.ToString(CultureInfo.InvariantCulture) + "\n");
            if (f.IncompatibleFeatures != 0) o.Write("  incompatible features: " + f.IncompatibleFeatures.ToString(CultureInfo.InvariantCulture) + "\n");
        }
        o.Write("\n");
        o.Write("Block Checksum Type Legend:\n--------------------------------------\n");
        o.Write("  0 = kNoChecksum\n  1 = kCRC32c\n  2 = kxxHash\n  3 = kxxHash64\n  4 = kXXH3\n");
        o.Write("  (This file uses checksum type: " + ((int)f.ChecksumType).ToString(CultureInfo.InvariantCulture) + ")\n\n");
        o.Write("Metaindex Details:\n--------------------------------------\n");
        var names = new List<string>(reader.MetaBlocks.Keys);
        names.Sort(StringComparer.Ordinal);
        foreach (string name in names)
        {
            BlockHandle h = reader.MetaBlocks[name];
            if (name == BlockBasedTableConstants.PropertiesBlockName) o.Write("  Properties block handle: " + HandleHex(h) + "\n");
            else if (name == BlockBasedTableConstants.CompressionDictBlockName) o.Write("  Compression dictionary block handle: " + HandleHex(h) + "\n");
            else if (name.Contains("filter.rocksdb.", StringComparison.Ordinal)) o.Write("  Filter block handle: " + HandleHex(h) + "\n");
            else if (name == BlockBasedTableConstants.RangeDelBlockName) o.Write("  Range deletion block handle: " + HandleHex(h) + "\n");
        }
        o.Write("\n");
        o.Write("Table Properties:\n--------------------------------------\n");
        o.Write("  " + ToolText.TablePropertiesToString(reader.Properties, "\n  ", ": ") + "\n");

        // Index Details (top-level index entries).
        o.Write("Index Details:\n--------------------------------------\n");
        o.Write("  Block key hex dump: Data block handle\n");
        o.Write("  Block key ascii\n\n");
        var handles = new List<BlockHandle>();
        BlockRef idx = reader.GetIndexBlockRef(TableReadFlags.DontFillCache);
        try
        {
            using var it = new BlockIter();
            reader.InitIndexIter(it, idx.Block!, isPartition: false);
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                ReadOnlySpan<byte> key = it.Key;
                ReadOnlySpan<byte> userKey = reader.IndexKeyIncludesSeq && key.Length >= 8 ? key[..^8] : key;
                BlockHandle h = it.BlockHandle;
                handles.Add(h);
                o.Write("  HEX    " + Convert.ToHexString(userKey) + ": " + HandleHex(h) + " offset " + h.Offset.ToString(CultureInfo.InvariantCulture)
                    + " size " + h.Size.ToString(CultureInfo.InvariantCulture) + "\n");
                o.Write("  ASCII  " + Spaced(userKey, escapeNul: false) + "\n");
                o.Write("  ------\n");
            }
        }
        finally
        {
            idx.Release();
        }
        o.Write("\n");

        if (reader.NewRangeTombstoneIterator() is { } rit)
        {
            using (rit)
            {
                rit.SeekToFirst();
                if (rit.Valid)
                {
                    o.Write("Range deletions:\n--------------------------------------\n");
                    for (; rit.Valid; rit.Next()) DumpKeyValue(o, rit.Key, rit.Value);
                    o.Write("\n");
                }
            }
        }

        if (reader.IndexType == IndexType.TwoLevelIndexSearch) return; // partitioned index: data blocks are behind the partitions
        ulong min = ulong.MaxValue, max = 0, sum = 0;
        int id = 1;
        foreach (BlockHandle h in handles)
        {
            min = Math.Min(min, h.Size);
            max = Math.Max(max, h.Size);
            sum += h.Size;
            o.Write("Data Block # " + id.ToString(CultureInfo.InvariantCulture) + " @ " + HandleHex(h) + "\n");
            o.Write("--------------------------------------\n");
            Block b = reader.GetBlock(h);
            try
            {
                using var it = new BlockIter();
                it.InitializeData(b, reader.InternalComparator, reader.GlobalSequenceNumber);
                for (it.SeekToFirst(); it.Valid; it.Next()) DumpKeyValue(o, it.Key, it.Value);
            }
            finally
            {
                b.Release();
            }
            o.Write("\n");
            id++;
        }
        if (handles.Count > 0)
        {
            o.Write("Data Block Summary:\n--------------------------------------\n");
            o.Write("  # data blocks: " + handles.Count.ToString(CultureInfo.InvariantCulture) + "\n");
            o.Write("  min data block size: " + min.ToString(CultureInfo.InvariantCulture) + "\n");
            o.Write("  max data block size: " + max.ToString(CultureInfo.InvariantCulture) + "\n");
            o.Write("  avg data block size: " + ToolText.Dbl((double)sum / handles.Count) + "\n");
        }
    }

    private static string HandleHex(BlockHandle h)
    {
        Span<byte> buf = stackalloc byte[BlockHandle.MaxEncodedLength];
        return Convert.ToHexString(buf[..h.EncodeTo(buf)]);
    }

    private static string Spaced(ReadOnlySpan<byte> s, bool escapeNul)
    {
        var sb = new System.Text.StringBuilder(s.Length * 2);
        foreach (byte b in s)
        {
            if (escapeNul && b == 0) sb.Append("\\0");
            else sb.Append((char)b);
            sb.Append(' ');
        }
        return sb.ToString();
    }

    /// <summary>BlockBasedTable::DumpKeyValue.</summary>
    private static void DumpKeyValue(TextWriter o, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (!DbFormat.TryParseInternalKey(key, out ParsedInternalKey p))
        {
            o.Write("Error parsing internal key - Skipped \n");
            return;
        }
        ReadOnlySpan<byte> uk = key[..p.UserKeyLength];
        o.Write("  HEX    " + Convert.ToHexString(uk) + ": " + Convert.ToHexString(value) + "\n");
        o.Write("  ASCII  " + Spaced(uk, escapeNul: true) + ": " + Spaced(value, escapeNul: true) + "\n");
        o.Write("  ------\n");
    }

    public void Dispose() => _reader?.Dispose();
}
