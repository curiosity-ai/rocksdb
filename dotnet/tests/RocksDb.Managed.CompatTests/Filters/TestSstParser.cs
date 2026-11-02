using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Filters;

/// <summary>
/// Minimal, test-only block-based SST parser (footer, metaindex, index -> block handles) used by the
/// filter/compression compat tests, so they don't depend on the table reader being built in parallel
/// (stream B). Supports format_version 2..7 footers; ignores checksums. Can be replaced by the real
/// table reader later. Port of just enough of table/format.cc (Footer::DecodeFrom) and block.cc.
/// </summary>
internal sealed class TestSstParser
{
    public const ulong BlockBasedTableMagicNumber = 0x88e241b785f4cff7UL;
    private const int FooterLength = 53; // Footer::kNewVersionsEncodedLength
    private const int BlockTrailerSize = 5;

    public byte[] File { get; }
    public uint FormatVersion { get; }
    public (ulong Offset, ulong Size) MetaindexHandle { get; }
    public List<(string Key, ulong Offset, ulong Size)> MetaIndex { get; } = [];

    public TestSstParser(byte[] file)
    {
        File = file;
        ReadOnlySpan<byte> footer = file.AsSpan(file.Length - FooterLength);
        ulong magic = Coding.DecodeFixed64(footer[^8..]);
        if (magic != BlockBasedTableMagicNumber) throw new InvalidDataException("not a block-based table");
        FormatVersion = Coding.DecodeFixed32(footer[^12..]);
        ulong footerOffset = (ulong)(file.Length - FooterLength);
        if (FormatVersion >= 6)
        {
            // checksum type(1) ext magic(4) footer checksum(4) base context checksum(4) metaindex size(4)
            uint metaindexSize = Coding.DecodeFixed32(footer[13..]);
            ulong gap = 0;
            if (FormatVersion >= 8)
            {
                gap = metaindexSize >> 16;
                metaindexSize &= 0xFFFF;
            }
            ulong end = footerOffset - BlockTrailerSize - gap;
            MetaindexHandle = (end - metaindexSize, metaindexSize);
        }
        else
        {
            ReadOnlySpan<byte> p = footer[1..];
            Coding.GetVarint64(ref p, out ulong off);
            Coding.GetVarint64(ref p, out ulong size);
            MetaindexHandle = (off, size);
        }
        foreach (var (key, value) in ParseBlockEntries(RawBlock(MetaindexHandle), valueLengthPresent: true))
        {
            ReadOnlySpan<byte> v = value;
            Coding.GetVarint64(ref v, out ulong off);
            Coding.GetVarint64(ref v, out ulong size);
            MetaIndex.Add((System.Text.Encoding.ASCII.GetString(key), off, size));
        }
    }

    public static TestSstParser Open(string dbDir)
    {
        string[] ssts = Directory.GetFiles(dbDir, "*.sst");
        if (ssts.Length != 1) throw new InvalidOperationException("expected exactly one .sst, found " + ssts.Length);
        return new TestSstParser(System.IO.File.ReadAllBytes(ssts[0]));
    }

    public byte[] RawBlock((ulong Offset, ulong Size) h) => File.AsSpan((int)h.Offset, (int)h.Size).ToArray();

    public CompressionType BlockCompressionType((ulong Offset, ulong Size) h) => (CompressionType)File[(int)(h.Offset + h.Size)];

    /// <summary>Block contents after decompression with the managed facade.</summary>
    public byte[] UncompressedBlock((ulong Offset, ulong Size) h)
    {
        using var w = new PooledBufferWriter();
        BlockCompression.Decompress(BlockCompressionType(h), RawBlock(h), (int)FormatVersion, w);
        return w.WrittenSpan.ToArray();
    }

    public (ulong Offset, ulong Size)? FindMeta(string name)
    {
        foreach (var e in MetaIndex)
        {
            if (e.Key == name) return (e.Offset, e.Size);
        }
        return null;
    }

    /// <summary>Data block handles from the index block (format_version &gt;= 4 needs index_block_restart_interval = 1).</summary>
    public List<(ulong Offset, ulong Size)> DataBlockHandles()
    {
        (ulong, ulong) indexHandle;
        if (FormatVersion >= 6)
        {
            indexHandle = FindMeta("rocksdb.index") ?? throw new InvalidDataException("no rocksdb.index");
        }
        else
        {
            ReadOnlySpan<byte> p = File.AsSpan(File.Length - FooterLength + 1);
            Coding.GetVarint64(ref p, out _);
            Coding.GetVarint64(ref p, out _);
            Coding.GetVarint64(ref p, out ulong off);
            Coding.GetVarint64(ref p, out ulong size);
            indexHandle = (off, size);
        }
        var result = new List<(ulong, ulong)>();
        // format_version >= 4 delta-encodes index values: no value length; at restart points full handles.
        foreach (var (_, value) in ParseBlockEntries(UncompressedBlock(indexHandle), valueLengthPresent: FormatVersion < 4))
        {
            ReadOnlySpan<byte> v = value;
            Coding.GetVarint64(ref v, out ulong off);
            Coding.GetVarint64(ref v, out ulong size);
            result.Add((off, size));
        }
        return result;
    }

    /// <summary>
    /// Parses block entries (shared/non_shared/[value_len] key-delta value). When value length is absent
    /// (delta-encoded index values) the value is assumed to be a BlockHandle (two varint64s).
    /// </summary>
    public static List<(byte[] Key, byte[] Value)> ParseBlockEntries(byte[] block, bool valueLengthPresent)
    {
        uint numRestarts = Coding.DecodeFixed32(block.AsSpan(block.Length - 4)) & 0x7FFFFFFF;
        int dataEnd = block.Length - (4 * ((int)numRestarts + 1));
        ReadOnlySpan<byte> p = block.AsSpan(0, dataEnd);
        var entries = new List<(byte[], byte[])>();
        byte[] lastKey = [];
        while (p.Length > 0)
        {
            Coding.GetVarint32(ref p, out uint shared);
            Coding.GetVarint32(ref p, out uint nonShared);
            uint valueLen = 0;
            if (valueLengthPresent) Coding.GetVarint32(ref p, out valueLen);
            byte[] key = new byte[shared + nonShared];
            lastKey.AsSpan(0, (int)shared).CopyTo(key);
            p[..(int)nonShared].CopyTo(key.AsSpan((int)shared));
            p = p[(int)nonShared..];
            if (!valueLengthPresent)
            {
                int n1 = Coding.DecodeVarint64(p, out _);
                int n2 = Coding.DecodeVarint64(p[n1..], out _);
                valueLen = (uint)(n1 + n2);
            }
            entries.Add((key, p[..(int)valueLen].ToArray()));
            p = p[(int)valueLen..];
            lastKey = key;
        }
        return entries;
    }
}
