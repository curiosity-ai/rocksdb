using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using CompressionType = RocksDb.Managed.Compression.CompressionType;

namespace RocksDb.Managed.CompatTests.Infrastructure;

/// <summary>
/// An SST file opened with the managed <see cref="BlockBasedTableReader"/> plus its raw bytes, so tests can compare
/// individual blocks (as stored, or decompressed) between native- and managed-written files.
/// </summary>
internal sealed class SstFile : IDisposable
{
    private SstFile(byte[] bytes, BlockBasedTableReader reader)
    {
        Bytes = bytes;
        Reader = reader;
    }

    public byte[] Bytes { get; }
    public BlockBasedTableReader Reader { get; }
    public int FormatVersion => Reader.Footer.FormatVersion;
    public TableProperties Properties => Reader.Properties;

    public static SstFile Open(string path, TableReaderOptions options) =>
        new(File.ReadAllBytes(path), BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(path), options));

    /// <summary>Opens the single .sst file of a DB directory.</summary>
    public static SstFile OpenSingle(string dbDir, TableReaderOptions options)
    {
        string[] ssts = Directory.GetFiles(dbDir, "*.sst");
        if (ssts.Length != 1) throw new InvalidOperationException("expected exactly one .sst, found " + ssts.Length);
        return Open(ssts[0], options);
    }

    public BlockHandle? FindMeta(string name) => Reader.MetaBlocks.TryGetValue(name, out BlockHandle h) ? h : null;

    /// <summary>Block payload as stored in the file (possibly compressed), without the trailer.</summary>
    public byte[] RawBlock(BlockHandle h) => Bytes.AsSpan((int)h.Offset, (int)h.Size).ToArray();

    /// <summary>The compression type byte of the block trailer.</summary>
    public CompressionType BlockCompressionType(BlockHandle h) => (CompressionType)Bytes[(int)(h.Offset + h.Size)];

    /// <summary>Block contents after checksum verification and decompression.</summary>
    public byte[] UncompressedBlock(BlockHandle h)
    {
        Block b = BlockFetcher.ReadBlock(new ByteArrayFile(Bytes), Reader.Footer, h, verifyChecksums: true);
        try { return b.Data.ToArray(); }
        finally { b.Release(); }
    }

    /// <summary>Data block handles in index order (non-partitioned index).</summary>
    public List<BlockHandle> DataBlockHandles()
    {
        var result = new List<BlockHandle>();
        using var it = new BlockIter();
        Reader.InitIndexIter(it, Reader.IndexBlock);
        for (it.SeekToFirst(); it.Valid; it.Next()) result.Add(it.BlockHandle);
        return result;
    }

    public void Dispose() => Reader.Dispose();

    private sealed class ByteArrayFile(byte[] data) : IRandomAccessFile
    {
        public long Length => data.Length;
        public int Read(long offset, Span<byte> destination)
        {
            int n = (int)Math.Min(destination.Length, data.Length - offset);
            data.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }
        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) => new(Read(offset, destination.Span));
        public void Dispose() { }
    }
}
