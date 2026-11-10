using System.Text;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4 follow-up, flush / compaction slice: differential checks against native librocksdb.</summary>
[TestClass]
public sealed class Audit4FollowUpCompactionCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static TableProperties ReadProperties(string path)
    {
        using var reader = new SstFileReader(new ColumnFamilyOptions());
        reader.Open(path);
        return reader.GetTableProperties();
    }

    /// <summary>
    /// BlockBasedTableBuilder::CompressAndVerifyBlock (block_based_table_builder.cc: "bool compression_attempted =
    /// !compressed_output->empty(); ... if (is_data_block) { if (compression_attempted)
    /// num_data_blocks_compression_rejected++ else num_data_blocks_compression_bypassed++ }"): a data block stored
    /// uncompressed counts as rejected when a compressor ran and its output was not worth keeping, and as bypassed
    /// when no compression was attempted (no compressor for the table).
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void UncompressedDataBlocksAreCountedLikeNative(bool snappy)
    {
        string dir = Path.Combine(Path.GetTempPath(), "audit4-fc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            // Incompressible values: with snappy every data block is attempted and rejected.
            var rnd = new Random(42);
            var values = new byte[40][];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = new byte[1000];
                rnd.NextBytes(values[i]);
            }
            string nativePath = Path.Combine(dir, "native.sst");
            var cfo = new RocksDbSharp.ColumnFamilyOptions().SetCompression(snappy ? RocksDbSharp.Compression.Snappy : RocksDbSharp.Compression.No);
            s_keepAlive.Add(cfo);
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
            {
                w.Open(nativePath);
                for (int i = 0; i < values.Length; i++) w.Put(B("key" + i.ToString("D4")), values[i]);
                w.Finish();
            }
            string managedPath = Path.Combine(dir, "managed.sst");
            var mo = new ColumnFamilyOptions { Compression = snappy ? RocksDb.Managed.Compression.CompressionType.Snappy : RocksDb.Managed.Compression.CompressionType.None };
            using (var w = new SstFileWriter(mo))
            {
                w.Open(managedPath);
                for (int i = 0; i < values.Length; i++) w.Put(B("key" + i.ToString("D4")), values[i]);
                w.Finish();
            }
            TableProperties native = ReadProperties(nativePath);
            TableProperties managed = ReadProperties(managedPath);
            Assert.AreEqual(native.NumDataBlocks, managed.NumDataBlocks);
            Assert.IsTrue(native.NumDataBlocks > 1);
            Assert.AreEqual(native.NumDataBlocksCompressionRejected, managed.NumDataBlocksCompressionRejected, "rejected");
            Assert.AreEqual(native.NumDataBlocksCompressionBypassed, managed.NumDataBlocksCompressionBypassed, "bypassed");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
