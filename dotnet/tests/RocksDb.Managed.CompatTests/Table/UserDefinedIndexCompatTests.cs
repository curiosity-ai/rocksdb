using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// User-defined index interoperability with native RocksDB 11.12 (rocksdb_block_based_options_set_user_defined_index_factory_from_string
/// with the built-in "trie_index" factory, use_udi_as_primary_index): files carrying a custom index written by either
/// side stay readable by the other through the (populated) standard index, with the custom index block and properties
/// preserved. The trie index format itself is not ported (utilities/trie_index).
/// </summary>
[TestClass]
public sealed class UserDefinedIndexCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    /// <summary>A minimal custom index for the managed writer: block handles only (8-byte entries).</summary>
    private sealed class HandleListIndexFactory : UserDefinedIndexFactory
    {
        public override string Name => "compat.HandleList";
        public override UserDefinedIndexBuilder NewBuilder(UserDefinedIndexOptions options) => new Builder();
        public override UserDefinedIndexReader NewReader(UserDefinedIndexOptions options, ReadOnlyMemory<byte> indexContents) =>
            throw RocksDbException.NotSupported("write-only test index");

        private sealed class Builder : UserDefinedIndexBuilder
        {
            private readonly ArrayBufferWriter<byte> _out = new();
            public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNextBlock,
                BlockHandle blockHandle, in UserDefinedIndexEntryContext context)
            {
                blockHandle.EncodeTo(_out);
                return lastKeyInCurrentBlock;
            }
            public override ReadOnlySpan<byte> Finish() => _out.WrittenSpan;
            public override ulong EstimatedSize => (ulong)_out.WrittenCount;
        }
    }

    private static RocksDbSharp.ColumnFamilyOptions NativeTrie(bool primary)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(7).SetBlockSize(512);
        IntPtr s = Marshal.StringToHGlobalAnsi("trie_index");
        try
        {
            n.rocksdb_block_based_options_set_user_defined_index_factory_from_string(bbto.Handle, s, (UIntPtr)"trie_index".Length);
        }
        finally
        {
            Marshal.FreeHGlobal(s);
        }
        n.rocksdb_block_based_options_set_use_udi_as_primary_index(bbto.Handle, (byte)(primary ? 1 : 0));
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        cf.SetBlockBasedTableFactory(bbto);
        cf.SetCompression(RocksDbSharp.Compression.No);
        s_keepAlive.Add(bbto);
        s_keepAlive.Add(cf);
        return cf;
    }

    private static SortedDictionary<string, string> Data(int n)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < n; i++) d["key" + (i * 7).ToString("D6", System.Globalization.CultureInfo.InvariantCulture)] = "value" + i + new string('x', i % 40);
        return d;
    }

    [TestMethod]
    public void NativeTrieIndexDb_ReadByManagedThroughStandardIndex()
    {
        foreach (bool primary in new[] { false, true })
        {
            SortedDictionary<string, string> data = Data(3000);
            using var dir = new TempDir("udi-n2m");
            var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            using (var db = RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(NativeTrie(primary))))
            {
                foreach (KeyValuePair<string, string> kv in data) db.Put(kv.Key, kv.Value);
                db.Flush(new RocksDbSharp.FlushOptions());
            }
            string sst = Directory.GetFiles(dir.Path, "*.sst")[0];
            using (var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions()))
            {
                Assert.IsTrue(reader.MetaBlocks.ContainsKey(UserDefinedIndexFactory.MetaBlockPrefix + "trie_index"));
                Assert.AreEqual(primary ? 1UL : 0UL, reader.Properties.UdiIsPrimaryIndex);
                Assert.AreEqual(0UL, reader.Properties.StandardIndexIsStub);
                Assert.AreEqual(0UL, reader.Footer.IncompatibleFeatures);
            }
            // No factory configured (the OPTIONS file's factory and legacy flags are preserved verbatim).
            using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), new ColumnFamilyOptions());
            foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, Encoding.ASCII.GetString(mdb.Get(Encoding.ASCII.GetBytes(kv.Key))!));
            using Iterator it = mdb.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(data.Count, n);
        }
    }

    [TestMethod]
    public void ManagedCustomIndexDb_ReadByNative()
    {
        // kStandardDefault: the managed DB itself reads through the standard index, so the write-only custom index works.
        foreach (BlockBasedIndexMode mode in new[] { BlockBasedIndexMode.StandardDefault, BlockBasedIndexMode.StandardOnly })
        {
            SortedDictionary<string, string> data = Data(3000);
            using var dir = new TempDir("udi-m2n");
            var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None };
            cf.TableOptions.BlockSize = 512;
            cf.TableOptions.UserDefinedIndexFactory = new HandleListIndexFactory();
            cf.TableOptions.IndexMode = mode;
            using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, cf))
            {
                foreach (KeyValuePair<string, string> kv in data) db.Put(Encoding.ASCII.GetBytes(kv.Key), Encoding.ASCII.GetBytes(kv.Value));
                db.Flush();
            }
            string sst = Directory.GetFiles(dir.Path, "*.sst")[0];
            byte[] file = File.ReadAllBytes(sst);
            Footer footer = Footer.Decode(file.AsSpan(file.Length - Footer.EncodedLength), (ulong)(file.Length - Footer.EncodedLength));
            Assert.AreEqual(0UL, footer.IncompatibleFeatures);
            using (var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions()))
                Assert.AreEqual(mode == BlockBasedIndexMode.StandardDefault, reader.MetaBlocks.ContainsKey(UserDefinedIndexFactory.MetaBlockPrefix + "compat.HandleList"));
            // Native (no UDI factory) reads through the standard index.
            using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path);
            foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, ndb.Get(kv.Key));
            using RocksDbSharp.Iterator it = ndb.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid(); it.Next()) n++;
            Assert.AreEqual(data.Count, n);
        }
    }
}
