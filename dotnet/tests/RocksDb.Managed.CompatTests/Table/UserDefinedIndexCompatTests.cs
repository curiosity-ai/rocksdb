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
/// preserved. The ported trie index (TrieIndexFactory) writes trie blocks byte-identical to native SstFileWriter output,
/// reads native trie files through the trie, and native reads managed trie files through its trie (primary UDI).
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

    // ---- the ported trie index (utilities/trie_index) ----------------------------------------------------------

    private const string TrieBlockName = UserDefinedIndexFactory.MetaBlockPrefix + TrieIndexFactory.ClassName;

    private static ColumnFamilyOptions ManagedTrie(BlockBasedIndexMode mode)
    {
        var cf = new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None };
        cf.TableOptions.BlockSize = 512;
        cf.TableOptions.FormatVersion = 7;
        cf.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
        cf.TableOptions.IndexMode = mode;
        return cf;
    }

    private static byte[] TrieBlock(string sst)
    {
        byte[] file = File.ReadAllBytes(sst);
        using var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions());
        BlockHandle h = reader.MetaBlocks[TrieBlockName];
        return file.AsSpan((int)h.Offset, (int)h.Size).ToArray();
    }

    /// <summary>Data with long common prefixes, prefix keys and many versions of a few user keys (overflow runs).</summary>
    private static List<(string Key, string Value)> TrieData(Random rnd, int n)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        while (d.Count < n)
        {
            int i = rnd.Next(1_000_000);
            string k = (i % 5) switch
            {
                0 => "user/" + i.ToString("D7", System.Globalization.CultureInfo.InvariantCulture),
                1 => "user/" + (i % 1000).ToString(System.Globalization.CultureInfo.InvariantCulture),
                2 => "zz" + new string('0', 20) + (i % 300).ToString(System.Globalization.CultureInfo.InvariantCulture),
                3 => Convert.ToHexString(BitConverter.GetBytes(i)),
                _ => "m" + (char)('a' + i % 26),
            };
            d[k] = "v" + i + new string('x', rnd.Next(60));
        }
        var list = new List<(string, string)>();
        foreach (KeyValuePair<string, string> kv in d) list.Add((kv.Key, kv.Value));
        return list;
    }

    [TestMethod]
    public void TrieIndex_SstFileWriter_ByteIdenticalTrieBlock()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        foreach (int n in new[] { 1, 40, 3000 })
        {
            List<(string Key, string Value)> data = TrieData(rnd, n);
            using var dir = new TempDir("trie-sst");
            string nativePath = System.IO.Path.Combine(dir.Path, "native.sst");
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), NativeTrie(primary: false)))
            {
                w.Open(nativePath);
                foreach ((string k, string v) in data) w.Add(k, v);
                w.Finish();
            }
            string managedPath = System.IO.Path.Combine(dir.Path, "managed.sst");
            using (var w = new SstFileWriter(ManagedTrie(BlockBasedIndexMode.StandardDefault)))
            {
                w.Open(managedPath);
                foreach ((string k, string v) in data) w.Put(Encoding.ASCII.GetBytes(k), Encoding.ASCII.GetBytes(v));
                w.Finish();
            }
            byte[] nt = TrieBlock(nativePath), mt = TrieBlock(managedPath);
            CollectionAssert.AreEqual(nt, mt, "trie block, seed " + seed + " n " + n);
            // The native trie parses and resolves every key's block like the managed one.
            using UserDefinedIndexReader r = TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions(), nt);
            Assert.IsTrue(((TrieIndexReader)r).Trie.NumKeys > 0);
        }
    }

    [TestMethod]
    public void TrieIndex_NativeDbReadByManagedThroughTrie()
    {
        var rnd = new Random(77);
        List<(string Key, string Value)> data = TrieData(rnd, 3000);
        using var dir = new TempDir("trie-n2m");
        var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        using (var db = RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(NativeTrie(primary: true))))
        {
            foreach ((string k, string v) in data) db.Put(k, v);
            // Same-user-key runs across blocks.
            var snaps = new List<RocksDbSharp.Snapshot>();
            for (int i = 0; i < 200; i++) { db.Put("user/hot", "hot" + i + new string('h', 40)); snaps.Add(db.CreateSnapshot()); }
            db.Flush(new RocksDbSharp.FlushOptions());
            foreach (RocksDbSharp.Snapshot s in snaps) s.Dispose();
        }
        data.Add(("user/hot", "hot199" + new string('h', 40)));
        data.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        foreach (BlockBasedIndexMode mode in new[] { BlockBasedIndexMode.CustomDefault, BlockBasedIndexMode.StandardDefault })
        {
            using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), ManagedTrie(mode));
            var ro = new ReadOptions { ReadIndex = ReadIndex.PreferCustom };
            foreach ((string k, string v) in data) Assert.AreEqual(v, Encoding.ASCII.GetString(mdb.Get(ro, Encoding.ASCII.GetBytes(k))!), k);
            Assert.IsNull(mdb.Get(ro, Encoding.ASCII.GetBytes("user/0000000x")));
            using Iterator it = mdb.NewIterator(ro);
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) Assert.AreEqual(data[n++].Key, Encoding.ASCII.GetString(it.Key));
            Assert.AreEqual(data.Count, n);
            for (it.SeekToLast(); it.Valid; it.Prev()) Assert.AreEqual(data[--n].Key, Encoding.ASCII.GetString(it.Key));
            Assert.AreEqual(0, n);
        }
        // The file's index is the native trie, opened through the managed trie reader.
        string sst = Directory.GetFiles(dir.Path, "*.sst")[0];
        using var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst),
            new TableReaderOptions { UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = BlockBasedIndexMode.CustomDefault });
        Assert.IsInstanceOfType<TrieIndexReader>(reader.UserDefinedIndex);
        Assert.IsTrue(((TrieIndexReader)reader.UserDefinedIndex!).Trie.NumOverflowBlocks > 0);
    }

    [TestMethod]
    public void TrieIndex_ManagedDbReadByNativeThroughTrie()
    {
        var rnd = new Random(78);
        List<(string Key, string Value)> data = TrieData(rnd, 3000);
        using var dir = new TempDir("trie-m2n");
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, ManagedTrie(BlockBasedIndexMode.CustomDefault)))
        {
            foreach ((string k, string v) in data) db.Put(Encoding.ASCII.GetBytes(k), Encoding.ASCII.GetBytes(v));
            var snaps = new List<Snapshot>();
            for (int i = 0; i < 200; i++) { db.Put(Encoding.ASCII.GetBytes("user/hot"), Encoding.ASCII.GetBytes("hot" + i + new string('h', 40))); snaps.Add(db.GetSnapshot()); }
            db.Flush();
            foreach (Snapshot s in snaps) s.Dispose();
        }
        data.Add(("user/hot", "hot199" + new string('h', 40)));
        data.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        string sst = Directory.GetFiles(dir.Path, "*.sst")[0];
        using (var reader = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions()))
            Assert.AreEqual(1UL, reader.Properties.UdiIsPrimaryIndex);
        // Native with the trie as primary index (11.12: use_udi_as_primary_index routes every read through the UDI and
        // fails the open when the UDI block cannot be loaded).
        using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path, new RocksDbSharp.ColumnFamilies(NativeTrie(primary: true)));
        foreach ((string k, string v) in data) Assert.AreEqual(v, ndb.Get(k), k);
        Assert.IsNull(ndb.Get("user/0000000x"));
        using RocksDbSharp.Iterator it = ndb.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next()) Assert.AreEqual(data[n++].Key, it.StringKey());
        Assert.AreEqual(data.Count, n);
        it.Seek("user/5");
        Assert.IsTrue(it.Valid());
        Assert.AreEqual(data[data.FindIndex(e => string.CompareOrdinal(e.Key, "user/5") >= 0)].Key, it.StringKey());
    }

    /// <summary>
    /// The same writes (including versions pinned by snapshots, so user keys span blocks and the trie gets overflow
    /// runs with real tags) flushed by native and by the managed port give byte-identical trie blocks.
    /// </summary>
    [TestMethod]
    public void TrieIndex_FlushWithOverflowRuns_ByteIdenticalTrieBlock()
    {
        List<(string Key, string Value)> data = TrieData(new Random(79), 2000);
        using var dir = new TempDir("trie-flush");
        string nativeDir = System.IO.Path.Combine(dir.Path, "native"), managedDir = System.IO.Path.Combine(dir.Path, "managed");
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true), nativeDir,
                   new RocksDbSharp.ColumnFamilies(NativeTrie(primary: false))))
        {
            var snaps = new List<RocksDbSharp.Snapshot>();
            for (int i = 0; i < data.Count; i++)
            {
                db.Put(data[i].Key, data[i].Value);
                if (i % 10 == 0) { db.Put("user/hot", "hot" + i + new string('h', 30)); snaps.Add(db.CreateSnapshot()); }
            }
            db.Flush(new RocksDbSharp.FlushOptions());
            foreach (RocksDbSharp.Snapshot s in snaps) s.Dispose();
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir, new DbOptions { CreateIfMissing = true }, ManagedTrie(BlockBasedIndexMode.StandardDefault)))
        {
            var snaps = new List<Snapshot>();
            for (int i = 0; i < data.Count; i++)
            {
                db.Put(Encoding.ASCII.GetBytes(data[i].Key), Encoding.ASCII.GetBytes(data[i].Value));
                if (i % 10 == 0) { db.Put(Encoding.ASCII.GetBytes("user/hot"), Encoding.ASCII.GetBytes("hot" + i + new string('h', 30))); snaps.Add(db.GetSnapshot()); }
            }
            db.Flush();
            foreach (Snapshot s in snaps) s.Dispose();
        }
        byte[] nt = TrieBlock(Directory.GetFiles(nativeDir, "*.sst")[0]);
        byte[] mt = TrieBlock(Directory.GetFiles(managedDir, "*.sst")[0]);
        using UserDefinedIndexReader r = TrieIndexFactory.Instance.NewReader(new UserDefinedIndexOptions(), nt);
        Assert.IsTrue(((TrieIndexReader)r).Trie.NumOverflowBlocks > 0);
        CollectionAssert.AreEqual(nt, mt);
    }
}
