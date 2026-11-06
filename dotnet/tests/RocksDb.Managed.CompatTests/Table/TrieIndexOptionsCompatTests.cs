using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// OPTIONS-file entries of the user-defined index against native RocksDB 11.12: native writes
/// <c>user_defined_index_factory=trie_index</c> (or <c>nullptr</c>), <c>index_mode</c> and the legacy
/// <c>use_udi_as_primary_index</c> / <c>fail_if_no_udi_on_open</c> booleans as set (11.12 routes reads by the booleans
/// only). The managed parser resolves the factory to <see cref="TrieIndexFactory"/>, derives the effective
/// <see cref="BlockBasedTableOptions.IndexMode"/>, writes the entries back unchanged, and a managed OPTIONS file
/// configures native's trie index the same way.
/// </summary>
[TestClass]
public sealed class TrieIndexOptionsCompatTests
{
    private const string Section = "[TableOptions/BlockBasedTable \"default\"]";

    /// <summary>Opens a native DB configured from an options string, writes a key, flushes; returns the OPTIONS file text.</summary>
    private static string NativeOptionsFile(string dir, string tableOptions)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions();
        var baseOptions = new RocksDbSharp.DbOptions();
        IntPtr str = Marshal.StringToHGlobalAnsi("create_if_missing=true;block_based_table_factory={" + tableOptions + "}");
        try
        {
            n.rocksdb_get_options_from_string(baseOptions.Handle, str, o.Handle);
        }
        finally
        {
            Marshal.FreeHGlobal(str);
        }
        using (var db = RocksDbSharp.RocksDb.Open(o, dir))
        {
            db.Put("a", "b");
            db.Flush(new RocksDbSharp.FlushOptions());
        }
        GC.KeepAlive(baseOptions);
        return File.ReadAllText(Directory.GetFiles(dir, "OPTIONS-*")[^1]);
    }

    private static string[] SectionLines(string text)
    {
        int i = text.IndexOf(Section, StringComparison.Ordinal);
        Assert.IsTrue(i >= 0);
        int j = text.IndexOf("\n[", i + Section.Length, StringComparison.Ordinal);
        string[] lines = (j < 0 ? text[i..] : text[i..j]).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Array.Sort(lines, StringComparer.Ordinal); // native iterates an unordered_map: compare the line sets
        return lines;
    }

    private static ulong UdiPrimaryOfFirstSst(string dir)
    {
        string sst = Directory.GetFiles(dir, "*.sst")[0];
        using var reader = BlockBasedTableReader.Open(LocalFileSystem.Instance.OpenRandomAccess(sst), new TableReaderOptions());
        Assert.IsTrue(reader.MetaBlocks.ContainsKey(UserDefinedIndexFactory.MetaBlockPrefix + TrieIndexFactory.ClassName));
        return reader.Properties.UdiIsPrimaryIndex;
    }

    [TestMethod]
    public void NativeOptionsNamingTrieIndex_ResolveAndRoundTrip()
    {
        (string Table, BlockBasedIndexMode Mode, ulong NativePrimary)[] cases =
        [
            ("user_defined_index_factory=trie_index", BlockBasedIndexMode.StandardDefault, 0),
            ("user_defined_index_factory=trie_index;use_udi_as_primary_index=true", BlockBasedIndexMode.CustomDefault, 1),
            ("user_defined_index_factory=trie_index;fail_if_no_udi_on_open=true", BlockBasedIndexMode.StandardRequired, 0),
            ("user_defined_index_factory=trie_index;index_mode=kStandardRequired", BlockBasedIndexMode.StandardRequired, 0),
            // 11.12 keeps index_mode=kCustomDefault but does not route by it; the managed port follows the mode.
            ("user_defined_index_factory=trie_index;index_mode=kCustomDefault", BlockBasedIndexMode.CustomDefault, 0),
            ("user_defined_index_factory={id=trie_index}", BlockBasedIndexMode.StandardDefault, 0),
        ];
        foreach ((string table, BlockBasedIndexMode mode, ulong nativePrimary) in cases)
        {
            using var dir = new TempDir("udi-opts");
            string text = NativeOptionsFile(dir.Path, "format_version=7;" + table);
            Assert.AreEqual(nativePrimary, UdiPrimaryOfFirstSst(dir.Path), table);

            OptionsFile parsed = OptionsFile.Parse(text, preserveUnknownOptions: true);
            BlockBasedTableOptions t = parsed.ColumnFamilies[0].Options.TableOptions;
            Assert.AreSame(TrieIndexFactory.Instance, t.UserDefinedIndexFactory, table);
            Assert.IsNull(t.UnresolvedUserDefinedIndexFactoryName);
            Assert.AreEqual(mode, t.IndexMode, table);
            Assert.IsFalse(t.UnknownOptions.ContainsKey("user_defined_index_factory"));
            Assert.IsFalse(t.UnknownOptions.ContainsKey("index_mode"));

            // Written back line-for-line as native wrote it.
            CollectionAssert.AreEqual(SectionLines(text), SectionLines(parsed.Serialize()), table);

            // The managed DB opened with these options builds the trie and routes by the same effective mode.
            using var managedDir = new TempDir("udi-opts-m");
            ColumnFamilyOptions cfo = parsed.ColumnFamilies[0].Options.Clone();
            Assert.AreSame(TrieIndexFactory.Instance, cfo.TableOptions.UserDefinedIndexFactory);
            using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo))
            {
                db.Put("a"u8, "b"u8);
                db.Flush();
                Assert.AreEqual("b", System.Text.Encoding.ASCII.GetString(db.Get("a"u8.ToArray())!));
            }
            Assert.AreEqual(mode == BlockBasedIndexMode.CustomDefault ? 1UL : 0UL, UdiPrimaryOfFirstSst(managedDir.Path), table);
        }
    }

    [TestMethod]
    public void NativeOptionsWithoutFactory_AndUnknownFactory_RoundTrip()
    {
        using var dir = new TempDir("udi-none");
        string text = NativeOptionsFile(dir.Path, "block_size=4096");
        StringAssert.Contains(text, "user_defined_index_factory=nullptr");
        OptionsFile parsed = OptionsFile.Parse(text, preserveUnknownOptions: true);
        BlockBasedTableOptions t = parsed.ColumnFamilies[0].Options.TableOptions;
        Assert.IsNull(t.UserDefinedIndexFactory);
        Assert.AreEqual(BlockBasedIndexMode.StandardDefault, t.IndexMode);
        CollectionAssert.AreEqual(SectionLines(text), SectionLines(parsed.Serialize()));

        // A factory name the port does not know is kept verbatim (and read as "no factory").
        string custom = text.Replace("user_defined_index_factory=nullptr", "user_defined_index_factory=my.CustomIndex", StringComparison.Ordinal);
        OptionsFile c = OptionsFile.Parse(custom, preserveUnknownOptions: true);
        Assert.IsNull(c.ColumnFamilies[0].Options.TableOptions.UserDefinedIndexFactory);
        Assert.AreEqual("my.CustomIndex", c.ColumnFamilies[0].Options.TableOptions.UnresolvedUserDefinedIndexFactoryName);
        StringAssert.Contains(c.Serialize(), "user_defined_index_factory=my.CustomIndex");
    }

    /// <summary>A managed OPTIONS file with the trie as primary index configures native 11.12 the same way.</summary>
    [TestMethod]
    public async Task ManagedOptionsFile_LoadedByNative_BuildsPrimaryTrie()
    {
        using var dir = new TempDir("udi-m2n-opts");
        var cfo = new ColumnFamilyOptions();
        cfo.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
        cfo.TableOptions.IndexMode = BlockBasedIndexMode.CustomDefault;
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            db.Put("k"u8, "v"u8);
        }
        string text = File.ReadAllText(OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir.Path)!);
        StringAssert.Contains(text, "  user_defined_index_factory=trie_index\n");
        StringAssert.Contains(text, "  use_udi_as_primary_index=true\n");
        StringAssert.Contains(text, "  index_mode=kCustomDefault\n");
        StringAssert.Contains(text, "  fail_if_no_udi_on_open=false\n");
        foreach (string f in Directory.GetFiles(dir.Path, "*.sst")) File.Delete(f); // only native's flush below

        // rocksdb_load_latest_options (strict: unknown options fail), then open native with the loaded CF options.
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr path = Marshal.StringToHGlobalAnsi(dir.Path);
        IntPtr env = n.rocksdb_create_default_env();
        IntPtr cache = n.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            n.rocksdb_load_latest_options(path, env, 0, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            Assert.AreEqual(1, num);
            IntPtr cfOptions = Marshal.ReadIntPtr(Marshal.ReadIntPtr(cfOptsOut));
            IntPtr db = n.rocksdb_open(cfOptions, dir.Path);
            IntPtr wo = n.rocksdb_writeoptions_create();
            IntPtr fo = n.rocksdb_flushoptions_create();
            try
            {
                n.rocksdb_put(db, wo, "a"u8.ToArray(), (UIntPtr)1, "b"u8.ToArray(), (UIntPtr)1, out IntPtr err);
                Assert.AreEqual(IntPtr.Zero, err);
                n.rocksdb_flush(db, fo, out err);
                Assert.AreEqual(IntPtr.Zero, err);
            }
            finally
            {
                n.rocksdb_writeoptions_destroy(wo);
                n.rocksdb_flushoptions_destroy(fo);
                n.rocksdb_close(db);
            }
            n.rocksdb_load_latest_options_destroy(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            n.rocksdb_cache_destroy(cache);
            n.rocksdb_env_destroy(env);
        }
        // Native built the trie and made it the primary index (it routes by use_udi_as_primary_index).
        Assert.AreEqual(1UL, UdiPrimaryOfFirstSst(dir.Path));
    }
}
