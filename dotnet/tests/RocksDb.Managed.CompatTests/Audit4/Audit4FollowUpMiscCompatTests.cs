using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4 follow-up (misc): what native RocksDB makes of the managed trie index's root prefix key.</summary>
[TestClass]
public sealed class Audit4FollowUpMiscCompatTests
{
    private static RocksDbSharp.DbOptions NativeTrieOptions()
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions();
        var baseOptions = new RocksDbSharp.DbOptions();
        IntPtr str = Marshal.StringToHGlobalAnsi(
            "disable_auto_compactions=true;block_based_table_factory={format_version=7;user_defined_index_factory=trie_index;use_udi_as_primary_index=true}");
        try
        {
            n.rocksdb_get_options_from_string(baseOptions.Handle, str, o.Handle);
        }
        finally
        {
            Marshal.FreeHGlobal(str);
        }
        GC.KeepAlive(baseOptions);
        return o;
    }

    /// <summary>
    /// The managed LoudsTrieBuilder stores the empty separator (a data block ending with the empty user key) as the
    /// root's prefix key, where upstream's LoudsTrieBuilder::Finish (utilities/trie_index/louds_trie.cc) drops the
    /// handle. Native's LoudsTrieIterator reads a root prefix key through its generic prefix-key paths
    /// (DescendToLeftmostLeaf, SeekImpl's "target fully consumed" check, Next's "Root is the prefix key" branch,
    /// Retreat), so a managed table with other separators next to the empty one reads correctly in native.
    /// </summary>
    [TestMethod]
    [DataRow(2)]
    [DataRow(200)]
    public void NativeReadsAManagedTrieIndexWithTheEmptySeparator(int otherKeys)
    {
        using var dir = new TempDir("a4-trie-empty");
        var expected = new List<string>();
        WriteManaged(dir.Path, otherKeys, expected);
        using var db = RocksDbSharp.RocksDb.Open(NativeTrieOptions(), dir.Path);
        Assert.IsNotNull(db.Get(Array.Empty<byte>()));
        Assert.IsNull(db.Get(new byte[] { 0x01 }));
        using RocksDbSharp.Iterator it = db.NewIterator();
        var forward = new List<string>();
        for (it.SeekToFirst(); it.Valid(); it.Next()) forward.Add(System.Text.Encoding.Latin1.GetString(it.Key()));
        CollectionAssert.AreEqual(expected, forward);
        var backward = new List<string>();
        for (it.SeekToLast(); it.Valid(); it.Prev()) backward.Add(System.Text.Encoding.Latin1.GetString(it.Key()));
        backward.Reverse();
        CollectionAssert.AreEqual(expected, backward);
        it.SeekForPrev(new byte[] { 0x01 });
        Assert.IsTrue(it.Valid());
        Assert.AreEqual(0, it.Key().Length);
    }

    /// <summary>
    /// A table holding only the empty user key: the trie is a label-less root with its prefix bit. Native finds it
    /// going forward (DescendToLeftmostLeaf checks the prefix key first) and by point lookup.
    /// </summary>
    [TestMethod]
    public void NativeReadsAManagedTrieIndexHoldingOnlyTheEmptySeparator_Forward()
    {
        using var dir = new TempDir("a4-trie-only-empty");
        var expected = new List<string>();
        WriteManaged(dir.Path, 0, expected);
        using var db = RocksDbSharp.RocksDb.Open(NativeTrieOptions(), dir.Path);
        Assert.IsNotNull(db.Get(Array.Empty<byte>()));
        using RocksDbSharp.Iterator it = db.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid());
        Assert.AreEqual(0, it.Key().Length);
        it.Next();
        Assert.IsFalse(it.Valid());
    }

    /// <summary>
    /// KNOWN LIMITATION, pinned. Same table, backward: native's DescendToRightmostLeaf (louds_trie.cc) does not look at
    /// prefix keys and gives up when the root has no label ("start >= s_labels_size_"), so native's SeekToLast /
    /// SeekForPrev find nothing in a managed table that holds only the empty user key (the managed iterator handles
    /// the label-less root itself). Native cannot write such a table at all (its builder drops the handle), so there
    /// is no encoding both readers iterate backward. If this test starts failing, native learned to read it.
    /// </summary>
    [TestMethod]
    public void NativeReadsAManagedTrieIndexHoldingOnlyTheEmptySeparator_Backward()
    {
        using var dir = new TempDir("a4-trie-only-empty-b");
        var expected = new List<string>();
        WriteManaged(dir.Path, 0, expected);
        using var db = RocksDbSharp.RocksDb.Open(NativeTrieOptions(), dir.Path);
        using RocksDbSharp.Iterator it = db.NewIterator();
        it.SeekToLast();
        Assert.IsFalse(it.Valid());
        it.SeekForPrev(new byte[] { 0x01 });
        Assert.IsFalse(it.Valid());
    }

    private static void WriteManaged(string path, int otherKeys, List<string> expected)
    {
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        cfo.TableOptions.UserDefinedIndexFactory = TrieIndexFactory.Instance;
        cfo.TableOptions.IndexMode = BlockBasedIndexMode.CustomDefault;
        cfo.TableOptions.BlockSize = 64;
        byte[] big = new byte[200];
        Array.Fill(big, (byte)'v');
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, [new ColumnFamilyDescriptor("default", cfo)]);
        db.Put(ReadOnlySpan<byte>.Empty, big);
        expected.Add("");
        for (int i = 0; i < otherKeys; i++)
        {
            byte[] k = [(byte)(i + 0x20)];
            db.Put(k, big);
            expected.Add(System.Text.Encoding.Latin1.GetString(k));
        }
        db.Flush();
    }
}
