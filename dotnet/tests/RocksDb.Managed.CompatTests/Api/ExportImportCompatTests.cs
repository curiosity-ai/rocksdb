using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Column family export / import across engines (Checkpoint::ExportColumnFamily, DB::CreateColumnFamilyWithImport through
/// the 11.12 C API): a column family exported by native is imported by the managed port and vice versa, with levels,
/// sequence numbers, deletions and range deletions preserved.
/// </summary>
[TestClass]
public sealed class ExportImportCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string Key(int i) => "key" + i.ToString("D6");
    private static string Expected(int i) => i >= 300 ? "late" + i : "v" + i;
    private static bool Deleted(int i) => (i < 300 && i % 7 == 0) || (i >= 100 && i < 120);

    [TestMethod]
    public void NativeExportManagedImport()
    {
        using var dir = new TempDir("expnative");
        string src = Path.Combine(dir.Path, "src"), exp = Path.Combine(dir.Path, "export"), dst = Path.Combine(dir.Path, "dst");
        (string Comparator, List<NativeRemoteCompaction.NativeFile> Files) native;
        ulong srcSeq;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        var cfs = new RocksDbSharp.ColumnFamilies { { "data", new RocksDbSharp.ColumnFamilyOptions() } };
        using (var ndb = RocksDbSharp.RocksDb.Open(o, src, cfs))
        {
            RocksDbSharp.ColumnFamilyHandle cf = ndb.GetColumnFamily("data");
            for (int i = 0; i < 300; i++) ndb.Put(U(Key(i)), U("v" + i), cf);
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(ndb.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, cf.Handle);
            ndb.CompactRange((byte[]?)null, null, cf);
            for (int i = 0; i < 300; i += 7) ndb.Remove(U(Key(i)), cf);
            using (var b = new RocksDbSharp.WriteBatch())
            {
                byte[] x = U(Key(100)), y = U(Key(120));
                RocksDbSharp.Native.Instance.rocksdb_writebatch_delete_range_cf(b.Handle, cf.Handle, x, (UIntPtr)x.Length, y, (UIntPtr)y.Length);
                ndb.Write(b);
            }
            for (int i = 300; i < 350; i++) ndb.Put(U(Key(i)), U("late" + i), cf);
            native = NativeRemoteCompaction.ExportColumnFamily(ndb, cf, exp);
            srcSeq = ndb.GetLatestSequenceNumber();
        }
        Assert.AreEqual("leveldb.BytewiseComparator", native.Comparator);
        Assert.IsTrue(native.Files.Count >= 2);
        var files = new List<LiveFileMetadata>();
        foreach (NativeRemoteCompaction.NativeFile f in native.Files)
        {
            files.Add(new LiveFileMetadata(f.Name, f.Directory, 0, "data", f.Level, f.Size, f.SmallestKey, f.LargestKey, f.SmallestSeqno, f.LargestSeqno, 0));
        }
        using RocksDatabase m = RocksDatabase.Open(dst, new DbOptions { CreateIfMissing = true });
        ColumnFamily imported = m.CreateColumnFamilyWithImport("imported", new ColumnFamilyOptions(), new ImportColumnFamilyOptions(),
            new ExportImportFilesMetadata(native.Comparator, files));
        Assert.AreEqual(srcSeq, m.GetLatestSequenceNumber());
        for (int i = 0; i < 350; i++)
        {
            byte[]? v = imported.Get(U(Key(i)));
            if (Deleted(i)) Assert.IsNull(v, Key(i));
            else Assert.AreEqual(Expected(i), Encoding.UTF8.GetString(v!), Key(i));
        }
        var levels = new SortedSet<int>();
        foreach (LiveFileMetadata f in m.GetLiveFilesMetadata()) if (f.ColumnFamilyName == "imported") levels.Add(f.Level);
        var nativeLevels = new SortedSet<int>();
        foreach (NativeRemoteCompaction.NativeFile f in native.Files) nativeLevels.Add(f.Level);
        CollectionAssert.AreEqual(nativeLevels.ToList(), levels.ToList());
    }

    [TestMethod]
    public void ManagedExportNativeImport()
    {
        using var dir = new TempDir("expmanaged");
        string src = Path.Combine(dir.Path, "src"), exp = Path.Combine(dir.Path, "export"), dst = Path.Combine(dir.Path, "dst");
        ExportImportFilesMetadata meta;
        using (RocksDatabase m = RocksDatabase.Open(src, new DbOptions { CreateIfMissing = true }))
        {
            ColumnFamily cf = m.CreateColumnFamily("data", new ColumnFamilyOptions());
            for (int i = 0; i < 300; i++) cf.Put(U(Key(i)), U("v" + i));
            cf.Flush();
            cf.CompactRange();
            for (int i = 0; i < 300; i += 7) cf.Delete(U(Key(i)));
            cf.DeleteRange(U(Key(100)), U(Key(120)));
            for (int i = 300; i < 350; i++) cf.Put(U(Key(i)), U("late" + i));
            meta = cf.ExportColumnFamily(exp);
        }
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        using var ndb = RocksDbSharp.RocksDb.Open(o, dst);
        IntPtr h = NativeRemoteCompaction.CreateColumnFamilyWithImport(ndb, new RocksDbSharp.ColumnFamilyOptions(), "imported", moveFiles: false, meta);
        for (int i = 0; i < 350; i++)
        {
            string? v = NativeRemoteCompaction.Get(ndb, h, Key(i));
            if (Deleted(i)) Assert.IsNull(v, Key(i));
            else Assert.AreEqual(Expected(i), v, Key(i));
        }
    }
}
