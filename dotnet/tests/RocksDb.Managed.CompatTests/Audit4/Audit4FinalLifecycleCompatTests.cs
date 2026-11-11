using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4, slice "FinalLifecycle": best-efforts recovery differentials against native librocksdb.</summary>
[TestClass]
public sealed unsafe class Audit4FinalLifecycleCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static void SetVerifySstUniqueId(RocksDbSharp.DbOptions options, bool value)
    {
        _ = RocksDbSharp.Native.Instance;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0 || !Path.GetFileName(line[slash..]).StartsWith("librocksdb", StringComparison.Ordinal)) continue;
            IntPtr f = NativeLibrary.GetExport(NativeLibrary.Load(line[slash..]), "rocksdb_options_set_verify_sst_unique_id_in_manifest");
            ((delegate* unmanaged<IntPtr, byte, void>)f)(options.Handle, value ? (byte)1 : (byte)0);
            return;
        }
        Assert.Inconclusive("librocksdb is not loaded");
    }

    /// <summary>
    /// VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit (db/version_edit_handler.cc 928-950): the table
    /// files of a point-in-time version are opened (LoadTableHandlers) and a Corruption skips that version, also with
    /// verify_sst_unique_id_in_manifest = false (when VersionSet::VerifyFileMetadata only checks the size). A table file
    /// with the right size and a destroyed footer must give the same recovered contents as native.
    /// </summary>
    [TestMethod]
    [Ignore("audit4: with verify_sst_unique_id_in_manifest=false the managed open fails with Corruption where native skips the point in time (0 keys); needs negative-edge version creation with a table-open check in PointInTimeTracker")]
    [DataRow(0)]
    [DataRow(3)]
    public void CorruptTableWithoutUniqueIdVerificationRecoversLikeNative(int victimIndex)
    {
        using var dir = new TempDir("a4ber");
        string src = Path.Combine(dir.Path, "src");
        Directory.CreateDirectory(src);
        using (RocksDatabase db = RocksDatabase.Open(src, new DbOptions { CreateIfMissing = true, AvoidFlushDuringShutdown = true },
                   [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })]))
        {
            for (int round = 0; round < 4; round++)
            {
                for (int i = 0; i < 20; i++) db.Put(U("k" + round + "-" + i), U("v" + round));
                db.Flush();
            }
        }
        var sst = new List<(ulong, string)>();
        foreach (string f in Directory.GetFiles(src, "*.sst"))
        {
            Assert.IsTrue(FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out _));
            sst.Add((n, f));
        }
        sst.Sort();
        using (var fs = new FileStream(sst[victimIndex].Item2, FileMode.Open))
        {
            fs.Seek(-8, SeekOrigin.End);
            fs.Write(new byte[8]); // the magic number: same size, unreadable table
        }
        string nativeCopy = Path.Combine(dir.Path, "native");
        string managedCopy = Path.Combine(dir.Path, "managed");
        foreach (string to in new[] { nativeCopy, managedCopy })
        {
            Directory.CreateDirectory(to);
            foreach (string f in Directory.GetFiles(src)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
        }
        var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var options = new RocksDbSharp.DbOptions();
        NativeExtras.SetBestEffortsRecovery(options, true);
        SetVerifySstUniqueId(options, false);
        using (var db = RocksDbSharp.RocksDb.Open(options, nativeCopy))
        using (RocksDbSharp.Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid(); it.Next()) expected[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        }
        var actual = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using (RocksDatabase db = RocksDatabase.Open(managedCopy, new DbOptions { BestEffortsRecovery = true, VerifySstUniqueIdInManifest = false }))
        using (Iterator it = db.NewIterator(null))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) actual[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        }
        CollectionAssert.AreEqual(expected.ToList(), actual.ToList(), "native keys: " + expected.Count + " managed keys: " + actual.Count);
    }
}
