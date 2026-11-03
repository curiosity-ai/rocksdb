using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// best_efforts_recovery against native RocksDB 11.12: a DB (written by either implementation) with missing, truncated
/// or corrupted table files is opened by both with best_efforts_recovery on copies of the same directory, and both must
/// recover the same point in time (same key/value contents). Neither replays the WAL.
/// </summary>
[TestClass]
public sealed class BestEffortsRecoveryCompatTests
{
    public enum Damage { DeleteNewest, DeleteOldest, TruncateMiddle, CorruptNewestFooter, DeleteL1 }

    private const int Rounds = 4;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>Several flushes (L0 files, optionally one compaction to L1 first) and an unflushed write left in the WAL.</summary>
    private static void BuildNative(string path, bool compactFirst)
    {
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        RocksDbSharp.Native.Instance.rocksdb_options_set_disable_auto_compactions(options.Handle, 1);
        using var db = RocksDbSharp.RocksDb.Open(options, path);
        var flush = new RocksDbSharp.FlushOptions();
        for (int round = 0; round < Rounds; round++)
        {
            for (int i = 0; i < 50; i++) db.Put(U("k" + round + "-" + i), U("v" + round));
            db.Put(U("common"), U("r" + round));
            db.Flush(flush);
            if (round == 1 && compactFirst) db.CompactRange((byte[])null!, (byte[])null!);
        }
        db.Put(U("wal-only"), U("x"));
    }

    private static void BuildManaged(string path, bool compactFirst)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, AvoidFlushDuringShutdown = true },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })]);
        for (int round = 0; round < Rounds; round++)
        {
            for (int i = 0; i < 50; i++) db.Put(U("k" + round + "-" + i), U("v" + round));
            db.Put(U("common"), U("r" + round));
            db.Flush();
            if (round == 1 && compactFirst) db.CompactRange(null, null);
        }
        db.Put(U("wal-only"), U("x"));
    }

    private static List<string> TableFiles(string path)
    {
        var r = new List<(ulong, string)>();
        foreach (string f in Directory.GetFiles(path, "*.sst"))
        {
            Assert.IsTrue(FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out _));
            r.Add((n, f));
        }
        r.Sort();
        return r.ConvertAll(static x => x.Item2);
    }

    private static string ApplyDamage(string path, Damage damage, bool compacted)
    {
        List<string> sst = TableFiles(path);
        string victim;
        switch (damage)
        {
            case Damage.DeleteNewest:
                victim = sst[^1];
                File.Delete(victim);
                break;
            case Damage.DeleteOldest:
                victim = sst[compacted ? 1 : 0];
                File.Delete(victim);
                break;
            case Damage.DeleteL1:
                victim = sst[0]; // the compaction output is the oldest live file
                File.Delete(victim);
                break;
            case Damage.TruncateMiddle:
                victim = sst[sst.Count / 2];
                using (var fs = new FileStream(victim, FileMode.Open)) fs.SetLength(fs.Length - 7);
                break;
            default:
                victim = sst[^1];
                using (var fs = new FileStream(victim, FileMode.Open))
                {
                    // The footer's magic number: same size, unreadable table.
                    fs.Seek(-8, SeekOrigin.End);
                    fs.Write(new byte[8]);
                }
                break;
        }
        return victim;
    }

    private static void CopyDir(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
    }

    private static SortedDictionary<string, string> DumpNative(string path)
    {
        var options = new RocksDbSharp.DbOptions();
        NativeExtras.SetBestEffortsRecovery(options, true);
        var r = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using var db = RocksDbSharp.RocksDb.Open(options, path);
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) r[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        return r;
    }

    private static SortedDictionary<string, string> DumpManaged(string path, bool bestEfforts)
    {
        var r = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { BestEffortsRecovery = bestEfforts });
        using Iterator it = db.NewIterator(null);
        for (it.SeekToFirst(); it.Valid; it.Next()) r[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        return r;
    }

    [TestMethod]
    [DataRow(false, Damage.DeleteNewest, false)]
    [DataRow(false, Damage.DeleteOldest, false)]
    [DataRow(false, Damage.TruncateMiddle, false)]
    [DataRow(false, Damage.CorruptNewestFooter, false)]
    [DataRow(false, Damage.DeleteL1, true)]
    [DataRow(false, Damage.DeleteOldest, true)]
    [DataRow(true, Damage.DeleteNewest, false)]
    [DataRow(true, Damage.DeleteOldest, false)]
    [DataRow(true, Damage.TruncateMiddle, false)]
    [DataRow(true, Damage.CorruptNewestFooter, false)]
    [DataRow(true, Damage.DeleteL1, true)]
    [DataRow(true, Damage.DeleteOldest, true)]
    public void SameRecoveredPointInTimeAsNative(bool managedWriter, Damage damage, bool compact)
    {
        using var dir = new TempDir("ber");
        string src = Path.Combine(dir.Path, "src");
        Directory.CreateDirectory(src);
        if (managedWriter) BuildManaged(src, compact);
        else BuildNative(src, compact);
        ApplyDamage(src, damage, compact);
        string nativeCopy = Path.Combine(dir.Path, "native");
        string managedCopy = Path.Combine(dir.Path, "managed");
        string plainCopy = Path.Combine(dir.Path, "plain");
        CopyDir(src, nativeCopy);
        CopyDir(src, managedCopy);
        CopyDir(src, plainCopy);

        // Without best-efforts recovery the damaged DB does not open (LoadTableHandlers: missing file -> IO error, size
        // mismatch or bad footer -> Corruption), in either implementation, and the failed open writes no new MANIFEST.
        string nativePlainCopy = Path.Combine(dir.Path, "native-plain");
        CopyDir(src, nativePlainCopy);
        var nex = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nativePlainCopy).Dispose());
        string[] before = Directory.GetFiles(plainCopy).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;
        string currentBefore = File.ReadAllText(Path.Combine(plainCopy, "CURRENT"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => DumpManaged(plainCopy, bestEfforts: false));
        StatusCode expectedCode = nex.Message.StartsWith("IO error", StringComparison.Ordinal) ? StatusCode.IOError : StatusCode.Corruption;
        StringAssert.Contains(ex.Message, "may be corrupted.");
        Assert.AreEqual(expectedCode, ex.Code, "managed: " + ex.Message + " native: " + nex.Message);
        if (damage == Damage.TruncateMiddle) StringAssert.Contains(ex.Message, "Sst file size mismatch");
        string[] after = Directory.GetFiles(plainCopy).Select(Path.GetFileName).Where(static n => !n!.StartsWith("LOG", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal).ToArray()!;
        CollectionAssert.AreEqual(before.Where(static n => !n!.StartsWith("LOG", StringComparison.Ordinal)).ToArray(), after, "no new MANIFEST/WAL/OPTIONS");
        Assert.AreEqual(currentBefore, File.ReadAllText(Path.Combine(plainCopy, "CURRENT")));

        SortedDictionary<string, string> expected = DumpNative(nativeCopy);
        SortedDictionary<string, string> actual = DumpManaged(managedCopy, bestEfforts: true);
        Assert.IsFalse(expected.ContainsKey("wal-only"), "native does not replay the WAL");
        CollectionAssert.AreEqual(expected.ToList(), actual.ToList(),
            "native: " + string.Join(",", expected.Select(static kv => kv.Key + "=" + kv.Value).Take(8)) + " managed: " +
            string.Join(",", actual.Select(static kv => kv.Key + "=" + kv.Value).Take(8)));
        // The recovered DB is consistent: it reopens normally (with a new MANIFEST) in both implementations.
        CollectionAssert.AreEqual(actual.ToList(), DumpManaged(managedCopy, bestEfforts: false).ToList());
        using (var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), managedCopy))
        {
            foreach (KeyValuePair<string, string> kv in actual) Assert.AreEqual(kv.Value, db.Get(kv.Key));
        }
    }
}
