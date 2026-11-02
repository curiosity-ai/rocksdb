using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// track_and_verify_wals_in_manifest against native RocksDB 11.12: the WalAddition / WalDeletion edits written by either
/// implementation are accepted by the other (with the option on), and a tracked WAL that is missing is detected by both.
/// </summary>
[TestClass]
public sealed class WalTrackingCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static RocksDbSharp.RocksDb OpenNative(string path, bool track)
    {
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        RocksDbSharp.Native.Instance.rocksdb_options_set_track_and_verify_wals_in_manifest(options.Handle, track ? (byte)1 : (byte)0);
        var cfs = new RocksDbSharp.ColumnFamilies { { "cf1", new RocksDbSharp.ColumnFamilyOptions() } };
        return RocksDbSharp.RocksDb.Open(options, path, cfs);
    }

    private static RocksDatabase OpenManaged(string path, bool track) =>
        RocksDatabase.Open(path, new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            TrackAndVerifyWalsInManifest = track,
            AvoidFlushDuringShutdown = true,
        }, [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("cf1", new ColumnFamilyOptions())]);

    private static RecoveredVersionState Manifest(string path) => ManifestReader.RecoverFromDirectory(LocalFileSystem.Instance, path, null, out _);

    [TestMethod]
    public void ManagedTrackedWalsOpenInNative()
    {
        using var dir = new TempDir("waltrack-m2n");
        ulong tracked;
        using (RocksDatabase db = OpenManaged(dir.Path, track: true))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 200; i++)
            {
                db.Put(U("d" + i), U("v" + i));
                cf1.Put(U("c" + i), U("w" + i));
            }
            db.Flush(); // the closed WAL keeps cf1's data and is tracked with its synced size
            db.Put(U("d-late"), U("x"));
            db.Flush();
        }
        RecoveredVersionState s = Manifest(dir.Path);
        Assert.IsTrue(s.Wals.Count > 0, "managed tracked the closed WAL");
        tracked = 0;
        foreach (KeyValuePair<ulong, ulong?> w in s.Wals) if (w.Value is not null) tracked = w.Key;
        Assert.AreNotEqual(0UL, tracked);

        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, track: true))
        {
            Assert.AreEqual("w7", db.Get("c7", db.GetColumnFamily("cf1")));
            Assert.AreEqual("x", db.Get("d-late"));
            db.Put("n1", "native");
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().Handle, db.GetColumnFamily("cf1").Handle);
            db.Flush(new RocksDbSharp.FlushOptions());
        }
        // Native recorded the deletion of the WALs its flushes made obsolete; managed accepts its MANIFEST.
        using (RocksDatabase db = OpenManaged(dir.Path, track: true))
        {
            Assert.AreEqual("native", Encoding.UTF8.GetString(db.Get(U("n1"))!));
            Assert.AreEqual("w7", Encoding.UTF8.GetString(db.GetColumnFamily("cf1").Get(U("c7"))!));
        }
        Assert.IsFalse(Manifest(dir.Path).Wals.ContainsKey(tracked));
    }

    [TestMethod]
    public void MissingTrackedWalIsCorruptionInBoth()
    {
        using var dir = new TempDir("waltrack-missing");
        ulong tracked = 0;
        using (RocksDatabase db = OpenManaged(dir.Path, track: true))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 100; i++)
            {
                db.Put(U("d" + i), U("v" + i));
                cf1.Put(U("c" + i), U("w" + i));
            }
            db.Flush();
            db.Put(U("d-late"), U("x"));
            db.Flush();
        }
        foreach (KeyValuePair<ulong, ulong?> w in Manifest(dir.Path).Wals) if (w.Value is not null) tracked = w.Key;
        Assert.AreNotEqual(0UL, tracked);
        File.Delete(FileNames.LogFileName(dir.Path, tracked));

        var native = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => OpenNative(dir.Path, track: true).Dispose());
        StringAssert.Contains(native.Message, "Missing WAL with log number: " + tracked);
        var managed = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(dir.Path, track: true).Dispose());
        Assert.AreEqual(StatusCode.Corruption, managed.Code);
        StringAssert.Contains(managed.Message, "Missing WAL with log number: " + tracked);
    }

    [TestMethod]
    public void NativeTrackedWalsOpenInManaged()
    {
        using var dir = new TempDir("waltrack-n2m");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, track: true))
        {
            RocksDbSharp.ColumnFamilyHandle cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 200; i++)
            {
                db.Put("d" + i, "v" + i);
                db.Put("c" + i, "w" + i, cf1);
            }
            db.Flush(new RocksDbSharp.FlushOptions());
            db.Put("d-late", "x");
            db.Flush(new RocksDbSharp.FlushOptions());
        }
        RecoveredVersionState s = Manifest(dir.Path);
        Assert.IsTrue(s.Wals.Count > 0, "native tracked the closed WAL");
        using (RocksDatabase db = OpenManaged(dir.Path, track: true))
        {
            Assert.AreEqual("w7", Encoding.UTF8.GetString(db.GetColumnFamily("cf1").Get(U("c7"))!));
            Assert.AreEqual("x", Encoding.UTF8.GetString(db.Get(U("d-late"))!));
            db.Put(U("m1"), U("managed"));
            db.GetColumnFamily("cf1").Put(U("m2"), U("managed"));
            db.Flush();
            db.GetColumnFamily("cf1").Flush();
            db.Put(U("m3"), U("managed"));
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, track: true))
        {
            Assert.AreEqual("managed", db.Get("m1"));
            Assert.AreEqual("managed", db.Get("m2", db.GetColumnFamily("cf1")));
            Assert.AreEqual("managed", db.Get("m3"));
        }
    }
}
