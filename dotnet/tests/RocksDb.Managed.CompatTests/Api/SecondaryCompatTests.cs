using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Secondary instances across implementations: a managed secondary tailing the MANIFEST and WALs of a live native
/// primary (flushes, compactions, MANIFEST rollovers), and a native secondary (rocksdb_open_as_secondary /
/// rocksdb_try_catch_up_with_primary) following a live managed primary. Scenarios after db/db_secondary_test.cc.
/// </summary>
[TestClass]
public sealed class SecondaryCompatTests
{
    private const int Keys = 300;
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
    private static string V(int i, int round) => "v" + round + "-" + i + new string((char)('a' + i % 26), i % 40);
    private static string? Str(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private static RocksDbSharp.DbOptions NativeOptions()
    {
        var o = new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetMaxManifestFileSize(1024) // several MANIFEST rollovers during the test
            .SetWriteBufferSize(64 * 1024)
            .SetLevel0FileNumCompactionTrigger(3)
            .SetMaxOpenFiles(-1); // required by a native secondary (keeps files the primary deletes readable)
        s_keepAlive.Add(o);
        return o;
    }

    /// <summary>Writes one round: every key overwritten, a few deleted.</summary>
    private static void Round(int round, Action<string, string?> write)
    {
        for (int i = 0; i < Keys; i++) write(K(i), V(i, round));
        for (int i = round; i < Keys; i += 37) write(K(i), null);
    }

    [TestMethod]
    public async Task ManagedSecondaryTailsNativePrimaryAcrossFlushesCompactionsAndManifestRollovers()
    {
        using var dir = new TempDir("sec-tail");
        string primaryPath = Path.Combine(dir.Path, "db");
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(), primaryPath);
        Round(0, (k, v) => { if (v is null) n.Remove(U(k)); else n.Put(U(k), U(v)); });

        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(primaryPath, Path.Combine(dir.Path, "sec"), new DbOptions());
        int opens = sec.SecondaryManifestTailStats.ManifestOpens;
        for (int round = 1; round <= 12; round++)
        {
            Round(round, (k, v) => { if (v is null) n.Remove(U(k)); else n.Put(U(k), U(v)); });
            n.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            if (round % 4 == 0) n.CompactRange((byte[])null!, (byte[])null!);
            Round(round + 100, (k, v) => { if (v is not null && k.EndsWith('7')) n.Put(U(k), U(v)); }); // WAL-only tail
            await sec.TryCatchUpWithPrimaryAsync();
            for (int i = 0; i < Keys; i++) Assert.AreEqual(Str(n.Get(U(K(i)))), Str(sec.Get(U(K(i)))), "round " + round + " key " + i);
            Assert.AreEqual(n.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber(), "round " + round);
        }
        Assert.IsGreaterThan(opens, sec.SecondaryManifestTailStats.ManifestOpens, "the native primary never rolled its MANIFEST over");
    }

    [TestMethod]
    public async Task NativeSecondaryFollowsManagedPrimary()
    {
        using var dir = new TempDir("sec-native");
        string primaryPath = Path.Combine(dir.Path, "db");
        var options = new DbOptions { CreateIfMissing = true, MaxManifestFileSize = 4096 };
        var cf = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, Level0FileNumCompactionTrigger = 3 };
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(primaryPath, options, cf);
        Round(0, (k, v) => { if (v is null) primary.Delete(U(k)); else primary.Put(U(k), U(v)); });

        RocksDbSharp.DbOptions no = new RocksDbSharp.DbOptions().SetMaxOpenFiles(-1);
        s_keepAlive.Add(no);
        using RocksDbSharp.RocksDb sec = RocksDbSharp.RocksDb.OpenAsSecondary(no, primaryPath, Path.Combine(dir.Path, "native-sec"));
        for (int i = 0; i < Keys; i++) Assert.AreEqual(Str(primary.Get(U(K(i)))), Str(sec.Get(U(K(i)))), "open key " + i);
        for (int round = 1; round <= 10; round++)
        {
            Round(round, (k, v) => { if (v is null) primary.Delete(U(k)); else primary.Put(U(k), U(v)); });
            await primary.FlushAsync();
            if (round % 4 == 0) primary.WaitForCompact();
            Round(round + 100, (k, v) => { if (v is not null && k.EndsWith('3')) primary.Put(U(k), U(v)); }); // WAL-only tail
            sec.TryCatchUpWithPrimary();
            for (int i = 0; i < Keys; i++) Assert.AreEqual(Str(primary.Get(U(K(i)))), Str(sec.Get(U(K(i)))), "round " + round + " key " + i);
            Assert.AreEqual(primary.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber(), "round " + round);
        }
    }
}
