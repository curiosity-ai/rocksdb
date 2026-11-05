using System.Globalization;
using System.Text;
using RocksDb.Managed.RoundTrip.Harness;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// A native secondary (rocksdb_open_as_secondary + rocksdb_try_catch_up_with_primary, RocksDB 11.12) over a primary with
/// recycle_log_file_num = 2, written by either engine. The native secondary runs in a child process because upstream's
/// log::FragmentBufferedReader can spin forever on a reused WAL (see <see cref="NativeSecondaryOnRecyclingPrimary"/>).
/// The managed secondary over the same states is in the compat suite (SecondaryCompatTests) and the unit suite.
/// </summary>
[TestClass]
public sealed class SecondaryRecyclingRoundTripTests
{
    /// <summary>First argument after <see cref="ChildHost.ChildArgument"/> that selects the native secondary child.</summary>
    public const string NativeSecondaryArgument = "--native-secondary";

    private const int Keys = 300;

    public TestContext TestContext { get; set; } = null!;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", CultureInfo.InvariantCulture);

    /// <summary>Same size for every key and round: every WAL record of every use of a WAL has the same size.</summary>
    private static string V(int i, int round) =>
        "v" + round.ToString("D4", CultureInfo.InvariantCulture) + "-" + i.ToString("D6", CultureInfo.InvariantCulture) + new string('x', 40);

    /// <summary>
    /// Child: <c>roundtrip-child --native-secondary PRIMARY SECONDARY OUT</c>. Opens a native secondary, catches up once
    /// and writes the latest sequence number and every key=value to OUT.
    /// </summary>
    internal static int RunNativeSecondaryChild(string[] args)
    {
        if (args.Length != 5)
        {
            Console.Error.WriteLine("usage: roundtrip-child --native-secondary PRIMARY SECONDARY OUT");
            return ChildHost.ExitUsage;
        }
        Console.WriteLine("OPENING");
        Console.Out.Flush();
        try
        {
            var options = new RocksDbSharp.DbOptions().SetMaxOpenFiles(-1);
            using RocksDbSharp.RocksDb sec = RocksDbSharp.RocksDb.OpenAsSecondary(options, args[2], args[3]);
            Console.WriteLine("OPENED");
            Console.Out.Flush();
            sec.TryCatchUpWithPrimary();
            var sb = new StringBuilder();
            sb.Append(sec.GetLatestSequenceNumber().ToString(CultureInfo.InvariantCulture)).Append('\n');
            using (RocksDbSharp.Iterator it = sec.NewIterator())
            {
                for (it.SeekToFirst(); it.Valid(); it.Next()) sb.Append(Encoding.UTF8.GetString(it.Key())).Append('=').Append(Encoding.UTF8.GetString(it.Value())).Append('\n');
            }
            File.WriteAllText(args[4], sb.ToString());
            GC.KeepAlive(options);
            Console.WriteLine("OK");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return ChildHost.ExitError;
        }
    }

    private sealed class Primary : IDisposable
    {
        private readonly RocksDatabase? _managed;
        private readonly RocksDbSharp.RocksDb? _native;
        private readonly RocksDbSharp.DbOptions? _nativeOptions;

        public Primary(string path, bool native)
        {
            if (native)
            {
                _nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
                RocksDbSharp.Native.Instance.rocksdb_options_set_recycle_log_file_num(_nativeOptions.Handle, (UIntPtr)2);
                _native = RocksDbSharp.RocksDb.Open(_nativeOptions, path);
            }
            else
            {
                _managed = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, RecycleLogFileNum = 2 });
            }
        }

        public void Put(string k, string v)
        {
            if (_native is not null) _native.Put(U(k), U(v));
            else _managed!.Put(U(k), U(v));
        }

        public void Flush()
        {
            if (_native is not null) _native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            else
            {
                _managed!.Flush();
                _managed.WaitForCompact();
            }
        }

        public ulong LatestSequence => _native is not null ? _native.GetLatestSequenceNumber() : _managed!.GetLatestSequenceNumber();

        public void Dispose()
        {
            _native?.Dispose();
            _managed?.Dispose();
            GC.KeepAlive(_nativeOptions);
        }
    }

    private async Task<(ChildResult Result, string? Output)> RunNativeSecondaryAsync(string primaryPath, string secondaryPath, string outFile,
        TimeSpan timeout)
    {
        if (File.Exists(outFile)) File.Delete(outFile);
        ChildResult r = await ChildLauncher.RunAsync([NativeSecondaryArgument, primaryPath, secondaryPath, outFile], timeout, null);
        TestContext.WriteLine(r.Describe());
        return (r, File.Exists(outFile) ? await File.ReadAllTextAsync(outFile) : null);
    }

    private static string Expected(Dictionary<string, string> model, ulong latest)
    {
        var sb = new StringBuilder();
        sb.Append(latest.ToString(CultureInfo.InvariantCulture)).Append('\n');
        var keys = new List<string>(model.Keys);
        keys.Sort(StringComparer.Ordinal);
        foreach (string k in keys) sb.Append(k).Append('=').Append(model[k]).Append('\n');
        return sb.ToString();
    }

    /// <summary>
    /// Large previous uses of the WALs, then a few records into a reused WAL: its live end is the header of a record of
    /// the previous use (same-size records). Upstream's FragmentBufferedReader returns kOldRecord there without consuming
    /// the record, and FragmentBufferedReader::ReadRecord loops on it (DBImplSecondary::RecoverLogFiles, under the DB
    /// mutex), so the native secondary's open never returns while the primary is live, whichever engine wrote the WAL.
    /// The managed reader stops there instead (deliberate deviation). Once the primary is closed, its reused WAL is cut
    /// to its written size, and the native secondary reads every record of the WALs either engine wrote.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task NativeSecondaryOnRecyclingPrimary(bool nativePrimary)
    {
        string dir = Path.Combine(Path.GetTempPath(), "rt-sec-recycle-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string primaryPath = Path.Combine(dir, "db");
            var model = new Dictionary<string, string>(StringComparer.Ordinal);
            ulong latest;
            using (var primary = new Primary(primaryPath, nativePrimary))
            {
                for (int r = 0; r < 3; r++)
                {
                    for (int i = 0; i < Keys; i++)
                    {
                        primary.Put(K(i), V(i, r));
                        model[K(i)] = V(i, r);
                    }
                    primary.Flush();
                }
                for (int i = 0; i < 30; i++)
                {
                    primary.Put(K(i), V(i, 9));
                    model[K(i)] = V(i, 9);
                }
                latest = primary.LatestSequence;
                (ChildResult live, _) = await RunNativeSecondaryAsync(primaryPath, Path.Combine(dir, "sec1"), Path.Combine(dir, "out1"),
                    TimeSpan.FromSeconds(10));
                // Upstream behaviour (RocksDB 11.12 and main): the open spins in FragmentBufferedReader::ReadRecord.
                Assert.IsTrue(live.TimedOut, "the native secondary returned on a live reused WAL (upstream fixed the kOldRecord spin?): " + live.Describe());
                CollectionAssert.Contains(live.Stdout.ToList(), "OPENING");
                CollectionAssert.DoesNotContain(live.Stdout.ToList(), "OPENED");
            }
            string reusedLog = File.ReadAllText(Path.Combine(primaryPath, "LOG"));
            StringAssert.Contains(reusedLog, "from recycle list", "the primary never reused a WAL");
            (ChildResult closed, string? output) = await RunNativeSecondaryAsync(primaryPath, Path.Combine(dir, "sec2"), Path.Combine(dir, "out2"),
                TimeSpan.FromSeconds(60));
            Assert.IsTrue(closed.Succeeded, closed.Describe());
            Assert.AreEqual(Expected(model, latest), output);
        }
        finally
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }
}
