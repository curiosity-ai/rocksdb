using System.Text;

namespace RocksDb.Managed.Replication.Tests.Infrastructure;

/// <summary>Disposable temporary directory.</summary>
public sealed class TempDir : IDisposable
{
    public TempDir(string prefix)
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rocksdb-repl-" + prefix + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public string Sub(string name) => System.IO.Path.Combine(Path, name);

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}

internal static class TestUtil
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    public static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    public static string K(int i) => "key" + i.ToString("D7", System.Globalization.CultureInfo.InvariantCulture);

    public static string V(int i, int round) => "v" + round + "-" + i + new string((char)('a' + i % 26), i % 50);

    /// <summary>A mixed batch: puts, deletes and (every 29th) a range delete - multi-entry batches are what would catch off-by-one chunk arithmetic.</summary>
    public static void FillBatch(WriteBatch b, int i, int round)
    {
        b.Put(U(K(i)), U(V(i, round)));
        if (i % 3 == 0) b.Put(U(K(i + 1)), U(V(i, round + 1)));
        if (i % 7 == 0) b.Delete(U(K(Math.Max(0, i - 5))));
        if (i % 29 == 0) b.DeleteRange(U(K(Math.Max(0, i - 20))), U(K(Math.Max(1, i - 15))));
    }

    public static async Task WriteWorkloadAsync(RocksDatabase db, int from, int to, int round)
    {
        for (int i = from; i < to; i++)
        {
            using WriteBatch b = db.CreateWriteBatch();
            FillBatch(b, i, round);
            await db.WriteAsync(b);
        }
    }

    public static SortedDictionary<string, string> Contents(RocksDatabase db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) d[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        return d;
    }

    public static SortedDictionary<string, string> Contents(RocksDbSharp.RocksDb db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) d[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        return d;
    }

    public static void AssertSameContents(SortedDictionary<string, string> expected, SortedDictionary<string, string> actual, string what)
    {
        Assert.AreEqual(expected.Count, actual.Count, what + ": key count");
        foreach (KeyValuePair<string, string> kv in expected)
        {
            Assert.IsTrue(actual.TryGetValue(kv.Key, out string? v), what + ": missing " + kv.Key);
            Assert.AreEqual(kv.Value, v, what + ": value of " + kv.Key);
        }
    }

    /// <summary>Same full scan and same latest sequence number.</summary>
    public static void AssertReplicaEquals(RocksDatabase source, RocksDatabase replica, string what)
    {
        Assert.AreEqual(source.GetLatestSequenceNumber(), replica.GetLatestSequenceNumber(), what + ": latest sequence");
        AssertSameContents(Contents(source), Contents(replica), what);
    }

    /// <summary>
    /// Waits for a condition that is driven by background activity no event exposes (role changes across nodes).
    /// Bounded by <paramref name="timeout"/>.
    /// </summary>
    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("timed out waiting for " + what);
            await Task.Delay(10);
        }
    }

    public static Task WaitUntilAsync(Func<bool> condition, string what) => WaitUntilAsync(condition, Timeout, what);

    /// <summary>Waits (signalled by the replica's write path) until <paramref name="replica"/> reaches <paramref name="sequence"/>.</summary>
    public static async Task WaitForSequenceAsync(RocksDatabase replica, ulong sequence, string what)
    {
        using var cts = new CancellationTokenSource(Timeout);
        try
        {
            Assert.IsTrue(await replica.WaitForSequenceAsync(sequence, cts.Token), what + ": database closed");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail(what + ": replica stuck at " + replica.GetLatestSequenceNumber() + ", expected " + sequence);
        }
    }

    public static DbOptions DbOpts(IFileSystem? fs) => new()
    {
        CreateIfMissing = true,
        FileSystem = fs ?? LocalFileSystem.Instance,
    };
}
