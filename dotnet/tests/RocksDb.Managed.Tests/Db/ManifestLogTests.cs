using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>MANIFEST record adapters over the WAL log reader/writer (<see cref="ManifestLogReader"/>, <see cref="ManifestLogWriter"/>).</summary>
[TestClass]
public sealed class ManifestLogTests
{
    private static string TempPath() => Path.Combine(Path.GetTempPath(), "manifest-log-" + Guid.NewGuid().ToString("N"));

    private static byte[] Record(int i, int length)
    {
        byte[] r = new byte[length];
        for (int j = 0; j < r.Length; j++) r[j] = (byte)(i * 31 + j);
        return r;
    }

    private static readonly int[] s_lengths = [10, 70000, 0, 32760, 5];

    private static async Task WriteRecordsAsync(string path)
    {
        await using ManifestLogWriter w = ManifestLogWriter.Create(LocalFileSystem.Instance, path);
        for (int i = 0; i < s_lengths.Length; i++) await w.AddRecordAsync(Record(i, s_lengths[i]), CancellationToken.None);
        await w.SyncAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task RoundTripIncludingFragmentedRecords()
    {
        string path = TempPath();
        try
        {
            await WriteRecordsAsync(path);
            using ManifestLogReader r = ManifestLogReader.Open(LocalFileSystem.Instance, path);
            for (int i = 0; i < s_lengths.Length; i++)
            {
                Assert.IsTrue(r.TryReadRecord(out ReadOnlySpan<byte> rec), "record " + i);
                CollectionAssert.AreEqual(Record(i, s_lengths[i]), rec.ToArray());
            }
            Assert.IsFalse(r.TryReadRecord(out _));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public async Task ChecksumMismatchFailsRecovery()
    {
        string path = TempPath();
        try
        {
            await WriteRecordsAsync(path);
            byte[] bytes = File.ReadAllBytes(path);
            bytes[7 + 3] ^= 0x40; // payload of the first record
            File.WriteAllBytes(path, bytes);
            using ManifestLogReader r = ManifestLogReader.Open(LocalFileSystem.Instance, path);
            var ex = Assert.ThrowsExactly<RocksDbException>(() => r.TryReadRecord(out _));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public async Task TruncatedTailIsEndOfFile()
    {
        string path = TempPath();
        try
        {
            await WriteRecordsAsync(path);
            byte[] bytes = File.ReadAllBytes(path);
            File.WriteAllBytes(path, bytes.AsSpan(0, bytes.Length - 3).ToArray()); // cut into the last record
            using ManifestLogReader r = ManifestLogReader.Open(LocalFileSystem.Instance, path);
            int n = 0;
            while (r.TryReadRecord(out _)) n++;
            Assert.AreEqual(s_lengths.Length - 1, n);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public async Task ManifestWriterSnapshotRecoversFromDirectory()
    {
        string dir = TempPath();
        Directory.CreateDirectory(dir);
        try
        {
            var r = new ManifestReplayer(null);
            r.Apply(new VersionEdit { DbId = "db-1", ComparatorName = "leveldb.BytewiseComparator", LogNumber = 3, NextFileNumber = 10, LastSequence = 77 });
            RecoveredVersionState original = r.Finish();
            await using (ManifestLogWriter w = ManifestLogWriter.Create(LocalFileSystem.Instance, FileNames.DescriptorFileName(dir, 5)))
            {
                await new ManifestWriter(w).WriteSnapshotAsync(original, static _ => 0, CancellationToken.None);
            }
            await FileNames.SetCurrentFileAsync(LocalFileSystem.Instance, dir, 5, syncDirectory: false, CancellationToken.None);
            RecoveredVersionState recovered = ManifestReader.RecoverFromDirectory(LocalFileSystem.Instance, dir, null, out ulong number);
            Assert.AreEqual(5UL, number);
            Assert.AreEqual("db-1", recovered.DbId);
            Assert.AreEqual(77UL, recovered.LastSequence);
            Assert.AreEqual(10UL, recovered.NextFileNumber);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // ---- the synchronous API writes MANIFEST / CURRENT / IDENTITY / OPTIONS with the synchronous file APIs ----

    /// <summary>
    /// A fault-injection file system that records every asynchronous writable-file call on a DB metadata file (MANIFEST,
    /// CURRENT, IDENTITY, OPTIONS and the *.dbtmp files they are written through).
    /// </summary>
    private static (FaultInjectionFileSystem Fs, System.Collections.Concurrent.ConcurrentQueue<string> AsyncCalls) NewRecordingFs()
    {
        var fs = new FaultInjectionFileSystem(DbTestUtil.NewFs());
        fs.CreateDirectoryIfMissing("/ext");
        var calls = new System.Collections.Concurrent.ConcurrentQueue<string>();
        fs.OnAsyncWrite = (op, path) =>
        {
            string name = Path.GetFileName(path);
            if (name.StartsWith("MANIFEST-", StringComparison.Ordinal) || name.StartsWith("OPTIONS-", StringComparison.Ordinal)
                || name is "CURRENT" or "IDENTITY" || name.EndsWith(".dbtmp", StringComparison.Ordinal))
            {
                calls.Enqueue(op + " " + path);
            }
        };
        return (fs, calls);
    }

    private static DbOptions RecordingOptions(IFileSystem fs) =>
        new() { CreateIfMissing = true, FileSystem = fs, VerifyManifestContentOnClose = true, MaxManifestFileSize = 1 };

    private static string WriteSst(IFileSystem fs, string path, string key)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), fs);
        w.Open(path);
        w.Put(DbTestUtil.B(key), DbTestUtil.B("v-" + key));
        w.Finish();
        return path;
    }

    /// <summary>
    /// Every synchronous DB API that writes the MANIFEST (LogAndApply, including a new MANIFEST + CURRENT on each edit
    /// via max_manifest_file_size = 1), the IDENTITY file or an OPTIONS file - open of a new and of an existing DB, flush
    /// installs, column family create/drop, SetOptions/SetDBOptions, CompactRange, ingestion, and close with the
    /// MANIFEST checks - uses only the synchronous file APIs (the sync API never waits for thread-pool work).
    /// </summary>
    [TestMethod]
    public void SyncApisWriteMetadataFilesWithSyncFileApis()
    {
        (FaultInjectionFileSystem fs, System.Collections.Concurrent.ConcurrentQueue<string> calls) = NewRecordingFs();
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, RecordingOptions(fs)))
        {
            db.Put(DbTestUtil.B("a"), DbTestUtil.B("1"));
            db.Flush();
            ColumnFamily cf = db.CreateColumnFamily("cf1", new ColumnFamilyOptions());
            cf.Put(DbTestUtil.B("b"), DbTestUtil.B("2"));
            cf.SetOptions([new KeyValuePair<string, string>("disable_auto_compactions", "true")]);
            db.SetDBOptions([new KeyValuePair<string, string>("max_background_jobs", "3")]);
            cf.Flush();
            db.Put(DbTestUtil.B("c"), DbTestUtil.B("3"));
            db.Flush();
            db.CompactRange();
            db.IngestExternalFiles([WriteSst(fs, "/ext/1.sst", "z")], new IngestExternalFileOptions());
            db.DropColumnFamily(cf);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, RecordingOptions(fs), [
            new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]))
        {
            Assert.AreEqual("3", DbTestUtil.GetString(db, "c"));
            Assert.AreEqual("v-z", DbTestUtil.GetString(db, "z"));
        }
        Assert.AreEqual(0, calls.Count, "async file calls on metadata files: " + string.Join(", ", calls));
    }

    /// <summary>The async counterparts do use the asynchronous file APIs (so the recording above can see them).</summary>
    [TestMethod]
    public async Task AsyncApisWriteMetadataFilesWithAsyncFileApis()
    {
        (FaultInjectionFileSystem fs, System.Collections.Concurrent.ConcurrentQueue<string> calls) = NewRecordingFs();
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbTestUtil.DbPath, RecordingOptions(fs)))
        {
            await db.PutAsync(DbTestUtil.B("a"), DbTestUtil.B("1"));
            ColumnFamily cf = await db.CreateColumnFamilyAsync("cf1", new ColumnFamilyOptions());
            await db.SetDBOptionsAsync(o => o.MaxBackgroundJobs = 3);
            await db.DropColumnFamilyAsync(cf);
        }
        string[] seen = [.. calls];
        foreach (string fragment in new[] { "/MANIFEST-", "/OPTIONS-", ".dbtmp" })
        {
            Assert.IsTrue(Array.Exists(seen, c => c.Contains(fragment, StringComparison.Ordinal)), fragment + " not written asynchronously: " + string.Join(", ", seen));
        }
    }
}
