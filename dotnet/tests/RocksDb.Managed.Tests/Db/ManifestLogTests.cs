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
}
