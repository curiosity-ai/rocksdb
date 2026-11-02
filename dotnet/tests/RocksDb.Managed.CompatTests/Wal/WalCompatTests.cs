using RocksDb.Managed.Compression;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>WAL file compatibility: managed reader on native WALs and native recovery from managed-written WALs.</summary>
[TestClass]
public sealed class WalCompatTests
{
    private sealed class FailingReporter : LogReporter
    {
        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber) =>
            Assert.Fail($"unexpected WAL corruption: {code} {reason} ({bytes} bytes)");
    }

    /// <summary>Writes a workload natively, closes without flushing (memtables stay in the WAL) and returns the WAL path.</summary>
    private static (string wal, List<string> expected) WriteNativeWal(string dir, RocksDbSharp.DbOptions options)
    {
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        var expected = new List<string>();
        var rnd = new Random(7);
        using (RocksDbSharp.RocksDb db = Open(dir, options, merge))
        {
            RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
            uint oneId = CfId(one);
            ulong seq = 1;
            for (int i = 0; i < 300; i++)
            {
                // Values of varying size, some spanning several 32KiB blocks.
                byte[] value = new byte[i % 50 == 0 ? 70_000 + i : rnd.Next(0, 200)];
                rnd.NextBytes(value);
                string v = Convert.ToHexString(value);
                if (i % 3 == 0)
                {
                    db.Put("key" + i, v);
                    expected.Add($"Put(0,key{i},{v})@{seq++}");
                }
                else if (i % 3 == 1)
                {
                    db.Put("key" + i, v, one);
                    expected.Add($"Put({oneId},key{i},{v})@{seq++}");
                }
                else
                {
                    using var b = new RocksDbSharp.WriteBatch();
                    b.Delete(System.Text.Encoding.UTF8.GetBytes("key" + (i - 1)), one);
                    b.Merge(System.Text.Encoding.UTF8.GetBytes("m"), System.Text.Encoding.UTF8.GetBytes(v));
                    db.Write(b);
                    expected.Add($"Delete({oneId},key{i - 1})@{seq} Merge(0,m,{v})@{seq + 1}");
                    seq += 2;
                }
            }
        }
        string? wal = FindWal(dir);
        Assert.IsNotNull(wal, "native DB left no WAL");
        return (wal, expected);
    }

    private static List<string> ReadWalBatches(string wal, bool async)
    {
        var batches = new List<string>();
        using var reader = new LogReader(LocalFileSystem.Instance.OpenSequential(wal), new FailingReporter(), checksum: true, LogNumberOf(wal))
        {
            RecoveryMode = WalRecoveryMode.AbsoluteConsistency,
        };
        if (async)
        {
            while (reader.ReadRecordAsync().AsTask().GetAwaiter().GetResult()) batches.Add(Describe(reader.Record.Span));
        }
        else
        {
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) batches.Add(Describe(record));
        }
        return batches;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ReadNativeWal(bool async)
    {
        using var dir = new TempDir("wal-native");
        (string wal, List<string> expected) = WriteNativeWal(dir.Path, new RocksDbSharp.DbOptions());
        CollectionAssert.AreEqual(expected, ReadWalBatches(wal, async));
    }

    [TestMethod]
    public void ReadNativeRecyclableWal()
    {
        using var dir = new TempDir("wal-recycle");
        var options = new RocksDbSharp.DbOptions()
            .SetRecycleLogFileNum(1)
            .SetWalRecoveryMode(RocksDbSharp.Recovery.PointInTime);
        (string wal, List<string> expected) = WriteNativeWal(dir.Path, options);
        byte type = File.ReadAllBytes(wal)[6];
        Assert.IsTrue(LogFormat.IsRecyclableType(type), "native WAL should use the recyclable format, type=" + type);
        CollectionAssert.AreEqual(expected, ReadWalBatches(wal, async: false));
    }

    [TestMethod]
    public void ReadNativeZstdCompressedWal()
    {
        using var dir = new TempDir("wal-zstd");
        var options = new RocksDbSharp.DbOptions().SetWalCompression(RocksDbSharp.Compression.Zstd);
        (string wal, List<string> expected) = WriteNativeWal(dir.Path, options);
        Assert.AreEqual((byte)LogRecordType.SetCompressionType, File.ReadAllBytes(wal)[6]);
        CollectionAssert.AreEqual(expected, ReadWalBatches(wal, async: false));
    }

    /// <summary>
    /// Replaces the (empty) live WAL of a freshly created native DB with a managed-written one under the same log number
    /// and checks that native recovery replays it.
    /// </summary>
    [TestMethod]
    [DataRow(false, false, false)]
    [DataRow(false, true, false)]
    [DataRow(true, false, true)]
    [DataRow(false, false, true)]
    public void NativeRecoversManagedWal(bool async, bool zstd, bool recyclable)
    {
        using var dir = new TempDir("wal-managed");
        // Fresh options / merge operator per open: rocksdb-sharp handles must not be shared across DB instances.
        RocksDbSharp.DbOptions NativeOptions()
        {
            var o = new RocksDbSharp.DbOptions();
            if (recyclable) o.SetRecycleLogFileNum(1).SetWalRecoveryMode(RocksDbSharp.Recovery.PointInTime);
            return o;
        }
        uint oneId;
        using (RocksDbSharp.RocksDb db = Open(dir.Path, NativeOptions(), CreateConcatMergeOperator()))
        {
            oneId = CfId(db.GetColumnFamily("one"));
            Assert.AreEqual(0UL, db.GetLatestSequenceNumber());
        }
        string? wal = FindWal(dir.Path);
        Assert.IsNotNull(wal);
        Assert.AreEqual(0L, new FileInfo(wal).Length);
        ulong logNumber = LogNumberOf(wal);

        var rnd = new Random(11);
        var expected = new Dictionary<string, byte[]?>();
        var expectedOne = new Dictionary<string, byte[]>();
        ulong seq = 1;
        IWritableFile file = LocalFileSystem.Instance.CreateWritable(wal, truncate: true);
        using (var writer = new LogWriter(file, logNumber, recyclable, manualFlush: false,
                   zstd ? CompressionType.Zstd : CompressionType.None, initialBlockOffset: 0))
        {
            writer.AddCompressionTypeRecord();
            using var batch = new WriteBatch();
            for (int i = 0; i < 200; i++)
            {
                batch.Clear();
                batch.Sequence = seq;
                byte[] v = new byte[i % 40 == 0 ? 50_000 : rnd.Next(1, 100)];
                rnd.NextBytes(v);
                batch.Put(System.Text.Encoding.UTF8.GetBytes("k" + i), v);
                expected["k" + i] = v;
                batch.Put(oneId, System.Text.Encoding.UTF8.GetBytes("c" + i), v);
                expectedOne["c" + i] = v;
                if (i > 0 && i % 5 == 0)
                {
                    batch.Delete(System.Text.Encoding.UTF8.GetBytes("k" + (i - 1)));
                    expected["k" + (i - 1)] = null;
                }
                batch.Merge("m"u8, "x"u8);
                if (async) writer.AddRecordAsync(batch.DataMemory, seq).AsTask().GetAwaiter().GetResult();
                else writer.AddRecord(batch.Data, seq);
                seq += (ulong)batch.Count;
            }
            writer.Sync();
        }

        using (RocksDbSharp.RocksDb db = Open(dir.Path, NativeOptions(), CreateConcatMergeOperator()))
        {
            Assert.AreEqual(seq - 1, db.GetLatestSequenceNumber());
            RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
            foreach (KeyValuePair<string, byte[]?> kv in expected)
            {
                byte[]? actual = db.Get(System.Text.Encoding.UTF8.GetBytes(kv.Key));
                if (kv.Value is null) Assert.IsNull(actual, kv.Key);
                else CollectionAssert.AreEqual(kv.Value, actual, kv.Key);
            }
            foreach (KeyValuePair<string, byte[]> kv in expectedOne)
            {
                CollectionAssert.AreEqual(kv.Value, db.Get(System.Text.Encoding.UTF8.GetBytes(kv.Key), one), kv.Key);
            }
            Assert.AreEqual(string.Join(",", Enumerable.Repeat("x", 200)), db.Get("m"));
        }
    }
}
