using System.Buffers.Binary;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.CompatTests.Audit15;

/// <summary>
/// Audit #15: Mosaik's flows (Mosaik/Graph/src/Graph/Storage, ROCKSDB_MANAGED build) through the engine code that
/// changed since audit #6, run side by side on native RocksDB (rocksdb-sharp, Mosaik's default build) and the managed
/// port with Mosaik's exact options (<see cref="MosaikProfile"/> / <see cref="NativeMosaikProfile"/>), covering what the
/// earlier Mosaik tests leave out:
/// <list type="bullet">
/// <item>the WAL lifecycle of 31 mostly idle column families under the low-resource max_total_wal_size, with the flush
/// thread's default-family flush, file deletions disabled while a replica is registered and a checkpoint taken then
/// (DBImpl::SwitchWAL switching empty column families and closed WALs kept while deletions are disabled, audit #14);</item>
/// <item>the read shapes the RocksDbManagedCompat shim maps onto the port: Get into a caller buffer
/// (LuceneGetFilePage), HasKey, the string overloads, Get with a stream deserializer;</item>
/// <item>an iterator held across ClearDeprecatedChangeLog (DeleteFilesInRange + DeleteRange + CompactRange);</item>
/// <item>MigrateDatabases (RocksDbStorage.cs:535): a read-only open with an empty family list, ListColumnFamilies, an
/// open of every listed family with default options and the old journal as wal_dir, and the copy by iteration.</item>
/// </list>
/// </summary>
[TestClass]
public sealed class Audit15MosaikFlowsCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;

    // ---------------------------------------------------------------------------------------------------------------
    // Engines: the calls Mosaik makes, with its call shapes (SafeRocksDb / RocksDbBase / RocksDbStorage)
    // ---------------------------------------------------------------------------------------------------------------

    private interface IMosaikDb : IDisposable
    {
        void WriteNode(byte[] uid, byte[] content);
        void PutSetting(string key, string value);
        string? GetSetting(string key);
        void FlushThreadPass();
        void WaitForBackgroundWork();
        void DisableFileDeletions();
        void EnableFileDeletions();
        void Checkpoint(string dir);
        string Property(string family, string property);
        byte[]? Get(string family, byte[] key);
        int Count(string family);
    }

    private const string Nodes = "nodes", Types = "types", NodeVersion = "nodeversion", Content = "content";

    private sealed class ManagedMosaikDb : IMosaikDb
    {
        // Mosaik's writes pass its (null) WriteOptions argument through the shim: the default write options.
        private static readonly WriteOptions s_noWriteOptions = null!;
        private readonly RocksDatabase _db;
        private readonly Dictionary<string, ColumnFamily> _cf;

        public ManagedMosaikDb(MosaikProfile profile, string path)
        {
            _db = profile.Open(path);
            _cf = MosaikProfile.Handles(_db);
            _cf["default"] = _db.DefaultColumnFamily;
        }

        public RocksDatabase Db => _db;

        public void WriteNode(byte[] uid, byte[] content)
        {
            // RocksDbStorage.WriteNode: one batch over nodes / types / nodeversion, the content under its own Put.
            using var batch = new Db.WriteBatch();
            batch.Put(_cf[Nodes], uid, Le8((ulong)uid[0]));
            batch.Put(_cf[Types], uid, BitConverter.GetBytes((uint)uid[1]));
            batch.Put(_cf[NodeVersion], uid, Le8((ulong)uid[2]));
            _db.Write(null, batch);
            _cf[Content].Put(s_noWriteOptions, uid, content);
        }

        // RocksDbManagedCompat.Put / Get (string): UTF-8 key and value.
        public void PutSetting(string key, string value) =>
            _cf[MosaikProfile.GraphConfigFamily].Put(s_noWriteOptions, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));

        public string? GetSetting(string key) =>
            _cf[MosaikProfile.GraphConfigFamily].Get(null, Encoding.UTF8.GetBytes(key)) is byte[] v ? Encoding.UTF8.GetString(v) : null;

        // RocksDbBase.FlushRocksDBMemory: db.Flush(new FlushOptions { Wait = true }) flushes the default family.
        public void FlushThreadPass() => _db.Flush(new FlushOptions { Wait = true });

        public void WaitForBackgroundWork() => _db.WaitForCompact();
        public void DisableFileDeletions() => _db.DisableFileDeletions();
        public void EnableFileDeletions() => _db.EnableFileDeletions();
        public void Checkpoint(string dir) => _db.CreateCheckpoint(dir, 0);
        public string Property(string family, string property) => _cf[family].GetProperty(property) ?? "<null>";
        public byte[]? Get(string family, byte[] key) => _cf[family].Get(key);

        public int Count(string family)
        {
            int n = 0;
            using Iterator it = _cf[family].NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            return n;
        }

        public void Dispose() => _db.Dispose();
    }

    private sealed class NativeMosaikDb : IMosaikDb
    {
        private readonly RocksDbSharp.RocksDb _db;
        private readonly Dictionary<string, RocksDbSharp.ColumnFamilyHandle> _cf = new(StringComparer.Ordinal);

        public NativeMosaikDb(NativeMosaikProfile profile, string path)
        {
            _db = profile.Open(path);
            foreach (MosaikProfile.Family f in MosaikProfile.Families) _cf[f.Name] = _db.GetColumnFamily(f.Name);
            _cf["default"] = _db.GetDefaultColumnFamily();
        }

        public RocksDbSharp.RocksDb Db => _db;

        public void WriteNode(byte[] uid, byte[] content)
        {
            using var batch = new RocksDbSharp.WriteBatch();
            batch.Put(uid, Le8((ulong)uid[0]), _cf[Nodes]);
            batch.Put(uid, BitConverter.GetBytes((uint)uid[1]), _cf[Types]);
            batch.Put(uid, Le8((ulong)uid[2]), _cf[NodeVersion]);
            _db.Write(batch);
            _db.Put(uid, content, _cf[Content]);
        }

        public void PutSetting(string key, string value) => _db.Put(key, value, _cf[MosaikProfile.GraphConfigFamily]);
        public string? GetSetting(string key) => _db.Get(key, _cf[MosaikProfile.GraphConfigFamily]);
        public void FlushThreadPass() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));

        public void WaitForBackgroundWork()
        {
            IntPtr wfc = N.rocksdb_wait_for_compact_options_create();
            try
            {
                N.rocksdb_wait_for_compact(_db.Handle, wfc, out IntPtr err);
                if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
            }
            finally
            {
                N.rocksdb_wait_for_compact_options_destroy(wfc);
            }
        }

        public void DisableFileDeletions() => _db.DisableFileDeletions();
        public void EnableFileDeletions() => _db.EnableFileDeletions();

        public void Checkpoint(string dir)
        {
            using RocksDbSharp.Checkpoint c = _db.Checkpoint();
            c.Save(dir, 0);
        }

        public string Property(string family, string property) => _db.GetProperty(property, _cf[family]) ?? "<null>";
        public byte[]? Get(string family, byte[] key) => _db.Get(key, _cf[family]);

        public int Count(string family)
        {
            int n = 0;
            using RocksDbSharp.Iterator it = _db.NewIterator(_cf[family]);
            for (it.SeekToFirst(); it.Valid(); it.Next()) n++;
            return n;
        }

        public void Dispose() => _db.Dispose();
    }

    private static byte[] Le8(ulong v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, v);
        return b;
    }

    private static byte[] Uid(int i)
    {
        var uid = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(uid, (ulong)i * 0x9E3779B97F4A7C15UL);
        BinaryPrimitives.WriteUInt64LittleEndian(uid.AsSpan(8), (ulong)i);
        return uid;
    }

    private static int CountFiles(string dir, string pattern) => Directory.Exists(dir) ? Directory.GetFiles(dir, pattern).Length : 0;

    // ---------------------------------------------------------------------------------------------------------------
    // 1. WAL lifecycle with 31 mostly idle families
    // ---------------------------------------------------------------------------------------------------------------

    private const int Steps = 420, ContentBytes = 12 * 1024;
    private const int DisableDeletionsAt = 150, CheckpointAt = 260, EnableDeletionsAt = 300;

    /// <summary>
    /// Runs Mosaik's node writes (low-resource mode: max_total_wal_size 1 MiB, the archive kept, a 32 MiB shared write
    /// buffer manager, so the WAL limit is what flushes) with the flush thread's default-family flush every 25 writes, a
    /// setting written every 40, file deletions disabled for a replica between write 150 and 300 and a checkpoint at 260.
    /// After every write the background work is waited for, so both engines run the same program; every 20 writes the
    /// observable state is recorded: live and archived WAL files, table files on disk, each written family's files per
    /// level and every family's super version number (Mosaik's rocksdb.current-super-version-number counter).
    /// </summary>
    private static (List<string> Trace, Dictionary<string, int> Checkpoint) RunWalProgram(IMosaikDb db, string dbPath, string walPath, string checkpointDir)
    {
        var trace = new List<string>();
        var rnd = new Random(15);
        var content = new byte[ContentBytes];
        for (int step = 0; step < Steps; step++)
        {
            rnd.NextBytes(content);
            db.WriteNode(Uid(step), content);
            if (step % 40 == 0) db.PutSetting("last-nodes-count", step.ToString(System.Globalization.CultureInfo.InvariantCulture));
            if (step % 25 == 24) db.FlushThreadPass();
            if (step == DisableDeletionsAt) db.DisableFileDeletions();
            if (step == CheckpointAt) db.Checkpoint(checkpointDir);
            if (step == EnableDeletionsAt) db.EnableFileDeletions();
            db.WaitForBackgroundWork();
            if (step % 20 == 19 || step == CheckpointAt || step == EnableDeletionsAt)
            {
                var sb = new StringBuilder();
                sb.Append("step ").Append(step)
                    .Append(": wal ").Append(CountFiles(walPath, "*.log"))
                    .Append(", archive ").Append(CountFiles(Path.Combine(walPath, "archive"), "*.log"))
                    .Append(", sst ").Append(CountFiles(dbPath, "*.sst"));
                foreach (string f in new[] { Nodes, Types, NodeVersion, Content, MosaikProfile.GraphConfigFamily })
                {
                    sb.Append(", ").Append(f).Append(" L0 ").Append(db.Property(f, "rocksdb.num-files-at-level0"))
                        .Append(" L1 ").Append(db.Property(f, "rocksdb.num-files-at-level1"))
                        .Append(" imm ").Append(db.Property(f, "rocksdb.num-immutable-mem-table"));
                }
                sb.Append(", sv default ").Append(db.Property("default", "rocksdb.current-super-version-number")).Append(',');
                foreach (MosaikProfile.Family f in MosaikProfile.Families) sb.Append(' ').Append(db.Property(f.Name, "rocksdb.current-super-version-number"));
                trace.Add(sb.ToString());
            }
        }
        var checkpoint = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            ["sst"] = CountFiles(checkpointDir, "*.sst"),
            ["log"] = CountFiles(checkpointDir, "*.log"),
            ["MANIFEST"] = CountFiles(checkpointDir, "MANIFEST-*"),
            ["OPTIONS"] = CountFiles(checkpointDir, "OPTIONS-*"),
            ["CURRENT"] = CountFiles(checkpointDir, "CURRENT"),
        };
        return (trace, checkpoint);
    }

    private static string SuperVersionNumbers(IMosaikDb db)
    {
        var sb = new StringBuilder("default ").Append(db.Property("default", "rocksdb.current-super-version-number"));
        foreach (MosaikProfile.Family f in MosaikProfile.Families) sb.Append(' ').Append(db.Property(f.Name, "rocksdb.current-super-version-number"));
        return sb.ToString();
    }

    private static void AssertNodes(IMosaikDb db, int nodes, string what)
    {
        Assert.AreEqual(nodes, db.Count(Nodes), what + ": nodes");
        Assert.AreEqual(nodes, db.Count(Content), what + ": content");
        Assert.AreEqual(nodes, db.Count(NodeVersion), what + ": nodeversion");
        for (int i = 0; i < nodes; i += 37) Assert.IsNotNull(db.Get(Content, Uid(i)), what + ": content " + i);
        Assert.AreEqual(((nodes - 1) / 40 * 40).ToString(System.Globalization.CultureInfo.InvariantCulture), db.GetSetting("last-nodes-count"), what + ": setting");
    }

    [TestMethod]
    [Timeout(120_000)]
    public void WalLifecycleOfIdleFamiliesMatchesNative()
    {
        using var dir = new TempDir("a15-wal");
        string ndb = Path.Combine(dir.Path, "n"), nwal = Path.Combine(dir.Path, "n-journal"), ncp = Path.Combine(dir.Path, "n-checkpoint");
        string mdb = Path.Combine(dir.Path, "m"), mwal = Path.Combine(dir.Path, "m-journal"), mcp = Path.Combine(dir.Path, "m-checkpoint");

        var np = new NativeMosaikProfile(nwal, lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        var mp = new MosaikProfile(mwal, lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);

        (List<string> Trace, Dictionary<string, int> Checkpoint) native, managed;
        using (var n = new NativeMosaikDb(np, ndb)) native = RunWalProgram(n, ndb, nwal, ncp);
        using (var m = new ManagedMosaikDb(mp, mdb)) managed = RunWalProgram(m, mdb, mwal, mcp);

        for (int i = 0; i < Math.Min(native.Trace.Count, managed.Trace.Count); i++)
        {
            Assert.AreEqual(native.Trace[i], managed.Trace[i], "observation " + i + " (native vs managed)");
        }
        Assert.AreEqual(native.Trace.Count, managed.Trace.Count);
        // The program did what it is for: WAL switches by max_total_wal_size (flushes of every family, archived WALs),
        // WALs held while deletions were disabled, released by the enable.
        StringAssert.Contains(native.Trace[^1], "wal 1, archive 4", "native trace end");
        Assert.IsTrue(native.Trace.Exists(static t => t.StartsWith("step 299: wal 3, archive 1", StringComparison.Ordinal)), "WALs kept while deletions were disabled");
        CollectionAssert.AreEquivalent(native.Checkpoint.ToList(), managed.Checkpoint.ToList(), "checkpoint files");

        // Reopen (recovery from the separate journal), and each engine opens the other's checkpoint (WALs inside it).
        string nativeReopened, managedReopened;
        using (var n = new NativeMosaikDb(np, ndb))
        {
            nativeReopened = SuperVersionNumbers(n);
            AssertNodes(n, Steps, "native reopened");
        }
        using (var m = new ManagedMosaikDb(mp, mdb))
        {
            managedReopened = SuperVersionNumbers(m);
            AssertNodes(m, Steps, "managed reopened");
        }
        Assert.AreEqual(nativeReopened, managedReopened, "super version numbers after a reopen");
        using (var n = new NativeMosaikDb(new NativeMosaikProfile(mcp, true, 2, 8L << 20, 32L << 20), mcp)) AssertNodes(n, CheckpointAt + 1, "managed checkpoint on native");
        using (var m = new ManagedMosaikDb(new MosaikProfile(ncp, true, 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20), ncp)) AssertNodes(m, CheckpointAt + 1, "native checkpoint on managed");
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 2. The read shapes of the RocksDbManagedCompat shim, against rocksdb-sharp
    // ---------------------------------------------------------------------------------------------------------------

    // RocksDbManagedCompat.Get(key, keyLength, buffer, offset, length): TryGet into the slice, the full value length or -1.
    private static long ShimGet(ColumnFamily cf, byte[] key, long keyLength, byte[] buffer, long offset, long length) =>
        cf.TryGet(null, key.AsSpan(0, checked((int)keyLength)), buffer.AsSpan(checked((int)offset), checked((int)length)), out int valueLength) ? valueLength : -1;

    // RocksDbManagedCompat.HasKey(key, cf).
    private static bool ShimHasKey(ColumnFamily cf, ReadOnlySpan<byte> key) => cf.TryGet(null, key, 0, static (_, _) => true, out _);

    // RocksDbManagedCompat.Get<T>(key, Func<Stream, T>), with its DeserializeFromStream (verbatim, or with an empty value
    // handed over as an empty stream).
    private static T? ShimGetFromStream<T>(ColumnFamily cf, ReadOnlySpan<byte> key, Func<Stream, T> deserializer, bool verbatim) =>
        cf.TryGet(null, key, deserializer, verbatim ? static (value, d) => DeserializeFromStream(value, d) : static (value, d) => DeserializeFromStreamEmptyAware(value, d),
            out T result) ? result : default;

    /// <summary>
    /// RocksDbManagedCompat.DeserializeFromStream (and SafeIterator.Deserialize) as Mosaik has it: for an empty span
    /// `fixed` yields a null pointer, and UnmanagedMemoryStream rejects it (ArgumentNullException), where rocksdb-sharp
    /// hands the deserializer an empty stream. A Mosaik-side defect, reported to the coordinator.
    /// </summary>
    private static unsafe T DeserializeFromStream<T>(ReadOnlySpan<byte> value, Func<Stream, T> deserializer)
    {
        fixed (byte* ptr = value)
        {
            using var stream = new UnmanagedMemoryStream(ptr, value.Length);
            return deserializer(stream);
        }
    }

    private static T DeserializeFromStreamEmptyAware<T>(ReadOnlySpan<byte> value, Func<Stream, T> deserializer)
    {
        if (!value.IsEmpty) return DeserializeFromStream(value, deserializer);
        using var empty = new MemoryStream([], writable: false);
        return deserializer(empty);
    }

    // What a deserializer sees of the stream it is handed (Mosaik's node content serializers read it to the end).
    private static string DescribeStream(Stream s)
    {
        var ms = new MemoryStream();
        s.CopyTo(ms);
        return "len " + s.Length + " canSeek " + s.CanSeek + " canWrite " + s.CanWrite + " bytes " + Convert.ToHexString(ms.ToArray());
    }

    private static byte[] PageKey(int len) =>
        Encoding.ASCII.GetBytes("lucene/segment_" + len.ToString("D6", System.Globalization.CultureInfo.InvariantCulture) + "/page");

    private static byte[] PageValue(int len)
    {
        var v = new byte[len];
        for (int i = 0; i < len; i++) v[i] = (byte)(i * 7 + len);
        return v;
    }

    private static readonly int[] s_pageLengths = [0, 1, 31, 32, 33, 1000, 40_000];
    private static readonly int[] s_probeLengths = [0, 1, 31, 32, 33, 1000, 40_000, 2, 7];
    private static readonly (int Offset, int Length)[] s_slices = [(0, 32), (5, 27), (0, 0)];

    // A setting key and value with characters outside ASCII (2-, 3- and 4-byte UTF-8 sequences).
    private const string SettingKey = "gr\u00F6\u00DFe-\u043A\u043B\u044E\u0447-\U0001F511", SettingValue = "\u00E9t\u00E9 \u2013 \u6F22\u5B57";

    /// <summary>
    /// LuceneGetFilePage's Get(key, keyLength, buffer, offset, length) (RocksDbIndexStorage.cs:286) returns the full value
    /// length with at most length bytes copied, and -1 for a missing key; HasKey, the UTF-8 string overloads (graph
    /// settings) and Get with a stream deserializer (TryGetNonEncryptedNodeContentWithSerializer, RocksDbStorage.cs:2327)
    /// see what rocksdb-sharp sees. The data is written by native and read by managed from the same directory: a Mosaik
    /// database written by the default build and opened by the managed one.
    /// </summary>
    [TestMethod]
    [Timeout(60_000)]
    public void ShimReadShapesMatchRocksDbSharp()
    {
        using var dir = new TempDir("a15-shim");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "journal");
        const string family = "lucene-file-pages";

        var np = new NativeMosaikProfile(wal, lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        var expected = new List<string>();
        using (RocksDbSharp.RocksDb n = np.Open(db))
        {
            RocksDbSharp.ColumnFamilyHandle cf = n.GetColumnFamily(family);
            RocksDbSharp.ColumnFamilyHandle settings = n.GetColumnFamily(MosaikProfile.GraphConfigFamily);
            foreach (int len in s_pageLengths) n.Put(PageKey(len), PageValue(len), cf);
            n.Put(SettingKey, SettingValue, settings);
            foreach (int len in s_probeLengths)
            {
                byte[] k = PageKey(len);
                foreach ((int offset, int length) in s_slices)
                {
                    byte[] buffer = new byte[32];
                    Array.Fill(buffer, (byte)0xEE);
                    long r = n.Get(k, k.Length, buffer, offset, length, cf);
                    expected.Add("get " + len + " @" + offset + "+" + length + ": " + r + " " + Convert.ToHexString(buffer));
                }
                // A key array passed with a shorter keyLength: its prefix is the key looked up.
                byte[] longer = [.. k, 0x42];
                byte[] b2 = new byte[32];
                expected.Add("prefix-key " + len + ": " + n.Get(longer, k.Length, b2, 0, b2.Length, cf) + " " + Convert.ToHexString(b2));
                expected.Add("haskey " + len + ": " + n.HasKey(k, cf));
                expected.Add("stream " + len + ": " + (n.Get<string>(k, DescribeStream, cf) ?? "<default>"));
            }
            expected.Add("setting: " + n.Get(SettingKey, settings));
            expected.Add("setting missing: " + (n.Get("no-such-setting", settings) ?? "<null>"));
        }

        var mp = new MosaikProfile(wal, lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        var actual = new List<string>();
        using (RocksDatabase m = mp.Open(db))
        {
            ColumnFamily cf = m.GetColumnFamily(family);
            ColumnFamily settings = m.GetColumnFamily(MosaikProfile.GraphConfigFamily);
            foreach (int len in s_probeLengths)
            {
                byte[] k = PageKey(len);
                foreach ((int offset, int length) in s_slices)
                {
                    byte[] buffer = new byte[32];
                    Array.Fill(buffer, (byte)0xEE);
                    long r = ShimGet(cf, k, k.Length, buffer, offset, length);
                    actual.Add("get " + len + " @" + offset + "+" + length + ": " + r + " " + Convert.ToHexString(buffer));
                }
                byte[] longer = [.. k, 0x42];
                byte[] b2 = new byte[32];
                actual.Add("prefix-key " + len + ": " + ShimGet(cf, longer, k.Length, b2, 0, b2.Length) + " " + Convert.ToHexString(b2));
                actual.Add("haskey " + len + ": " + ShimHasKey(cf, k));
                actual.Add("stream " + len + ": " + (ShimGetFromStream<string>(cf, k, DescribeStream, verbatim: len != 0) ?? "<default>"));
            }
            // Mosaik's DeserializeFromStream on the empty value (the engine hands over an empty span, as it should).
            Assert.ThrowsExactly<ArgumentNullException>(() => ShimGetFromStream<string>(cf, PageKey(0), DescribeStream, verbatim: true));
            actual.Add("setting: " + (settings.Get(null, Encoding.UTF8.GetBytes(SettingKey)) is byte[] sv ? Encoding.UTF8.GetString(sv) : null));
            actual.Add("setting missing: " + (settings.Get(null, Encoding.UTF8.GetBytes("no-such-setting")) is byte[] mv ? Encoding.UTF8.GetString(mv) : "<null>"));
        }
        for (int i = 0; i < Math.Min(expected.Count, actual.Count); i++) Assert.AreEqual(expected[i], actual[i], "rocksdb-sharp vs the shim, line " + i);
        Assert.AreEqual(expected.Count, actual.Count);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 3. An iterator held across ClearDeprecatedChangeLog
    // ---------------------------------------------------------------------------------------------------------------

    // RocksDBExt.VersionToBytes: the version little endian, reversed (big endian, so versions sort bytewise).
    private static byte[] VersionKey(long version)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(b, version);
        return b;
    }

    private interface IChangeLogDb : IDisposable
    {
        void Put(byte[] key, byte[] value);
        void Flush();
        void WaitForBackgroundWork();
        void CompactAll();
        void ClearDeprecatedChangeLog();
        IDisposable OpenIterator(byte[] seek, out Func<string?> next);
        string Levels();
    }

    private sealed class ManagedChangeLogDb(MosaikProfile profile, string path) : IChangeLogDb
    {
        private readonly RocksDatabase _db = profile.Open(path);
        private ColumnFamily Cf => _db.GetColumnFamily("changelog");

        public void Put(byte[] key, byte[] value) => Cf.Put(key, value);
        public void Flush() => Cf.Flush();
        public void WaitForBackgroundWork() => _db.WaitForCompact();
        public void CompactAll() => Cf.CompactRange(null, null);

        public void ClearDeprecatedChangeLog()
        {
            // RocksDbStorage.ClearDeprecatedChangeLog, managed branch (RocksDbStorage.cs:1738-1762).
            byte[] start = new byte[8], end = new byte[8];
            Array.Fill(end, byte.MaxValue);
            Cf.DeleteFilesInRange(start, end, includeEnd: true);
            Cf.DeleteRange(new WriteOptions(), start, end);
            Cf.CompactRange(null, null);
        }

        public IDisposable OpenIterator(byte[] seek, out Func<string?> next)
        {
            Iterator it = Cf.NewIterator();
            it.Seek(seek);
            next = () =>
            {
                if (!it.Valid) return null;
                string s = Convert.ToHexString(it.Key) + "=" + Convert.ToHexString(it.Value);
                it.Next();
                return s;
            };
            return it;
        }

        public string Levels()
        {
            var sb = new StringBuilder();
            for (int l = 0; l < 7; l++) sb.Append(Cf.GetProperty("rocksdb.num-files-at-level" + l)).Append(' ');
            return sb.ToString();
        }

        public void Dispose() => _db.Dispose();
    }

    private sealed class NativeChangeLogDb(NativeMosaikProfile profile, string path) : IChangeLogDb
    {
        private readonly RocksDbSharp.RocksDb _db = profile.Open(path);
        private RocksDbSharp.ColumnFamilyHandle Cf => _db.GetColumnFamily("changelog");

        public void Put(byte[] key, byte[] value) => _db.Put(key, value, Cf);

        public void Flush()
        {
            N.rocksdb_flush_cf(_db.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, Cf.Handle, out IntPtr err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
        }

        public void WaitForBackgroundWork()
        {
            IntPtr wfc = N.rocksdb_wait_for_compact_options_create();
            try
            {
                N.rocksdb_wait_for_compact(_db.Handle, wfc, out IntPtr err);
                if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
            }
            finally
            {
                N.rocksdb_wait_for_compact_options_destroy(wfc);
            }
        }

        public void CompactAll() => _db.CompactRange((byte[]?)null, null, Cf);

        public void ClearDeprecatedChangeLog()
        {
            // RocksDbStorage.ClearDeprecatedChangeLog, native branch (RocksDbStorage.cs:1745-1762).
            byte[] start = new byte[8], end = new byte[8];
            Array.Fill(end, byte.MaxValue);
            N.rocksdb_delete_file_in_range_cf(_db.Handle, Cf.Handle, start, (UIntPtr)start.Length, end, (UIntPtr)end.Length, out IntPtr err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
            var wo = new RocksDbSharp.WriteOptions();
            N.rocksdb_delete_range_cf(_db.Handle, wo.Handle, Cf.Handle, start, (UIntPtr)start.Length, end, (UIntPtr)end.Length, out err);
            if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
            _db.CompactRange((byte[]?)null, null, Cf);
        }

        public IDisposable OpenIterator(byte[] seek, out Func<string?> next)
        {
            RocksDbSharp.Iterator it = _db.NewIterator(Cf);
            it.Seek(seek);
            next = () =>
            {
                if (!it.Valid()) return null;
                string s = Convert.ToHexString(it.Key()) + "=" + Convert.ToHexString(it.Value());
                it.Next();
                return s;
            };
            return it;
        }

        public string Levels()
        {
            var sb = new StringBuilder();
            for (int l = 0; l < 7; l++) sb.Append(_db.GetProperty("rocksdb.num-files-at-level" + l, Cf)).Append(' ');
            return sb.ToString();
        }

        public void Dispose() => _db.Dispose();
    }

    /// <summary>
    /// The changelog spread over the last level, L0 and the memtable (and the all-0xFF key ClearDeprecatedChangeLog's
    /// exclusive DeleteRange end leaves), an iterator positioned in the middle of it, then ClearDeprecatedChangeLog
    /// (DeleteFilesInRange [0^8, FF^8] include_end, DeleteRange [0^8, FF^8), CompactRange) while the iterator is held:
    /// the iterator goes on reading its own view to the end, a new iterator sees what is left, the deleted files stay on
    /// disk until the held iterator is released and are purged by its release. Normal-resource mode (two subcompactions).
    /// </summary>
    private static List<string> RunChangeLogProgram(IChangeLogDb db, string dbPath)
    {
        var trace = new List<string>();
        byte[] Value(long v) => Encoding.ASCII.GetBytes("change-" + v.ToString(System.Globalization.CultureInfo.InvariantCulture));
        for (long v = 1; v <= 3000; v++)
        {
            db.Put(VersionKey(v), Value(v));
            if (v % 500 == 0)
            {
                db.Flush();
                db.WaitForBackgroundWork();
                trace.Add("flushed " + v + ": " + db.Levels() + "sst " + CountFiles(dbPath, "*.sst"));
            }
        }
        db.CompactAll();
        db.WaitForBackgroundWork();
        trace.Add("compacted: " + db.Levels() + "sst " + CountFiles(dbPath, "*.sst"));
        for (long v = 3001; v <= 3600; v++) db.Put(VersionKey(v), Value(v));
        db.Flush();
        db.WaitForBackgroundWork();
        for (long v = 3601; v <= 3700; v++) db.Put(VersionKey(v), Value(v));
        byte[] allFf = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        db.Put(allFf, "last"u8.ToArray());
        trace.Add("levels before " + db.Levels() + "sst " + CountFiles(dbPath, "*.sst"));

        IDisposable held = db.OpenIterator(VersionKey(1000), out Func<string?> next);
        try
        {
            for (int i = 0; i < 100; i++) trace.Add("held " + next());
            db.ClearDeprecatedChangeLog();
            db.WaitForBackgroundWork();
            trace.Add("levels after " + db.Levels() + "sst " + CountFiles(dbPath, "*.sst"));
            int rest = 0;
            string? last = null;
            for (string? s = next(); s is not null; s = next())
            {
                rest++;
                last = s;
            }
            trace.Add("held rest " + rest + " last " + last);
            using (db.OpenIterator(new byte[8], out Func<string?> fresh))
            {
                for (string? s = fresh(); s is not null; s = fresh()) trace.Add("fresh " + s);
            }
        }
        finally
        {
            held.Dispose();
        }
        db.WaitForBackgroundWork();
        trace.Add("released: sst " + CountFiles(dbPath, "*.sst"));
        return trace;
    }

    /// <summary>
    /// The shared write buffer manager of the changelog test: above what the normal-resource profile's empty memtables
    /// charge (up to the content family's 128 MiB arena block of Mosaik before curiosity-ai/mosaik#2776, see
    /// <see cref="EmptyMemtablesChargeTheWriteBufferManagerLikeNative"/>),
    /// so that only the program's own flushes and compactions run and both engines run the same program.
    /// </summary>
    private const long ChangeLogWriteBufferBytes = 1L << 30;

    [TestMethod]
    [Timeout(120_000)]
    public void IteratorHeldAcrossClearDeprecatedChangeLogMatchesNative()
    {
        using var dir = new TempDir("a15-changelog");
        string ndb = Path.Combine(dir.Path, "n"), mdb = Path.Combine(dir.Path, "m");
        List<string> native, managed;
        using (var n = new NativeChangeLogDb(new NativeMosaikProfile(Path.Combine(dir.Path, "nj"), false, 8, 16L << 20, ChangeLogWriteBufferBytes), ndb))
            native = RunChangeLogProgram(n, ndb);
        using (var m = new ManagedChangeLogDb(new MosaikProfile(Path.Combine(dir.Path, "mj"), false, 8, cacheBytes: 16L << 20, writeBufferBytes: ChangeLogWriteBufferBytes), mdb))
            managed = RunChangeLogProgram(m, mdb);
        for (int i = 0; i < Math.Min(native.Count, managed.Count); i++) Assert.AreEqual(native[i], managed[i], "line " + i);
        Assert.AreEqual(native.Count, managed.Count);
        // The held iterator read its own view to the end: versions 1100..3700 (2601) and the all-0xFF key.
        Assert.IsTrue(native.Exists(static l => l.StartsWith("held rest 2602 last FFFFFFFFFFFFFFFF=", StringComparison.Ordinal)), string.Join("\n", native.Where(static l => !l.StartsWith("held 0", StringComparison.Ordinal))));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 4. MigrateDatabases
    // ---------------------------------------------------------------------------------------------------------------

    // The old separate datadb's families (RocksDbDataStorage), and the settings family MigrateDatabases skips.
    private static readonly string[] s_oldDataFamilies = ["data-paths", "data-length", "data-pages", "data-vectors", "data-vectors-hashes", MosaikProfile.GraphConfigFamily];

    private static byte[] OldKey(string family, int i)
    {
        var k = new byte[24];
        BinaryPrimitives.WriteUInt64LittleEndian(k, (ulong)family.Length * 0x0101010101010101UL);
        BinaryPrimitives.WriteUInt64BigEndian(k.AsSpan(8), (ulong)i);
        BinaryPrimitives.WriteUInt64LittleEndian(k.AsSpan(16), (ulong)i * 31);
        return k;
    }

    private static byte[] OldValue(string family, int i) => Encoding.UTF8.GetBytes(family + "/" + i + new string('v', i % 300));

    /// <summary>
    /// The old database as Mosaik's default build wrote it: datadb with its families under a separate journal, part of
    /// the data in table files, the rest only in the WAL.
    /// </summary>
    private static void WriteOldDataDb(string path, string journal)
    {
        var np = new NativeMosaikProfile(journal, lowMemoryMode: false, processorCount: 8, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20);
        var families = new RocksDbSharp.ColumnFamilies();
        foreach (string name in s_oldDataFamilies) families.Add(name, np.CreateFamilyOptions(MosaikProfile.Families.First(f => f.Name == name)));
        using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(np.DbOptions, path, families);
        for (int i = 0; i < 2000; i++)
        {
            foreach (string name in s_oldDataFamilies) db.Put(OldKey(name, i), OldValue(name, i), db.GetColumnFamily(name));
            if (i == 1200)
            {
                foreach (string name in s_oldDataFamilies)
                {
                    N.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, db.GetColumnFamily(name).Handle, out IntPtr err);
                    if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
                }
            }
        }
        // A few deletions in the WAL and the memtable.
        for (int i = 0; i < 2000; i += 97) db.Remove(OldKey("data-pages", i), db.GetColumnFamily("data-pages"));
    }

    private static Dictionary<string, List<string>> ExpectedOldContents()
    {
        var all = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (string name in s_oldDataFamilies)
        {
            if (name == MosaikProfile.GraphConfigFamily) continue;
            var rows = new SortedDictionary<string, string>(StringComparer.Ordinal);
            for (int i = 0; i < 2000; i++)
            {
                if (name == "data-pages" && i % 97 == 0) continue;
                rows[Convert.ToHexString(OldKey(name, i))] = Convert.ToHexString(OldValue(name, i));
            }
            all[name] = rows.Select(static kv => kv.Key + "=" + kv.Value).ToList();
        }
        return all;
    }

    /// <summary>
    /// MigrateDatabases (RocksDbStorage.cs:535), managed build: a read-only open with an empty family list (the engine
    /// adds the default family), ListColumnFamilies, then the old database opened with SetRockDbDefaultOptions on its
    /// own journal (create_if_missing off) and every listed family at default options, each family but default and
    /// graphsettings copied by iteration on its own thread into the new database with SafeRocksDb.Put. Native: the
    /// same with rocksdb-sharp's calls. The copies hold the same data, which is the old database's, WAL included.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void MigrateDatabasesCopiesLikeNative()
    {
        using var dir = new TempDir("a15-migrate");
        Dictionary<string, List<string>> expected = ExpectedOldContents();
        var results = new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.Ordinal);

        foreach (string engine in new[] { "native", "managed" })
        {
            string parent = Path.Combine(dir.Path, engine), journalRoot = Path.Combine(dir.Path, engine + "-journal");
            string oldDb = Path.Combine(parent, "datadb"), oldJournal = Path.Combine(journalRoot, "datawal");
            string newDb = Path.Combine(parent, "graph"), newJournal = Path.Combine(journalRoot, "graph");
            // RocksDbStorage's constructor creates the database and journal directories (rocksdb_open does not mkdir -p).
            Directory.CreateDirectory(parent);
            Directory.CreateDirectory(journalRoot);
            WriteOldDataDb(oldDb, oldJournal);
            results[engine] = engine == "native" ? MigrateNative(oldDb, oldJournal, newDb, newJournal) : MigrateManaged(oldDb, oldJournal, newDb, newJournal);
        }
        foreach (string engine in results.Keys)
        {
            foreach ((string family, List<string> rows) in expected)
            {
                List<string> got = results[engine][family];
                Assert.AreEqual(rows.Count, got.Count, engine + " " + family + ": rows");
                for (int i = 0; i < rows.Count; i++) Assert.AreEqual(rows[i], got[i], engine + " " + family + " row " + i);
            }
            Assert.AreEqual(0, results[engine][MosaikProfile.GraphConfigFamily].Count, engine + ": graphsettings is not migrated");
        }
    }

    private static Dictionary<string, List<string>> MigrateManaged(string oldDb, string oldJournal, string newDb, string newJournal)
    {
        var profile = new MosaikProfile(newJournal, lowMemoryMode: false, processorCount: 8, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20);
        using RocksDatabase target = profile.Open(newDb);
        var tempOptions = new DbOptions { CreateIfMissing = false };
        var cfs = new List<ColumnFamilyDescriptor>();
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(oldDb, tempOptions, new List<ColumnFamilyDescriptor>(), false))
        {
            foreach (string name in RocksDatabase.ListColumnFamilies(oldDb, tempOptions.FileSystem)) cfs.Add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
        }
        DbOptions options = new MosaikProfile(oldJournal, lowMemoryMode: false, processorCount: 8, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20).DbOptions;
        options.CreateIfMissing = false;
        using (RocksDatabase old = RocksDatabase.Open(oldDb, options, cfs))
        {
            var threads = new List<Thread>();
            Exception? failure = null;
            foreach (ColumnFamilyDescriptor d in cfs)
            {
                if (d.Name is "default" or MosaikProfile.GraphConfigFamily or MosaikProfile.ReplicaChangeTrackerFamily) continue;
                ColumnFamily source = old.GetColumnFamily(d.Name), destination = target.GetColumnFamily(d.Name);
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        using Iterator it = source.NewIterator();
                        it.SeekToFirst();
                        while (it.Valid)
                        {
                            destination.Put((WriteOptions)null!, it.Key, it.Value); // SafeRocksDb.Put -> RocksDbManagedCompat.Put(key, value, cf, writeOptions: null)
                            it.Next();
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }
                }));
            }
            foreach (Thread t in threads) t.Start();
            foreach (Thread t in threads) t.Join();
            if (failure is not null) throw failure;
        }
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (string name in s_oldDataFamilies)
        {
            var rows = new List<string>();
            using Iterator it = target.GetColumnFamily(name).NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) rows.Add(Convert.ToHexString(it.Key) + "=" + Convert.ToHexString(it.Value));
            result[name] = rows;
        }
        return result;
    }

    private static Dictionary<string, List<string>> MigrateNative(string oldDb, string oldJournal, string newDb, string newJournal)
    {
        var profile = new NativeMosaikProfile(newJournal, lowMemoryMode: false, processorCount: 8, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20);
        using RocksDbSharp.RocksDb target = profile.Open(newDb);
        var tempOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(false);
        var names = new List<string>();
        using (RocksDbSharp.RocksDb ro = RocksDbSharp.RocksDb.OpenReadOnly(tempOptions, oldDb, new RocksDbSharp.ColumnFamilies(), false))
        {
            names.AddRange(RocksDbSharp.RocksDb.ListColumnFamilies(tempOptions, oldDb));
        }
        // rocksdb-sharp's ColumnFamilies starts with "default" (default options), which ListColumnFamilies also names.
        var cfs = new RocksDbSharp.ColumnFamilies();
        foreach (string name in names)
        {
            if (name != "default") cfs.Add(name, new RocksDbSharp.ColumnFamilyOptions());
        }
        RocksDbSharp.DbOptions options = new NativeMosaikProfile(oldJournal, lowMemoryMode: false, processorCount: 8, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20)
            .DbOptions.SetCreateIfMissing(false);
        // Mosaik's native build adds every listed name, "default" included, to a fresh ColumnFamilies (RocksDbStorage.cs:563),
        // which already holds "default": rocksdb-sharp opens that list too.
        var verbatim = new RocksDbSharp.ColumnFamilies();
        foreach (string name in names) verbatim.Add(name, new RocksDbSharp.ColumnFamilyOptions());
        using (RocksDbSharp.RocksDb.Open(options, oldDb, verbatim)) { }
        using (RocksDbSharp.RocksDb old = RocksDbSharp.RocksDb.Open(options, oldDb, cfs))
        {
            var threads = new List<Thread>();
            Exception? failure = null;
            foreach (string name in names)
            {
                if (name is "default" or MosaikProfile.GraphConfigFamily or MosaikProfile.ReplicaChangeTrackerFamily) continue;
                RocksDbSharp.ColumnFamilyHandle source = old.GetColumnFamily(name), destination = target.GetColumnFamily(name);
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        using RocksDbSharp.Iterator it = old.NewIterator(source);
                        it.SeekToFirst();
                        while (it.Valid())
                        {
                            target.Put(it.Key(), it.Value(), destination);
                            it.Next();
                        }
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref failure, e, null);
                    }
                }));
            }
            foreach (Thread t in threads) t.Start();
            foreach (Thread t in threads) t.Join();
            if (failure is not null) throw failure;
        }
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (string name in s_oldDataFamilies)
        {
            var rows = new List<string>();
            using RocksDbSharp.Iterator it = target.NewIterator(target.GetColumnFamily(name));
            for (it.SeekToFirst(); it.Valid(); it.Next()) rows.Add(Convert.ToHexString(it.Key()) + "=" + Convert.ToHexString(it.Value()));
            result[name] = rows;
        }
        return result;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 5. What Mosaik's empty memtables charge the shared write buffer manager
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Right after open, before any write, every memtable of Mosaik's families is charged to the shared write buffer
    /// manager (memory/arena.cc: the 2 KiB inline block; db/memtable.cc: the memtable bloom of the point-lookup families,
    /// memtable_prefix_bloom_size_ratio 0.02 of the 64 MiB write buffer, allocated from the arena at creation, which for
    /// "content" takes a whole arena block: 128 MiB in the normal-resource branch, 1 MiB in low-memory mode, as Mosaik set
    /// it before curiosity-ai/mosaik#2776 - now it uses the engine's default). Both engines charge the same. Against a manager below about 1/0.875 of that charge (Mosaik's floor is 64 MiB in the
    /// normal branch) every write leader asks for a flush; the measured flush storm is in the test output.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    [DataRow(true, 2)]
    [DataRow(false, 8)]
    public void EmptyMemtablesChargeTheWriteBufferManagerLikeNative(bool lowMemoryMode, int processorCount)
    {
        using var dir = new TempDir("a15-wbm");
        long nativeOpen, managedOpen;
        ulong arena = MosaikProfile.ContentArenaBlockSizeBefore2776(lowMemoryMode);
        var np = new NativeMosaikProfile(Path.Combine(dir.Path, "nj"), lowMemoryMode, processorCount, 16L << 20, 1L << 30, contentArenaBlockSize: arena);
        using (RocksDbSharp.RocksDb n = np.Open(Path.Combine(dir.Path, "n"))) nativeOpen = (long)(ulong)N.rocksdb_write_buffer_manager_memory_usage(np.WriteBufferManager);
        var mp = new MosaikProfile(Path.Combine(dir.Path, "mj"), lowMemoryMode, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 1L << 30, contentArenaBlockSize: arena);
        using (RocksDatabase m = mp.Open(Path.Combine(dir.Path, "m"))) managedOpen = mp.WriteBufferManager.MemoryUsage;
        Console.WriteLine("write buffer manager charge after open (" + (lowMemoryMode ? "low-memory" : "normal") + "): native " + nativeOpen + ", managed " + managedOpen);
        // Native rounds each allocation up to malloc_usable_size (docs/validation.md, audit #6); a few KiB at most here.
        Assert.IsTrue(Math.Abs(nativeOpen - managedOpen) <= 64 * 1024, "native " + nativeOpen + " managed " + managedOpen);
        if (!lowMemoryMode) Assert.IsTrue(nativeOpen > 128L << 20, "the empty content memtable holds a 128 MiB arena block: " + nativeOpen);

        // Under Mosaik's normal-branch floor (64 MiB) the idle charge alone is over the mutable limit (7/8): every write
        // that finds a non-empty memtable with no flush pending switches it (DBImpl::HandleWriteBufferManagerFlush picks
        // the oldest such memtable). With the background work waited for after each write, each write flushes the one
        // before it: 40 writes to one family, 39 flushes of one key each.
        if (!lowMemoryMode)
        {
            var np64 = new NativeMosaikProfile(Path.Combine(dir.Path, "nj64"), false, processorCount, 16L << 20, 64L << 20, contentArenaBlockSize: arena);
            var mp64 = new MosaikProfile(Path.Combine(dir.Path, "mj64"), false, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 64L << 20, contentArenaBlockSize: arena);
            string nativeShape, managedShape;
            using (var n = new NativeMosaikDb(np64, Path.Combine(dir.Path, "n64")))
            {
                for (int i = 0; i < 40; i++)
                {
                    n.Db.Put(Uid(i), Le8((ulong)i), n.Db.GetColumnFamily(Nodes));
                    n.WaitForBackgroundWork();
                }
                nativeShape = FlushStormShape(n, Path.Combine(dir.Path, "n64"));
            }
            using (var m = new ManagedMosaikDb(mp64, Path.Combine(dir.Path, "m64")))
            {
                for (int i = 0; i < 40; i++)
                {
                    m.Db.GetColumnFamily(Nodes).Put(Uid(i), Le8((ulong)i));
                    m.WaitForBackgroundWork();
                }
                managedShape = FlushStormShape(m, Path.Combine(dir.Path, "m64"));
            }
            Console.WriteLine("after 40 writes under a 64 MiB manager: native " + nativeShape + "; managed " + managedShape);
            Assert.AreEqual(nativeShape, managedShape);
            StringAssert.StartsWith(nativeShape, "sv 106 L0 3 L6 36 sst 39 ", "39 one-key flushes, their L0 files moved to the last level four at a time");
        }
    }

    private static string FlushStormShape(IMosaikDb db, string path) =>
        "sv " + db.Property(Nodes, "rocksdb.current-super-version-number") + " L0 " + db.Property(Nodes, "rocksdb.num-files-at-level0") +
        " L6 " + db.Property(Nodes, "rocksdb.num-files-at-level6") + " sst " + CountFiles(path, "*.sst") +
        " entries " + db.Property(Nodes, "rocksdb.num-entries-active-mem-table");

    // ---------------------------------------------------------------------------------------------------------------
    // 6. max_total_wal_size while file deletions are disabled (a primary with registered replicas)
    // ---------------------------------------------------------------------------------------------------------------

    private const string SwitchWalLogLine = "Flushing all column families with data in WAL number";

    private static int CountLogLines(string dbPath, string text)
    {
        int n = 0;
        foreach (string file in Directory.GetFiles(dbPath, "LOG*"))
        {
            using var reader = new StreamReader(new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete));
            for (string? line = reader.ReadLine(); line is not null; line = reader.ReadLine())
            {
                if (line.Contains(text, StringComparison.Ordinal)) n++;
            }
        }
        return n;
    }

    /// <summary>
    /// RocksDbBase.RegisterReplica disables file deletions for as long as a replica is registered, so obsolete WALs stay
    /// alive and the WAL total stays above the low-resource max_total_wal_size (1 MiB). The family flushes before that
    /// (here the flush thread's and the content family's own) left no column family needing the oldest WAL. DBImpl::SwitchWAL
    /// (db_impl_write.cc) marks the oldest alive WAL as getting flushed whatever it finds to switch (outside 2PC) and logs
    /// it once; every later write returns at the mark. Compared through the info LOG on both engines (Info level, which
    /// Mosaik's normal branch uses), and nothing flushed or switched by the checks.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void SwitchWalWithFileDeletionsDisabledMarksTheOldestWalOnceLikeNative()
    {
        using var dir = new TempDir("a15-switchwal");
        string ndb = Path.Combine(dir.Path, "n"), mdb = Path.Combine(dir.Path, "m");
        var np = new NativeMosaikProfile(Path.Combine(dir.Path, "nj"), lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        np.DbOptions.SetInfoLogLevel(RocksDbSharp.InfoLogLevel.Info);
        var mp = new MosaikProfile(Path.Combine(dir.Path, "mj"), lowMemoryMode: true, processorCount: 2, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        mp.DbOptions.InfoLogLevel = InfoLogLevel.Info;
        var value = new byte[4096];
        new Random(6).NextBytes(value);

        string RunNative()
        {
            using RocksDbSharp.RocksDb n = np.Open(ndb);
            RocksDbSharp.ColumnFamilyHandle content = n.GetColumnFamily(Content), nodes = n.GetColumnFamily(Nodes);
            var waiter = new NativeMosaikDbWaiter(n);
            n.DisableFileDeletions();
            for (int round = 0; round < 8; round++)
            {
                for (int i = 0; i < 60; i++) n.Put(Uid(round * 1000 + i), value, content);
                N.rocksdb_flush_cf(n.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, content.Handle, out IntPtr err);
                if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
                waiter.Wait();
            }
            // The writes that run SwitchWAL: over the limit, nothing needs the oldest WAL.
            for (int i = 0; i < 200; i++) n.Put(Uid(100_000 + i), Le8((ulong)i), nodes);
            waiter.Wait();
            string state = "while disabled: switch logs " + CountLogLines(ndb, SwitchWalLogLine) + ", wal " + CountFiles(Path.Combine(dir.Path, "nj"), "*.log") +
                ", nodes L0 " + n.GetProperty("rocksdb.num-files-at-level0", nodes) + ", nodes sv " + n.GetProperty("rocksdb.current-super-version-number", nodes);
            n.EnableFileDeletions();
            for (int i = 0; i < 200; i++) n.Put(Uid(200_000 + i), Le8((ulong)i), nodes);
            waiter.Wait();
            return state + "; enabled: wal " + CountFiles(Path.Combine(dir.Path, "nj"), "*.log") + ", archive " + CountFiles(Path.Combine(dir.Path, "nj", "archive"), "*.log");
        }

        string RunManaged()
        {
            using RocksDatabase m = mp.Open(mdb);
            ColumnFamily content = m.GetColumnFamily(Content), nodes = m.GetColumnFamily(Nodes);
            m.DisableFileDeletions();
            for (int round = 0; round < 8; round++)
            {
                for (int i = 0; i < 60; i++) content.Put(Uid(round * 1000 + i), value);
                content.Flush();
                m.WaitForCompact();
            }
            for (int i = 0; i < 200; i++) nodes.Put(Uid(100_000 + i), Le8((ulong)i));
            m.WaitForCompact();
            string state = "while disabled: switch logs " + CountLogLines(mdb, SwitchWalLogLine) + ", wal " + CountFiles(Path.Combine(dir.Path, "mj"), "*.log") +
                ", nodes L0 " + nodes.GetProperty("rocksdb.num-files-at-level0") + ", nodes sv " + nodes.GetProperty("rocksdb.current-super-version-number");
            m.EnableFileDeletions();
            for (int i = 0; i < 200; i++) nodes.Put(Uid(200_000 + i), Le8((ulong)i));
            m.WaitForCompact();
            return state + "; enabled: wal " + CountFiles(Path.Combine(dir.Path, "mj"), "*.log") + ", archive " + CountFiles(Path.Combine(dir.Path, "mj", "archive"), "*.log");
        }

        string native = RunNative(), managed = RunManaged();
        Console.WriteLine("native: " + native + "\nmanaged: " + managed);
        Assert.AreEqual(native, managed);
        StringAssert.StartsWith(native, "while disabled: switch logs 1, ", "native marks the oldest WAL once");
    }

    private readonly struct NativeMosaikDbWaiter(RocksDbSharp.RocksDb db)
    {
        public void Wait()
        {
            IntPtr wfc = N.rocksdb_wait_for_compact_options_create();
            try
            {
                N.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
                if (err != IntPtr.Zero) throw new RocksDbSharp.RocksDbException(err);
            }
            finally
            {
                N.rocksdb_wait_for_compact_options_destroy(wfc);
            }
        }
    }
}
