using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Manifest;

/// <summary>
/// Binary compatibility of MANIFEST / CURRENT / OPTIONS files with native RocksDB (rocksdb-sharp).
/// </summary>
[TestClass]
public sealed class ManifestCompatTests
{
    private sealed record NativeLiveFile(string ColumnFamily, ulong Number, int Level, ulong Size, byte[] SmallestKey, byte[] LargestKey,
        ulong SmallestSeqno, ulong LargestSeqno);

    private static readonly string[] s_cfNames = ["cf1", "cf2"];

    private static byte[] Key(string prefix, int i) => Encoding.UTF8.GetBytes(prefix + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>Creates a native DB with several column families, flushes, a compaction and a dropped CF.</summary>
    private static (List<NativeLiveFile> Files, ulong LastSequence) CreateNativeDb(string path)
    {
        var options = new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetMaxOpenFiles(1234);
        var cfs = new RocksDbSharp.ColumnFamilies();
        foreach (string n in s_cfNames) cfs.Add(n, new RocksDbSharp.ColumnFamilyOptions().SetWriteBufferSize(8 << 20));
        using var db = RocksDbSharp.RocksDb.Open(options, path, cfs);
        RocksDbSharp.ColumnFamilyHandle cf1 = db.GetColumnFamily("cf1");
        RocksDbSharp.ColumnFamilyHandle cf2 = db.GetColumnFamily("cf2");
        var flush = new RocksDbSharp.FlushOptions();
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 200; i++)
            {
                db.Put(Key("d", i * 3 + round), Encoding.UTF8.GetBytes("v" + round));
                db.Put(Key("c", i * 3 + round), Encoding.UTF8.GetBytes("w" + round), cf1);
            }
            db.Remove(Key("d", round));
            db.Flush(flush);
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, flush.Handle, cf1.Handle);
        }
        db.Put("only"u8.ToArray(), "x"u8.ToArray(), cf2);
        RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, flush.Handle, cf2.Handle);
        db.CompactRange((byte[])null!, (byte[])null!, cf1);

        db.CreateColumnFamily(new RocksDbSharp.ColumnFamilyOptions(), "temp");
        db.DropColumnFamily("temp");

        return (GetNativeLiveFiles(db), db.GetLatestSequenceNumber());
    }

    private static List<NativeLiveFile> GetNativeLiveFiles(RocksDbSharp.RocksDb db)
    {
        var native = RocksDbSharp.Native.Instance;
        IntPtr lf = native.rocksdb_livefiles(db.Handle);
        var result = new List<NativeLiveFile>();
        try
        {
            int count = native.rocksdb_livefiles_count(lf);
            for (int i = 0; i < count; i++)
            {
                string cf = Marshal.PtrToStringUTF8(native.rocksdb_livefiles_column_family_name(lf, i))!;
                string name = Marshal.PtrToStringUTF8(native.rocksdb_livefiles_name(lf, i))!;
                Assert.IsTrue(FileNames.TryParseFileName(name.TrimStart('/'), out ulong number, out FileType type));
                Assert.AreEqual(FileType.TableFile, type);
                IntPtr sk = native.rocksdb_livefiles_smallestkey(lf, i, out UIntPtr skLen);
                IntPtr lk = native.rocksdb_livefiles_largestkey(lf, i, out UIntPtr lkLen);
                var smallest = new byte[(int)skLen];
                var largest = new byte[(int)lkLen];
                Marshal.Copy(sk, smallest, 0, smallest.Length);
                Marshal.Copy(lk, largest, 0, largest.Length);
                result.Add(new NativeLiveFile(cf, number, native.rocksdb_livefiles_level(lf, i), (ulong)native.rocksdb_livefiles_size(lf, i),
                    smallest, largest, native.rocksdb_livefiles_smallest_seqno(lf, i), native.rocksdb_livefiles_largest_seqno(lf, i)));
            }
        }
        finally
        {
            native.rocksdb_livefiles_destroy(lf);
        }
        return result;
    }

    private static ManifestLogReader OpenManifest(string dbPath, out string manifestPath)
    {
        manifestPath = FileNames.GetCurrentManifestPath(LocalFileSystem.Instance, dbPath, out _);
        return ManifestLogReader.Open(LocalFileSystem.Instance, manifestPath);
    }

    private static void AssertLiveFilesMatch(List<NativeLiveFile> native, RecoveredVersionState state)
    {
        int managedCount = 0;
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values) foreach (var _ in cf.AllFiles()) managedCount++;
        Assert.AreEqual(native.Count, managedCount, "live file count");
        foreach (NativeLiveFile nf in native)
        {
            ColumnFamilyVersion? cf = state.FindColumnFamily(nf.ColumnFamily);
            Assert.IsNotNull(cf, nf.ColumnFamily);
            FileMetaData? match = null;
            int level = -1;
            foreach ((int l, FileMetaData f) in cf.AllFiles())
            {
                if (f.FileNumber == nf.Number) { match = f; level = l; }
            }
            Assert.IsNotNull(match, "file " + nf.Number);
            Assert.AreEqual(nf.Level, level, "level of " + nf.Number);
            Assert.AreEqual(nf.Size, match.FileSize, "size of " + nf.Number);
            CollectionAssert.AreEqual(nf.SmallestKey, DbFormat.ExtractUserKey(match.Smallest).ToArray(), "smallest of " + nf.Number);
            CollectionAssert.AreEqual(nf.LargestKey, DbFormat.ExtractUserKey(match.Largest).ToArray(), "largest of " + nf.Number);
            Assert.AreEqual(nf.SmallestSeqno, match.SmallestSeqno);
            Assert.AreEqual(nf.LargestSeqno, match.LargestSeqno);
            Assert.AreNotEqual(FileMetaData.UnknownEpochNumber, match.EpochNumber);
            Assert.IsFalse(match.UniqueId.IsNull, "native records unique ids");
        }
    }

    [TestMethod]
    public void ReadNativeManifestAndRebuildLiveFiles()
    {
        using var dir = new TempDir("manifest");
        (List<NativeLiveFile> nativeFiles, ulong lastSeq) = CreateNativeDb(dir.Path);
        Assert.IsTrue(nativeFiles.Count >= 5);

        RecoveredVersionState state;
        using (ManifestLogReader manifest = OpenManifest(dir.Path, out _)) state = ManifestReader.Recover(manifest, null);
        Assert.AreEqual(lastSeq, state.LastSequence);
        Assert.AreEqual(3, state.ColumnFamilies.Count); // default, cf1, cf2 ("temp" was dropped)
        Assert.IsNotNull(state.FindColumnFamily("cf1"));
        Assert.IsNull(state.FindColumnFamily("temp"));
        Assert.AreEqual(3u, state.MaxColumnFamily);
        Assert.AreEqual("leveldb.BytewiseComparator", state.ColumnFamilies[0].ComparatorName);
        Assert.AreEqual(FileNames.ReadIdentityFile(LocalFileSystem.Instance, dir.Path), state.DbId);
        Assert.IsTrue(state.NextFileNumber > 0);
        AssertLiveFilesMatch(nativeFiles, state);

        // Same result through the directory-level entry point.
        RecoveredVersionState viaDir = ManifestReader.RecoverFromDirectory(LocalFileSystem.Instance, dir.Path, null, out ulong manifestNumber);
        Assert.AreEqual(state.LastSequence, viaDir.LastSequence);
        Assert.IsTrue(manifestNumber > 0);
    }

    [TestMethod]
    public void ReEncodedNativeEditsAreByteIdentical()
    {
        using var dir = new TempDir("manifest-bytes");
        CreateNativeDb(dir.Path);
        using ManifestLogReader reader = OpenManifest(dir.Path, out _);
        int records = 0, withFiles = 0;
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            byte[] original = record.ToArray();
            VersionEdit edit = VersionEdit.DecodeFrom(original);
            byte[] reencoded = edit.Encode(0);
            CollectionAssert.AreEqual(original, reencoded, "record " + records + ": " + edit);
            records++;
            if (edit.NewFiles.Count > 0) withFiles++;
        }
        Assert.IsTrue(records > 10, "records=" + records);
        Assert.IsTrue(withFiles > 3);
    }

    [TestMethod]
    public async Task ManagedWrittenManifestOpensNatively()
    {
        using var dir = new TempDir("manifest-write");
        (List<NativeLiveFile> nativeFiles, ulong lastSeq) = CreateNativeDb(dir.Path);
        RecoveredVersionState state;
        string oldManifest;
        using (ManifestLogReader manifest = OpenManifest(dir.Path, out oldManifest)) state = ManifestReader.Recover(manifest, null);

        // Write a fresh MANIFEST with the managed writer (snapshot of the recovered state) and point CURRENT at it.
        ulong manifestNumber = state.NextFileNumber;
        state.NextFileNumber = manifestNumber + 1;
        string newManifest = FileNames.DescriptorFileName(dir.Path, manifestNumber);
        await using (ManifestLogWriter log = ManifestLogWriter.Create(LocalFileSystem.Instance, newManifest))
        {
            await new ManifestWriter(log).WriteSnapshotAsync(state, static _ => 0, CancellationToken.None);
        }
        await FileNames.SetCurrentFileAsync(LocalFileSystem.Instance, dir.Path, manifestNumber, syncDirectory: true, CancellationToken.None);
        File.Delete(oldManifest);

        // Our own reader agrees with what we wrote.
        using (ManifestLogReader reread = ManifestLogReader.Open(LocalFileSystem.Instance, newManifest))
            AssertLiveFilesMatch(nativeFiles, ManifestReader.Recover(reread, null));

        // Native opens the DB through the managed MANIFEST and sees all data.
        var options = new RocksDbSharp.DbOptions();
        var cfs = new RocksDbSharp.ColumnFamilies();
        foreach (string n in s_cfNames) cfs.Add(n, new RocksDbSharp.ColumnFamilyOptions());
        using var db = RocksDbSharp.RocksDb.Open(options, dir.Path, cfs);
        Assert.AreEqual(lastSeq, db.GetLatestSequenceNumber());
        CollectionAssert.AreEqual("v2"u8.ToArray(), db.Get(Key("d", 5)));
        Assert.IsNull(db.Get(Key("d", 1)));
        CollectionAssert.AreEqual("w1"u8.ToArray(), db.Get(Key("c", 7), db.GetColumnFamily("cf1")));
        CollectionAssert.AreEqual("x"u8.ToArray(), db.Get("only"u8.ToArray(), db.GetColumnFamily("cf2")));
        List<NativeLiveFile> reopened = GetNativeLiveFiles(db);
        Assert.AreEqual(nativeFiles.Count, reopened.Count);
    }

    // ---- OPTIONS ---------------------------------------------------------------------------

    [TestMethod]
    public async Task NativeOptionsFileRoundTrip()
    {
        using var dir = new TempDir("options");
        CreateNativeDb(dir.Path);
        IFileSystem fs = LocalFileSystem.Instance;
        string latest = OptionsFile.FindLatestOptionsFile(fs, dir.Path)!;
        Assert.IsNotNull(latest);
        OptionsFile parsed = OptionsFile.Load(fs, latest, preserveUnknownOptions: true);
        Assert.AreEqual(11, parsed.RocksDbVersion.Major);
        Assert.AreEqual(1234, parsed.DbOptions.MaxOpenFiles);
        Assert.IsTrue(parsed.DbOptions.CreateIfMissing);
        Assert.AreEqual(3, parsed.ColumnFamilies.Count); // default, cf1, cf2 (DropColumnFamily rewrote the OPTIONS file)
        Assert.AreEqual("default", parsed.ColumnFamilies[0].Name);
        ColumnFamilyOptions cf1 = parsed.FindColumnFamily("cf1")!;
        Assert.AreEqual(8UL << 20, cf1.WriteBufferSize);
        Assert.AreEqual("leveldb.BytewiseComparator", cf1.ComparatorName);
        Assert.AreEqual(7, cf1.NumLevels);
        Assert.AreEqual(CompactionStyle.Level, cf1.CompactionStyle);
        Assert.AreEqual(RocksDb.Managed.Table.ChecksumType.Xxh3, cf1.TableOptions.Checksum);
        Assert.AreEqual(4096UL, cf1.TableOptions.BlockSize);

        // Re-serialize: parsing our output yields the same option set.
        string text = parsed.Serialize();
        OptionsFile reparsed = OptionsFile.Parse(text, preserveUnknownOptions: true);
        Assert.AreEqual(text, reparsed.Serialize());
        // Every native option survives (typed or preserved).
        OptionsFile nativeAgain = OptionsFile.Load(fs, latest, preserveUnknownOptions: true);
        CollectionAssert.AreEquivalent(OptionNames(nativeAgain.DbOptions.ToOptionsList()), OptionNames(reparsed.DbOptions.ToOptionsList()));

        // Change something, write it as the newest OPTIONS file and let native load it strictly.
        cf1.WriteBufferSize = 12345678;
        parsed.DbOptions.MaxOpenFiles = 4321;
        await parsed.WriteToDbDirectoryAsync(fs, dir.Path, 999999, CancellationToken.None);
        NativeLoadedOptions loaded = NativeLoadLatestOptions(dir.Path);
        Assert.AreEqual(4321, loaded.MaxOpenFiles);
        CollectionAssert.AreEqual(new[] { "default", "cf1", "cf2" }, loaded.ColumnFamilies.Keys.ToArray());
        Assert.AreEqual(12345678UL, loaded.ColumnFamilies["cf1"]);

        // Native still opens the DB with our OPTIONS file present.
        var cfs = new RocksDbSharp.ColumnFamilies();
        foreach (string n in s_cfNames) cfs.Add(n, new RocksDbSharp.ColumnFamilyOptions());
        using var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), dir.Path, cfs);
        CollectionAssert.AreEqual("v2"u8.ToArray(), db.Get(Key("d", 5)));
    }

    [TestMethod]
    public async Task ManagedDefaultOptionsLoadNativelyStrict()
    {
        // Every option name/value the managed model writes must be understood by native with ignore_unknown_options=false.
        using var dir = new TempDir("options-defaults");
        Directory.CreateDirectory(dir.Path);
        var file = new OptionsFile();
        file.DbOptions.CreateIfMissing = true;
        var def = new ColumnFamilyOptions();
        def.TableOptions.FilterPolicy = "bloomfilter:10:false";
        def.CompressionPerLevel.AddRange([RocksDb.Managed.Compression.CompressionType.None, RocksDb.Managed.Compression.CompressionType.LZ4, RocksDb.Managed.Compression.CompressionType.Zstd]);
        file.ColumnFamilies.Add(new ColumnFamilyDescriptor("default", def));
        var other = new ColumnFamilyOptions { WriteBufferSize = 777777, CompactionStyle = CompactionStyle.Universal };
        file.ColumnFamilies.Add(new ColumnFamilyDescriptor("with:special#chars", other));
        await file.WriteToDbDirectoryAsync(LocalFileSystem.Instance, dir.Path, 5, CancellationToken.None);

        NativeLoadedOptions loaded = NativeLoadLatestOptions(dir.Path);
        Assert.AreEqual(-1, loaded.MaxOpenFiles);
        Assert.AreEqual(2, loaded.ColumnFamilies.Count);
        Assert.AreEqual(777777UL, loaded.ColumnFamilies["with:special#chars"]);
    }

    private static List<string> OptionNames(List<KeyValuePair<string, string>> list)
    {
        var names = new List<string>();
        foreach (KeyValuePair<string, string> kv in list)
        {
            if (kv.Value.Length > 0) names.Add(kv.Key);
        }
        return names;
    }

    private sealed record NativeLoadedOptions(int MaxOpenFiles, Dictionary<string, ulong> ColumnFamilies);

    /// <summary>rocksdb_load_latest_options with ignore_unknown_options = false; returns write_buffer_size per CF.</summary>
    private static NativeLoadedOptions NativeLoadLatestOptions(string dbPath)
    {
        var native = RocksDbSharp.Native.Instance;
        IntPtr path = Marshal.StringToHGlobalAnsi(dbPath);
        IntPtr env = native.rocksdb_create_default_env();
        IntPtr cache = native.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            native.rocksdb_load_latest_options(path, env, 0, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            IntPtr dbOpts = Marshal.ReadIntPtr(dbOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            IntPtr names = Marshal.ReadIntPtr(namesOut);
            IntPtr cfOpts = Marshal.ReadIntPtr(cfOptsOut);
            var cfs = new Dictionary<string, ulong>();
            for (int i = 0; i < num; i++)
            {
                string name = Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(names, i * IntPtr.Size))!;
                cfs[name] = (ulong)native.rocksdb_options_get_write_buffer_size(Marshal.ReadIntPtr(cfOpts, i * IntPtr.Size));
            }
            int maxOpenFiles = native.rocksdb_options_get_max_open_files(dbOpts);
            native.rocksdb_load_latest_options_destroy(dbOpts, names, cfOpts, (UIntPtr)num);
            return new NativeLoadedOptions(maxOpenFiles, cfs);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            native.rocksdb_cache_destroy(cache);
            native.rocksdb_env_destroy(env);
        }
    }
}
