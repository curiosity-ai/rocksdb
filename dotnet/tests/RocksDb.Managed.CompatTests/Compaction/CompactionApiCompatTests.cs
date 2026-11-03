using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Compaction APIs and triggers compared with native RocksDB 11.12 (rocksdb-sharp): DB::CompactFiles (input
/// sanitization, output level, trivial move, error statuses) and bottommost files marked for compaction after a snapshot
/// release (with bottommost_file_compaction_delay), plus the delay option's OPTIONS-file round trip.
/// </summary>
[TestClass]
public sealed class CompactionApiCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
    private static byte[] V(int i, int round) => U(new string((char)('a' + round % 26), 200) + i.ToString(System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>A file of the LSM: level, smallest and largest user key (number-independent description).</summary>
    private readonly record struct FileShape(int Level, string Smallest, string Largest);

    private static int CompareShape(FileShape a, FileShape b)
    {
        int c = a.Level.CompareTo(b.Level);
        if (c == 0) c = string.CompareOrdinal(a.Smallest, b.Smallest);
        return c != 0 ? c : string.CompareOrdinal(a.Largest, b.Largest);
    }

    private static List<FileShape> NativeShape(RocksDbSharp.RocksDb db)
    {
        var l = new List<FileShape>();
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            l.Add(new FileShape(f.FileMetadata.FileLevel, f.FileDataMetadata.SmallestKeyInFile, f.FileDataMetadata.LargestKeyInFile));
        }
        l.Sort(CompareShape);
        return l;
    }

    private static List<FileShape> ManagedShape(RocksDatabase db)
    {
        var l = new List<FileShape>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            l.Add(new FileShape(f.Level, Encoding.UTF8.GetString(f.SmallestKey), Encoding.UTF8.GetString(f.LargestKey)));
        }
        l.Sort(CompareShape);
        return l;
    }

    private static string Describe(List<FileShape> s) => string.Join("; ", s);

    private static void AssertSameShape(List<FileShape> native, List<FileShape> managed, string what) =>
        Assert.AreEqual(Describe(native), Describe(managed), what);

    /// <summary>Live table files of a level ("/000012.sst"), L0 newest first like GetLiveFilesMetadata.</summary>
    private static List<string> NativeFiles(RocksDbSharp.RocksDb db, int level)
    {
        var l = new List<string>();
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (f.FileMetadata.FileLevel == level) l.Add(f.FileMetadata.FileName);
        }
        l.Sort(StringComparer.Ordinal);
        return l;
    }

    private static List<string> ManagedFiles(RocksDatabase db, int level)
    {
        var l = new List<string>();
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
        {
            if (f.Level == level) l.Add(f.Name);
        }
        l.Sort(StringComparer.Ordinal);
        return l;
    }

    /// <summary>rocksdb_compact_files with output_path_id -1; returns the output file names or the error message.</summary>
    private static (List<string>? Outputs, string? Error) NativeCompactFiles(RocksDbSharp.RocksDb db, IReadOnlyList<string> inputs, int outputLevel,
        bool allowTrivialMove)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr options = native.rocksdb_compaction_options_create();
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 2);
        try
        {
            native.rocksdb_compaction_options_set_allow_trivial_move(options, allowTrivialMove ? (byte)1 : (byte)0);
            Marshal.WriteIntPtr(outs, IntPtr.Zero);
            Marshal.WriteIntPtr(outs + IntPtr.Size, IntPtr.Zero);
            string[] names = inputs.ToArray();
            native.rocksdb_compact_files(db.Handle, options, names, (UIntPtr)names.Length, outputLevel, -1, outs, outs + IntPtr.Size, IntPtr.Zero,
                out IntPtr err);
            if (err != IntPtr.Zero)
            {
                string msg = Marshal.PtrToStringUTF8(err) ?? "error";
                native.rocksdb_free(err);
                return (null, msg);
            }
            IntPtr array = Marshal.ReadIntPtr(outs);
            int count = (int)Marshal.ReadIntPtr(outs + IntPtr.Size);
            var result = new List<string>();
            for (int i = 0; i < count; i++) result.Add(Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(array, i * IntPtr.Size))!);
            if (array != IntPtr.Zero) native.rocksdb_compact_files_output_file_names_destroy(array, (UIntPtr)count);
            return (result, null);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            native.rocksdb_compaction_options_destroy(options);
        }
    }

    private static RocksDbSharp.DbOptions NativeManualOptions()
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No).SetDisableAutoCompactions(1);
        native.rocksdb_options_set_level_compaction_dynamic_level_bytes(o.Handle, 0);
        s_keepAlive.Add(o);
        return o;
    }

    private static ColumnFamilyOptions ManagedManualOptions() => new()
    {
        DisableAutoCompactions = true,
        LevelCompactionDynamicLevelBytes = false,
        Compression = RocksDb.Managed.Compression.CompressionType.None,
    };

    /// <summary>Writes keys [from, to) with stride, one value round, and flushes (one L0 file).</summary>
    private static void NativeBatch(RocksDbSharp.RocksDb db, int from, int to, int stride, int round)
    {
        for (int i = from; i < to; i += stride) db.Put(U(K(i)), V(i, round));
        db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
    }

    private static void ManagedBatch(RocksDatabase db, int from, int to, int stride, int round)
    {
        for (int i = from; i < to; i += stride) db.Put(U(K(i)), V(i, round));
        db.Flush();
    }

    /// <summary>
    /// db_compaction_test.cc CompactFiles* / compact_files_test.cc: the same DB on both engines, the same CompactFiles
    /// calls (by the names of corresponding files), the same resulting LSM. Covers the sanitization that adds files
    /// (older L0 files when compacting to L1+, overlapping files of the levels down to the output level), the trivial
    /// move option and the InvalidArgument errors; the managed result is then read back by native.
    /// </summary>
    [TestMethod]
    public void CompactFilesMatchesNative()
    {
        using var nativeDir = new TempDir("cf-native");
        using var managedDir = new TempDir("cf-managed");
        using (RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.Open(NativeManualOptions(), nativeDir.Path))
        using (RocksDatabase mdb = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, ManagedManualOptions()))
        {
            // Three overlapping L0 files; compacting the middle one to L1 must take the older one too.
            for (int r = 0; r < 3; r++)
            {
                NativeBatch(ndb, r * 10, 600, 3, r);
                ManagedBatch(mdb, r * 10, 600, 3, r);
            }
            List<string> nL0 = NativeFiles(ndb, 0), mL0 = ManagedFiles(mdb, 0);
            Assert.AreEqual(3, nL0.Count);
            (List<string>? nOut, string? nErr) = NativeCompactFiles(ndb, [nL0[1]], 1, allowTrivialMove: false);
            Assert.IsNull(nErr, nErr);
            IReadOnlyList<string> mOut = mdb.CompactFiles([mL0[1]], 1);
            Assert.AreEqual(nOut!.Count, mOut.Count, "output files");
            AssertSameShape(NativeShape(ndb), ManagedShape(mdb), "after L0 -> L1");
            Assert.AreEqual(1, ManagedFiles(mdb, 0).Count, "the newest L0 file stays");

            // Two more L0 files, then compact the newest L0 file to L2: all L0 files and the overlapping L1 file go.
            for (int r = 3; r < 5; r++)
            {
                NativeBatch(ndb, 100 * r, 100 * r + 200, 1, r);
                ManagedBatch(mdb, 100 * r, 100 * r + 200, 1, r);
            }
            nL0 = NativeFiles(ndb, 0);
            mL0 = ManagedFiles(mdb, 0);
            (nOut, nErr) = NativeCompactFiles(ndb, [nL0[^1]], 2, allowTrivialMove: false);
            Assert.IsNull(nErr, nErr);
            mOut = mdb.CompactFiles([mL0[^1]], 2);
            Assert.AreEqual(nOut!.Count == 0 ? 0 : 1, mOut.Count == 0 ? 0 : 1);
            AssertSameShape(NativeShape(ndb), ManagedShape(mdb), "after L0 -> L2");

            // Errors: unknown file, compaction to an upper level, level beyond the last one.
            (_, nErr) = NativeCompactFiles(ndb, ["/999999.sst"], 3, false);
            var ex = Assert.ThrowsExactly<RocksDbException>(() => mdb.CompactFiles(["/999999.sst"], 3));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.StartsWith(nErr, "Invalid argument");
            StringAssert.Contains(nErr, "does not exist");
            StringAssert.Contains(ex.Message, "does not exist");
            string nL2 = NativeFiles(ndb, 2)[0], mL2 = ManagedFiles(mdb, 2)[0];
            (_, nErr) = NativeCompactFiles(ndb, [nL2], 1, false);
            ex = Assert.ThrowsExactly<RocksDbException>(() => mdb.CompactFiles([mL2], 1));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.Contains(nErr, "Cannot compact file to up level");
            StringAssert.Contains(ex.Message, "Cannot compact file to up level");
            (_, nErr) = NativeCompactFiles(ndb, [nL2], 7, false);
            ex = Assert.ThrowsExactly<RocksDbException>(() => mdb.CompactFiles([mL2], 7));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.StartsWith(nErr, "Invalid argument");

            // allow_trivial_move: a single L2 file without overlap below moves to L4 unchanged (same file number).
            (nOut, nErr) = NativeCompactFiles(ndb, [nL2], 4, allowTrivialMove: true);
            Assert.IsNull(nErr, nErr);
            mOut = mdb.CompactFilesAsync(new CompactFilesOptions { AllowTrivialMove = true }, [mL2], 4).AsTask().GetAwaiter().GetResult();
            AssertSameShape(NativeShape(ndb), ManagedShape(mdb), "after trivial move");
            Assert.AreEqual(1, mOut.Count);
            Assert.IsTrue(mOut[0].EndsWith(mL2, StringComparison.Ordinal), mOut[0] + " should be the moved " + mL2);
            Assert.IsTrue(nOut![0].EndsWith(nL2, StringComparison.Ordinal), nOut[0] + " should be the moved " + nL2);

            // Same contents.
            for (int i = 0; i < 700; i++)
            {
                byte[]? nv = ndb.Get(U(K(i)));
                Assert.AreEqual(nv is null ? null : Encoding.UTF8.GetString(nv), mdb.Get(U(K(i))) is { } mv ? Encoding.UTF8.GetString(mv) : null, K(i));
            }
        }

        // The managed outputs are valid for native: same contents as the native DB.
        using (RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), nativeDir.Path))
        using (RocksDbSharp.RocksDb mdbByNative = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), managedDir.Path))
        {
            int found = 0;
            for (int i = 0; i < 700; i++)
            {
                byte[]? nv = ndb.Get(U(K(i))), mv = mdbByNative.Get(U(K(i)));
                CollectionAssert.AreEqual(nv, mv, K(i));
                if (nv is not null) found++;
            }
            Assert.IsTrue(found > 300, "found " + found);
        }
    }

    // ---- bottommost files marked for compaction --------------------------------------------------------------

    private static void NativeWaitForCompact(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr wfc = native.rocksdb_wait_for_compact_options_create();
        try
        {
            native.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
            if (err != IntPtr.Zero) Assert.Fail("native WaitForCompact failed");
        }
        finally
        {
            native.rocksdb_wait_for_compact_options_destroy(wfc);
        }
    }

    private static ulong NativeTotalSize(RocksDbSharp.RocksDb db)
    {
        ulong s = 0;
        foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata()) s += (ulong)f.FileMetadata.FileSize;
        return s;
    }

    private static ulong ManagedTotalSize(RocksDatabase db)
    {
        ulong s = 0;
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) s += f.Size;
        return s;
    }

    /// <summary>
    /// db_compaction_test.cc CompactBottomLevelFilesWithDeletions / DelayCompactBottomLevelFilesWithDeletions: deletes
    /// written under a snapshot survive the full compaction into the bottommost level; releasing the snapshot marks the
    /// bottommost files (ComputeBottommostFilesMarkedForCompaction) and a BottommostFiles compaction drops the deleted
    /// data -- immediately without a delay, not at all within bottommost_file_compaction_delay. Both engines end with
    /// the same files.
    /// </summary>
    [TestMethod]
    [DataRow(0u, DisplayName = "no delay")]
    [DataRow(3600u, DisplayName = "delay 3600 s")]
    public void BottommostFilesCompactionAfterSnapshotReleaseMatchesNative(uint delay)
    {
        using var nativeDir = new TempDir("bottom-native");
        using var managedDir = new TempDir("bottom-managed");
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCompression(RocksDbSharp.Compression.No);
        native.rocksdb_options_set_level0_file_num_compaction_trigger(no.Handle, 100);
        native.rocksdb_options_set_bottommost_file_compaction_delay(no.Handle, delay);
        s_keepAlive.Add(no);
        var mo = new ColumnFamilyOptions
        {
            Level0FileNumCompactionTrigger = 100,
            BottommostFileCompactionDelay = delay,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
        };

        ulong nBefore, nAfter, mBefore, mAfter;
        List<FileShape> nShape, mShape;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(no, nativeDir.Path))
        {
            for (int i = 0; i < 2000; i++) db.Put(U(K(i)), V(i, 0));
            RocksDbSharp.Snapshot snap = db.CreateSnapshot();
            for (int i = 0; i < 1500; i++) db.Remove(U(K(i)));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null!, null!);
            nBefore = NativeTotalSize(db);
            // A write after the deletions, so the oldest snapshot after the release (the last sequence) is above them.
            db.Put(U("zzz"), U("x"));
            snap.Dispose();
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            NativeWaitForCompact(db);
            nAfter = NativeTotalSize(db);
            nShape = NativeShape(db);
        }
        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, mo))
        {
            for (int i = 0; i < 2000; i++) db.Put(U(K(i)), V(i, 0));
            Snapshot snap = db.GetSnapshot();
            for (int i = 0; i < 1500; i++) db.Delete(U(K(i)));
            db.Flush();
            db.CompactRange();
            mBefore = ManagedTotalSize(db);
            db.Put(U("zzz"), U("x"));
            snap.Dispose();
            db.Flush();
            db.WaitForCompact();
            mAfter = ManagedTotalSize(db);
            mShape = ManagedShape(db);
        }
        string sizes = $"native {nBefore} -> {nAfter}, managed {mBefore} -> {mAfter}";
        if (delay == 0)
        {
            Assert.IsTrue(nAfter < nBefore / 2, "native should drop the deleted data: " + sizes);
            Assert.IsTrue(mAfter < mBefore / 2, "managed should drop the deleted data: " + sizes);
        }
        else
        {
            Assert.IsTrue(nAfter >= nBefore, "native keeps the files within the delay: " + sizes);
            Assert.IsTrue(mAfter >= mBefore, "managed keeps the files within the delay: " + sizes);
        }
        AssertSameShape(nShape, mShape, sizes);
    }

    /// <summary>bottommost_file_compaction_delay is written to and read from the OPTIONS file like native.</summary>
    [TestMethod]
    public void BottommostFileCompactionDelayOptionRoundTrips()
    {
        using var nativeDir = new TempDir("bfcd-native");
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        native.rocksdb_options_set_bottommost_file_compaction_delay(no.Handle, 7200);
        s_keepAlive.Add(no);
        using (RocksDbSharp.RocksDb.Open(no, nativeDir.Path)) { }
        string? path = RocksDb.Managed.OptionsFile.FindLatestOptionsFile(RocksDb.Managed.Env.LocalFileSystem.Instance, nativeDir.Path);
        Assert.IsNotNull(path);
        RocksDb.Managed.OptionsFile parsed = RocksDb.Managed.OptionsFile.Load(RocksDb.Managed.Env.LocalFileSystem.Instance, path, preserveUnknownOptions: true);
        Assert.AreEqual(7200u, parsed.FindColumnFamily("default")!.BottommostFileCompactionDelay);

        // Managed writes it; native opens the DB with the persisted options and reads the value back.
        using var managedDir = new TempDir("bfcd-managed");
        using (RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions { BottommostFileCompactionDelay = 900 })) { }
        string text = File.ReadAllText(RocksDb.Managed.OptionsFile.FindLatestOptionsFile(RocksDb.Managed.Env.LocalFileSystem.Instance, managedDir.Path)!);
        StringAssert.Contains(text, "bottommost_file_compaction_delay=900");
        using (RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions(), managedDir.Path)) { }
    }
}
