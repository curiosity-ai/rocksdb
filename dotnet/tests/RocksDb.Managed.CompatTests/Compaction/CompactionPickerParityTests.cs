using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Compaction;

/// <summary>
/// Compaction-picking parity with native RocksDB: the same seeded random-write stream runs against native (rocksdb-sharp)
/// and managed with identical leveled-compaction options (dynamic level bytes, small memtables, target file size and
/// level base) and <c>max_background_jobs = 1</c>. To remove scheduling nondeterminism the workload runs in lock step:
/// after every slice of writes both engines flush and wait for all compactions (DB::WaitForCompact), so the sequence
/// of LSM shapes depends only on the flush / picker / output-cutting logic. Both LOGs are parsed ("Compacting ..." /
/// "Moved #..." / flush and output table lines), so a divergence is reported with the first differing
/// compaction.
/// </summary>
[TestClass]
public sealed class CompactionPickerParityTests
{
    private const int KeySize = 16;
    private const int ValueSize = 100;
    private const ulong TargetFileSize = 256 * 1024;
    private const ulong LevelBase = 1024 * 1024;

    public TestContext TestContext { get; set; } = null!;

    /// <summary>LSM shape after one step: files and bytes per level.</summary>
    private sealed record Step(int[] Files, ulong[] Bytes);

    private sealed class Run
    {
        public List<Step> Steps { get; } = new();
        /// <summary>Compactions and trivial moves in LOG order ("4@0 + 3@5 -> L5", "move 1 -> L6").</summary>
        public List<string> Compactions { get; } = new();
        public ulong UserBytes { get; set; }
        public ulong WrittenBytes { get; set; }
        public double WriteAmp => (double)WrittenBytes / UserBytes;
    }

    /// <summary>db_bench GenerateKeyFromInt: 8-byte big-endian number padded with '0'.</summary>
    private static void FillKey(Span<byte> key, ulong k)
    {
        BinaryPrimitives.WriteUInt64BigEndian(key, k);
        key[8..].Fill((byte)'0');
    }

    /// <summary>SplitMix64.</summary>
    private static ulong Next(ref ulong state)
    {
        ulong z = state += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    private static byte[] ValuePool(ulong seed)
    {
        // 50 random printable bytes repeated twice per 100-byte piece (db_bench compression_ratio 0.5).
        var pool = new byte[64 * 1024];
        for (int piece = 0; piece + ValueSize <= pool.Length; piece += ValueSize)
        {
            for (int i = 0; i < ValueSize / 2; i++) pool[piece + i] = (byte)(' ' + (int)(Next(ref seed) % 95));
            pool.AsSpan(piece, ValueSize / 2).CopyTo(pool.AsSpan(piece + ValueSize / 2));
        }
        return pool;
    }

    /// <summary>
    /// The workload: <paramref name="steps"/> slices of <paramref name="putsPerStep"/> random puts over
    /// <paramref name="keySpace"/> keys (<see cref="SequentialKeys"/>: ascending keys, db_bench fillseq).
    /// </summary>
    private static Run Drive(Action<byte[], byte[]?> write, Action flushAndWait, Func<(int[] Files, ulong[] Bytes)> shape,
        int steps, int putsPerStep, ulong keySpace, ulong seed, int firstStep, int deletePercent)
    {
        var run = new Run();
        byte[] pool = ValuePool(seed);
        ulong state = seed;
        var key = new byte[KeySize];
        var value = new byte[ValueSize];
        int pos = 0;
        ulong sequence = 0;
        for (int s = 0; s < steps; s++)
        {
            for (int i = 0; i < putsPerStep; i++)
            {
                FillKey(key, keySpace == SequentialKeys ? sequence++ : Next(ref state) % keySpace);
                if (pos + ValueSize > pool.Length) pos = 0;
                pool.AsSpan(pos, ValueSize).CopyTo(value);
                pos += ValueSize;
                // deletePercent: this share of the operations deletes the key instead (a separate draw, only when > 0).
                bool delete = deletePercent > 0 && Next(ref state) % 100 < (ulong)deletePercent;
                if (s < firstStep) continue; // written by an earlier run on this DB
                write(key, delete ? null : value);
                run.UserBytes += KeySize + ValueSize;
            }
            if (s < firstStep) continue;
            flushAndWait();
            (int[] files, ulong[] bytes) = shape();
            run.Steps.Add(new Step(files, bytes));
        }
        return run;
    }

    // The plain-text LOG lines both engines write (flush_job.cc, compaction_job.cc, db_impl_compaction_flush.cc). The
    // EVENT_LOG_v1 JSON records are not used: in a .NET 11 apphost without the libstdc++ preload (tests/Shared/
    // NativeStdCxxPreload.cs) the native JSON writer loses every number after the first field.
    private static readonly Regex s_flush = new(@"\] Level-0 flush table #\d+: (\d+) bytes OK", RegexOptions.CultureInvariant);
    private static readonly Regex s_compacting = new(@"\] Compacting (.+) files to L(\d+), score", RegexOptions.CultureInvariant);
    private static readonly Regex s_generated = new(@"\] Generated table #\d+: (?:\d+ keys, )?(\d+) bytes", RegexOptions.CultureInvariant);
    private static readonly Regex s_moved = new(@"\] Moved #(\d+) files to level-(\d+) ", RegexOptions.CultureInvariant);

    /// <summary>
    /// Reads the LOG of the closed DB (native buffers its LOG, so it is complete only after close): the ordered
    /// compactions and trivial moves, and the written bytes (flush and compaction outputs). Compactions are not
    /// attributed to steps: native writes some lines through a LogBuffer, so a flush line and the compaction it
    /// triggered can appear in either order.
    /// </summary>
    private static void ReadLog(Run run, string dbPath)
    {
        string log = Path.Combine(dbPath, "LOG");
        if (!File.Exists(log)) Assert.Fail("no LOG in " + dbPath + ": " + string.Join(", ", Directory.GetFiles(dbPath)));
        int flushes = 0;
        foreach (string line in File.ReadAllLines(log))
        {
            Match m;
            if ((m = s_flush.Match(line)).Success)
            {
                flushes++;
                run.WrittenBytes += ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
            }
            else if ((m = s_generated.Match(line)).Success)
            {
                run.WrittenBytes += ulong.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
            }
            else if ((m = s_compacting.Match(line)).Success)
            {
                run.Compactions.Add(m.Groups[1].Value + " -> L" + m.Groups[2].Value);
            }
            else if ((m = s_moved.Match(line)).Success)
            {
                run.Compactions.Add("move " + m.Groups[1].Value + " -> L" + m.Groups[2].Value);
            }
        }
        Assert.AreEqual(run.Steps.Count, flushes, "flushes in the LOG of " + dbPath);
    }

    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static Run RunNative(string path, int steps, int putsPerStep, ulong keySpace, ulong seed) =>
        RunNative(path, steps, putsPerStep, keySpace, seed, static (_, _) => { }, 0, 0);

    /// <summary>
    /// Native run; <paramref name="configure"/> adjusts the base options (it gets the C options handle for settings
    /// rocksdb-sharp has no wrapper for, and must keep any native objects it creates alive).
    /// </summary>
    private static Run RunNative(string path, int steps, int putsPerStep, ulong keySpace, ulong seed,
        Action<RocksDbSharp.DbOptions, RocksDbSharp.Native> configure, int firstStep, int deletePercent)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        var table = new RocksDbSharp.BlockBasedTableOptions().SetBlockSize(4096).SetFormatVersion(6)
            .SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        var options = new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetCompression(RocksDbSharp.Compression.No)
            .SetWriteBufferSize(64 << 20)
            .SetTargetFileSizeBase(TargetFileSize)
            .SetMaxBytesForLevelBase(LevelBase)
            .SetLevel0FileNumCompactionTrigger(4)
            .SetBlockBasedTableFactory(table);
        native.rocksdb_options_set_max_background_jobs(options.Handle, 1);
        configure(options, native);
        s_keepAlive.Add(table);
        s_keepAlive.Add(options);
        var wo = new RocksDbSharp.WriteOptions().DisableWal(1);
        var fo = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        Run run;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, path))
        {
            run = Drive(
            (k, v) => { if (v is null) db.Remove(k, null, wo); else db.Put(k, v, null, wo); },
            () =>
            {
                db.Flush(fo);
                IntPtr wfc = native.rocksdb_wait_for_compact_options_create();
                try
                {
                    native.rocksdb_wait_for_compact(db.Handle, wfc, out IntPtr err);
                    if (err != IntPtr.Zero)
                    {
                        string msg = Marshal.PtrToStringUTF8(err) ?? "error";
                        native.rocksdb_free(err);
                        Assert.Fail("native WaitForCompact: " + msg);
                    }
                }
                finally
                {
                    native.rocksdb_wait_for_compact_options_destroy(wfc);
                }
            },
            () =>
            {
                var files = new int[7];
                var bytes = new ulong[7];
                foreach (RocksDbSharp.LiveFileMetadata f in db.GetLiveFilesMetadata())
                {
                    files[f.FileMetadata.FileLevel]++;
                    bytes[f.FileMetadata.FileLevel] += (ulong)f.FileMetadata.FileSize;
                }
                return (files, bytes);
            },
            steps, putsPerStep, keySpace, seed, firstStep, deletePercent);
        }
        ReadLog(run, path);
        return run;
    }

    private static Run RunManaged(string path, int steps, int putsPerStep, ulong keySpace, ulong seed) =>
        RunManaged(path, steps, putsPerStep, keySpace, seed, static _ => { }, 0, 0);

    /// <summary>Managed run; <paramref name="configure"/> adjusts the base column family options.</summary>
    private static Run RunManaged(string path, int steps, int putsPerStep, ulong keySpace, ulong seed, Action<ColumnFamilyOptions> configure, int firstStep, int deletePercent)
    {
        var cf = new ColumnFamilyOptions
        {
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            WriteBufferSize = 64 << 20,
            TargetFileSizeBase = TargetFileSize,
            MaxBytesForLevelBase = LevelBase,
            Level0FileNumCompactionTrigger = 4,
        };
        cf.TableOptions.BlockSize = 4096;
        cf.TableOptions.FormatVersion = 6;
        cf.TableOptions.FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10);
        configure(cf);
        var wo = new WriteOptions { DisableWal = true };
        Run run;
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, MaxBackgroundJobs = 1 }, cf))
        {
            run = Drive(
            (k, v) => { if (v is null) db.Delete(wo, k); else db.Put(wo, k, v); },
            () =>
            {
                db.Flush();
                db.WaitForCompact();
            },
            () =>
            {
                var files = new int[7];
                var bytes = new ulong[7];
                foreach (LiveFileMetadata f in db.GetLiveFilesMetadata())
                {
                    files[f.Level]++;
                    bytes[f.Level] += f.Size;
                }
                return (files, bytes);
            },
            steps, putsPerStep, keySpace, seed, firstStep, deletePercent);
        }
        ReadLog(run, path);
        return run;
    }

    private static string Shape(Step s) => string.Join(' ', s.Files);

    private static string MiB(ulong[] bytes)
    {
        var sb = new StringBuilder();
        foreach (ulong b in bytes) sb.Append((b / 1048576.0).ToString("F2", CultureInfo.InvariantCulture)).Append(' ');
        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Side-by-side trace of both runs: the LSM shape after every step, then the compactions in order (written to the
    /// test output, and on failure into the message). <paramref name="firstShape"/> / <paramref name="firstCompaction"/>
    /// are the first differing step / compaction (-1 if none).
    /// </summary>
    private static string Trace(Run native, Run managed, out int firstShape, out int firstCompaction)
    {
        firstShape = firstCompaction = -1;
        var sb = new StringBuilder();
        for (int i = 0; i < Math.Max(native.Steps.Count, managed.Steps.Count); i++)
        {
            Step? n = i < native.Steps.Count ? native.Steps[i] : null;
            Step? m = i < managed.Steps.Count ? managed.Steps[i] : null;
            bool same = n is not null && m is not null && Shape(n) == Shape(m);
            if (!same && firstShape < 0) firstShape = i;
            sb.Append(CultureInfo.InvariantCulture,
                $"{i,3} {(same ? ' ' : '*')} native [{(n is null ? "" : Shape(n))}] ({(n is null ? "" : MiB(n.Bytes))})  managed [{(m is null ? "" : Shape(m))}] ({(m is null ? "" : MiB(m.Bytes))})\n");
        }
        for (int i = 0; i < Math.Max(native.Compactions.Count, managed.Compactions.Count); i++)
        {
            string n = i < native.Compactions.Count ? native.Compactions[i] : "";
            string m = i < managed.Compactions.Count ? managed.Compactions[i] : "";
            if (n != m && firstCompaction < 0) firstCompaction = i;
            sb.Append(CultureInfo.InvariantCulture, $"c{i,3} {(n == m ? ' ' : '*')} native {n,-22} managed {m}\n");
        }
        sb.Append(CultureInfo.InvariantCulture,
            $"write-amp native {native.WriteAmp:F3} ({native.WrittenBytes} B), managed {managed.WriteAmp:F3} ({managed.WrittenBytes} B), user {native.UserBytes} B\n");
        sb.Append(CultureInfo.InvariantCulture, $"first divergence: shape at step {firstShape}, compaction #{firstCompaction}\n");
        return sb.ToString();
    }

    /// <summary>
    /// Fill + overwrite of a scaled-down copy of the compaction-heavy dbbench configuration (bench/run-all.sh label
    /// <c>compaction</c>: memtable = target file size = level base / 4), one memtable per step: the LSM shape after
    /// every step must match native within one file per level, and write amplification within 5%.
    /// </summary>
    [TestMethod]
    public void LockStepRandomWritesMatchNativeLsmShape()
    {
        const int Steps = 60;
        const int PutsPerStep = 2200; // ~255 KB of key + value bytes: one target-size L0 file per step
        const ulong KeySpace = 60000;
        const ulong Seed = 301;
        using var nativeDir = new TempDir("parity-native");
        using var managedDir = new TempDir("parity-managed");
        Run native = RunNative(nativeDir.Path, Steps, PutsPerStep, KeySpace, Seed);
        Run managed = RunManaged(managedDir.Path, Steps, PutsPerStep, KeySpace, Seed);
        string trace = Trace(native, managed, out _, out _);
        TestContext.WriteLine(trace);
        // PARITY_TRACE=<file>: keep the side-by-side trace of a passing run too.
        if (Environment.GetEnvironmentVariable("PARITY_TRACE") is { Length: > 0 } traceFile) File.WriteAllText(traceFile, trace);

        for (int i = 0; i < Steps; i++)
        {
            for (int level = 0; level < 7; level++)
            {
                int n = native.Steps[i].Files[level], m = managed.Steps[i].Files[level];
                if (Math.Abs(n - m) > 1) Assert.Fail($"step {i} L{level}: native {n} files, managed {m}\n{trace}");
            }
        }
        double ratio = managed.WriteAmp / native.WriteAmp;
        Assert.IsTrue(ratio is > 0.95 and < 1.05, $"write-amp native {native.WriteAmp:F3} managed {managed.WriteAmp:F3}\n{trace}");
    }
    /// <summary>
    /// Runs the lock-step workload on both engines with the given style configuration and requires identical LSM
    /// shapes after every step and the same sequence of compactions.
    /// </summary>
    private void AssertLockStepParity(string name, int steps, int putsPerStep, ulong keySpace, ulong seed,
        Action<RocksDbSharp.DbOptions, RocksDbSharp.Native> configureNative, Action<ColumnFamilyOptions> configureManaged) =>
        AssertLockStepParity(name, steps, putsPerStep, keySpace, seed, configureNative, configureManaged, 0);

    /// <summary>As above, with <paramref name="deletePercent"/> of the operations being deletes.</summary>
    private void AssertLockStepParity(string name, int steps, int putsPerStep, ulong keySpace, ulong seed,
        Action<RocksDbSharp.DbOptions, RocksDbSharp.Native> configureNative, Action<ColumnFamilyOptions> configureManaged, int deletePercent)
    {
        using var nativeDir = new TempDir(name + "-native");
        using var managedDir = new TempDir(name + "-managed");
        Run native = RunNative(nativeDir.Path, steps, putsPerStep, keySpace, seed, configureNative, 0, deletePercent);
        Run managed = RunManaged(managedDir.Path, steps, putsPerStep, keySpace, seed, configureManaged, 0, deletePercent);
        string trace = Trace(native, managed, out int firstShape, out int firstCompaction);
        TestContext.WriteLine(trace);
        if (Environment.GetEnvironmentVariable("PARITY_TRACE") is { Length: > 0 } traceFile) File.WriteAllText(traceFile + "." + name, trace);
        Assert.AreEqual(-1, firstShape, "first differing LSM shape\n" + trace);
        Assert.AreEqual(-1, firstCompaction, "first differing compaction\n" + trace);
        Assert.IsTrue(native.Compactions.Count > 0, "the workload should compact\n" + trace);
        double ratio = managed.WriteAmp / native.WriteAmp;
        Assert.IsTrue(ratio is > 0.98 and < 1.02, $"write-amp native {native.WriteAmp:F3} managed {managed.WriteAmp:F3}\n{trace}");
    }

    /// <summary>
    /// Universal compaction (compaction_picker_universal.cc): sorted-run count, size ratio and size amplification
    /// picking, output levels and trivial-move decisions must match native step by step.
    /// </summary>
    [TestMethod]
    public void LockStepUniversalMatchesNative()
    {
        AssertLockStepParity("parity-universal", steps: 40, putsPerStep: 2200, keySpace: 60000, seed: 401,
            static (o, _) => o.SetCompactionStyle(RocksDbSharp.Compaction.Universal),
            static cf => cf.CompactionStyle = CompactionStyle.Universal);
    }

    /// <summary>
    /// Universal compaction with max_read_amp auto-tuning (0), a larger size ratio and trivial moves: exercises
    /// GetMaxNumFilesToCompactBasedOnMaxReadAmp, the similar-size stop style is not used by native's default.
    /// </summary>
    [TestMethod]
    public void LockStepUniversalMaxReadAmpMatchesNative()
    {
        AssertLockStepParity("parity-universal-readamp", steps: 40, putsPerStep: 2200, keySpace: 60000, seed: 402,
            static (o, native) =>
            {
                o.SetCompactionStyle(RocksDbSharp.Compaction.Universal);
                IntPtr uco = native.rocksdb_universal_compaction_options_create();
                native.rocksdb_universal_compaction_options_set_size_ratio(uco, 20);
                native.rocksdb_universal_compaction_options_set_max_read_amp(uco, 0);
                native.rocksdb_universal_compaction_options_set_allow_trivial_move(uco, 1);
                o.SetUniversalCompactionOptions(uco); // copied into the options
                native.rocksdb_universal_compaction_options_destroy(uco);
            },
            static cf =>
            {
                cf.CompactionStyle = CompactionStyle.Universal;
                cf.CompactionOptionsUniversal.SizeRatio = 20;
                cf.CompactionOptionsUniversal.MaxReadAmp = 0;
                cf.CompactionOptionsUniversal.AllowTrivialMove = true;
            });
    }

    /// <summary>
    /// FIFO compaction (compaction_picker_fifo.cc) with intra-L0 compaction: size-based deletion of the oldest files
    /// and cost-based intra-L0 merging must match native step by step.
    /// </summary>
    [TestMethod]
    public void LockStepFifoMatchesNative()
    {
        const ulong MaxTableFilesSize = 3 * 1024 * 1024;
        AssertLockStepParity("parity-fifo", steps: 40, putsPerStep: 2200, keySpace: 60000, seed: 403,
            static (o, native) =>
            {
                o.SetCompactionStyle(RocksDbSharp.Compaction.Fifo);
                IntPtr fifo = native.rocksdb_fifo_compaction_options_create();
                native.rocksdb_fifo_compaction_options_set_max_table_files_size(fifo, MaxTableFilesSize);
                native.rocksdb_fifo_compaction_options_set_allow_compaction(fifo, 1);
                o.SetFifoCompactionOptions(fifo); // copied into the options
                native.rocksdb_fifo_compaction_options_destroy(fifo);
            },
            static cf =>
            {
                cf.CompactionStyle = CompactionStyle.Fifo;
                cf.CompactionOptionsFifo.MaxTableFilesSize = MaxTableFilesSize;
                cf.CompactionOptionsFifo.AllowCompaction = true;
            });
    }

    /// <summary>
    /// Deletion-heavy writes (70% point deletes): flushed files have more deletions than values, so their compensated
    /// size gets the point-deletion boost (Version::UpdateAccumulatedStats + ComputeCompensatedSizes), which raises the
    /// level scores and orders the files of a level (kByCompensatedSize; kMinOverlappingRatio divides by it). The
    /// compactions must match native step by step.
    /// </summary>
    [TestMethod]
    [DataRow(0, DisplayName = "kByCompensatedSize")]
    [DataRow(3, DisplayName = "kMinOverlappingRatio")]
    public void LockStepDeletionHeavyMatchesNative(int pri)
    {
        AssertLockStepParity("parity-deletes-" + pri, steps: 50, putsPerStep: 2200, keySpace: 60000, seed: 406,
            (o, native) => native.rocksdb_options_set_compaction_pri(o.Handle, pri),
            cf => cf.CompactionPri = (CompactionPri)pri, deletePercent: 70);
    }

    private const int RoundRobinPri = 4; // kRoundRobin

    /// <summary>keySpace value of <see cref="Drive"/> for ascending keys (non-overlapping files: trivial moves).</summary>
    private const ulong SequentialKeys = 0;

    private static void NativeRoundRobin(RocksDbSharp.DbOptions o, RocksDbSharp.Native native) =>
        native.rocksdb_options_set_compaction_pri(o.Handle, RoundRobinPri);

    private static void ManagedRoundRobin(ColumnFamilyOptions cf) => cf.CompactionPri = CompactionPri.RoundRobin;

    /// <summary>
    /// Leveled kRoundRobin compaction: the files picked from each level follow the compact cursor (SortFileByRoundRobin,
    /// round-robin expansion, output split at the output level's cursor), which advances with every compaction
    /// (GetNextCompactCursor recorded in the version edit) -- same sequence as native.
    /// </summary>
    [TestMethod]
    public void LockStepRoundRobinMatchesNative()
    {
        AssertLockStepParity("parity-roundrobin", steps: 50, putsPerStep: 2200, keySpace: 60000, seed: 404, NativeRoundRobin, ManagedRoundRobin);
    }

    /// <summary>
    /// kRoundRobin with ascending keys: most compactions are trivial moves, and a trivially moved kLevelMaxLevelSize
    /// compaction advances the compact cursor like a rewriting one (db_impl_compaction_flush.cc BackgroundCompaction,
    /// GetNextCompactCursor). The LSM shapes, the compaction / move sequence and the cursors persisted in the MANIFEST
    /// must match native.
    /// </summary>
    [TestMethod]
    public void RoundRobinTrivialMovesAdvanceCursorLikeNative()
    {
        using var nativeDir = new TempDir("rr-seq-native");
        using var managedDir = new TempDir("rr-seq-managed");
        Run native = RunNative(nativeDir.Path, 40, 2200, SequentialKeys, 406, NativeRoundRobin, 0, 0);
        Run managed = RunManaged(managedDir.Path, 40, 2200, SequentialKeys, 406, ManagedRoundRobin, 0, 0);
        string trace = Trace(native, managed, out int firstShape, out int firstCompaction);
        TestContext.WriteLine(trace);
        Assert.AreEqual(-1, firstShape, "first differing LSM shape\n" + trace);
        Assert.AreEqual(-1, firstCompaction, "first differing compaction\n" + trace);
        Assert.IsTrue(native.Compactions.Exists(c => c.StartsWith("move ", StringComparison.Ordinal) && !c.EndsWith("-> L1", StringComparison.Ordinal)),
            "the workload should trivially move files below L1\n" + trace);
        SortedDictionary<int, byte[]> nativeCursors = RecordedCursors(nativeDir.Path), managedCursors = RecordedCursors(managedDir.Path);
        Assert.IsTrue(nativeCursors.Count > 0, "native should record a compact cursor\n" + trace);
        CollectionAssert.AreEqual(nativeCursors.Keys.ToList(), managedCursors.Keys.ToList());
        foreach (int level in nativeCursors.Keys) CollectionAssert.AreEqual(nativeCursors[level], managedCursors[level], "cursor of L" + level);
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from))
        {
            string name = Path.GetFileName(f);
            if (name is "LOCK" || name.StartsWith("LOG", StringComparison.Ordinal)) continue;
            File.Copy(f, Path.Combine(to, name), overwrite: true);
        }
    }

    /// <summary>The round-robin compact cursors recorded in the DB's MANIFEST (VersionEdit kCompactCursor records).</summary>
    private static SortedDictionary<int, byte[]> RecordedCursors(string dbPath)
    {
        RocksDb.Managed.Db.RecoveredVersionState state = RocksDb.Managed.Db.ManifestReader.RecoverFromDirectory(RocksDb.Managed.Env.LocalFileSystem.Instance, dbPath, null, out _);
        return state.ColumnFamilies[0].CompactCursors;
    }

    /// <summary>
    /// Round-robin compact cursors persisted in the MANIFEST are byte-compatible: the first half of the workload runs on
    /// <paramref name="writerIsNative"/>'s engine with kRoundRobin, which records cursors; then copies of that DB are
    /// reopened by both engines and the second half runs on each. Both must resume from the recovered cursors
    /// (VersionBuilder applying the kCompactCursor records) and produce the same LSM shapes and compactions; a lost or
    /// misread cursor makes the first L5 compaction pick different files.
    /// </summary>
    [TestMethod]
    [DataRow(true, DisplayName = "native writes, both resume")]
    [DataRow(false, DisplayName = "managed writes, both resume")]
    public void RoundRobinCursorsResumeAcrossEngines(bool writerIsNative)
    {
        const int Steps = 50, Half = 25, PutsPerStep = 2200;
        const ulong KeySpace = 60000, Seed = 405;
        using var first = new TempDir("rr-first");
        using var nativeDir = new TempDir("rr-native");
        using var managedDir = new TempDir("rr-managed");
        if (writerIsNative) RunNative(first.Path, Half, PutsPerStep, KeySpace, Seed, NativeRoundRobin, 0, 0);
        else RunManaged(first.Path, Half, PutsPerStep, KeySpace, Seed, ManagedRoundRobin, 0, 0);
        SortedDictionary<int, byte[]> cursors = RecordedCursors(first.Path);
        Assert.IsTrue(cursors.Count > 0, "the first half should record a compact cursor");
        CopyDirectory(first.Path, nativeDir.Path);
        CopyDirectory(first.Path, managedDir.Path);

        Run native = RunNative(nativeDir.Path, Steps, PutsPerStep, KeySpace, Seed, NativeRoundRobin, Half, 0);
        Run managed = RunManaged(managedDir.Path, Steps, PutsPerStep, KeySpace, Seed, ManagedRoundRobin, Half, 0);
        string trace = Trace(native, managed, out int firstShape, out int firstCompaction);
        TestContext.WriteLine("cursors after the first half: " + string.Join(", ", cursors.Keys) + "\n" + trace);
        Assert.AreEqual(-1, firstShape, "first differing LSM shape\n" + trace);
        Assert.AreEqual(-1, firstCompaction, "first differing compaction\n" + trace);
        Assert.IsTrue(native.Compactions.Count > 0, "the second half should compact\n" + trace);

        // Both engines keep writing (rewrite the cursors into a new MANIFEST on reopen) in the same format.
        SortedDictionary<int, byte[]> nativeCursors = RecordedCursors(nativeDir.Path), managedCursors = RecordedCursors(managedDir.Path);
        CollectionAssert.AreEqual(nativeCursors.Keys.ToList(), managedCursors.Keys.ToList());
        foreach (int level in nativeCursors.Keys) CollectionAssert.AreEqual(nativeCursors[level], managedCursors[level], "cursor of L" + level);
    }
}
