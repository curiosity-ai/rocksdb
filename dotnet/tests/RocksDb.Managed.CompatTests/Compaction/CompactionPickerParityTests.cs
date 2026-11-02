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

    /// <summary>The workload: <paramref name="steps"/> slices of <paramref name="putsPerStep"/> random puts over <paramref name="keySpace"/> keys.</summary>
    private static Run Drive(Action<byte[], byte[]> put, Action flushAndWait, Func<(int[] Files, ulong[] Bytes)> shape,
        int steps, int putsPerStep, ulong keySpace, ulong seed)
    {
        var run = new Run();
        byte[] pool = ValuePool(seed);
        ulong state = seed;
        var key = new byte[KeySize];
        var value = new byte[ValueSize];
        int pos = 0;
        for (int s = 0; s < steps; s++)
        {
            for (int i = 0; i < putsPerStep; i++)
            {
                FillKey(key, Next(ref state) % keySpace);
                if (pos + ValueSize > pool.Length) pos = 0;
                pool.AsSpan(pos, ValueSize).CopyTo(value);
                pos += ValueSize;
                put(key, value);
                run.UserBytes += KeySize + ValueSize;
            }
            flushAndWait();
            (int[] files, ulong[] bytes) = shape();
            run.Steps.Add(new Step(files, bytes));
        }
        return run;
    }

    // The plain-text LOG lines both engines write (flush_job.cc, compaction_job.cc, db_impl_compaction_flush.cc). The
    // EVENT_LOG_v1 JSON records are not used: the native library's JSON writer emits only the first field when the
    // host runs on .NET 11.
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

    private static Run RunNative(string path, int steps, int putsPerStep, ulong keySpace, ulong seed)
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
        s_keepAlive.Add(table);
        s_keepAlive.Add(options);
        var wo = new RocksDbSharp.WriteOptions().DisableWal(1);
        var fo = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        Run run;
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(options, path))
        {
            run = Drive(
            (k, v) => db.Put(k, v, null, wo),
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
            steps, putsPerStep, keySpace, seed);
        }
        ReadLog(run, path);
        return run;
    }

    private static Run RunManaged(string path, int steps, int putsPerStep, ulong keySpace, ulong seed)
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
        var wo = new WriteOptions { DisableWal = true };
        Run run;
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, MaxBackgroundJobs = 1 }, cf))
        {
            run = Drive(
            (k, v) => db.Put(wo, k, v),
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
            steps, putsPerStep, keySpace, seed);
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
}
