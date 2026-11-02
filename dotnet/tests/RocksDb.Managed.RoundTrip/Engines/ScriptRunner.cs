using RocksDb.Managed.Compression;
using System.Globalization;
using System.Text;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary>A verification command found the DB different from the expected contents.</summary>
public sealed class VerificationException(string message) : Exception(message);

/// <summary>
/// Executes an op script against an <see cref="IEngine"/>. The same code runs in the child process (native or
/// managed engine) and in-process (managed engine), so both sides of a round trip do exactly the same thing.
/// Progress lines ("ACK n", "PROP ...", "RECOVERED n", "SKIP ...") go to <paramref name="output"/>.
/// </summary>
public sealed class ScriptRunner(IEngine engine, string dbPath, DbSpec spec, TextWriter output)
{
    private readonly List<Op> _batch = [];
    private bool _inBatch;
    private bool _batchSync;
    private bool _sync;
    private List<Op>? _sst;
    private string? _sstCf, _sstPath;

    public List<string> Skipped { get; } = [];

    public void Run(IEnumerable<Op> ops)
    {
        int line = 0;
        foreach (Op op in ops)
        {
            line++;
            try
            {
                Execute(op);
            }
            catch (VerificationException)
            {
                throw;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                throw new InvalidOperationException($"{engine.Kind} engine failed at script op #{line} ({Describe(op)}): {ex.GetType().Name}: {ex.Message}", ex);
            }
        }
    }

    private static string Describe(Op op) =>
        op.Kind + (op.Cf is null ? "" : " cf=" + op.Cf) + (op.Key is null ? "" : " key=" + Hex.Show(op.Key)) + (op.Arg is null ? "" : " arg=" + op.Arg);

    private void Execute(Op op)
    {
        if (_sst is not null && op.Kind is OpKind.SstPut or OpKind.SstMerge or OpKind.SstDelete or OpKind.SstDeleteRange)
        {
            _sst.Add(op);
            return;
        }
        if (op.IsDataOp)
        {
            if (_inBatch) _batch.Add(op);
            else engine.Write([op], _sync);
            return;
        }
        switch (op.Kind)
        {
            case OpKind.BatchBegin:
                _inBatch = true;
                _batchSync = op.N != 0;
                _batch.Clear();
                break;
            case OpKind.BatchEnd:
                engine.Write(_batch, _batchSync);
                _inBatch = false;
                _batch.Clear();
                break;
            case OpKind.Sync: _sync = op.N != 0; break;
            case OpKind.Flush: engine.Flush(op.Cf!); break;
            case OpKind.Compact:
                if (!engine.CompactRange(op.Cf!))
                {
                    string msg = $"SKIP compact {op.Cf}: CompactRange is not available in the {engine.Kind} engine";
                    Skipped.Add(msg);
                    output.WriteLine(msg);
                }
                break;
            case OpKind.CreateCf: engine.CreateColumnFamily(op.Cf!); break;
            case OpKind.DropCf: engine.DropColumnFamily(op.Cf!); break;
            case OpKind.SstBegin:
                _sst = [];
                _sstCf = op.Cf;
                _sstPath = op.Arg;
                break;
            case OpKind.SstEnd:
                engine.WriteSst(_sstCf!, _sstPath!, _sst!);
                _sst = null;
                break;
            case OpKind.Ingest: engine.Ingest(op.Cf!, op.Arg!.Split('|'), op.N != 0); break;
            case OpKind.Checkpoint: engine.Checkpoint(op.Arg!); break;
            case OpKind.Snap: engine.TakeSnapshot(op.N); break;
            case OpKind.SnapRelease: engine.ReleaseSnapshot(op.N); break;
            case OpKind.SnapVerify: Verify(Contents.Read(op.Arg!), op.N, "snapshot " + op.N); break;
            case OpKind.Verify: Verify(Contents.Read(op.Arg!), null, Path.GetFileName(op.Arg!)); break;
            case OpKind.Dump: ReadAll(engine, null).Write(op.Arg!); break;
            case OpKind.Ack:
                output.WriteLine("ACK " + op.N.ToString(CultureInfo.InvariantCulture));
                output.Flush();
                break;
            case OpKind.Crash:
                output.Flush();
                Environment.FailFast("round-trip child: crash op (harness self-test)");
                break;
            case OpKind.Hang:
                output.WriteLine("HANGING");
                output.Flush();
                Thread.Sleep(Timeout.Infinite);
                break;
            case OpKind.Reopen:
                engine.Close();
                engine.Open(dbPath, spec);
                break;
            case OpKind.CrashWriter: CrashWorkload.Write(engine, op.N, int.Parse(op.Arg!, CultureInfo.InvariantCulture), output); break;
            case OpKind.CrashCheck:
                long n = CrashWorkload.Check(engine, op.N);
                output.WriteLine("RECOVERED " + n.ToString(CultureInfo.InvariantCulture));
                output.Flush();
                break;
            case OpKind.Props:
                int[] files = engine.FilesPerLevel(op.Cf!);
                for (int level = 0; level < files.Length; level++)
                    output.WriteLine($"PROP {op.Cf} files-at-level{level} {files[level]}");
                output.Flush();
                break;
            default:
                throw new InvalidOperationException("unexpected op " + op.Kind);
        }
    }

    /// <summary>Reads every column family into canonical <see cref="Contents"/>.</summary>
    public static Contents ReadAll(IEngine engine, long? snapshot) => ReadAll(engine, snapshot, engine.ColumnFamilyNames);

    public static Contents ReadAll(IEngine engine, long? snapshot, IEnumerable<string> columnFamilies)
    {
        var c = new Contents();
        foreach (string cf in columnFamilies)
        {
            var cc = new CfContents();
            byte[]? prev = null;
            foreach (KeyValuePair<byte[], byte[]> kv in engine.Scan(cf, snapshot))
            {
                if (prev is not null && ByteComparer.Instance.Compare(prev, kv.Key) >= 0)
                    throw new VerificationException($"{engine.Kind}: [{cf}] iterator out of order: {Hex.Show(prev)} then {Hex.Show(kv.Key)}");
                cc.Live.Add(kv.Key, kv.Value);
                prev = kv.Key;
            }
            c.ColumnFamilies.Add(cf, cc);
        }
        return c;
    }

    /// <summary>Full scan of every CF compared with <paramref name="expected"/>, plus a point lookup of every expected
    /// live key and every expected-absent key.</summary>
    private void Verify(Contents expected, long? snapshot, string what)
    {
        // Snapshot reads only cover the column families the expected file lists (CFs may have been created or
        // dropped since the snapshot was taken).
        Contents actual = snapshot is null ? ReadAll(engine, null) : ReadAll(engine, snapshot, expected.ColumnFamilies.Keys);
        var problems = Contents.Diff(expected, actual, 40);
        foreach (KeyValuePair<string, CfContents> cf in expected.ColumnFamilies)
        {
            if (!actual.ColumnFamilies.ContainsKey(cf.Key)) continue;
            foreach (KeyValuePair<byte[], byte[]> kv in cf.Value.Live)
            {
                if (problems.Count >= 60) break;
                byte[]? v = engine.Get(cf.Key, kv.Key, snapshot);
                if (v is null) problems.Add($"[{cf.Key}] Get({Hex.Show(kv.Key)}) = NotFound, expected {Hex.Show(kv.Value)}");
                else if (!v.AsSpan().SequenceEqual(kv.Value)) problems.Add($"[{cf.Key}] Get({Hex.Show(kv.Key)}) = {Hex.Show(v)}, expected {Hex.Show(kv.Value)}");
            }
            foreach (byte[] k in cf.Value.Absent)
            {
                if (problems.Count >= 60) break;
                byte[]? v = engine.Get(cf.Key, k, snapshot);
                if (v is not null) problems.Add($"[{cf.Key}] Get({Hex.Show(k)}) of a deleted key = {Hex.Show(v)}");
            }
        }
        if (problems.Count > 0)
        {
            var sb = new StringBuilder();
            sb.Append(engine.Kind).Append(" engine: verification of ").Append(what).Append(" failed (")
              .Append(expected.LiveCount).Append(" expected live entries, ").Append(actual.LiveCount).Append(" actual):");
            foreach (string p in problems) sb.Append("\n  ").Append(p);
            throw new VerificationException(sb.ToString());
        }
    }
}

/// <summary>
/// The crash-recovery workload: batch i atomically puts <c>crash/i</c> into default and blobs, adds 1 to the
/// <c>count</c> counter (uint64add merge), and moves the single <c>tmp/</c> key forward. Any prefix of the batches is
/// a consistent recovered state; nothing else is.
/// </summary>
public static class CrashWorkload
{
    public static DbSpec Spec(ulong writeBufferSize)
    {
        var spec = new DbSpec();
        spec.ColumnFamilies.Add(new CfSpec { Name = "default", Compression = CompressionType.LZ4, WriteBufferSize = writeBufferSize, Merge = MergeKind.StringAppend });
        spec.ColumnFamilies.Add(new CfSpec { Name = "counters", Compression = CompressionType.Zstd, FormatVersion = 5, WriteBufferSize = writeBufferSize, Merge = MergeKind.UInt64Add });
        spec.ColumnFamilies.Add(new CfSpec { Name = "blobs", Compression = CompressionType.None, Filter = FilterKind.Ribbon, WriteBufferSize = writeBufferSize, Merge = MergeKind.None });
        return spec;
    }

    private static byte[] Key(string prefix, long i) => Encoding.ASCII.GetBytes(prefix + i.ToString("D9", CultureInfo.InvariantCulture));

    public static byte[] Value(long i, int size)
    {
        byte[] v = new byte[size];
        for (int j = 0; j < v.Length; j++) v[j] = (byte)(i * 31 + j * 7);
        return v;
    }

    private static int SmallSize(long i) => (int)(i % 97) * 3;
    private static int BigSize(long i) => 500 + (int)(i % 13) * 700;

    public static void Write(IEngine engine, long batches, int syncEvery, TextWriter output)
    {
        byte[] one = ReferenceModel.U64(1);
        for (long i = 0; i < batches; i++)
        {
            bool sync = (i + 1) % syncEvery == 0;
            var ops = new List<Op>
            {
                Op.Put("default", Key("crash/", i), Value(i, SmallSize(i))),
                Op.Put("blobs", Key("crash/", i), Value(i, BigSize(i))),
                Op.Merge("counters", "count"u8.ToArray(), one),
                Op.Put("default", Key("tmp/", i), Value(i, 5)),
            };
            if (i > 0) ops.Add(Op.Delete("default", Key("tmp/", i - 1)));
            engine.Write(ops, sync);
            if (sync)
            {
                // Batch i and everything before it is durable now.
                output.WriteLine("ACK " + (i + 1).ToString(CultureInfo.InvariantCulture));
                output.Flush();
            }
        }
        output.WriteLine("DONE");
        output.Flush();
    }

    /// <summary>Returns the number of recovered batches after checking they form a consistent prefix of at least
    /// <paramref name="minBatches"/> batches.</summary>
    public static long Check(IEngine engine, long minBatches)
    {
        Contents c = ScriptRunner.ReadAll(engine, null);
        CfContents def = c["default"], blobs = c["blobs"], counters = c["counters"];
        var problems = new List<string>();
        long n = 0;
        var tmpKeys = new List<byte[]>();
        foreach (KeyValuePair<byte[], byte[]> kv in def.Live)
        {
            string k = Encoding.ASCII.GetString(kv.Key);
            if (k.StartsWith("crash/", StringComparison.Ordinal))
            {
                long i = long.Parse(k.AsSpan(6), CultureInfo.InvariantCulture);
                if (i != n) problems.Add($"default: hole/out-of-order key {k}, expected crash/{n:D9}");
                if (!kv.Value.AsSpan().SequenceEqual(Value(i, SmallSize(i)))) problems.Add($"default: wrong value for {k}");
                n = i + 1;
            }
            else if (k.StartsWith("tmp/", StringComparison.Ordinal)) tmpKeys.Add(kv.Key);
            else problems.Add("default: unexpected key " + k);
        }
        if (blobs.Live.Count != n) problems.Add($"blobs has {blobs.Live.Count} entries but default has {n} batches (batch not atomic)");
        foreach (KeyValuePair<byte[], byte[]> kv in blobs.Live)
        {
            string k = Encoding.ASCII.GetString(kv.Key);
            long i = long.Parse(k.AsSpan(6), CultureInfo.InvariantCulture);
            if (i >= n) problems.Add($"blobs: key {k} beyond recovered prefix {n}");
            else if (!kv.Value.AsSpan().SequenceEqual(Value(i, BigSize(i)))) problems.Add($"blobs: wrong value for {k}");
        }
        ulong count = counters.Live.TryGetValue("count"u8.ToArray(), out byte[]? cv) && cv.Length == 8
            ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(cv) : 0;
        if ((long)count != n) problems.Add($"counters/count = {count} but {n} batches recovered (merge not atomic with batch)");
        if (n == 0 ? tmpKeys.Count != 0 : tmpKeys.Count != 1 || !tmpKeys[0].AsSpan().SequenceEqual(Key("tmp/", n - 1)))
            problems.Add($"tmp keys inconsistent: {string.Join(",", tmpKeys.ConvertAll(k => Encoding.ASCII.GetString(k)))} for {n} batches");
        if (n < minBatches) problems.Add($"only {n} batches recovered but {minBatches} were acknowledged as synced");
        if (problems.Count > 0)
            throw new VerificationException($"{engine.Kind} engine: crash recovery check failed ({n} batches recovered):\n  " + string.Join("\n  ", problems.Take(30)));
        return n;
    }
}
