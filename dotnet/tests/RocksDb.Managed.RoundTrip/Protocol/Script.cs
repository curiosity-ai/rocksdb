using System.Globalization;
using System.Text;

namespace RocksDb.Managed.RoundTrip.Protocol;

/// <summary>Operations of an op script. Data ops change the DB; the rest drive or verify the executing engine.</summary>
public enum OpKind
{
    // data (single writes; inside BatchBegin/BatchEnd they go into one WriteBatch)
    Put, Merge, Delete, SingleDelete, DeleteRange,
    /// <summary>Starts a write batch; <see cref="Op.N"/> = 1 for a synced write.</summary>
    BatchBegin, BatchEnd,
    /// <summary>Subsequent single writes use sync=true (<see cref="Op.N"/> = 1) or false.</summary>
    Sync,
    Flush,
    /// <summary>Full-range manual compaction of <see cref="Op.Cf"/>.</summary>
    Compact,
    CreateCf, DropCf,
    /// <summary>Starts an external SST file (<see cref="Op.Arg"/> = path) using <see cref="Op.Cf"/>'s options.</summary>
    SstBegin, SstPut, SstMerge, SstDelete, SstDeleteRange, SstEnd,
    /// <summary>Ingests the files in <see cref="Op.Arg"/> ('|'-separated) into <see cref="Op.Cf"/>; N = 1 moves them.</summary>
    Ingest,
    /// <summary>Creates a checkpoint in directory <see cref="Op.Arg"/>.</summary>
    Checkpoint,
    /// <summary>Adds a backup of the DB to the BackupEngine directory <see cref="Op.Arg"/>; N = 1 flushes first.</summary>
    Backup,
    /// <summary>Restores the latest backup of <see cref="Op.Arg"/> = "backupDir|targetDir" into targetDir (the open DB is untouched).</summary>
    Restore,
    /// <summary>Takes snapshot number <see cref="Op.N"/>.</summary>
    Snap,
    /// <summary>Verifies the DB as of snapshot <see cref="Op.N"/> against the expected-contents file <see cref="Op.Arg"/>.</summary>
    SnapVerify,
    SnapRelease,
    /// <summary>Verifies the live DB against the expected-contents file <see cref="Op.Arg"/> (scan + point gets).</summary>
    Verify,
    /// <summary>Writes all column families in canonical form to <see cref="Op.Arg"/>.</summary>
    Dump,
    /// <summary>Prints "ACK n" to stdout (and flushes) so the parent can observe progress.</summary>
    Ack,
    /// <summary>Environment.FailFast (SIGABRT on Linux) -- harness self-test.</summary>
    Crash,
    /// <summary>Blocks forever -- harness self-test of the timeout.</summary>
    Hang,
    /// <summary>Closes and re-opens the DB.</summary>
    Reopen,
    /// <summary>
    /// Writes <see cref="Op.N"/> crash-test batches (see <see cref="CrashWorkload"/>), synced every
    /// <see cref="Op.Arg"/> batches, printing "ACK i" after each synced batch i. The parent kills the process mid-stream.
    /// </summary>
    CrashWriter,
    /// <summary>Checks a recovered crash-test DB: at least <see cref="Op.N"/> batches, prefix-consistent; prints "RECOVERED n".</summary>
    CrashCheck,
    /// <summary>Prints "PROP cf name value" lines for the per-level file counts of <see cref="Op.Cf"/>.</summary>
    Props,
    /// <summary>FlushWAL(sync = <see cref="Op.N"/> != 0).</summary>
    FlushWal,
    /// <summary>Subsequent single writes and batches use disableWAL (<see cref="Op.N"/> = 1) or not.</summary>
    NoWal,
    /// <summary>Flushes every column family in one call (rocksdb_flush_cfs / FlushAll; atomic with atomic_flush).</summary>
    FlushAll,
    /// <summary>CompactRange of <see cref="Op.Cf"/> over [Key, Value) (null = open) with the options in <see cref="Op.Arg"/>
    /// (<see cref="CompactSpec"/>).</summary>
    CompactEx,
    /// <summary>DeleteFilesInRange(Key, Value) on <see cref="Op.Cf"/>; the model is unchanged (the generator follows it
    /// with a DeleteRange over the same range, which makes the result deterministic).</summary>
    DeleteFiles,
    /// <summary>Begins transaction <see cref="Op.Arg"/>; data ops up to the TxnPrepare go into it.</summary>
    TxnBegin,
    /// <summary>Names the transaction (2PC) and prepares it (sync write).</summary>
    TxnPrepare,
    /// <summary>Commits the prepared (possibly recovered) transaction <see cref="Op.Arg"/>.</summary>
    TxnCommit,
    /// <summary>Rolls back the prepared (possibly recovered) transaction <see cref="Op.Arg"/>.</summary>
    TxnRollback,
    /// <summary>Checks that the prepared transactions are exactly the '|'-separated names in <see cref="Op.Arg"/>.</summary>
    TxnCheckPrepared,
    /// <summary>Ends the child process abruptly (SIGKILL itself) with the DB open: no close, no flush, no destructors.
    /// Data that reached the OS survives, as after a process crash. Child runs only.</summary>
    Abandon,
}

/// <summary>Flags of <see cref="OpKind.Ingest"/> (<see cref="Op.N"/>).</summary>
[Flags]
public enum IngestFlags
{
    None = 0,
    Move = 1,
    /// <summary>ingest_behind (needs allow_ingest_behind); files must only hold keys the CF never had.</summary>
    IngestBehind = 2,
    /// <summary>allow_global_seqno = false (files must not overlap existing data).</summary>
    NoGlobalSeqno = 4,
}

/// <summary>Options of <see cref="OpKind.CompactEx"/>, serialized in <see cref="Op.Arg"/> as "b,e,c,t,s"
/// (bottommost_level_compaction, exclusive, change_level, target_level, max_subcompactions).</summary>
public readonly record struct CompactSpec(BottommostLevelCompaction Bottommost, bool Exclusive, bool ChangeLevel, int TargetLevel, uint MaxSubcompactions)
{
    public string Encode() => string.Join(',', (int)Bottommost, Exclusive ? 1 : 0, ChangeLevel ? 1 : 0, TargetLevel, MaxSubcompactions);

    public static CompactSpec Decode(string s)
    {
        string[] f = s.Split(',');
        return new CompactSpec((BottommostLevelCompaction)int.Parse(f[0], CultureInfo.InvariantCulture), f[1] == "1", f[2] == "1",
            int.Parse(f[3], CultureInfo.InvariantCulture), uint.Parse(f[4], CultureInfo.InvariantCulture));
    }
}

/// <summary>One script line. Serialized as tab-separated fields; byte strings are hex, "~" = empty, "." = absent.</summary>
public sealed record Op(OpKind Kind, string? Cf, byte[]? Key, byte[]? Value, string? Arg, long N)
{
    public static Op Put(string cf, byte[] k, byte[] v) => new(OpKind.Put, cf, k, v, null, 0);
    public static Op Merge(string cf, byte[] k, byte[] v) => new(OpKind.Merge, cf, k, v, null, 0);
    public static Op Delete(string cf, byte[] k) => new(OpKind.Delete, cf, k, null, null, 0);
    public static Op SingleDelete(string cf, byte[] k) => new(OpKind.SingleDelete, cf, k, null, null, 0);
    public static Op DeleteRange(string cf, byte[] begin, byte[] end) => new(OpKind.DeleteRange, cf, begin, end, null, 0);
    public static Op BatchBegin(bool sync) => new(OpKind.BatchBegin, null, null, null, null, sync ? 1 : 0);
    public static Op BatchEnd() => new(OpKind.BatchEnd, null, null, null, null, 0);
    public static Op Sync(bool on) => new(OpKind.Sync, null, null, null, null, on ? 1 : 0);
    public static Op Flush(string cf) => new(OpKind.Flush, cf, null, null, null, 0);
    public static Op Compact(string cf) => new(OpKind.Compact, cf, null, null, null, 0);
    public static Op CreateCf(string cf) => new(OpKind.CreateCf, cf, null, null, null, 0);
    public static Op DropCf(string cf) => new(OpKind.DropCf, cf, null, null, null, 0);
    public static Op SstBegin(string cf, string path) => new(OpKind.SstBegin, cf, null, null, path, 0);
    public static Op SstPut(byte[] k, byte[] v) => new(OpKind.SstPut, null, k, v, null, 0);
    public static Op SstMerge(byte[] k, byte[] v) => new(OpKind.SstMerge, null, k, v, null, 0);
    public static Op SstDelete(byte[] k) => new(OpKind.SstDelete, null, k, null, null, 0);
    public static Op SstDeleteRange(byte[] begin, byte[] end) => new(OpKind.SstDeleteRange, null, begin, end, null, 0);
    public static Op SstEnd() => new(OpKind.SstEnd, null, null, null, null, 0);
    public static Op Ingest(string cf, IEnumerable<string> files, bool move) => new(OpKind.Ingest, cf, null, null, string.Join('|', files), move ? 1 : 0);
    public static Op Checkpoint(string dir) => new(OpKind.Checkpoint, null, null, null, dir, 0);
    public static Op Backup(string backupDir, bool flush) => new(OpKind.Backup, null, null, null, backupDir, flush ? 1 : 0);
    public static Op Restore(string backupDir, string targetDir) => new(OpKind.Restore, null, null, null, backupDir + "|" + targetDir, 0);
    public static Op Snap(long id) => new(OpKind.Snap, null, null, null, null, id);
    public static Op SnapVerify(long id, string expectedFile) => new(OpKind.SnapVerify, null, null, null, expectedFile, id);
    public static Op SnapRelease(long id) => new(OpKind.SnapRelease, null, null, null, null, id);
    public static Op Verify(string expectedFile) => new(OpKind.Verify, null, null, null, expectedFile, 0);
    public static Op Dump(string file) => new(OpKind.Dump, null, null, null, file, 0);
    public static Op Ack(long n) => new(OpKind.Ack, null, null, null, null, n);
    public static Op Crash() => new(OpKind.Crash, null, null, null, null, 0);
    public static Op Hang() => new(OpKind.Hang, null, null, null, null, 0);
    public static Op Reopen() => new(OpKind.Reopen, null, null, null, null, 0);
    public static Op CrashWriter(long batches, int syncEvery) =>
        new(OpKind.CrashWriter, null, null, null, syncEvery.ToString(CultureInfo.InvariantCulture), batches);
    /// <summary>Crash writer with <see cref="Engines.CrashWorkload.Write"/> flags ('n', 't', 'w').</summary>
    public static Op CrashWriter(long batches, int syncEvery, string flags) =>
        new(OpKind.CrashWriter, null, null, null, syncEvery.ToString(CultureInfo.InvariantCulture) + ":" + flags, batches);
    public static Op CrashCheck(long minBatches) => new(OpKind.CrashCheck, null, null, null, null, minBatches);
    public static Op Props(string cf) => new(OpKind.Props, cf, null, null, null, 0);
    public static Op FlushWal(bool sync) => new(OpKind.FlushWal, null, null, null, null, sync ? 1 : 0);
    public static Op NoWal(bool on) => new(OpKind.NoWal, null, null, null, null, on ? 1 : 0);
    public static Op FlushAll() => new(OpKind.FlushAll, null, null, null, null, 0);
    public static Op CompactEx(string cf, byte[]? begin, byte[]? end, CompactSpec spec) => new(OpKind.CompactEx, cf, begin, end, spec.Encode(), 0);
    public static Op DeleteFiles(string cf, byte[] begin, byte[] end) => new(OpKind.DeleteFiles, cf, begin, end, null, 0);
    public static Op Ingest(string cf, IEnumerable<string> files, IngestFlags flags) => new(OpKind.Ingest, cf, null, null, string.Join('|', files), (long)flags);
    public static Op TxnBegin(string name) => new(OpKind.TxnBegin, null, null, null, name, 0);
    public static Op TxnPrepare(string name) => new(OpKind.TxnPrepare, null, null, null, name, 0);
    public static Op TxnCommit(string name) => new(OpKind.TxnCommit, null, null, null, name, 0);
    public static Op TxnRollback(string name) => new(OpKind.TxnRollback, null, null, null, name, 0);
    public static Op TxnCheckPrepared(IEnumerable<string> names) => new(OpKind.TxnCheckPrepared, null, null, null, "|" + string.Join('|', names), 0);
    public static Op Abandon() => new(OpKind.Abandon, null, null, null, null, 0);

    public bool IsDataOp => Kind is OpKind.Put or OpKind.Merge or OpKind.Delete or OpKind.SingleDelete or OpKind.DeleteRange;

    public override string ToString() => Script.Format(this);
}

/// <summary>Reads and writes op scripts (one <see cref="Op"/> per line).</summary>
public static class Script
{
    public static string Format(Op op)
    {
        var sb = new StringBuilder();
        sb.Append(op.Kind.ToString()).Append('\t')
          .Append(op.Cf ?? ".").Append('\t')
          .Append(Hex.EncodeOrNull(op.Key)).Append('\t')
          .Append(Hex.EncodeOrNull(op.Value)).Append('\t')
          .Append(op.Arg ?? ".").Append('\t')
          .Append(op.N.ToString(CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    public static Op Parse(string line)
    {
        string[] f = line.Split('\t');
        if (f.Length != 6) throw new InvalidDataException("bad script line: " + line);
        return new Op(
            Enum.Parse<OpKind>(f[0]),
            f[1] == "." ? null : f[1],
            Hex.DecodeOrNull(f[2]),
            Hex.DecodeOrNull(f[3]),
            f[4] == "." ? null : f[4],
            long.Parse(f[5], CultureInfo.InvariantCulture));
    }

    public static void Write(string path, IEnumerable<Op> ops)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false));
        foreach (Op op in ops) w.WriteLine(Format(op));
    }

    public static IEnumerable<Op> Read(string path)
    {
        foreach (string line in File.ReadLines(path))
        {
            if (line.Length == 0 || line[0] == '#') continue;
            yield return Parse(line);
        }
    }
}

public static class Hex
{
    public static string Encode(ReadOnlySpan<byte> b) => b.Length == 0 ? "~" : Convert.ToHexString(b);

    public static byte[] Decode(string s) => s == "~" ? [] : Convert.FromHexString(s);

    public static string EncodeOrNull(byte[]? b) => b is null ? "." : Encode(b);

    public static byte[]? DecodeOrNull(string s) => s == "." ? null : Decode(s);

    /// <summary>Short human-readable form for diagnostics: printable ASCII or hex, truncated.</summary>
    public static string Show(ReadOnlySpan<byte> b)
    {
        bool printable = true;
        foreach (byte c in b)
        {
            if (c < 0x20 || c > 0x7e) { printable = false; break; }
        }
        string s = printable ? "\"" + Encoding.ASCII.GetString(b[..Math.Min(b.Length, 48)]) + "\"" : "0x" + Convert.ToHexString(b[..Math.Min(b.Length, 24)]);
        return b.Length > (printable ? 48 : 24) ? s + "...(" + b.Length + " bytes)" : s;
    }
}
