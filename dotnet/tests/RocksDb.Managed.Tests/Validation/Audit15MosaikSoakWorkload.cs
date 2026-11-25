using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.TestShared;

/// <summary>
/// Audit #15 soak: what the tests check after a run (shared by the unit and compat tests; the workload is
/// <see cref="MosaikSoak"/>, the model <see cref="SoakModel"/>, the checks <see cref="SoakVerifier"/>).
/// </summary>
internal static class SoakChecks
{
    public static MosaikProfile Profile(MosaikSoak soak, string walDir) =>
        new(walDir, soak.Settings.LowMemoryMode, soak.Settings.ProcessorCount, soak.Settings.FileSystem, soak.Settings.CacheBytes, soak.Settings.WriteBufferBytes);

    /// <summary>The run itself: no violation, no hang, no failed close.</summary>
    public static string RunFailures(MosaikSoak soak) =>
        (soak.Hung ? "HUNG" + Environment.NewLine : "") + string.Join(Environment.NewLine + "---" + Environment.NewLine, soak.Violations);

    /// <summary>Reopens the database with the same profile and compares every family with the model.</summary>
    public static SoakDump ReopenAndCheck(MosaikSoak soak, List<string> errors, List<string> notes)
    {
        using RocksDatabase db = Profile(soak, soak.WalDir).Open(soak.DbPath);
        SoakDump d = SoakDump.Of(db);
        SoakVerifier.CheckFinal(soak.M, d, "reopened database", errors, out bool latest);
        notes.Add("reopened database: " + d.Count + " entries; the graphsettings version key " + (latest ? "holds" : "does NOT hold") + " the last issued version " + soak.M.TrackerIssued);
        // SafeRocksDb.Open on a primary then empties the change tracker.
        MosaikSoak.MosaikOpenTrackerCleanup(db);
        using Iterator it = db.GetColumnFamily(MosaikProfile.ReplicaChangeTrackerFamily).NewIterator();
        it.SeekToFirst();
        if (it.Valid) errors.Add("reopened database: the change tracker still holds " + Convert.ToHexString(it.Key) + " after Mosaik's open-time cleanup");
        return d;
    }

    /// <summary>
    /// Opens every checkpoint and every backup still kept (PurgeOldBackups(2)) and checks that each is a point in time.
    /// <paramref name="alsoCheck"/> gets an untouched copy (database dir, WAL dir) of each, the record and the managed dump.
    /// </summary>
    public static void CheckSnapshots(MosaikSoak soak, List<string> errors, List<string> notes, Action<string, string, SoakSnapshotRecord, SoakDump>? alsoCheck)
    {
        List<SoakSnapshotRecord> records;
        lock (soak.Snapshots) records = [.. soak.Snapshots];
        List<BackupInfo> kept = [];
        if (records.Exists(r => r.Kind == "backup"))
        {
            using BackupEngine engine = BackupEngine.Open(new BackupEngineOptions(soak.BackupDir), soak.Fs);
            kept = engine.GetBackupInfo();
            foreach (BackupInfo info in kept) engine.VerifyBackup(info.BackupId, verifyWithChecksum: true);
        }
        foreach (SoakSnapshotRecord rec in records)
        {
            string db, wal, copyDb = "", copyWal = "";
            if (rec.Kind == "checkpoint")
            {
                db = wal = rec.Path;
                if (alsoCheck is not null) CopyDirectory(rec.Path, copyDb = copyWal = rec.Path + "-copy");
            }
            else
            {
                if (!kept.Exists(i => i.BackupId == rec.BackupId))
                {
                    notes.Add("backup " + rec.BackupId + " was purged");
                    continue;
                }
                string restore = Path.Combine(soak.Settings.Root, "restore-" + rec.BackupId);
                db = Path.Combine(restore, "db");
                wal = Path.Combine(restore, "wal");
                using (BackupEngine engine = BackupEngine.Open(new BackupEngineOptions(soak.BackupDir), soak.Fs))
                {
                    engine.RestoreDbFromBackup(rec.BackupId, db, wal, new RestoreOptions());
                    if (alsoCheck is not null) engine.RestoreDbFromBackup(rec.BackupId, copyDb = restore + "-copy/db", copyWal = restore + "-copy/wal", new RestoreOptions());
                }
            }
            string what = rec.Kind + " " + (rec.Kind == "backup" ? rec.BackupId.ToString() : Path.GetFileName(rec.Path));
            SoakDump d;
            using (RocksDatabase opened = Profile(soak, wal).Open(db)) d = SoakDump.Of(opened);
            notes.Add(SoakVerifier.CheckCut(soak.M, d, rec, what, errors) + " (" + d.Count + " entries)");
            alsoCheck?.Invoke(copyDb, copyWal, rec, d);
        }
    }

    public static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from)) File.Copy(f, Path.Combine(to, Path.GetFileName(f)));
        foreach (string d in Directory.GetDirectories(from)) CopyDirectory(d, Path.Combine(to, Path.GetFileName(d)));
    }
}

/// <summary>
/// Audit #15 soak: a self-describing value. Every value the soak writes (except the families where Mosaik writes empty
/// values and the two raw 8-byte counters) starts with a header naming the slot it was written to, the slot version, the
/// writing worker and its operation number; the rest is a payload generated from those fields, so a reader can tell a
/// torn value (payload mismatch), a foreign value (another slot) and a stale or future one (version out of its window).
/// </summary>
internal static class SoakValue
{
    public const int HeaderSize = 19;
    private const ushort Magic = 0x5AA1;

    [ThreadStatic] private static byte[]? t_scratch;

    public static void Encode(Span<byte> dest, int slot, int version, int writer, int op)
    {
        if (dest.Length < HeaderSize) throw new ArgumentOutOfRangeException(nameof(dest));
        BinaryPrimitives.WriteUInt16LittleEndian(dest, Magic);
        BinaryPrimitives.WriteInt32LittleEndian(dest[2..], slot);
        BinaryPrimitives.WriteInt32LittleEndian(dest[6..], version);
        dest[10] = (byte)writer;
        BinaryPrimitives.WriteInt32LittleEndian(dest[11..], op);
        BinaryPrimitives.WriteInt32LittleEndian(dest[15..], dest.Length);
        FillPayload(dest[HeaderSize..], Seed(slot, version, op));
    }

    public static bool TryParse(ReadOnlySpan<byte> v, out int slot, out int version, out int writer, out int op)
    {
        slot = version = writer = op = 0;
        if (v.Length < HeaderSize || BinaryPrimitives.ReadUInt16LittleEndian(v) != Magic) return false;
        if (BinaryPrimitives.ReadInt32LittleEndian(v[15..]) != v.Length) return false;
        slot = BinaryPrimitives.ReadInt32LittleEndian(v[2..]);
        version = BinaryPrimitives.ReadInt32LittleEndian(v[6..]);
        writer = v[10];
        op = BinaryPrimitives.ReadInt32LittleEndian(v[11..]);
        return true;
    }

    /// <summary>Parses the header and checks the payload; null when the value is intact, else what is wrong.</summary>
    public static string? Verify(ReadOnlySpan<byte> v, out int slot, out int version, out int writer, out int op)
    {
        if (!TryParse(v, out slot, out version, out writer, out op)) return "unparsable header (len " + v.Length + ")";
        int n = v.Length - HeaderSize;
        byte[] scratch = t_scratch is { } s && s.Length >= n ? s : (t_scratch = new byte[Math.Max(n, 4096)]);
        FillPayload(scratch.AsSpan(0, n), Seed(slot, version, op));
        if (!v[HeaderSize..].SequenceEqual(scratch.AsSpan(0, n))) return "payload mismatch (torn value) slot " + slot + " v" + version + " op" + op;
        return null;
    }

    public static ulong Mix(ulong x)
    {
        x += 0x9E3779B97F4A7C15UL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9UL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBUL;
        return x ^ (x >> 31);
    }

    private static ulong Seed(int slot, int version, int op) =>
        Mix(((ulong)(uint)slot << 32) ^ (uint)version ^ ((ulong)(uint)op << 17) ^ 0xA15A15UL);

    /// <summary>Pseudo-random words, some of them repeated so that the compressed families have something to compress.</summary>
    private static void FillPayload(Span<byte> dest, ulong state)
    {
        int i = 0;
        while (i + 8 <= dest.Length)
        {
            state = Mix(state);
            int repeats = (state & 3) == 0 ? 3 : 1;
            for (int r = 0; r < repeats && i + 8 <= dest.Length; r++, i += 8) BinaryPrimitives.WriteUInt64LittleEndian(dest[i..], state);
        }
        if (i < dest.Length)
        {
            Span<byte> tail = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(tail, Mix(state));
            tail[..(dest.Length - i)].CopyTo(dest[i..]);
        }
    }
}

/// <summary>How a slot's value is made.</summary>
internal enum SlotKind : byte
{
    /// <summary>A <see cref="SoakValue"/>.</summary>
    Codec,
    /// <summary>Always empty (Mosaik's to-index-queue and simple-text entries): presence only.</summary>
    Empty,
    /// <summary>An 8-byte little-endian counter that only grows (Mosaik's tracker acknowledgement, change log reservation).</summary>
    RawCounter,
}

/// <summary>
/// One change of a slot: the operation, the slot version it produced, delete or put, and in <see cref="Raw"/> the value
/// length of a codec put or the value of a raw counter.
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct SlotEvent(int Op, int Version, bool Delete, long Raw);

/// <summary>One database write of a worker (a call that can change the database: Put, Delete, Write, DeleteRange, ...).</summary>
[StructLayout(LayoutKind.Auto)]
internal struct OpRecord
{
    public long Invoke;
    public long Ack;
    public int FirstChange;
    public int ChangeCount;
    public bool Failed;
}

/// <summary>The operation log of one writing worker (append-only, by that worker or under its lock).</summary>
internal sealed class SoakWriterLog
{
    public readonly int Id;
    public readonly string Name;
    public readonly List<OpRecord> Ops = [default]; // op numbers start at 1
    public readonly List<int> ChangeSlots = [];
    private readonly Dictionary<int, int> _pending = [];
    private volatile int _opCount;

    public SoakWriterLog(int id, string name)
    {
        Id = id;
        Name = name;
    }

    /// <summary>Operations begun so far (safe to read from any thread).</summary>
    public int OpCount => _opCount;

    public int CurrentOp { get; private set; }

    public int Begin()
    {
        if (CurrentOp != 0) throw new InvalidOperationException(Name + ": operation " + CurrentOp + " still open");
        int op = Ops.Count;
        Ops.Add(new OpRecord { Invoke = Stopwatch.GetTimestamp(), FirstChange = ChangeSlots.Count });
        _pending.Clear();
        CurrentOp = op;
        _opCount = op;
        return op;
    }

    /// <summary>Records a change of <paramref name="slot"/> in the current operation; a slot changed twice keeps its net change.</summary>
    public int Change(SoakModel m, int slot, bool delete, long raw = 0)
    {
        int op = CurrentOp;
        if (op == 0) throw new InvalidOperationException(Name + ": no open operation");
        if (m.Writer[slot] != Id) throw new InvalidOperationException(Name + " changes slot " + slot + " owned by writer " + m.Writer[slot]);
        int v = ++m.Version[slot];
        List<SlotEvent> h = m.History[slot] ??= [];
        if (_pending.ContainsKey(slot)) h[^1] = new SlotEvent(op, v, delete, raw);
        else
        {
            h.Add(new SlotEvent(op, v, delete, raw));
            _pending[slot] = ChangeSlots.Count;
            ChangeSlots.Add(slot);
        }
        if (delete) Volatile.Write(ref m.LastDelete[slot], v);
        else Volatile.Write(ref m.LastPut[slot], v);
        if (m.Kind[slot] == SlotKind.RawCounter) Volatile.Write(ref m.RawInvoked[slot], raw);
        Volatile.Write(ref m.Invoked[slot], v);
        return v;
    }

    public void Commit(SoakModel m)
    {
        int op = CurrentOp;
        OpRecord r = Ops[op];
        r.ChangeCount = ChangeSlots.Count - r.FirstChange;
        for (int i = r.FirstChange; i < ChangeSlots.Count; i++)
        {
            int slot = ChangeSlots[i];
            SlotEvent e = m.History[slot]![^1];
            if (m.Kind[slot] == SlotKind.RawCounter) Volatile.Write(ref m.RawAcked[slot], e.Raw);
            Volatile.Write(ref m.Acked[slot], ((long)e.Version << 1) | (e.Delete ? 1L : 0L));
        }
        r.Ack = Stopwatch.GetTimestamp();
        Ops[op] = r;
        CurrentOp = 0;
    }

    /// <summary>The operation failed (only expected once the file system was "crashed"): its outcome is unknown.</summary>
    public void Fail()
    {
        int op = CurrentOp;
        if (op == 0) return;
        OpRecord r = Ops[op];
        r.ChangeCount = ChangeSlots.Count - r.FirstChange;
        r.Failed = true;
        Ops[op] = r;
        CurrentOp = 0;
    }
}

/// <summary>A growable array of longs written at distinct indexes by several threads (one writer per index).</summary>
internal sealed class ChunkedLongArray
{
    private const int ChunkBits = 16;
    private readonly long[]?[] _chunks = new long[16384][];
    private readonly Lock _lock = new();

    public long this[long index]
    {
        get
        {
            long[]? c = Volatile.Read(ref _chunks[index >> ChunkBits]);
            return c is null ? 0 : Volatile.Read(ref c[index & ((1 << ChunkBits) - 1)]);
        }
        set
        {
            ref long[]? slot = ref _chunks[index >> ChunkBits];
            long[]? c = Volatile.Read(ref slot);
            if (c is null)
            {
                lock (_lock) c = slot ??= new long[1 << ChunkBits];
            }
            Volatile.Write(ref c[index & ((1 << ChunkBits) - 1)], value);
        }
    }
}

/// <summary>Byte-array keys with span lookups.</summary>
internal sealed class ByteArrayComparer : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public static readonly ByteArrayComparer Instance = new();
    public bool Equals(byte[]? x, byte[]? y) => x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
    public int GetHashCode(byte[] obj) => GetHashCode((ReadOnlySpan<byte>)obj);
    public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
    public int GetHashCode(ReadOnlySpan<byte> alternate)
    {
        var h = new HashCode();
        h.AddBytes(alternate);
        return h.ToHashCode();
    }
    public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
}

/// <summary>
/// The model of the soak: every key the workload may write is a slot owned by exactly one writer (partitioned, so the
/// final state of every family is known exactly), with its history of changes and the four counters readers use to
/// bound what a read may return: the last acknowledged change (taken before the read) and the last invoked change, put
/// and delete (taken after it). The replica change tracker, whose keys are versions handed out across writers, has a
/// model of its own (versions issued, in flight, being removed).
/// </summary>
internal sealed class SoakModel
{
    public static readonly int FamilyCount = MosaikProfile.Families.Count;
    public static readonly int TrackerFamily = IndexOf(MosaikProfile.ReplicaChangeTrackerFamily);
    public static readonly int SettingsFamily = IndexOf(MosaikProfile.GraphConfigFamily);
    public static readonly byte[] VersionKey = "global-primary-to-replica-version"u8.ToArray();
    public static readonly byte[] VersionAckKey = "global-primary-to-replica-version-ack"u8.ToArray();
    public const int TrackerValueLength = 1 + SoakValue.HeaderSize + 16;

    public static int IndexOf(string family)
    {
        for (int i = 0; i < MosaikProfile.Families.Count; i++)
        {
            if (MosaikProfile.Families[i].Name == family) return i;
        }
        throw new ArgumentException(family);
    }

    private readonly List<(int Family, byte[] Key, int Writer, SlotKind Kind)> _pending = [];

    public int SlotCount { get; private set; }
    public int[] Family = [];
    public byte[][] Key = [];
    public byte[] Writer = [];
    public SlotKind[] Kind = [];
    public int[] Version = [];
    public long[] Acked = [], Invoked = [], LastPut = [], LastDelete = [], RawAcked = [], RawInvoked = [];
    public List<SlotEvent>?[] History = [];
    public readonly Dictionary<byte[], int>[] ByKey = new Dictionary<byte[], int>[FamilyCount];
    public readonly int[][] Sorted = new int[FamilyCount][];
    /// <summary>Families whose clear uses DeleteFilesInRange, which may expose older versions (include/rocksdb/convenience.h).</summary>
    public readonly bool[] Relaxed = new bool[FamilyCount];
    public readonly List<SoakWriterLog> Writers = [];

    /// <summary>Odd while a DeleteFilesInRange clear runs; incremented at its start and its end.</summary>
    public int ClearEpoch;

    // ---- replica change tracker ----
    public readonly Lock TrackerLock = new();
    public long TrackerIssued;
    public long[] TrackerInFlight = [];
    /// <summary>Version -> (writer &lt;&lt; 32 | operation of its tracker Put).</summary>
    public readonly ChunkedLongArray TrackerOwner = new();
    /// <summary>Version -> the acknowledgement thread's operation that removed it.</summary>
    public readonly ChunkedLongArray TrackerRemovalOp = new();
    /// <summary>Versions below this may be gone: the acknowledgement thread publishes it before it removes them.</summary>
    public long TrackerRemoving;
    public long TrackerAck;
    /// <summary>The largest version whose Put of the graphsettings version key was acknowledged.</summary>
    public long VersionKeyAckedMax;
    public long VersionKeyRegressionsSeen;

    public SoakWriterLog AddWriter(string name)
    {
        var w = new SoakWriterLog(Writers.Count, name);
        Writers.Add(w);
        return w;
    }

    public int AddSlot(int family, byte[] key, SoakWriterLog writer, SlotKind kind)
    {
        _pending.Add((family, key, writer.Id, kind));
        return _pending.Count - 1;
    }

    public void Freeze()
    {
        int n = _pending.Count;
        SlotCount = n;
        Family = new int[n];
        Key = new byte[n][];
        Writer = new byte[n];
        Kind = new SlotKind[n];
        Version = new int[n];
        Acked = new long[n];
        Invoked = new long[n];
        LastPut = new long[n];
        LastDelete = new long[n];
        RawAcked = new long[n];
        RawInvoked = new long[n];
        History = new List<SlotEvent>?[n];
        var perFamily = new List<int>[FamilyCount];
        for (int f = 0; f < FamilyCount; f++)
        {
            ByKey[f] = new Dictionary<byte[], int>(ByteArrayComparer.Instance);
            perFamily[f] = [];
        }
        for (int i = 0; i < n; i++)
        {
            (int f, byte[] k, int w, SlotKind kind) = _pending[i];
            Family[i] = f;
            Key[i] = k;
            Writer[i] = (byte)w;
            Kind[i] = kind;
            if (!ByKey[f].TryAdd(k, i)) throw new InvalidOperationException("duplicate key in " + MosaikProfile.Families[f].Name);
            perFamily[f].Add(i);
        }
        for (int f = 0; f < FamilyCount; f++)
        {
            List<int> l = perFamily[f];
            l.Sort((a, b) => Key[a].AsSpan().SequenceCompareTo(Key[b]));
            Sorted[f] = l.ToArray();
        }
        TrackerInFlight = new long[Writers.Count];
    }

    public bool TryGetSlot(int family, ReadOnlySpan<byte> key, out int slot) =>
        ByKey[family].GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(key, out slot);

    /// <summary>The slots of a family whose key is in [lower, upper) (bytewise; null = unbounded).</summary>
    public ArraySegment<int> SlotsInRange(int family, ReadOnlySpan<byte> lower, bool hasLower, ReadOnlySpan<byte> upper, bool hasUpper)
    {
        int[] s = Sorted[family];
        int lo = hasLower ? LowerBound(s, lower) : 0;
        int hi = hasUpper ? LowerBound(s, upper) : s.Length;
        return new ArraySegment<int>(s, lo, Math.Max(0, hi - lo));
    }

    private int LowerBound(int[] sorted, ReadOnlySpan<byte> key)
    {
        int lo = 0, hi = sorted.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (Key[sorted[mid]].AsSpan().SequenceCompareTo(key) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /// <summary>The model's final state of a slot: its last change (all operations are acknowledged when the run ends).</summary>
    public SlotEvent? LastEvent(int slot) => History[slot] is { Count: > 0 } h ? h[^1] : null;

    /// <summary>The exact value bytes a put event wrote (a codec event keeps its value length in <see cref="SlotEvent.Raw"/>).</summary>
    public byte[] ValueOf(int slot, SlotEvent e)
    {
        int length = (int)e.Raw;
        switch (Kind[slot])
        {
            case SlotKind.Empty: return [];
            case SlotKind.RawCounter:
            {
                var b = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(b, e.Raw);
                return b;
            }
            default:
            {
                var b = new byte[length];
                SoakValue.Encode(b, slot, e.Version, Writer[slot], e.Op);
                return b;
            }
        }
    }

    /// <summary>The highest version all of whose predecessors are written and acknowledged.</summary>
    public long TrackerCompletedWatermark()
    {
        lock (TrackerLock)
        {
            long c = TrackerIssued;
            foreach (long f in TrackerInFlight)
            {
                if (f != 0 && f - 1 < c) c = f - 1;
            }
            return c;
        }
    }

    public static byte[] TrackerKey(long version)
    {
        var k = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(k, (ulong)version);
        return k;
    }

    public static byte TrackerChangeType(int writer) => (byte)(1 + writer % 6);

    public static void EncodeTrackerValue(Span<byte> dest, long version, int writer, int op)
    {
        dest[0] = TrackerChangeType(writer);
        SoakValue.Encode(dest[1..], -1, checked((int)version), writer, op);
    }

    /// <summary>Checks a tracker entry: its change type, codec and version; null when intact.</summary>
    public static string? VerifyTrackerValue(long version, ReadOnlySpan<byte> v, out int writer, out int op)
    {
        writer = op = 0;
        if (v.Length != TrackerValueLength) return "tracker value of " + v.Length + " bytes";
        string? e = SoakValue.Verify(v[1..], out int slot, out int ver, out writer, out op);
        if (e is not null) return "tracker v" + version + ": " + e;
        if (slot != -1 || ver != version) return "tracker key v" + version + " holds the value of v" + ver + " (slot " + slot + ")";
        if (v[0] != TrackerChangeType(writer)) return "tracker v" + version + " change type " + v[0];
        return null;
    }
}

/// <summary>Settings of one soak run.</summary>
internal sealed record SoakSettings
{
    /// <summary>The run's directory: db, wal, checkpoints and backups go below it.</summary>
    public required string Root { get; init; }
    public bool LowMemoryMode { get; init; }
    public int ProcessorCount { get; init; }
    /// <summary>Shared block cache capacity (0 = Mosaik's value for the mode).</summary>
    public long CacheBytes { get; init; }
    /// <summary>Shared write buffer manager size (0 = Mosaik's low-memory value or its floor).</summary>
    public long WriteBufferBytes { get; init; }
    public int Seed { get; init; }
    public TimeSpan Duration { get; init; }
    /// <summary>Null for the local file system.</summary>
    public IFileSystem? FileSystem { get; init; }
    /// <summary>Scale of the large values (blob, Lucene and data pages); 1 = Mosaik's page sizes.</summary>
    public double ValueScale { get; init; } = 1;
    public int Checkpoints { get; init; } = 2;
    public int Backups { get; init; } = 3;
    /// <summary>RocksDbBase.ReplicaStaleTimeout (ten minutes in Mosaik), scaled to the run.</summary>
    public TimeSpan WalArchiveStaleTimeout { get; init; } = TimeSpan.FromSeconds(1.5);
    /// <summary>Power-loss variant: when the file system "crashes" (null = never).</summary>
    public TimeSpan? CrashAt { get; init; }
    /// <summary>A worker that makes no progress for this long is reported as hung.</summary>
    public TimeSpan HangTimeout { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>AUDIT15_SOAK_SEED replays a run; otherwise a time-based seed (printed with every result).</summary>
    public static int SeedFromEnvironment() =>
        int.TryParse(Environment.GetEnvironmentVariable("AUDIT15_SOAK_SEED"), out int s) ? s : (int)(DateTime.UtcNow.Ticks % 1_000_000_007);

    /// <summary>AUDIT15_SOAK_SECONDS lengthens (or shortens) every configuration.</summary>
    public static TimeSpan DurationFromEnvironment(double defaultSeconds) =>
        TimeSpan.FromSeconds(double.TryParse(Environment.GetEnvironmentVariable("AUDIT15_SOAK_SECONDS"), System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out double d) && d > 0 ? d : defaultSeconds);

    public override string ToString() =>
        "seed=" + Seed + " lowMemory=" + LowMemoryMode + " processors=" + ProcessorCount + " duration=" + Duration.TotalSeconds + "s" +
        (FileSystem is null ? " fs=local" : " fs=" + FileSystem.GetType().Name) + (CrashAt is { } c ? " crashAt=" + c.TotalSeconds.ToString("0.00") + "s" : "");
}

/// <summary>A checkpoint or backup taken during the run, with what the writers had begun by the time it returned.</summary>
internal sealed record SoakSnapshotRecord(string Kind, string Path, uint BackupId, int[] MaxOps, long TrackerIssued, long TrackerRemoving, long StartTicks);

/// <summary>
/// Audit #15: Mosaik's whole concurrent mix on one database opened with <see cref="MosaikProfile"/>: node batches, blob
/// page rewrites, Lucene pages, the file and to-index queues, command-score, vector and text index entries, the change
/// log and its clear, the replica change tracker with its acknowledgement, Mosaik's flush thread (flush, ~30 properties
/// on every family, memory usage, the WAL archive cleanup behind the engine's back, file deletions disabled while a
/// replica is registered), concurrent manual compactions, readers in Mosaik's iterator shapes, checkpoints and backups
/// mid-run. Every check failure is a <see cref="Violations"/> entry; a worker that stops making progress is reported with
/// what each worker was doing.
/// </summary>
internal sealed class MosaikSoak
{
    public SoakSettings Settings { get; }
    public SoakModel M { get; } = new();
    public string DbPath { get; }
    public string WalDir { get; }
    public string BackupDir { get; }
    public IFileSystem Fs { get; }
    public MosaikProfile Profile { get; private set; } = null!;
    public RocksDatabase Db { get; private set; } = null!;
    /// <summary>The families' handles, by index into <see cref="MosaikProfile.Families"/>.</summary>
    public ColumnFamily[] Cf { get; private set; } = [];
    public readonly CancellationTokenSource Stop = new();
    public volatile bool Crashed;
    public volatile bool Closing;
    public readonly ConcurrentQueue<string> Violations = new();
    public readonly List<SoakWorker> Workers = [];
    public readonly List<SoakSnapshotRecord> Snapshots = [];
    /// <summary>Per family: the start of the latest successful flush or full-range compaction of it (power-loss durability).</summary>
    public readonly long[] Horizon = new long[SoakModel.FamilyCount];
    public readonly ConcurrentDictionary<string, long> Counters = new(StringComparer.Ordinal);
    public long StartTicks { get; private set; }
    public TimeSpan RunTime { get; private set; }
    public bool Hung { get; private set; }
    /// <summary>Exception of a Dispose that failed (only expected after a crash).</summary>
    public Exception? CloseError { get; private set; }
    /// <summary>Makes the file system fail every mutation from now on (set by the power-loss test).</summary>
    public Action? SoakCrash { get; set; }

    private readonly ConcurrentQueue<(string Name, Thread Thread)> _spawned = new();
    private int _spawnedRunning;
    private readonly Lock _reservationLock = new();
    private long _changeLogVersion, _changeLogPersisted;
    private SoakWriterLog _reservation = null!;
    private int _reservationSlot;
    private static readonly byte[] s_changeLogVersionKey = "changelog-version"u8.ToArray();

    public static readonly int FNodes = SoakModel.IndexOf("nodes"), FTypes = SoakModel.IndexOf("types"), FEdges = SoakModel.IndexOf("edges"),
        FContent = SoakModel.IndexOf("content"), FNodeVersion = SoakModel.IndexOf("nodeversion"), FDocMeta = SoakModel.IndexOf("docmetadata"),
        FChangeLog = SoakModel.IndexOf("changelog"), FBlobLength = SoakModel.IndexOf("blob-length"), FBlobPages = SoakModel.IndexOf("blob-pages"),
        FLuceneLength = SoakModel.IndexOf("lucene-file-length"), FLuceneSize = SoakModel.IndexOf("lucene-file-size"),
        FLuceneCount = SoakModel.IndexOf("lucene-file-pages-count"), FLucenePages = SoakModel.IndexOf("lucene-file-pages"),
        FQueue = SoakModel.IndexOf("file-extraction-queue"), FQueueStats = SoakModel.IndexOf("file-extraction-queue-stats"),
        FToIndex = SoakModel.IndexOf("to-index-queue"), FCommand = SoakModel.IndexOf("command-score-index-entries"),
        FVectors = SoakModel.IndexOf("data-vectors"), FVectorHashes = SoakModel.IndexOf("data-vectors-hashes"),
        FTextKeys = SoakModel.IndexOf("simple-text-index-keys"), FTextEntries = SoakModel.IndexOf("simple-text-index-entries"),
        FDataPaths = SoakModel.IndexOf("data-paths"), FDataLength = SoakModel.IndexOf("data-length"), FDataPages = SoakModel.IndexOf("data-pages"),
        FNodeSchemas = SoakModel.IndexOf("nodeschemas"), FEdgeSchemas = SoakModel.IndexOf("edgeschemas"), FDeletion = SoakModel.IndexOf("deletion-queue"),
        FChangedUids = SoakModel.IndexOf("changeduidslog"), FSettings = SoakModel.SettingsFamily, FTracker = SoakModel.TrackerFamily;

    public MosaikSoak(SoakSettings settings)
    {
        Settings = settings;
        Fs = settings.FileSystem ?? LocalFileSystem.Instance;
        DbPath = Path.Combine(settings.Root, "db");
        WalDir = Path.Combine(settings.Root, "wal");
        BackupDir = Path.Combine(settings.Root, "backups");
        M.Relaxed[FChangeLog] = true;
    }

    public static byte[] Uid(int kind, int writer, int index)
    {
        var k = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(k, SoakValue.Mix(((ulong)(uint)kind << 40) | ((ulong)(uint)writer << 32) | (uint)index));
        k[8] = (byte)kind;
        k[9] = (byte)writer;
        BinaryPrimitives.WriteInt32LittleEndian(k.AsSpan(10), index);
        return k;
    }

    public static byte[] Le64(long v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(b, v);
        return b;
    }

    public void Count(string counter, long by = 1) => Counters.AddOrUpdate(counter, by, (_, v) => v + by);

    public void Violation(string what)
    {
        if (Violations.Count < 50) Violations.Enqueue(what);
        Stop.Cancel();
    }

    public void NoteHorizon(int family, long startTicks)
    {
        long cur;
        while ((cur = Volatile.Read(ref Horizon[family])) < startTicks && Interlocked.CompareExchange(ref Horizon[family], startTicks, cur) != cur)
        {
        }
    }

    public void NoteHorizonAll(long startTicks)
    {
        for (int f = 0; f < SoakModel.FamilyCount; f++) NoteHorizon(f, startTicks);
    }

    /// <summary>RocksDbStorage.NextChangeLogVersion: the reservation Put of a new high-water mark happens under its lock.</summary>
    public void NextChangeLogVersion()
    {
        long version = Interlocked.Increment(ref _changeLogVersion);
        if (version <= Interlocked.Read(ref _changeLogPersisted)) return;
        lock (_reservationLock)
        {
            if (version <= _changeLogPersisted) return;
            long reserved = version + 100;
            _reservation.Begin();
            _reservation.Change(M, _reservationSlot, delete: false, raw: reserved);
            try
            {
                Cf[FSettings].Put(s_changeLogVersionKey, Le64(reserved));
            }
            catch
            {
                _reservation.Fail();
                throw;
            }
            _reservation.Commit(M);
            Interlocked.Exchange(ref _changeLogPersisted, reserved);
        }
    }

    /// <summary>
    /// A compaction thread as Mosaik starts them (ClearToIndexQueue, ClearCommandScore, the file queue, the text index):
    /// a new thread per request. At most four run at once here (Mosaik does not bound them); the others are skipped.
    /// </summary>
    public void Spawn(string name, Action compaction)
    {
        if (Closing || Stop.IsCancellationRequested) return;
        if (Interlocked.Increment(ref _spawnedRunning) > 4)
        {
            Interlocked.Decrement(ref _spawnedRunning);
            Count("spawned compactions skipped");
            return;
        }
        var t = new Thread(() =>
        {
            try
            {
                compaction();
                Count("spawned compactions");
            }
            catch (Exception e) when (Crashed || (Closing && (e is ObjectDisposedException || e is RocksDbException { Code: StatusCode.ShutdownInProgress })))
            {
                Count("spawned compactions ended by close/crash");
            }
            catch (Exception e)
            {
                Violation("spawned " + name + ": " + e);
            }
            finally
            {
                Interlocked.Decrement(ref _spawnedRunning);
            }
        })
        { IsBackground = true, Name = name };
        _spawned.Enqueue((name, t));
        t.Start();
    }

    public readonly struct SlotBefore(long acked, long rawAcked, int epoch)
    {
        public readonly long Acked = acked;
        public readonly long RawAcked = rawAcked;
        public readonly int Epoch = epoch;
    }

    public SlotBefore Before(int slot) => new(Volatile.Read(ref M.Acked[slot]), Volatile.Read(ref M.RawAcked[slot]), Volatile.Read(ref M.ClearEpoch));

    /// <summary>
    /// Checks one observation of a slot against what its writer had acknowledged before the read began (<paramref name="b"/>)
    /// and invoked by the time it ended. <paramref name="valueKnown"/> false: presence only (HasKey). Null when consistent.
    /// </summary>
    public string? CheckRead(int slot, in SlotBefore b, bool found, ReadOnlySpan<byte> value, bool valueKnown = true)
    {
        long inv = Volatile.Read(ref M.Invoked[slot]);
        long lastPut = Volatile.Read(ref M.LastPut[slot]);
        long lastDelete = Volatile.Read(ref M.LastDelete[slot]);
        int epochAfter = Volatile.Read(ref M.ClearEpoch);
        long ackedVersion = b.Acked >> 1;
        bool ackedAbsent = b.Acked == 0 || (b.Acked & 1) != 0;
        string family = MosaikProfile.Families[M.Family[slot]].Name;
        bool relaxed = M.Relaxed[M.Family[slot]] && (b.Epoch != epochAfter || (epochAfter & 1) != 0);
        if (found && valueKnown)
        {
            switch (M.Kind[slot])
            {
                case SlotKind.Codec:
                {
                    string? e = SoakValue.Verify(value, out int vs, out int ver, out int w, out _);
                    if (e is not null) return family + " slot " + slot + ": " + e;
                    if (vs != slot || w != M.Writer[slot]) return family + " slot " + slot + " holds the value of slot " + vs + " (writer " + w + ")";
                    if (ver > inv) return family + " slot " + slot + " returned v" + ver + ", newer than any invoked write (v" + inv + ")";
                    if (!relaxed && ver < ackedVersion) return family + " slot " + slot + " returned stale v" + ver + ", v" + ackedVersion + " was acknowledged before the read";
                    if (!relaxed && ver == ackedVersion && ackedAbsent) return family + " slot " + slot + " returned v" + ver + ", which was acknowledged as a delete";
                    return null;
                }
                case SlotKind.Empty:
                    if (value.Length != 0) return family + " slot " + slot + ": " + value.Length + "-byte value where only empty ones are written";
                    break;
                case SlotKind.RawCounter:
                {
                    if (value.Length != 8) return family + " slot " + slot + ": " + value.Length + "-byte counter";
                    long raw = BinaryPrimitives.ReadInt64LittleEndian(value);
                    long rawInv = Volatile.Read(ref M.RawInvoked[slot]);
                    if (raw > rawInv) return family + " slot " + slot + " counter " + raw + " was never written (newest invoked " + rawInv + ")";
                    if (raw < b.RawAcked) return family + " slot " + slot + " counter went back to " + raw + " after " + b.RawAcked + " was acknowledged";
                    return null;
                }
            }
        }
        if (relaxed) return null;
        if (found && ackedAbsent && lastPut <= ackedVersion)
            return family + " slot " + slot + " is present, but its last acknowledged change (v" + ackedVersion + ") is a delete and no put followed";
        if (!found && !ackedAbsent && lastDelete <= ackedVersion)
            return family + " slot " + slot + " is missing, but v" + ackedVersion + " was acknowledged before the read and no delete followed";
        return null;
    }

    // ---- the run ----

    public void Run()
    {
        Fs.CreateDirectoryIfMissing(Settings.Root);
        Profile = new MosaikProfile(WalDir, Settings.LowMemoryMode, Settings.ProcessorCount, Settings.FileSystem, Settings.CacheBytes, Settings.WriteBufferBytes);
        Db = Profile.Open(DbPath);
        Cf = new ColumnFamily[SoakModel.FamilyCount];
        for (int f = 0; f < SoakModel.FamilyCount; f++) Cf[f] = Db.GetColumnFamily(MosaikProfile.Families[f].Name);
        MosaikOpenTrackerCleanup(Db);

        // Writers first (they register their slots), then the pseudo-writer of the change log reservation.
        double scale = Settings.ValueScale;
        Workers.Add(new NodeWriter(this, 0));
        Workers.Add(new NodeWriter(this, 1));
        Workers.Add(new BlobWriter(this, scale));
        Workers.Add(new LuceneWriter(this, scale));
        Workers.Add(new FileQueueWriter(this));
        Workers.Add(new ToIndexQueueWriter(this));
        Workers.Add(new CommandScoreWriter(this));
        Workers.Add(new VectorWriter(this));
        Workers.Add(new TextIndexWriter(this));
        Workers.Add(new MiscWriter(this, scale));
        Workers.Add(new TrackerAckWriter(this));
        _reservation = M.AddWriter("changelog-version reservation");
        _reservationSlot = M.AddSlot(FSettings, s_changeLogVersionKey, _reservation, SlotKind.RawCounter);
        M.Freeze();
        for (int i = 0; i < 3; i++) Workers.Add(new ReaderWorker(this, i));
        Workers.Add(new FlushThreadWorker(this));
        Workers.Add(new MaintenanceWorker(this, 0));
        Workers.Add(new MaintenanceWorker(this, 1));
        Workers.Add(new SnapshotWorker(this));

        StartTicks = Stopwatch.GetTimestamp();
        foreach (SoakWorker w in Workers)
        {
            w.LastProgress = StartTicks;
            w.Thread = new Thread(() => WorkerMain(w)) { IsBackground = true, Name = "soak " + w.Name };
            w.Thread.Start();
        }

        Watch();
        RunTime = Stopwatch.GetElapsedTime(StartTicks);
        Stop.Cancel();
        if (!JoinAll(Settings.HangTimeout))
        {
            Hung = true;
            return; // the database cannot be closed safely under a hung call; the report says what each worker was doing
        }

        Closing = true;
        var close = Task.Run(Db.Dispose);
        try
        {
            if (!close.Wait(TimeSpan.FromSeconds(30)))
            {
                Hung = true;
                Violation("Dispose did not return within 30 s" + Environment.NewLine + Report());
                return;
            }
        }
        catch (AggregateException e) when (Crashed)
        {
            CloseError = e.InnerException;
        }
        catch (AggregateException e)
        {
            CloseError = e.InnerException;
            Violation("Dispose failed: " + e.InnerException);
        }
        long deadline = Stopwatch.GetTimestamp() + Stopwatch.Frequency * 30;
        foreach ((string name, Thread t) in _spawned)
        {
            double left = Math.Max(0, deadline - Stopwatch.GetTimestamp()) / (double)Stopwatch.Frequency;
            if (!t.Join(TimeSpan.FromSeconds(left)))
            {
                Hung = true;
                Violation("spawned compaction thread " + name + " did not end within 30 s of the close");
            }
        }
    }

    /// <summary>SafeRocksDb.Open on a primary: every change tracker entry is removed (no replica is attached yet).</summary>
    public static void MosaikOpenTrackerCleanup(RocksDatabase db)
    {
        ColumnFamily tracker = db.GetColumnFamily(MosaikProfile.ReplicaChangeTrackerFamily);
        using Iterator it = tracker.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) tracker.Delete(it.Key);
    }

    private void WorkerMain(SoakWorker w)
    {
        try
        {
            while (!Stop.IsCancellationRequested)
            {
                w.Step();
                w.Steps++;
                Volatile.Write(ref w.LastProgress, Stopwatch.GetTimestamp());
            }
            w.Status = "stopped";
        }
        catch (Exception e) when (Crashed)
        {
            (w as SoakWriterWorker)?.W.Fail();
            w.Status = "ended after the crash: " + e.GetType().Name + " " + First(e.Message);
        }
        catch (Exception e)
        {
            (w as SoakWriterWorker)?.W.Fail();
            Violation(w.Name + " (" + w.Status + "): " + e);
        }
        finally
        {
            w.Done = true;
        }
    }

    private static string First(string s) => s.Length <= 160 ? s : s[..160];

    private void Watch()
    {
        long end = StartTicks + (long)(Settings.Duration.TotalSeconds * Stopwatch.Frequency);
        long? crashAt = Settings.CrashAt is { } c ? StartTicks + (long)(c.TotalSeconds * Stopwatch.Frequency) : null;
        long hang = (long)(Settings.HangTimeout.TotalSeconds * Stopwatch.Frequency);
        while (!Stop.IsCancellationRequested)
        {
            long now = Stopwatch.GetTimestamp();
            if (now >= end) return;
            if (crashAt is { } ca && now >= ca && !Crashed)
            {
                Crashed = true;
                Count("crash at ms", (long)Stopwatch.GetElapsedTime(StartTicks).TotalMilliseconds);
                (SoakCrash ?? throw new InvalidOperationException("the crash needs a fault-injecting file system"))();
                // Let the failures reach every worker, then stop: nothing that happens after the crash reaches the disk.
                end = Math.Min(end, now + Stopwatch.Frequency / 2);
            }
            foreach (SoakWorker w in Workers)
            {
                if (!w.Done && now - Volatile.Read(ref w.LastProgress) > hang)
                {
                    Hung = true;
                    Violation("no progress for " + Settings.HangTimeout.TotalSeconds + " s in " + w.Name + Environment.NewLine + Report());
                    return;
                }
            }
            Stop.Token.WaitHandle.WaitOne(100);
        }
    }

    private bool JoinAll(TimeSpan timeout)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
        foreach (SoakWorker w in Workers)
        {
            double left = Math.Max(0, deadline - Stopwatch.GetTimestamp()) / (double)Stopwatch.Frequency;
            if (!w.Thread!.Join(TimeSpan.FromSeconds(left)))
            {
                Violation("worker " + w.Name + " did not stop within " + timeout.TotalSeconds + " s of the stop request" + Environment.NewLine + Report());
                return false;
            }
        }
        return true;
    }

    /// <summary>What every worker is doing and how long ago it last made progress.</summary>
    public string Report()
    {
        var sb = new System.Text.StringBuilder();
        long now = Stopwatch.GetTimestamp();
        foreach (SoakWorker w in Workers)
        {
            double idle = (now - Volatile.Read(ref w.LastProgress)) / (double)Stopwatch.Frequency;
            sb.Append("  ").Append(w.Name).Append(": ").Append(w.Done ? "done" : "running").Append(", ").Append(w.Steps).Append(" steps, last progress ")
              .Append(idle.ToString("0.0")).Append(" s ago, ").Append(w.Status).AppendLine();
        }
        foreach ((string name, Thread t) in _spawned)
        {
            if (t.IsAlive) sb.Append("  spawned ").Append(name).AppendLine(": running");
        }
        return sb.ToString();
    }

    public string Summary()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append(Settings).Append(", ran ").Append(RunTime.TotalSeconds.ToString("0.0")).AppendLine(" s");
        foreach (SoakWriterLog w in M.Writers)
        {
            // The longest time a writer waited for one write, and when its last write was acknowledged.
            double longest = 0, last = 0;
            for (int i = 1; i < w.Ops.Count; i++)
            {
                OpRecord r = w.Ops[i];
                if (r.Ack == 0) continue;
                longest = Math.Max(longest, (r.Ack - r.Invoke) / (double)Stopwatch.Frequency);
                last = (r.Ack - StartTicks) / (double)Stopwatch.Frequency;
            }
            sb.Append("  ").Append(w.Name).Append(": ").Append(w.OpCount).Append(" writes, longest ").Append(longest.ToString("0.000"))
              .Append(" s, last acknowledged at ").Append(last.ToString("0.0")).AppendLine(" s");
        }
        foreach (SoakWorker w in Workers)
        {
            if (w is not SoakWriterWorker) sb.Append("  ").Append(w.Name).Append(": ").Append(w.Steps).AppendLine(" steps");
        }
        foreach (KeyValuePair<string, long> kv in Counters.OrderBy(kv => kv.Key, StringComparer.Ordinal)) sb.Append("  ").Append(kv.Key).Append(": ").Append(kv.Value).AppendLine();
        sb.Append("  tracker: issued ").Append(M.TrackerIssued).Append(", acknowledged ").Append(M.TrackerAck).Append(", version key regressions seen by readers ")
          .Append(Interlocked.Read(ref M.VersionKeyRegressionsSeen)).AppendLine();
        foreach (SoakSnapshotRecord r in Snapshots) sb.Append("  ").Append(r.Kind).Append(' ').Append(r.BackupId).Append(" at ").Append(Path.GetFileName(r.Path)).AppendLine();
        return sb.ToString();
    }
}

/// <summary>One thread of the soak.</summary>
internal abstract class SoakWorker
{
    protected readonly MosaikSoak S;
    protected readonly SoakModel M;
    protected readonly Random Rng;
    public readonly string Name;
    /// <summary>What the worker is doing (for the hang report).</summary>
    public volatile string Status = "starting";
    public long Steps;
    public long LastProgress;
    public volatile bool Done;
    public Thread? Thread;

    protected SoakWorker(MosaikSoak s, string name, int salt)
    {
        S = s;
        M = s.M;
        Name = name;
        Rng = new Random(unchecked(s.Settings.Seed * 1_000_003 + salt * 7919));
    }

    public abstract void Step();

    protected ColumnFamily Cf(int family) => S.Cf[family];

    /// <summary>Mosaik's HasKey (RocksDbManagedCompat): TryGet with a callback that only says "found".</summary>
    protected static bool HasKey(ColumnFamily cf, ReadOnlySpan<byte> key) => cf.TryGet(null, key, 0, static (_, _) => true, out _);

    /// <summary>Waits, unless the run is stopping (then true).</summary>
    protected bool Wait(TimeSpan t) => S.Stop.Token.WaitHandle.WaitOne(t);

    protected static byte[] Bound(byte[] prefix, int length, byte fill)
    {
        var b = new byte[length];
        b.AsSpan().Fill(fill);
        prefix.CopyTo(b, 0);
        return b;
    }

    protected static byte[] Concat(byte[] a, byte[] b)
    {
        var r = new byte[a.Length + b.Length];
        a.CopyTo(r, 0);
        b.CopyTo(r, a.Length);
        return r;
    }

    protected static byte[] Int32Key(byte[] prefix, int value)
    {
        var r = new byte[prefix.Length + 4];
        prefix.CopyTo(r, 0);
        BinaryPrimitives.WriteInt32LittleEndian(r.AsSpan(prefix.Length), value);
        return r;
    }

    protected static byte[] IndexUid64(int kind, int index) => MosaikSoak.Le64((long)SoakValue.Mix(0x1D5UL << 20 | (ulong)(uint)kind << 8 | (uint)index));
}

/// <summary>A worker that owns slots and writes them.</summary>
internal abstract class SoakWriterWorker : SoakWorker
{
    public readonly SoakWriterLog W;
    private byte[] _value = new byte[4096];

    protected SoakWriterWorker(MosaikSoak s, string name, int salt) : base(s, name, salt) => W = s.M.AddWriter(name);

    protected int Slot(int family, byte[] key, SlotKind kind = SlotKind.Codec) => M.AddSlot(family, key, W, kind);

    /// <summary>Records a codec put in the open operation and returns its value (valid until the next call).</summary>
    protected ReadOnlySpan<byte> PutValue(int slot, int length)
    {
        length = Math.Max(length, SoakValue.HeaderSize);
        int v = W.Change(M, slot, delete: false, raw: length);
        if (_value.Length < length) _value = new byte[Math.Max(length, _value.Length * 2)];
        Span<byte> b = _value.AsSpan(0, length);
        SoakValue.Encode(b, slot, v, W.Id, W.CurrentOp);
        return b;
    }

    protected void PutEmpty(int slot) => W.Change(M, slot, delete: false);

    protected void Delete(int slot) => W.Change(M, slot, delete: true);

    protected int Begin(string status)
    {
        Status = status;
        return W.Begin();
    }

    protected void Commit() => W.Commit(M);

    protected bool Present(int slot) => M.LastEvent(slot) is { Delete: false };

    /// <summary>Exact read-your-writes: no other worker writes the slot and none of this worker's operations is open.</summary>
    protected void ExpectOwn(int slot, bool found, ReadOnlySpan<byte> value, bool valueKnown, string what)
    {
        SlotEvent? e = M.LastEvent(slot);
        bool present = e is { Delete: false };
        if (found != present)
        {
            S.Violation(Name + " " + what + ": slot " + slot + " (" + FamilyName(slot) + ") is " + (found ? "present" : "missing") + ", its last write " + Describe(e));
            return;
        }
        if (found && valueKnown && !value.SequenceEqual(M.ValueOf(slot, e!.Value)))
        {
            string got = SoakValue.Verify(value, out int s, out int v, out _, out int o) ?? ("slot " + s + " v" + v + " op" + o);
            S.Violation(Name + " " + what + ": slot " + slot + " (" + FamilyName(slot) + ") does not hold its last write " + Describe(e) + ", it holds " + got);
        }
    }

    /// <summary>An own scan must return exactly the model's present slots, in key order (reversed for a backward scan).</summary>
    protected void ExpectOwnScan(string what, IReadOnlyList<int> expectedSlots, List<(byte[] Key, byte[] Value)> got, bool valuesKnown)
    {
        int n = 0;
        foreach (int slot in expectedSlots)
        {
            if (!Present(slot)) continue;
            if (n >= got.Count)
            {
                S.Violation(Name + " " + what + ": scan ended after " + got.Count + " entries, slot " + slot + " (" + Convert.ToHexString(M.Key[slot]) + ") is missing");
                return;
            }
            if (!got[n].Key.AsSpan().SequenceEqual(M.Key[slot]))
            {
                S.Violation(Name + " " + what + ": entry " + n + " is " + Convert.ToHexString(got[n].Key) + ", expected slot " + slot + " " + Convert.ToHexString(M.Key[slot]));
                return;
            }
            ExpectOwn(slot, true, got[n].Value, valuesKnown, what);
            n++;
        }
        if (n != got.Count) S.Violation(Name + " " + what + ": scan returned " + got.Count + " entries, the model has " + n + "; first extra " + Convert.ToHexString(got[n].Key));
    }

    protected string FamilyName(int slot) => MosaikProfile.Families[M.Family[slot]].Name;

    protected static string Describe(SlotEvent? e) => e is { } x ? (x.Delete ? "deleted" : "put") + " v" + x.Version + " in op " + x.Op : "never written";

    protected static List<(byte[] Key, byte[] Value)> Collect(Iterator it, bool forward, int limit = int.MaxValue)
    {
        var l = new List<(byte[], byte[])>();
        while (it.Valid && l.Count < limit)
        {
            l.Add((it.Key.ToArray(), it.Value.ToArray()));
            if (forward) it.Next();
            else it.Prev();
        }
        return l;
    }

    /// <summary>RocksDbBase.TrackChangesHandler: a new version, its Put into graphsettings, then the tracker entry.</summary>
    protected void TrackChanges()
    {
        long v;
        lock (M.TrackerLock)
        {
            v = ++M.TrackerIssued;
            M.TrackerInFlight[W.Id] = v;
        }
        Begin("Put(graphsettings version key)");
        S.Cf[MosaikSoak.FSettings].Put(SoakModel.VersionKey, MosaikSoak.Le64(v));
        Commit();
        long cur;
        while ((cur = Interlocked.Read(ref M.VersionKeyAckedMax)) < v && Interlocked.CompareExchange(ref M.VersionKeyAckedMax, v, cur) != cur)
        {
        }
        int op = Begin("Put(replica-change-tracker)");
        M.TrackerOwner[v] = ((long)W.Id << 32) | (uint)op;
        Span<byte> value = stackalloc byte[SoakModel.TrackerValueLength];
        SoakModel.EncodeTrackerValue(value, v, W.Id, op);
        S.Cf[MosaikSoak.FTracker].Put(SoakModel.TrackerKey(v), value);
        Commit();
        lock (M.TrackerLock) M.TrackerInFlight[W.Id] = 0;
    }
}

/// <summary>
/// RocksDbStorage.SetNode / SetNodeContent / TryRemoveNode / SetDocumentMetadata (RocksDbStorage.cs:2425-2580): the node
/// batch over nodes, types, nodeversion and the edge buckets, the content Put, the removal batch.
/// </summary>
internal sealed class NodeWriter : SoakWriterWorker
{
    public const int Nodes = 1200, Buckets = 4;
    public readonly int[] NodesSlot = new int[Nodes], TypesSlot = new int[Nodes], VersionSlot = new int[Nodes], ContentSlot = new int[Nodes], DocSlot = new int[Nodes];
    public readonly int[,] EdgeSlot = new int[Nodes, Buckets];
    private readonly int[] _bucketsWritten = new int[Nodes];

    public NodeWriter(MosaikSoak s, int index) : base(s, "nodes-" + index, 100 + index)
    {
        for (int n = 0; n < Nodes; n++)
        {
            byte[] uid = MosaikSoak.Uid(1, index, n);
            NodesSlot[n] = Slot(MosaikSoak.FNodes, uid);
            TypesSlot[n] = Slot(MosaikSoak.FTypes, uid);
            VersionSlot[n] = Slot(MosaikSoak.FNodeVersion, uid);
            ContentSlot[n] = Slot(MosaikSoak.FContent, uid);
            DocSlot[n] = Slot(MosaikSoak.FDocMeta, uid);
            for (int b = 0; b < Buckets; b++) EdgeSlot[n, b] = Slot(MosaikSoak.FEdges, Int32Key(uid, b));
        }
    }

    public override void Step()
    {
        int n = Rng.Next(Nodes);
        int r = Rng.Next(100);
        if (r < 55) SetNode(n);
        else if (r < 75) SetContent(n);
        else if (r < 85) Remove(n);
        else if (r < 90) SetDocumentMetadata(n);
        else OwnReads(n);
    }

    private byte[] KeyOf(int slot) => M.Key[slot];

    private void SetNode(int n)
    {
        S.NextChangeLogVersion();
        bool separate = Rng.Next(2) == 0;
        int buckets = 0, first = 0;
        if (separate)
        {
            buckets = 1 + Rng.Next(Buckets);
            Status = "HasKey(edges bucket 0)";
            bool newOnEdges = !HasKey(Cf(MosaikSoak.FEdges), KeyOf(EdgeSlot[n, 0]));
            ExpectOwn(EdgeSlot[n, 0], !newOnEdges, default, valueKnown: false, "SetNode HasKey(edges)");
            first = newOnEdges ? 0 : Rng.Next(buckets);
        }
        using (var batch = new WriteBatch())
        {
            Begin("Write(node batch)");
            batch.Put(Cf(MosaikSoak.FNodes), KeyOf(NodesSlot[n]), PutValue(NodesSlot[n], 40 + Rng.Next(560)));
            batch.Put(Cf(MosaikSoak.FTypes), KeyOf(TypesSlot[n]), PutValue(TypesSlot[n], SoakValue.HeaderSize));
            batch.Put(Cf(MosaikSoak.FNodeVersion), KeyOf(VersionSlot[n]), PutValue(VersionSlot[n], SoakValue.HeaderSize + 8));
            for (int b = first; b < buckets; b++) batch.Put(Cf(MosaikSoak.FEdges), KeyOf(EdgeSlot[n, b]), PutValue(EdgeSlot[n, b], 200 + Rng.Next(1800)));
            S.Db.Write(batch);
            Commit();
        }
        _bucketsWritten[n] = separate ? buckets : 0;
        TrackChanges();
    }

    private void SetContent(int n)
    {
        Begin("Put(content)");
        Cf(MosaikSoak.FContent).Put(KeyOf(ContentSlot[n]), PutValue(ContentSlot[n], 100 + Rng.Next(2900)));
        Commit();
        TrackChanges();
    }

    private void Remove(int n)
    {
        Status = "HasKey(types)";
        bool exists = HasKey(Cf(MosaikSoak.FTypes), KeyOf(TypesSlot[n]));
        ExpectOwn(TypesSlot[n], exists, default, valueKnown: false, "TryRemoveNode HasKey(types)");
        if (!exists) return;
        using (var batch = new WriteBatch())
        {
            Begin("Write(node removal batch)");
            batch.Delete(Cf(MosaikSoak.FTypes), KeyOf(TypesSlot[n]));
            Delete(TypesSlot[n]);
            batch.Delete(Cf(MosaikSoak.FNodes), KeyOf(NodesSlot[n]));
            Delete(NodesSlot[n]);
            batch.Delete(Cf(MosaikSoak.FContent), KeyOf(ContentSlot[n]));
            Delete(ContentSlot[n]);
            batch.Delete(Cf(MosaikSoak.FDocMeta), KeyOf(DocSlot[n]));
            Delete(DocSlot[n]);
            batch.Delete(Cf(MosaikSoak.FNodeVersion), KeyOf(VersionSlot[n]));
            Delete(VersionSlot[n]);
            for (int b = 0; b < _bucketsWritten[n]; b++)
            {
                batch.Delete(Cf(MosaikSoak.FEdges), KeyOf(EdgeSlot[n, b]));
                Delete(EdgeSlot[n, b]);
            }
            S.Db.Write(batch);
            Commit();
        }
        TrackChanges();
    }

    private void SetDocumentMetadata(int n)
    {
        Begin("Put(docmetadata)");
        Cf(MosaikSoak.FDocMeta).Put(KeyOf(DocSlot[n]), PutValue(DocSlot[n], SoakValue.HeaderSize + 20));
        Commit();
    }

    private void OwnReads(int n)
    {
        Status = "Get(nodeversion)";
        byte[]? v = Cf(MosaikSoak.FNodeVersion).Get(KeyOf(VersionSlot[n]));
        ExpectOwn(VersionSlot[n], v is not null, v, valueKnown: true, "GetNodeVersion");
        Status = "TryGet(content, deserializer)";
        bool found = Cf(MosaikSoak.FContent).TryGet(null, KeyOf(ContentSlot[n]), 0, static (value, _) => value.ToArray(), out byte[] content);
        ExpectOwn(ContentSlot[n], found, content, valueKnown: true, "Get(content)");
        Status = "Get(nodes)";
        byte[]? node = Cf(MosaikSoak.FNodes).Get(RocksDBExtDefaultReadOptions, KeyOf(NodesSlot[n]));
        ExpectOwn(NodesSlot[n], node is not null, node, valueKnown: true, "Get(nodes)");
    }

    /// <summary>RocksDBExt.DefaultReadOptions: one ReadOptions instance shared by every thread.</summary>
    public static readonly ReadOptions RocksDBExtDefaultReadOptions = new();
}

/// <summary>
/// RocksDbBlobStorage.StoreBlob / TryDeleteFromRocksDB / GetPage (RocksDbBlobStorage.cs:75-260): a rewrite deletes the old
/// pages and puts the new ones in the same batch as the length.
/// </summary>
internal sealed class BlobWriter : SoakWriterWorker
{
    public const int Blobs = 200, MaxPages = 4;
    public readonly int PageSize;
    public readonly int[] LengthSlot = new int[Blobs];
    public readonly int[,] PageSlot = new int[Blobs, MaxPages];
    private readonly long[] _length = new long[Blobs];

    public BlobWriter(MosaikSoak s, double scale) : base(s, "blobs", 200)
    {
        PageSize = Math.Max(512, (int)(32768 * scale));
        for (int b = 0; b < Blobs; b++)
        {
            byte[] uid = MosaikSoak.Uid(2, 0, b);
            LengthSlot[b] = Slot(MosaikSoak.FBlobLength, uid);
            for (int p = 0; p < MaxPages; p++) PageSlot[b, p] = Slot(MosaikSoak.FBlobPages, Int32Key(uid, p));
            _length[b] = -1;
        }
    }

    /// <summary>The blob length a version of the length slot stands for (so readers can check the pages against it).</summary>
    public long BlobLength(int slot, int version)
    {
        ulong h = SoakValue.Mix(((ulong)(uint)slot << 32) | (uint)version ^ 0xB10BUL);
        if (h % 7 == 0) return 0;
        int pages = 1 + (int)((h >> 8) % MaxPages);
        int last = 64 + (int)((h >> 20) % (ulong)(PageSize - 64));
        return (long)(pages - 1) * PageSize + last;
    }

    public int Pages(long length) => length <= 0 ? 0 : (int)((length + PageSize - 1) / PageSize);

    public override void Step()
    {
        int b = Rng.Next(Blobs);
        int r = Rng.Next(100);
        if (r < 65) Store(b);
        else if (r < 80) TryDelete(b);
        else GetPage(b, Rng.Next(MaxPages));
    }

    private void Store(int b)
    {
        Status = "Get(blob-length)";
        byte[]? existing = Cf(MosaikSoak.FBlobLength).Get(M.Key[LengthSlot[b]]);
        ExpectOwn(LengthSlot[b], existing is not null, existing, valueKnown: true, "StoreBlob Get(length)");
        long existingLength = _length[b];
        using var batch = new WriteBatch();
        Begin("Write(blob rewrite)");
        batch.Put(Cf(MosaikSoak.FBlobLength), M.Key[LengthSlot[b]], PutValue(LengthSlot[b], SoakValue.HeaderSize + 8));
        long length = BlobLength(LengthSlot[b], M.Version[LengthSlot[b]]);
        if (existingLength > 0)
        {
            for (int p = 0; p < Pages(existingLength); p++)
            {
                batch.Delete(Cf(MosaikSoak.FBlobPages), M.Key[PageSlot[b, p]]);
                Delete(PageSlot[b, p]);
            }
        }
        int pages = Pages(length);
        for (int p = 0; p < pages; p++)
        {
            int size = p < pages - 1 ? PageSize : (int)(length - (long)(pages - 1) * PageSize);
            batch.Put(Cf(MosaikSoak.FBlobPages), M.Key[PageSlot[b, p]], PutValue(PageSlot[b, p], size));
        }
        S.Db.Write(batch);
        Commit();
        _length[b] = length;
    }

    private void TryDelete(int b)
    {
        Status = "Get(blob-length)";
        byte[]? existing = Cf(MosaikSoak.FBlobLength).Get(M.Key[LengthSlot[b]]);
        ExpectOwn(LengthSlot[b], existing is not null, existing, valueKnown: true, "TryDeleteFromRocksDB Get(length)");
        if (existing is null) return;
        using var batch = new WriteBatch();
        Begin("Write(blob delete)");
        batch.Delete(Cf(MosaikSoak.FBlobLength), M.Key[LengthSlot[b]]);
        Delete(LengthSlot[b]);
        for (int p = 0; p < Pages(_length[b]); p++)
        {
            batch.Delete(Cf(MosaikSoak.FBlobPages), M.Key[PageSlot[b, p]]);
            Delete(PageSlot[b, p]);
        }
        S.Db.Write(batch);
        Commit();
        _length[b] = -1;
    }

    private void GetPage(int b, int p)
    {
        Status = "GetPooled(blob-pages)";
        bool found = Cf(MosaikSoak.FBlobPages).TryGet(NodeWriter.RocksDBExtDefaultReadOptions, M.Key[PageSlot[b, p]], 0, static (value, _) => value.ToArray(), out byte[] page);
        ExpectOwn(PageSlot[b, p], found, page, valueKnown: true, "GetPage");
    }
}

/// <summary>
/// RocksDbIndexStorage's Lucene file calls (RocksDbIndexStorage.cs:225-410): 64 KiB pages, page count, length and size;
/// a page read into a fixed page buffer (LuceneGetFilePage); a directory listing by prefix (LuceneGetFilesOnDirectory).
/// </summary>
internal sealed class LuceneWriter : SoakWriterWorker
{
    public const int Dirs = 6, FilesPerDir = 20, Files = Dirs * FilesPerDir, Pages = 3;
    public readonly int PageSize;
    public readonly int[] LengthSlot = new int[Files], SizeSlot = new int[Files], CountSlot = new int[Files];
    public readonly int[,] PageSlot = new int[Files, Pages];
    private readonly int[] _count = new int[Files];
    private readonly byte[] _pageBuffer, _small = new byte[100];

    public LuceneWriter(MosaikSoak s, double scale) : base(s, "lucene", 300)
    {
        PageSize = Math.Max(1024, (int)(65536 * scale));
        _pageBuffer = new byte[PageSize];
        for (int f = 0; f < Files; f++)
        {
            string path = DirectoryOf(f / FilesPerDir) + "_" + (f % FilesPerDir) + ".cfs";
            byte[] key = System.Text.Encoding.UTF8.GetBytes(path);
            LengthSlot[f] = Slot(MosaikSoak.FLuceneLength, key);
            SizeSlot[f] = Slot(MosaikSoak.FLuceneSize, key);
            CountSlot[f] = Slot(MosaikSoak.FLuceneCount, key);
            for (int i = 0; i < Pages; i++) PageSlot[f, i] = Slot(MosaikSoak.FLucenePages, System.Text.Encoding.UTF8.GetBytes(path + "/" + i));
        }
    }

    public static string DirectoryOf(int d) => "indexes/idx" + d + "/";

    public override void Step()
    {
        int f = Rng.Next(Files);
        int r = Rng.Next(100);
        if (r < 30) SetFilePages(f);
        else if (r < 42) SetFilePage(f, Rng.Next(Pages));
        else if (r < 55) SetFileLength(f);
        else if (r < 65) IncrementFileSize(f);
        else if (r < 72) DeleteFile(f);
        else if (r < 90) GetFilePage(f, Rng.Next(Pages));
        else FilesOnDirectory(Rng.Next(Dirs));
    }

    private void SetFilePages(int f)
    {
        int k = 1 + Rng.Next(Pages);
        int start = Rng.Next(Pages - k + 1);
        int pageCount = Math.Max(_count[f], start + k);
        using (var batch = new WriteBatch())
        {
            Begin("Write(lucene pages)");
            for (int i = start; i < start + k; i++) batch.Put(Cf(MosaikSoak.FLucenePages), M.Key[PageSlot[f, i]], PutValue(PageSlot[f, i], PageSize));
            batch.Put(Cf(MosaikSoak.FLuceneCount), M.Key[CountSlot[f]], PutValue(CountSlot[f], SoakValue.HeaderSize + 4));
            S.Db.Write(batch);
            Commit();
        }
        _count[f] = pageCount;
        TrackChanges();
    }

    private void SetFilePage(int f, int i)
    {
        Begin("Put(lucene page)");
        Cf(MosaikSoak.FLucenePages).Put(M.Key[PageSlot[f, i]], PutValue(PageSlot[f, i], PageSize));
        Commit();
        TrackChanges();
    }

    private void SetFileLength(int f)
    {
        if (Rng.Next(10) == 0)
        {
            DeleteFile(f);
            return;
        }
        Begin("Put(lucene length)");
        Cf(MosaikSoak.FLuceneLength).Put(M.Key[LengthSlot[f]], PutValue(LengthSlot[f], SoakValue.HeaderSize + 8));
        Commit();
        TrackChanges();
    }

    private void IncrementFileSize(int f)
    {
        Status = "Get(lucene size)";
        byte[]? cur = Cf(MosaikSoak.FLuceneSize).Get(M.Key[SizeSlot[f]]);
        ExpectOwn(SizeSlot[f], cur is not null, cur, valueKnown: true, "LuceneIncrementFileSize Get");
        Begin("Put(lucene size)");
        Cf(MosaikSoak.FLuceneSize).Put(M.Key[SizeSlot[f]], PutValue(SizeSlot[f], SoakValue.HeaderSize + 8));
        Commit();
        TrackChanges();
    }

    private void DeleteFile(int f)
    {
        Status = "Get(lucene page count)";
        byte[]? count = Cf(MosaikSoak.FLuceneCount).Get(M.Key[CountSlot[f]]);
        ExpectOwn(CountSlot[f], count is not null, count, valueKnown: true, "LuceneDeleteFile Get(count)");
        using (var batch = new WriteBatch())
        {
            Begin("Write(lucene delete file)");
            batch.Delete(Cf(MosaikSoak.FLuceneLength), M.Key[LengthSlot[f]]);
            Delete(LengthSlot[f]);
            batch.Delete(Cf(MosaikSoak.FLuceneSize), M.Key[SizeSlot[f]]);
            Delete(SizeSlot[f]);
            batch.Delete(Cf(MosaikSoak.FLuceneCount), M.Key[CountSlot[f]]);
            Delete(CountSlot[f]);
            for (int i = 0; i < _count[f]; i++)
            {
                batch.Delete(Cf(MosaikSoak.FLucenePages), M.Key[PageSlot[f, i]]);
                Delete(PageSlot[f, i]);
            }
            S.Db.Write(batch);
            Commit();
        }
        _count[f] = 0;
        TrackChanges();
    }

    private void GetFilePage(int f, int i)
    {
        // RocksDbIndexStorage.LuceneGetFilePage: Get into the page buffer; a length other than the buffer's zeroes it.
        Status = "TryGet(lucene page, fixed buffer)";
        _pageBuffer.AsSpan().Fill(0);
        long read = Cf(MosaikSoak.FLucenePages).TryGet(null, M.Key[PageSlot[f, i]], _pageBuffer, out int valueLength) ? valueLength : -1;
        ExpectOwn(PageSlot[f, i], read >= 0, _pageBuffer.AsSpan(0, (int)Math.Clamp(read, 0, PageSize)), valueKnown: true, "LuceneGetFilePage");
        if (read >= 0 && read != PageSize) S.Violation(Name + " LuceneGetFilePage: value length " + read + ", the page is " + PageSize);
        Status = "TryGet(lucene page, short buffer)";
        bool found = Cf(MosaikSoak.FLucenePages).TryGet(null, M.Key[PageSlot[f, i]], _small, out int fullLength);
        if (found != Present(PageSlot[f, i])) S.Violation(Name + " TryGet into a short buffer: found " + found);
        else if (found)
        {
            byte[] expected = M.ValueOf(PageSlot[f, i], M.LastEvent(PageSlot[f, i])!.Value);
            if (fullLength != expected.Length || !_small.AsSpan().SequenceEqual(expected.AsSpan(0, _small.Length)))
                S.Violation(Name + " TryGet into a short buffer: length " + fullLength + " (expected " + expected.Length + ") or the copied prefix differs");
        }
    }

    private void FilesOnDirectory(int d)
    {
        // RocksDbIndexStorage.LuceneGetFilesOnDirectory: Seek to the directory, take keys while they start with it.
        byte[] prefix = System.Text.Encoding.UTF8.GetBytes(DirectoryOf(d));
        Status = "Seek(lucene directory)";
        var got = new List<(byte[] Key, byte[] Value)>();
        using (Iterator it = Cf(MosaikSoak.FLuceneLength).NewIterator())
        {
            for (it.Seek(prefix); it.Valid && it.Key.StartsWith(prefix); it.Next()) got.Add((it.Key.ToArray(), it.Value.ToArray()));
        }
        var expected = new List<int>();
        foreach (int slot in M.SlotsInRange(MosaikSoak.FLuceneLength, prefix, true, default, false))
        {
            if (!M.Key[slot].AsSpan().StartsWith(prefix)) break;
            expected.Add(slot);
        }
        ExpectOwnScan("LuceneGetFilesOnDirectory", expected, got, valuesKnown: true);
    }
}

/// <summary>
/// RocksDbIndexStorage.FileQueue (FileQueue.cs:255-735): TryAdd / TryRemove behind HasKey, re-prioritisation as a Remove and
/// a Put, EnumerateFileQueue (SeekToLast + Prev), ClearFileQueue's DeleteRange over [0^17, FF^17), the queue statistics,
/// and the compaction threads it starts.
/// </summary>
internal sealed class FileQueueWriter : SoakWriterWorker
{
    public const int Files = 300, Stats = 40;
    public static readonly byte[] Prios = [1, 64, 128, 250];
    public readonly int[,] QueueSlot = new int[Files, 4];
    public readonly int[] StatsSlot = new int[Stats];
    private readonly int[] _current = new int[Files];
    private int _removes, _isCompacting;
    private readonly int[] _allQueueSlots;

    public FileQueueWriter(MosaikSoak s) : base(s, "file-queue", 400)
    {
        var all = new List<int>();
        for (int u = 0; u < Files; u++)
        {
            byte[] uid = MosaikSoak.Uid(4, 0, u);
            for (int p = 0; p < Prios.Length; p++)
            {
                QueueSlot[u, p] = Slot(MosaikSoak.FQueue, Concat([Prios[p]], uid));
                all.Add(QueueSlot[u, p]);
            }
            _current[u] = -1;
        }
        _allQueueSlots = all.ToArray();
        for (int i = 0; i < Stats; i++)
        {
            var k = new byte[11];
            BinaryPrimitives.WriteUInt64LittleEndian(k, SoakValue.Mix(0x57A7UL + (ulong)(i / 4)));
            BinaryPrimitives.WriteUInt16LittleEndian(k.AsSpan(8), (ushort)(i % 4));
            k[10] = (byte)(i % 3);
            StatsSlot[i] = Slot(MosaikSoak.FQueueStats, k);
        }
    }

    private ColumnFamily Queue => Cf(MosaikSoak.FQueue);

    public override void Step()
    {
        int r = Rng.Next(100);
        if (r < 35) TryAdd();
        else if (r < 60) TryRemove(PickQueued());
        else if (r < 75) ReEnqueue(PickQueued());
        else if (r < 79) Recompute();
        else if (r < 81) Clear();
        else if (r < 90)
        {
            int i = Rng.Next(Stats);
            Begin("Put(file-extraction-queue-stats)");
            Cf(MosaikSoak.FQueueStats).Put(M.Key[StatsSlot[i]], PutValue(StatsSlot[i], 40));
            Commit();
        }
        else
        {
            int u = Rng.Next(Files), p = Rng.Next(Prios.Length);
            Status = "HasKey(file queue)";
            ExpectOwn(QueueSlot[u, p], HasKey(Queue, M.Key[QueueSlot[u, p]]), default, valueKnown: false, "IsFileInQueue");
        }
    }

    private int PickQueued()
    {
        for (int i = 0; i < 8; i++)
        {
            int u = Rng.Next(Files);
            if (_current[u] >= 0) return u;
        }
        return -1;
    }

    private void TryAdd()
    {
        int u = Rng.Next(Files);
        if (_current[u] >= 0) return;
        int p = Rng.Next(Prios.Length);
        Status = "HasKey(file queue)";
        bool has = HasKey(Queue, M.Key[QueueSlot[u, p]]);
        ExpectOwn(QueueSlot[u, p], has, default, valueKnown: false, "TryAdd HasKey");
        if (has) return;
        Begin("Put(file queue)");
        Queue.Put(M.Key[QueueSlot[u, p]], PutValue(QueueSlot[u, p], 26));
        Commit();
        _current[u] = p;
    }

    private void TryRemove(int u)
    {
        if (u < 0) return;
        int slot = QueueSlot[u, _current[u]];
        Status = "HasKey(file queue)";
        bool has = HasKey(Queue, M.Key[slot]);
        ExpectOwn(slot, has, default, valueKnown: false, "TryRemove HasKey");
        if (!has) return;
        Begin("Remove(file queue)");
        Delete(slot);
        Queue.Delete(M.Key[slot]);
        Commit();
        _current[u] = -1;
        if (++_removes % 200 == 0 && Interlocked.CompareExchange(ref _isCompacting, 1, 0) == 0)
        {
            S.Spawn("Compacting file queue family", () =>
            {
                try
                {
                    Queue.CompactRange(null, null);
                }
                finally
                {
                    Interlocked.Exchange(ref _isCompacting, 0);
                }
            });
            if (S.Closing || S.Stop.IsCancellationRequested) Interlocked.Exchange(ref _isCompacting, 0);
        }
    }

    private void ReEnqueue(int u)
    {
        if (u < 0) return;
        int p = (_current[u] + 1 + Rng.Next(Prios.Length - 1)) % Prios.Length;
        int old = QueueSlot[u, _current[u]], now = QueueSlot[u, p];
        Begin("Remove(file queue, re-prioritise)");
        Delete(old);
        Queue.Delete(M.Key[old]);
        Commit();
        Begin("Put(file queue, re-prioritise)");
        Queue.Put(M.Key[now], PutValue(now, 26));
        Commit();
        _current[u] = p;
    }

    private void Recompute()
    {
        // RecomputeFilePriorityAndRemoveDeleted: EnumerateFileQueue (SeekToLast + Prev), then re-enqueue or remove each.
        Status = "EnumerateFileQueue (SeekToLast + Prev)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Queue.NewIterator())
        {
            it.SeekToLast();
            got = Collect(it, forward: false);
        }
        var expected = new List<int>(_allQueueSlots.Length);
        int[] sorted = M.Sorted[MosaikSoak.FQueue];
        for (int i = sorted.Length - 1; i >= 0; i--) expected.Add(sorted[i]);
        ExpectOwnScan("EnumerateFileQueue", expected, got, valuesKnown: true);
        for (int u = 0; u < Files; u++)
        {
            if (_current[u] < 0) continue;
            if (Rng.Next(10) < 7) ReEnqueue(u);
            else TryRemove(u);
        }
        S.Spawn("Compacting file indexing queue after change of priorities", () => Queue.CompactRange(null, null));
    }

    private void Clear()
    {
        Begin("DeleteRange(file queue)");
        foreach (int slot in _allQueueSlots) Delete(slot);
        Queue.DeleteRange(new WriteOptions(), new byte[17], Bound([], 17, 0xFF));
        Commit();
        Array.Fill(_current, -1);
    }
}

/// <summary>
/// RocksDbIndexStorage's to-index queue (RocksDbIndexStorage.cs:885-1095): empty-valued entries per index, bounded scans,
/// ClearToIndexQueue deleting while it iterates and then compacting the range from a new thread with
/// exclusive_manual_compaction = false, target_level = -1.
/// </summary>
internal sealed class ToIndexQueueWriter : SoakWriterWorker
{
    public const int Indexes = 4, Uids = 1000;
    public readonly int[,] EntrySlot = new int[Indexes, Uids];
    public readonly byte[][] From = new byte[Indexes][], To = new byte[Indexes][];

    public ToIndexQueueWriter(MosaikSoak s) : base(s, "to-index-queue", 500)
    {
        for (int x = 0; x < Indexes; x++)
        {
            byte[] index = IndexUid64(5, x);
            From[x] = Bound(index, 24, 0);
            To[x] = Bound(index, 24, 0xFF);
            for (int u = 0; u < Uids; u++) EntrySlot[x, u] = Slot(MosaikSoak.FToIndex, Concat(index, MosaikSoak.Uid(5, x, u)), SlotKind.Empty);
        }
    }

    private ColumnFamily Queue => Cf(MosaikSoak.FToIndex);

    public IReadOnlyList<int> SlotsOf(int x) => M.SlotsInRange(MosaikSoak.FToIndex, From[x], true, To[x], true);

    public override void Step()
    {
        int x = Rng.Next(Indexes), u = Rng.Next(Uids);
        int r = Rng.Next(100);
        if (r < 40)
        {
            Begin("Put(to-index-queue)");
            PutEmpty(EntrySlot[x, u]);
            Queue.Put(M.Key[EntrySlot[x, u]], ReadOnlySpan<byte>.Empty);
            Commit();
        }
        else if (r < 55)
        {
            Status = "HasKey(to-index-queue)";
            bool has = HasKey(Queue, M.Key[EntrySlot[x, u]]);
            ExpectOwn(EntrySlot[x, u], has, default, valueKnown: false, "RemoveFromToIndexQueue HasKey");
            if (!has) return;
            Begin("Remove(to-index-queue)");
            Delete(EntrySlot[x, u]);
            Queue.Delete(M.Key[EntrySlot[x, u]]);
            Commit();
        }
        else if (r < 63)
        {
            for (int i = 0; i < 20; i++)
            {
                int v = Rng.Next(Uids);
                Begin("Remove(to-index-queue, many)");
                Delete(EntrySlot[x, v]);
                Queue.Delete(M.Key[EntrySlot[x, v]]);
                Commit();
            }
        }
        else if (r < 75) Enumerate(x, 1 + Rng.Next(300));
        else if (r < 78) Clear(x);
        else if (r < 85) Count(x);
        else
        {
            Status = "HasKey(to-index-queue)";
            ExpectOwn(EntrySlot[x, u], HasKey(Queue, M.Key[EntrySlot[x, u]]), default, valueKnown: false, "IsInToIndexQueue");
        }
    }

    private ReadOptions Bounded(int x) => new() { IterateLowerBound = From[x], IterateUpperBound = To[x] };

    private void Enumerate(int x, int limit)
    {
        Status = "EnumerateNextUIDsToIndex (bounded scan)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Queue.NewIterator(Bounded(x)))
        {
            it.SeekToFirst();
            got = Collect(it, forward: true, limit);
        }
        var expected = new List<int>();
        foreach (int slot in SlotsOf(x))
        {
            if (expected.Count == limit) break;
            if (Present(slot)) expected.Add(slot);
        }
        ExpectOwnScan("EnumerateNextUIDsToIndex", expected, got, valuesKnown: true);
    }

    private void Count(int x)
    {
        Status = "GetEstimatedQueueCount (bounded scan)";
        long count = 0;
        using (Iterator it = Queue.NewIterator(Bounded(x)))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) count++;
        }
        long expected = 0;
        foreach (int slot in SlotsOf(x)) if (Present(slot)) expected++;
        if (count != expected) S.Violation(Name + " GetEstimatedQueueCount: " + count + " entries, the model has " + expected);
    }

    private void Clear(int x)
    {
        // ClearToIndexQueue: every key the iterator returns is removed while the iterator goes on (it reads its own view).
        Status = "ClearToIndexQueue (delete while iterating)";
        var expected = new List<int>();
        foreach (int slot in SlotsOf(x)) if (Present(slot)) expected.Add(slot);
        var got = new List<(byte[] Key, byte[] Value)>();
        using (Iterator it = Queue.NewIterator(Bounded(x)))
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                byte[] key = it.Key.ToArray();
                got.Add((key, it.Value.ToArray()));
                if (!M.TryGetSlot(MosaikSoak.FToIndex, key, out int slot))
                {
                    S.Violation(Name + " ClearToIndexQueue: unknown key " + Convert.ToHexString(key));
                    return;
                }
                Begin("Remove(to-index-queue, clear)");
                Delete(slot);
                Queue.Delete(it.Key);
                Commit();
            }
        }
        // The removals above changed the model; compare with the view the iterator was created on.
        int n = 0;
        foreach (int slot in expected)
        {
            if (n >= got.Count || !got[n].Key.AsSpan().SequenceEqual(M.Key[slot]))
            {
                S.Violation(Name + " ClearToIndexQueue: the iterator's entry " + n + " is not slot " + slot + " of its view (" + got.Count + " entries, " + expected.Count + " expected)");
                return;
            }
            n++;
        }
        if (n != got.Count) S.Violation(Name + " ClearToIndexQueue: " + got.Count + " entries, the view had " + expected.Count);
        byte[] from = From[x], to = To[x];
        S.Spawn("Compacting to index queue family after clearing", () =>
            Queue.CompactRange(new CompactRangeOptions { ExclusiveManualCompaction = false, TargetLevel = -1 }, from, to));
    }
}

/// <summary>
/// RocksDbIndexStorage's command score entries (RocksDbIndexStorage.cs:718-885): values of 16-byte UIDs, read with
/// prefix_same_as_start from the 8-byte index prefix; ClearCommandScore removes while iterating and compacts from a thread.
/// </summary>
internal sealed class CommandScoreWriter : SoakWriterWorker
{
    public const int Indexes = 4, Terms = 200;
    public readonly int[,] TermSlot = new int[Indexes, Terms];
    public readonly byte[][] Prefix = new byte[Indexes][];

    public CommandScoreWriter(MosaikSoak s) : base(s, "command-score", 600)
    {
        for (int x = 0; x < Indexes; x++)
        {
            Prefix[x] = IndexUid64(6, x);
            for (int t = 0; t < Terms; t++) TermSlot[x, t] = Slot(MosaikSoak.FCommand, Concat(Prefix[x], System.Text.Encoding.UTF8.GetBytes("term " + (t * 7919 % 1000))));
        }
    }

    private ColumnFamily Entries => Cf(MosaikSoak.FCommand);

    public IReadOnlyList<int> SlotsOf(int x) => M.SlotsInRange(MosaikSoak.FCommand, Prefix[x], true, Bound(Prefix[x], 9, 0xFF), true);

    public override void Step()
    {
        int x = Rng.Next(Indexes), t = Rng.Next(Terms);
        int r = Rng.Next(100);
        if (r < 45)
        {
            Begin("Put(command score)");
            Entries.Put(M.Key[TermSlot[x, t]], PutValue(TermSlot[x, t], 16 * (2 + Rng.Next(11))));
            Commit();
        }
        else if (r < 65)
        {
            Begin("Remove(command score)");
            Delete(TermSlot[x, t]);
            Entries.Delete(M.Key[TermSlot[x, t]]);
            Commit();
        }
        else if (r < 85) GetEntries(x);
        else if (r < 89) Clear(x);
        else TrackChanges();
    }

    private void GetEntries(int x)
    {
        Status = "GetCommandScoreIndexEntries (prefix_same_as_start)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Entries.NewIterator(new ReadOptions { PrefixSameAsStart = true }))
        {
            it.Seek(Prefix[x]);
            got = Collect(it, forward: true);
        }
        foreach ((byte[] k, _) in got)
        {
            if (!k.AsSpan().StartsWith(Prefix[x])) S.Violation(Name + " prefix_same_as_start returned " + Convert.ToHexString(k) + " outside the prefix " + Convert.ToHexString(Prefix[x]));
        }
        ExpectOwnScan("GetCommandScoreIndexEntries", SlotsOf(x), got, valuesKnown: true);
    }

    private void Clear(int x)
    {
        Status = "ClearCommandScore (delete while iterating)";
        byte[] start = Bound(Prefix[x], 12, 0); // new byte[SIZEOF_PREFIX] with the index UID in its first 8 bytes
        using (Iterator it = Entries.NewIterator(new ReadOptions { PrefixSameAsStart = true }))
        {
            for (it.Seek(start); it.Valid; it.Next())
            {
                if (!M.TryGetSlot(MosaikSoak.FCommand, it.Key, out int slot))
                {
                    S.Violation(Name + " ClearCommandScore: unknown key " + Convert.ToHexString(it.Key));
                    return;
                }
                Begin("Remove(command score, clear)");
                Delete(slot);
                Entries.Delete(it.Key);
                Commit();
            }
        }
        foreach (int slot in SlotsOf(x))
        {
            if (Present(slot)) S.Violation(Name + " ClearCommandScore left slot " + slot + " (" + Convert.ToHexString(M.Key[slot]) + ")");
        }
        S.Spawn("Compacting command score index family after clearing", () => Entries.CompactRange(null, null));
    }
}

/// <summary>
/// RocksDbDataStorage's vectors (RocksDbDataStorage.cs:303-600, 995-1060): 28- and 30-byte keys under an 8-byte index prefix,
/// read with a 16 MiB readahead over bounds; hashes; clearing an index by deleting while iterating.
/// </summary>
internal sealed class VectorWriter : SoakWriterWorker
{
    public const int Indexes = 3, Vectors = 600, Hashes = 300, WideUids = 300;
    public readonly int[,] VectorSlot = new int[Indexes, Vectors];
    public readonly int[,] HashSlot = new int[Indexes, Hashes];
    public readonly int[,] WideSlot = new int[WideUids, 2];
    public readonly byte[][] Prefix = new byte[Indexes + 1][];

    public VectorWriter(MosaikSoak s) : base(s, "vectors", 700)
    {
        for (int x = 0; x <= Indexes; x++) Prefix[x] = IndexUid64(7, x);
        for (int x = 0; x < Indexes; x++)
        {
            for (int i = 0; i < Vectors; i++) VectorSlot[x, i] = Slot(MosaikSoak.FVectors, Concat(Int32Key(Prefix[x], i), MosaikSoak.Uid(7, x, i)));
            for (int i = 0; i < Hashes; i++) HashSlot[x, i] = Slot(MosaikSoak.FVectorHashes, Concat(Prefix[x], MosaikSoak.Uid(8, x, i)));
        }
        for (int i = 0; i < WideUids; i++)
        {
            for (int vi = 0; vi < 2; vi++)
            {
                byte[] k = Concat(Int32Key(Prefix[Indexes], i), MosaikSoak.Uid(7, Indexes, i));
                var wide = new byte[k.Length + 2];
                k.CopyTo(wide, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(wide.AsSpan(k.Length), (ushort)vi);
                WideSlot[i, vi] = Slot(MosaikSoak.FVectors, wide);
            }
        }
    }

    private ColumnFamily Vecs => Cf(MosaikSoak.FVectors);

    public (byte[] From, byte[] To) Range(int x) => x < Indexes ? (Bound(Prefix[x], 28, 0), Bound(Prefix[x], 28, 0xFF)) : (Bound(Prefix[x], 30, 0), Bound(Prefix[x], 30, 0xFF));

    public override void Step()
    {
        int x = Rng.Next(Indexes), i = Rng.Next(Vectors);
        int r = Rng.Next(100);
        if (r < 35)
        {
            Begin("Put(data-vectors)");
            Vecs.Put(M.Key[VectorSlot[x, i]], PutValue(VectorSlot[x, i], 4 * (64 << Rng.Next(3)) + 4));
            Commit();
            TrackChanges();
        }
        else if (r < 45)
        {
            int u = Rng.Next(WideUids), vi = Rng.Next(2);
            Begin("Put(data-vectors with vector index)");
            Vecs.Put(M.Key[WideSlot[u, vi]], PutValue(WideSlot[u, vi], 4 * 128 + 4));
            Commit();
            TrackChanges();
        }
        else if (r < 55)
        {
            int h = Rng.Next(Hashes);
            Begin("Put(data-vectors-hashes)");
            Cf(MosaikSoak.FVectorHashes).Put(M.Key[HashSlot[x, h]], PutValue(HashSlot[x, h], 32));
            Commit();
        }
        else if (r < 63)
        {
            Begin("Remove(data-vectors)");
            Delete(VectorSlot[x, i]);
            Vecs.Delete(M.Key[VectorSlot[x, i]]);
            Commit();
        }
        else if (r < 78) ReadVectors(Rng.Next(Indexes + 1));
        else if (r < 80) ClearIndex(x);
        else
        {
            Status = "Get(data-vectors)";
            byte[]? v = Vecs.Get(M.Key[VectorSlot[x, i]]);
            ExpectOwn(VectorSlot[x, i], v is not null, v, valueKnown: true, "GetVector");
        }
    }

    private void ReadVectors(int x)
    {
        (byte[] from, byte[] to) = Range(x);
        Status = "ReadVectors (bounds, readahead 16 MiB)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Vecs.NewIterator(new ReadOptions { IterateLowerBound = from, IterateUpperBound = to, ReadaheadSize = 16UL * 1024 * 1024 }))
        {
            it.SeekToFirst();
            got = Collect(it, forward: true);
        }
        ExpectOwnScan("ReadVectors", M.SlotsInRange(MosaikSoak.FVectors, from, true, to, true), got, valuesKnown: true);
    }

    private void ClearIndex(int x)
    {
        (byte[] from, byte[] to) = Range(x);
        Status = "clear vectors (delete while iterating)";
        using (Iterator it = Vecs.NewIterator(new ReadOptions { IterateLowerBound = from, IterateUpperBound = to }))
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (!M.TryGetSlot(MosaikSoak.FVectors, it.Key, out int slot))
                {
                    S.Violation(Name + " clear vectors: unknown key " + Convert.ToHexString(it.Key));
                    return;
                }
                Begin("Remove(data-vectors, clear)");
                Delete(slot);
                Vecs.Delete(it.Key);
                Commit();
            }
        }
        byte[] hashFrom = Prefix[x], hashTo = Bound(Prefix[x], 26, 0xFF);
        Status = "clear vector hashes (delete while iterating)";
        using (Iterator it = Cf(MosaikSoak.FVectorHashes).NewIterator(new ReadOptions { IterateLowerBound = hashFrom, IterateUpperBound = hashTo }))
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (!M.TryGetSlot(MosaikSoak.FVectorHashes, it.Key, out int slot))
                {
                    S.Violation(Name + " clear vector hashes: unknown key " + Convert.ToHexString(it.Key));
                    return;
                }
                Begin("Remove(data-vectors-hashes, clear)");
                Delete(slot);
                Cf(MosaikSoak.FVectorHashes).Delete(it.Key);
                Commit();
            }
        }
        foreach (int slot in M.SlotsInRange(MosaikSoak.FVectors, from, true, to, true))
        {
            if (Present(slot)) S.Violation(Name + " clear vectors left slot " + slot);
        }
    }
}

/// <summary>
/// RocksDbIndexStorage's simple text index (RocksDbIndexStorage.cs:430-715): WriteKeyEntryUID's batch of deletes under the
/// other key entries and puts under the new ones (empty values), key entries, bounded reads, ClearSimpleTextIndex.
/// </summary>
internal sealed class TextIndexWriter : SoakWriterWorker
{
    public const int Indexes = 3, KeyEntries = 6, Uids = 120;
    public readonly int[,,] EntrySlot = new int[Indexes, KeyEntries, Uids];
    public readonly int[,] KeySlot = new int[Indexes, KeyEntries];
    public readonly byte[][] Prefix = new byte[Indexes][];

    public TextIndexWriter(MosaikSoak s) : base(s, "simple-text", 800)
    {
        for (int x = 0; x < Indexes; x++)
        {
            Prefix[x] = IndexUid64(8, x);
            for (int k = 0; k < KeyEntries; k++)
            {
                byte[] entryPrefix = Int32Key(Prefix[x], k);
                KeySlot[x, k] = Slot(MosaikSoak.FTextKeys, entryPrefix);
                for (int u = 0; u < Uids; u++) EntrySlot[x, k, u] = Slot(MosaikSoak.FTextEntries, Concat(entryPrefix, MosaikSoak.Uid(9, x, u)), SlotKind.Empty);
            }
        }
    }

    public override void Step()
    {
        int x = Rng.Next(Indexes), k = Rng.Next(KeyEntries), u = Rng.Next(Uids);
        int r = Rng.Next(100);
        if (r < 45) WriteKeyEntryUid(x, [k], u);
        else if (r < 55) WriteKeyEntryUid(x, [k, (k + 1 + Rng.Next(KeyEntries - 1)) % KeyEntries], u);
        else if (r < 65)
        {
            Begin("Put(simple-text-index-keys)");
            Cf(MosaikSoak.FTextKeys).Put(M.Key[KeySlot[x, k]], PutValue(KeySlot[x, k], 30 + Rng.Next(50)));
            Commit();
        }
        else if (r < 75) ReadKeyEntries(x);
        else if (r < 88) ReadEntries(x, k);
        else if (r < 90) Clear(x);
        else
        {
            Status = "HasKey(simple-text-index-entries)";
            ExpectOwn(EntrySlot[x, k, u], HasKey(Cf(MosaikSoak.FTextEntries), M.Key[EntrySlot[x, k, u]]), default, valueKnown: false, "HasKey(text entry)");
        }
    }

    private void WriteKeyEntryUid(int x, int[] keyEntries, int u)
    {
        using (var batch = new WriteBatch())
        {
            Begin("Write(simple text entries)");
            for (int k = 0; k < KeyEntries; k++)
            {
                if (Array.IndexOf(keyEntries, k) >= 0) continue;
                batch.Delete(Cf(MosaikSoak.FTextEntries), M.Key[EntrySlot[x, k, u]]);
                Delete(EntrySlot[x, k, u]);
            }
            foreach (int k in keyEntries)
            {
                batch.Put(Cf(MosaikSoak.FTextEntries), M.Key[EntrySlot[x, k, u]], ReadOnlySpan<byte>.Empty);
                PutEmpty(EntrySlot[x, k, u]);
            }
            S.Db.Write(batch);
            Commit();
        }
        TrackChanges();
    }

    private void ReadKeyEntries(int x)
    {
        byte[] from = Bound(Prefix[x], 28, 0), to = Bound(Prefix[x], 28, 0xFF);
        Status = "ReadKeyEntries (bounded scan)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Cf(MosaikSoak.FTextKeys).NewIterator(new ReadOptions { IterateLowerBound = from, IterateUpperBound = to }))
        {
            it.SeekToFirst();
            got = Collect(it, forward: true);
        }
        ExpectOwnScan("ReadKeyEntries", M.SlotsInRange(MosaikSoak.FTextKeys, from, true, to, true), got, valuesKnown: true);
    }

    private void ReadEntries(int x, int k)
    {
        byte[] entryPrefix = Int32Key(Prefix[x], k);
        byte[] from = Bound(entryPrefix, 28, 0), to = Bound(entryPrefix, 28, 0xFF);
        Status = "read text entries (bounded scan)";
        List<(byte[] Key, byte[] Value)> got;
        using (Iterator it = Cf(MosaikSoak.FTextEntries).NewIterator(new ReadOptions { IterateLowerBound = from, IterateUpperBound = to }))
        {
            it.SeekToFirst();
            got = Collect(it, forward: true);
        }
        ExpectOwnScan("read text entries", M.SlotsInRange(MosaikSoak.FTextEntries, from, true, to, true), got, valuesKnown: true);
    }

    private void Clear(int x)
    {
        byte[] from = Bound(Prefix[x], 28, 0), to = Bound(Prefix[x], 28, 0xFF);
        ClearRange(MosaikSoak.FTextEntries, from, to);
        S.Spawn("Compacting simple text family after clearing", () => Cf(MosaikSoak.FTextEntries).CompactRange(from, to));
        byte[] keysFrom = Bound(Prefix[x], 12, 0), keysTo = Bound(Prefix[x], 12, 0xFF);
        ClearRange(MosaikSoak.FTextKeys, keysFrom, keysTo);
        S.Spawn("Compacting simple text keys family after clearing", () => Cf(MosaikSoak.FTextKeys).CompactRange(keysFrom, keysTo));
    }

    private void ClearRange(int family, byte[] from, byte[] to)
    {
        Status = "ClearSimpleTextIndex (delete while iterating)";
        using Iterator it = Cf(family).NewIterator(new ReadOptions { IterateLowerBound = from, IterateUpperBound = to });
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            if (!M.TryGetSlot(family, it.Key, out int slot))
            {
                S.Violation(Name + " ClearSimpleTextIndex: unknown key " + Convert.ToHexString(it.Key));
                return;
            }
            Begin("Remove(simple text, clear)");
            Delete(slot);
            Cf(family).Delete(it.Key);
            Commit();
        }
    }
}

/// <summary>
/// The smaller families: the deprecated change log and ClearDeprecatedChangeLog's DeleteFilesInRange (include_end) +
/// DeleteRange [0^8, FF^8) + CompactRange (RocksDbStorage.cs:1720-1763); data-by-path storage (RocksDbDataStorage.cs:220-260,
/// 800-845: path, length and pages in one batch, old pages beyond the new length left as Mosaik leaves them); schemas,
/// the deletion queue, changeduidslog and graphsettings values.
/// </summary>
internal sealed class MiscWriter : SoakWriterWorker
{
    public const int ChangeLog = 800, DataPaths = 80, MaxDataPages = 4, Schemas = 20, Deletions = 100, ChangedUids = 20;
    public readonly int DataPageSize;
    public readonly int[] ChangeLogSlot = new int[ChangeLog], DataPathSlot = new int[DataPaths], DataLengthSlot = new int[DataPaths];
    public readonly int[,] DataPageSlot = new int[DataPaths, MaxDataPages];
    private readonly int[] _nodeSchema = new int[Schemas], _edgeSchema = new int[Schemas], _deletion = new int[Deletions], _changed = new int[ChangedUids], _settings = new int[2];
    private readonly long[] _dataLength = new long[DataPaths];
    private static readonly byte[] s_changeLogStart = new byte[8], s_changeLogEnd = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

    public MiscWriter(MosaikSoak s, double scale) : base(s, "misc", 900)
    {
        DataPageSize = Math.Max(512, (int)(16384 * scale));
        for (int i = 0; i < ChangeLog; i++)
        {
            var k = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(k, i * 13L + 1); // KeyAsVersion: the version bytes reversed
            ChangeLogSlot[i] = Slot(MosaikSoak.FChangeLog, k);
        }
        for (int p = 0; p < DataPaths; p++)
        {
            byte[] uid = MosaikSoak.Uid(10, 0, p);
            DataPathSlot[p] = Slot(MosaikSoak.FDataPaths, System.Text.Encoding.UTF8.GetBytes("data/" + p + "/payload.bin"));
            DataLengthSlot[p] = Slot(MosaikSoak.FDataLength, uid);
            for (int i = 0; i < MaxDataPages; i++) DataPageSlot[p, i] = Slot(MosaikSoak.FDataPages, Int32Key(uid, i));
            _dataLength[p] = -1;
        }
        for (int i = 0; i < Schemas; i++)
        {
            _nodeSchema[i] = Slot(MosaikSoak.FNodeSchemas, Int32Key([], 1000 + i));
            _edgeSchema[i] = Slot(MosaikSoak.FEdgeSchemas, Int32Key([], 2000 + i));
        }
        for (int i = 0; i < Deletions; i++) _deletion[i] = Slot(MosaikSoak.FDeletion, MosaikSoak.Uid(11, 0, i));
        for (int i = 0; i < ChangedUids; i++) _changed[i] = Slot(MosaikSoak.FChangedUids, MosaikSoak.Uid(12, 0, i));
        _settings[0] = Slot(MosaikSoak.FSettings, "last-nodes-count"u8.ToArray());
        _settings[1] = Slot(MosaikSoak.FSettings, "last-start-timings"u8.ToArray());
    }

    public long DataLength(int slot, int version)
    {
        ulong h = SoakValue.Mix(((ulong)(uint)slot << 32) | (uint)version ^ 0xDA7AUL);
        int pages = 1 + (int)(h % MaxDataPages);
        int last = 64 + (int)((h >> 20) % (ulong)(DataPageSize - 64));
        return (long)(pages - 1) * DataPageSize + last;
    }

    public int Pages(long length) => length <= 0 ? 0 : (int)((length + DataPageSize - 1) / DataPageSize);

    public override void Step()
    {
        int r = Rng.Next(1000);
        if (r < 300) PutOne(ChangeLogSlot[Rng.Next(ChangeLog)], 30 + Rng.Next(170));
        else if (r < 350)
        {
            int slot = ChangeLogSlot[Rng.Next(ChangeLog)];
            Begin("Remove(changelog)");
            Delete(slot);
            Cf(MosaikSoak.FChangeLog).Delete(M.Key[slot]);
            Commit();
        }
        else if (r < 362) ClearDeprecatedChangeLog();
        else if (r < 540) StoreData(Rng.Next(DataPaths));
        else if (r < 590) DeleteData(Rng.Next(DataPaths));
        else if (r < 690) PutOne(Rng.Next(2) == 0 ? _nodeSchema[Rng.Next(Schemas)] : _edgeSchema[Rng.Next(Schemas)], 200 + Rng.Next(800));
        else if (r < 710) RemoveOne(Rng.Next(2) == 0 ? _nodeSchema[Rng.Next(Schemas)] : _edgeSchema[Rng.Next(Schemas)]);
        else if (r < 770) PutOne(_deletion[Rng.Next(Deletions)], 40);
        else if (r < 810) RemoveOne(_deletion[Rng.Next(Deletions)]);
        else if (r < 830) PutOne(_changed[Rng.Next(ChangedUids)], 30);
        else if (r < 870) PutOne(_settings[Rng.Next(2)], 19 + Rng.Next(40));
        else OwnRead();
    }

    private void PutOne(int slot, int length)
    {
        Begin("Put(" + FamilyName(slot) + ")");
        Cf(M.Family[slot]).Put(M.Key[slot], PutValue(slot, length));
        Commit();
    }

    private void RemoveOne(int slot)
    {
        Begin("Remove(" + FamilyName(slot) + ")");
        Delete(slot);
        Cf(M.Family[slot]).Delete(M.Key[slot]);
        Commit();
    }

    private void ClearDeprecatedChangeLog()
    {
        Interlocked.Increment(ref M.ClearEpoch);
        try
        {
            Begin("DeleteFilesInRange(changelog, include_end)");
            foreach (int slot in ChangeLogSlot) Delete(slot);
            Cf(MosaikSoak.FChangeLog).DeleteFilesInRange(s_changeLogStart, s_changeLogEnd, includeEnd: true);
            Status = "DeleteRange(changelog)";
            Cf(MosaikSoak.FChangeLog).DeleteRange(new WriteOptions(), s_changeLogStart, s_changeLogEnd);
            Commit();
        }
        finally
        {
            Interlocked.Increment(ref M.ClearEpoch);
        }
        long start = Stopwatch.GetTimestamp();
        Status = "CompactRange(changelog, null, null)";
        Cf(MosaikSoak.FChangeLog).CompactRange(null, null);
        S.NoteHorizon(MosaikSoak.FChangeLog, start);
        S.Count("changelog clears");
    }

    private void StoreData(int p)
    {
        using var batch = new WriteBatch();
        Begin("Write(data path, length, pages)");
        batch.Put(Cf(MosaikSoak.FDataPaths), M.Key[DataPathSlot[p]], PutValue(DataPathSlot[p], SoakValue.HeaderSize + 16));
        batch.Put(Cf(MosaikSoak.FDataLength), M.Key[DataLengthSlot[p]], PutValue(DataLengthSlot[p], SoakValue.HeaderSize + 8));
        long length = DataLength(DataLengthSlot[p], M.Version[DataLengthSlot[p]]);
        int pages = Pages(length);
        for (int i = 0; i < pages; i++)
        {
            int size = i < pages - 1 ? DataPageSize : (int)(length - (long)(pages - 1) * DataPageSize);
            batch.Put(Cf(MosaikSoak.FDataPages), M.Key[DataPageSlot[p, i]], PutValue(DataPageSlot[p, i], size));
        }
        S.Db.Write(batch);
        Commit();
        _dataLength[p] = length;
    }

    private void DeleteData(int p)
    {
        Status = "Get(data-paths)";
        byte[]? key = Cf(MosaikSoak.FDataPaths).Get(M.Key[DataPathSlot[p]]);
        ExpectOwn(DataPathSlot[p], key is not null, key, valueKnown: true, "TryDelete Get(path)");
        if (key is null) return;
        Status = "Get(data-length)";
        byte[]? length = Cf(MosaikSoak.FDataLength).Get(M.Key[DataLengthSlot[p]]);
        ExpectOwn(DataLengthSlot[p], length is not null, length, valueKnown: true, "TryDelete Get(length)");
        if (length is null) return;
        using var batch = new WriteBatch();
        Begin("Write(data delete)");
        batch.Delete(Cf(MosaikSoak.FDataPaths), M.Key[DataPathSlot[p]]);
        Delete(DataPathSlot[p]);
        batch.Delete(Cf(MosaikSoak.FDataLength), M.Key[DataLengthSlot[p]]);
        Delete(DataLengthSlot[p]);
        for (int i = 0; i < Pages(_dataLength[p]); i++)
        {
            batch.Delete(Cf(MosaikSoak.FDataPages), M.Key[DataPageSlot[p, i]]);
            Delete(DataPageSlot[p, i]);
        }
        S.Db.Write(batch);
        Commit();
        _dataLength[p] = -1;
    }

    private void OwnRead()
    {
        int slot = Rng.Next(4) switch
        {
            0 => ChangeLogSlot[Rng.Next(ChangeLog)],
            1 => DataPageSlot[Rng.Next(DataPaths), Rng.Next(MaxDataPages)],
            2 => _nodeSchema[Rng.Next(Schemas)],
            _ => _deletion[Rng.Next(Deletions)],
        };
        Status = "Get(" + FamilyName(slot) + ")";
        byte[]? v = Cf(M.Family[slot]).Get(M.Key[slot]);
        // DeleteFilesInRange may expose an older version until the DeleteRange that follows it lands; both are this
        // worker's, so its own reads of the change log are exact again once the clear returned.
        ExpectOwn(slot, v is not null, v, valueKnown: true, "Get(" + FamilyName(slot) + ")");
    }
}

/// <summary>
/// RocksDbBase.ClearReplicationChangesAlreadyReplicated (RocksDbBase.cs:325-364): one Remove per acknowledged tracker
/// version, the acknowledgement Put into graphsettings, and CompactRange(0, ack) on the tracker every so many versions.
/// The acknowledged version stands in for the lowest version every replica reported; it never passes the versions whose
/// tracker Put is still in flight.
/// </summary>
internal sealed class TrackerAckWriter : SoakWriterWorker
{
    public readonly int AckSlot;
    private long _lastAck, _compactAt = 2000;

    public TrackerAckWriter(MosaikSoak s) : base(s, "tracker-ack", 1000) => AckSlot = Slot(MosaikSoak.FSettings, SoakModel.VersionAckKey, SlotKind.RawCounter);

    public override void Step()
    {
        Status = "waiting for replicas";
        if (Wait(TimeSpan.FromMilliseconds(20 + Rng.Next(60)))) return;
        long completed = M.TrackerCompletedWatermark();
        long ack = Math.Max(_lastAck, completed - Rng.Next(64));
        if (ack <= _lastAck) return;
        Volatile.Write(ref M.TrackerRemoving, ack);
        for (long v = _lastAck; v < ack; v++)
        {
            int op = Begin("Remove(replica-change-tracker)");
            M.TrackerRemovalOp[v] = op;
            Cf(MosaikSoak.FTracker).Delete(SoakModel.TrackerKey(v));
            Commit();
        }
        Begin("Put(graphsettings ack key)");
        W.Change(M, AckSlot, delete: false, raw: ack);
        Cf(MosaikSoak.FSettings).Put(SoakModel.VersionAckKey, MosaikSoak.Le64(ack));
        Commit();
        _lastAck = ack;
        Volatile.Write(ref M.TrackerAck, ack);
        if (ack > _compactAt)
        {
            Status = "CompactRange(replica-change-tracker, 0, ack)";
            Cf(MosaikSoak.FTracker).CompactRange(SoakModel.TrackerKey(0), SoakModel.TrackerKey(ack));
            _compactAt = ack + 2000;
            S.Count("tracker compactions");
        }
    }
}

/// <summary>
/// Readers in Mosaik's shapes, checking every observation against the model: point reads (Get, HasKey, TryGet into a
/// fixed buffer, GetPooled), node / blob / data batches under one snapshot, bounded and prefix scans (with and without a
/// snapshot), SeekToLast + Prev, the replica's tracker scan, the graphsettings version key, scans against point reads
/// under one snapshot.
/// </summary>
internal sealed class ReaderWorker : SoakWorker
{
    private readonly NodeWriter[] _nodes;
    private readonly BlobWriter _blobs;
    private readonly MiscWriter _misc;
    private readonly ToIndexQueueWriter _toIndex;
    private readonly CommandScoreWriter _command;
    private readonly VectorWriter _vectors;
    private readonly TextIndexWriter _text;
    private readonly LuceneWriter _lucene;
    private readonly byte[] _fixed = new byte[65536 + 64], _tiny = new byte[48];

    public ReaderWorker(MosaikSoak s, int index) : base(s, "reader-" + index, 2000 + index)
    {
        _nodes = s.Workers.OfType<NodeWriter>().ToArray();
        _blobs = s.Workers.OfType<BlobWriter>().Single();
        _misc = s.Workers.OfType<MiscWriter>().Single();
        _toIndex = s.Workers.OfType<ToIndexQueueWriter>().Single();
        _command = s.Workers.OfType<CommandScoreWriter>().Single();
        _vectors = s.Workers.OfType<VectorWriter>().Single();
        _text = s.Workers.OfType<TextIndexWriter>().Single();
        _lucene = s.Workers.OfType<LuceneWriter>().Single();
    }

    private void Check(string what, int slot, in MosaikSoak.SlotBefore b, bool found, ReadOnlySpan<byte> value, bool valueKnown = true)
    {
        if (S.CheckRead(slot, b, found, value, valueKnown) is string e) S.Violation(Name + " " + what + ": " + e);
    }

    public override void Step()
    {
        int r = Rng.Next(100);
        if (r < 30) PointRead(Rng.Next(M.SlotCount));
        else if (r < 40) NodeBatchUnderSnapshot();
        else if (r < 45) BlobUnderSnapshot();
        else if (r < 48) DataUnderSnapshot();
        else if (r < 70) RangeScan();
        else if (r < 74) CheckedScan("EnumerateFileQueue", MosaikSoak.FQueue, null, null, null, null, false, reverse: true, 0, Rng.Next(2) == 0);
        else if (r < 82) TrackerScan();
        else if (r < 85) VersionKey();
        else if (r < 95) ScanVersusGets();
        else CheckedScan("file queue stats", MosaikSoak.FQueueStats, null, null, null, null, false, reverse: false, 0, false);
    }

    private void PointRead(int slot)
    {
        ColumnFamily cf = S.Cf[M.Family[slot]];
        byte[] key = M.Key[slot];
        MosaikSoak.SlotBefore b = S.Before(slot);
        switch (Rng.Next(5))
        {
            case 0:
            {
                Status = "Get";
                byte[]? v = cf.Get(key);
                Check("Get", slot, b, v is not null, v);
                break;
            }
            case 1:
                Status = "HasKey";
                Check("HasKey", slot, b, cf.TryGet(null, key, 0, static (_, _) => true, out _), default, valueKnown: false);
                break;
            case 2:
            {
                Status = "TryGet(fixed buffer)";
                bool found = cf.TryGet(null, key, _fixed, out int length);
                Check("TryGet(fixed buffer)", slot, b, found, found ? _fixed.AsSpan(0, Math.Min(length, _fixed.Length)) : default, valueKnown: length <= _fixed.Length);
                break;
            }
            case 3:
            {
                Status = "TryGet(short buffer, shared options)";
                bool found = cf.TryGet(NodeWriter.RocksDBExtDefaultReadOptions, key, _tiny, out int length);
                Check("TryGet(short buffer)", slot, b, found, found ? _tiny.AsSpan(0, Math.Min(length, _tiny.Length)) : default, valueKnown: length <= _tiny.Length);
                break;
            }
            default:
            {
                Status = "GetPooled";
                bool found = cf.TryGet(null, key, 0, static (value, _) => value.ToArray(), out byte[] v);
                Check("GetPooled", slot, b, found, v);
                break;
            }
        }
    }

    /// <summary>Reads <paramref name="slots"/> under one snapshot, checks each, and returns the values (null = absent).</summary>
    private byte[]?[] UnderSnapshot(string what, int[] slots)
    {
        var befores = new MosaikSoak.SlotBefore[slots.Length];
        for (int i = 0; i < slots.Length; i++) befores[i] = S.Before(slots[i]);
        var values = new byte[]?[slots.Length];
        Status = what + " under a snapshot";
        using (Snapshot snap = S.Db.GetSnapshot())
        {
            var ro = new ReadOptions { Snapshot = snap };
            for (int i = 0; i < slots.Length; i++) values[i] = S.Cf[M.Family[slots[i]]].Get(ro, M.Key[slots[i]]);
        }
        for (int i = 0; i < slots.Length; i++) Check(what, slots[i], befores[i], values[i] is not null, values[i]);
        return values;
    }

    private static int OpOf(byte[] value) => SoakValue.TryParse(value, out _, out _, out _, out int op) ? op : -1;

    private static int VersionOf(byte[] value) => SoakValue.TryParse(value, out _, out int version, out _, out _) ? version : -1;

    private void NodeBatchUnderSnapshot()
    {
        NodeWriter w = _nodes[Rng.Next(_nodes.Length)];
        int n = Rng.Next(NodeWriter.Nodes);
        byte[]?[] v = UnderSnapshot("node batch", [w.NodesSlot[n], w.TypesSlot[n], w.VersionSlot[n]]);
        int present = (v[0] is null ? 0 : 1) + (v[1] is null ? 0 : 1) + (v[2] is null ? 0 : 1);
        if (present is not (0 or 3)) S.Violation(Name + " node batch torn under a snapshot: " + w.Name + " node " + n + " nodes/types/nodeversion present " + (v[0] is not null) + "/" + (v[1] is not null) + "/" + (v[2] is not null));
        else if (present == 3 && (OpOf(v[0]!) != OpOf(v[1]!) || OpOf(v[1]!) != OpOf(v[2]!)))
            S.Violation(Name + " node batch torn under a snapshot: " + w.Name + " node " + n + " written by ops " + OpOf(v[0]!) + "/" + OpOf(v[1]!) + "/" + OpOf(v[2]!));
    }

    private void BlobUnderSnapshot()
    {
        int b = Rng.Next(BlobWriter.Blobs);
        var slots = new int[1 + BlobWriter.MaxPages];
        slots[0] = _blobs.LengthSlot[b];
        for (int p = 0; p < BlobWriter.MaxPages; p++) slots[1 + p] = _blobs.PageSlot[b, p];
        byte[]?[] v = UnderSnapshot("blob", slots);
        int pages = 0, op = -1;
        long length = 0;
        if (v[0] is { } l)
        {
            length = _blobs.BlobLength(slots[0], VersionOf(l));
            pages = _blobs.Pages(length);
            op = OpOf(l);
        }
        for (int p = 0; p < BlobWriter.MaxPages; p++)
        {
            byte[]? page = v[1 + p];
            if (p < pages)
            {
                int size = p < pages - 1 ? _blobs.PageSize : (int)(length - (long)(pages - 1) * _blobs.PageSize);
                if (page is null || OpOf(page) != op || page.Length != size)
                    S.Violation(Name + " blob " + b + " torn under a snapshot: length " + length + " (op " + op + "), page " + p + (page is null ? " missing" : " from op " + OpOf(page) + ", " + page.Length + " bytes"));
            }
            else if (page is not null)
            {
                S.Violation(Name + " blob " + b + " torn under a snapshot: length " + (v[0] is null ? "missing" : length.ToString()) + ", page " + p + " present (op " + OpOf(page) + ")");
            }
        }
    }

    private void DataUnderSnapshot()
    {
        int p = Rng.Next(MiscWriter.DataPaths);
        var slots = new int[2 + MiscWriter.MaxDataPages];
        slots[0] = _misc.DataPathSlot[p];
        slots[1] = _misc.DataLengthSlot[p];
        for (int i = 0; i < MiscWriter.MaxDataPages; i++) slots[2 + i] = _misc.DataPageSlot[p, i];
        byte[]?[] v = UnderSnapshot("data path", slots);
        if ((v[0] is null) != (v[1] is null) || (v[0] is not null && OpOf(v[0]!) != OpOf(v[1]!)))
        {
            S.Violation(Name + " data path " + p + " torn under a snapshot: path " + (v[0] is null ? "missing" : "op " + OpOf(v[0]!)) + ", length " + (v[1] is null ? "missing" : "op " + OpOf(v[1]!)));
            return;
        }
        if (v[1] is null) return;
        int pages = _misc.Pages(_misc.DataLength(slots[1], VersionOf(v[1]!)));
        for (int i = 0; i < pages; i++)
        {
            if (v[2 + i] is not { } page || OpOf(page) != OpOf(v[1]!))
                S.Violation(Name + " data path " + p + " torn under a snapshot: page " + i + " of " + pages + (v[2 + i] is null ? " missing" : " from op " + OpOf(v[2 + i]!)) + ", length from op " + OpOf(v[1]!));
        }
    }

    private void RangeScan()
    {
        bool snap = Rng.Next(2) == 0;
        switch (Rng.Next(8))
        {
            case 0:
            {
                int x = Rng.Next(ToIndexQueueWriter.Indexes);
                CheckedScan("to-index-queue bounded scan", MosaikSoak.FToIndex, _toIndex.From[x], _toIndex.To[x], null, null, false, false, 0, snap);
                break;
            }
            case 1:
            {
                int x = Rng.Next(CommandScoreWriter.Indexes);
                CheckedScan("command score prefix_same_as_start", MosaikSoak.FCommand, null, null, _command.Prefix[x], null, true, false, 0, snap);
                break;
            }
            case 2:
            {
                (byte[] from, byte[] to) = _vectors.Range(Rng.Next(VectorWriter.Indexes + 1));
                CheckedScan("data-vectors bounded scan, readahead 16 MiB", MosaikSoak.FVectors, from, to, null, null, false, false, 16UL * 1024 * 1024, snap);
                break;
            }
            case 3:
            {
                byte[] prefix = _vectors.Prefix[Rng.Next(VectorWriter.Indexes)];
                var to = new byte[26];
                to.AsSpan().Fill(0xFF);
                prefix.CopyTo(to, 0);
                CheckedScan("data-vectors-hashes bounded scan", MosaikSoak.FVectorHashes, prefix, to, null, null, false, false, 0, snap);
                break;
            }
            case 4:
            {
                int x = Rng.Next(TextIndexWriter.Indexes), k = Rng.Next(TextIndexWriter.KeyEntries);
                var from = new byte[28];
                var to = new byte[28];
                to.AsSpan().Fill(0xFF);
                _text.Prefix[x].CopyTo(from, 0);
                _text.Prefix[x].CopyTo(to, 0);
                BinaryPrimitives.WriteInt32LittleEndian(from.AsSpan(8), k);
                BinaryPrimitives.WriteInt32LittleEndian(to.AsSpan(8), k);
                CheckedScan("simple-text entries bounded scan", MosaikSoak.FTextEntries, from, to, null, null, false, false, 0, snap);
                break;
            }
            case 5:
            {
                byte[] prefix = System.Text.Encoding.UTF8.GetBytes(LuceneWriter.DirectoryOf(Rng.Next(LuceneWriter.Dirs)));
                CheckedScan("lucene directory listing", MosaikSoak.FLuceneLength, null, null, prefix, prefix, false, false, 0, snap);
                break;
            }
            default:
            {
                // A bounded range of a plain family (no prefix extractor): the node families, edges, content, pages.
                int[] families = [MosaikSoak.FNodes, MosaikSoak.FEdges, MosaikSoak.FContent, MosaikSoak.FBlobPages, MosaikSoak.FLucenePages, MosaikSoak.FTypes];
                int f = families[Rng.Next(families.Length)];
                int[] sorted = M.Sorted[f];
                int a = Rng.Next(sorted.Length), c = Math.Min(sorted.Length - 1, a + 1 + Rng.Next(400));
                CheckedScan("bounded scan of " + MosaikProfile.Families[f].Name, f, M.Key[sorted[a]], M.Key[sorted[c]], null, null, false, Rng.Next(4) == 0, 0, snap);
                break;
            }
        }
    }

    private int IndexIn(ArraySegment<int> range, ReadOnlySpan<byte> key)
    {
        int lo = 0, hi = range.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = M.Key[range[mid]].AsSpan().SequenceCompareTo(key);
            if (c == 0) return mid;
            if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -1;
    }

    private ArraySegment<int> PrefixRange(int family, byte[] prefix)
    {
        ArraySegment<int> from = M.SlotsInRange(family, prefix, true, default, false);
        int n = 0;
        while (n < from.Count && M.Key[from[n]].AsSpan().StartsWith(prefix)) n++;
        return from.Slice(0, n);
    }

    /// <summary>
    /// One scan with Mosaik's options, every entry checked: inside the bounds (or the prefix), strictly ordered, a known
    /// key of the range, a valid value within its window; every slot of the range it did not return must be allowed to
    /// be absent.
    /// </summary>
    private void CheckedScan(string what, int family, byte[]? lower, byte[]? upper, byte[]? seek, byte[]? takeWhilePrefix, bool prefixSameAsStart,
        bool reverse, ulong readahead, bool snapshot)
    {
        ArraySegment<int> range = seek is not null ? PrefixRange(family, takeWhilePrefix ?? seek) : M.SlotsInRange(family, lower ?? [], lower is not null, upper ?? [], upper is not null);
        var befores = new MosaikSoak.SlotBefore[range.Count];
        for (int i = 0; i < range.Count; i++) befores[i] = S.Before(range[i]);
        var seen = new bool[range.Count];
        var returned = new List<byte[]>();
        ulong sequence = 0;
        Status = what + (snapshot ? " under a snapshot" : "");
        Snapshot? snap = snapshot ? S.Db.GetSnapshot() : null;
        try
        {
            // Not "= lower": a null byte[] converts to an empty ReadOnlyMemory, i.e. an empty (non-null) bound that excludes
            // every key (RocksDbManagedCompat.Bound guards against the same conversion).
            var ro = new ReadOptions
            {
                Snapshot = snap,
                IterateLowerBound = lower is null ? (ReadOnlyMemory<byte>?)null : lower,
                IterateUpperBound = upper is null ? (ReadOnlyMemory<byte>?)null : upper,
                PrefixSameAsStart = prefixSameAsStart,
                ReadaheadSize = readahead,
            };
            using Iterator it = S.Cf[family].NewIterator(ro);
            sequence = it.Sequence;
            if (reverse) it.SeekToLast();
            else if (seek is not null) it.Seek(seek);
            else it.SeekToFirst();
            byte[]? prev = null;
            while (it.Valid)
            {
                ReadOnlySpan<byte> k = it.Key;
                if (takeWhilePrefix is not null && !k.StartsWith(takeWhilePrefix)) break;
                if (prefixSameAsStart && !k.StartsWith(seek.AsSpan(0, 8))) S.Violation(Name + " " + what + ": " + Convert.ToHexString(k) + " is outside the seek prefix");
                if (lower is not null && k.SequenceCompareTo(lower) < 0) S.Violation(Name + " " + what + ": " + Convert.ToHexString(k) + " is below the lower bound " + Convert.ToHexString(lower));
                if (upper is not null && k.SequenceCompareTo(upper) >= 0) S.Violation(Name + " " + what + ": " + Convert.ToHexString(k) + " is not below the upper bound " + Convert.ToHexString(upper));
                if (prev is not null && (reverse ? k.SequenceCompareTo(prev) >= 0 : k.SequenceCompareTo(prev) <= 0))
                    S.Violation(Name + " " + what + ": " + Convert.ToHexString(k) + " after " + Convert.ToHexString(prev) + " breaks the " + (reverse ? "descending" : "ascending") + " order");
                prev = k.ToArray();
                returned.Add(prev);
                int idx = IndexIn(range, k);
                if (idx < 0) S.Violation(Name + " " + what + ": key " + Convert.ToHexString(k) + " is not a key of the scanned range" + (M.TryGetSlot(family, k, out int other) ? " (it is slot " + other + ")" : ""));
                else
                {
                    seen[idx] = true;
                    Check(what, range[idx], befores[idx], true, it.Value);
                }
                if (S.Stop.IsCancellationRequested && S.Violations.Count > 0) return;
                if (reverse) it.Prev();
                else it.Next();
            }
            for (int i = 0; i < range.Count; i++)
            {
                if (!seen[i] && S.CheckRead(range[i], befores[i], false, default) is string e)
                {
                    int same = 0;
                    using (Iterator again = S.Cf[family].NewIterator(ro))
                    {
                        if (reverse) again.SeekToLast();
                        else if (seek is not null) again.Seek(seek);
                        else again.SeekToFirst();
                        for (; again.Valid; same++)
                        {
                            if (reverse) again.Prev();
                            else again.Next();
                        }
                    }
                    S.Violation(Name + " " + what + " (not returned): " + e + "; key " + Convert.ToHexString(M.Key[range[i]]) + ", history " + HistoryOf(range[i]) +
                        "; the scan returned " + returned.Count + " keys" + (returned.Count > 0 ? " from " + Convert.ToHexString(returned[0]) + " to " + Convert.ToHexString(returned[^1]) : "") +
                        ", iterator sequence " + sequence + (snap is null ? " (implicit)" : " (snapshot " + snap.SequenceNumber + ")") + ", latest sequence now " + S.Db.GetLatestSequenceNumber() +
                        "; the same scan again: " + same + " keys; " + Diagnose(family, M.Key[range[i]], reverse));
                    return;
                }
            }
        }
        finally
        {
            snap?.Dispose();
        }
    }

    private string Diagnose(int family, byte[] key, bool reverse)
    {
        ColumnFamily cf = S.Cf[family];
        int again = 0, againSameThread = 0;
        using (Iterator it = cf.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) again++;
        }
        using (Iterator it = cf.NewIterator())
        {
            if (reverse)
            {
                for (it.SeekToLast(); it.Valid; it.Prev()) againSameThread++;
            }
            else
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) againSameThread++;
            }
        }
        int otherThread = 0;
        var t = new Thread(() =>
        {
            using Iterator it = cf.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) otherThread++;
        });
        t.Start();
        t.Join();
        return "now: Get " + (cf.Get(key) is null ? "null" : "found") + ", new iterator " + again + " keys, again " + againSameThread + ", from another thread " + otherThread +
            ", super version " + cf.GetProperty("rocksdb.current-super-version-number") + ", immutables " + cf.GetProperty("rocksdb.num-immutable-mem-table") +
            ", active entries " + cf.GetProperty("rocksdb.num-entries-active-mem-table");
    }

    private string HistoryOf(int slot)
    {
        try
        {
            SlotEvent[] h = [.. M.History[slot] ?? []];
            return string.Join(" ", h.Select(e => (e.Delete ? "del" : "put") + " v" + e.Version + "@op" + e.Op));
        }
        catch (Exception e) when (e is InvalidOperationException or ArgumentException or IndexOutOfRangeException)
        {
            return "(changing)";
        }
    }

    private void TrackerScan()
    {
        // RocksDbBase.EnumerateChangesFromPrimary: iterate_lower_bound at the replica's version, SeekToFirst, Next.
        long completed = M.TrackerCompletedWatermark();
        long lower = 1 + Rng.NextInt64(Math.Max(1, completed + 1));
        Status = "tracker scan (lower bound, SeekToFirst)";
        long prev = -1, inWindow = 0;
        var found = new List<long>();
        using (Iterator it = S.Cf[MosaikSoak.FTracker].NewIterator(new ReadOptions { IterateLowerBound = SoakModel.TrackerKey(lower) }))
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (it.Key.Length != 8)
                {
                    S.Violation(Name + " tracker key of " + it.Key.Length + " bytes");
                    return;
                }
                long v = (long)BinaryPrimitives.ReadUInt64LittleEndian(it.Key);
                if (v <= prev) S.Violation(Name + " tracker scan: v" + v + " after v" + prev);
                if (v < lower) S.Violation(Name + " tracker scan: v" + v + " below the lower bound v" + lower);
                if (SoakModel.VerifyTrackerValue(v, it.Value, out _, out _) is string e) S.Violation(Name + " tracker scan: " + e);
                prev = v;
                found.Add(v);
            }
        }
        long issued;
        lock (M.TrackerLock) issued = M.TrackerIssued;
        long removing = Volatile.Read(ref M.TrackerRemoving);
        long from = Math.Max(lower, removing);
        foreach (long v in found)
        {
            if (v > issued) S.Violation(Name + " tracker scan: v" + v + " was never issued (issued " + issued + ")");
            if (v >= from && v <= completed) inWindow++;
        }
        long expected = Math.Max(0, completed - from + 1);
        if (inWindow != expected) S.Violation(Name + " tracker scan from v" + lower + ": " + inWindow + " versions in [" + from + ", " + completed + "], all " + expected + " were acknowledged and not removed");
    }

    private void VersionKey()
    {
        long ackedMax = Interlocked.Read(ref M.VersionKeyAckedMax);
        Status = "Get(graphsettings version key)";
        byte[]? v = S.Cf[MosaikSoak.FSettings].Get(SoakModel.VersionKey);
        long issued;
        lock (M.TrackerLock) issued = M.TrackerIssued;
        if (v is null)
        {
            if (ackedMax > 0) S.Violation(Name + " the graphsettings version key is missing after v" + ackedMax + " was acknowledged");
            return;
        }
        long version = BinaryPrimitives.ReadInt64LittleEndian(v);
        if (v.Length != 8 || version < 1 || version > issued) S.Violation(Name + " graphsettings version key holds " + version + " (" + v.Length + " bytes), issued " + issued);
        // RocksDbBase.TrackChangesHandler increments, then Puts, without a lock: a slower writer can Put an older version last.
        if (version < ackedMax) Interlocked.Increment(ref M.VersionKeyRegressionsSeen);
    }

    private void ScanVersusGets()
    {
        int family;
        ArraySegment<int> range;
        var ro = new ReadOptions();
        switch (Rng.Next(4))
        {
            case 0:
            {
                int x = Rng.Next(ToIndexQueueWriter.Indexes);
                family = MosaikSoak.FToIndex;
                ro.IterateLowerBound = _toIndex.From[x];
                ro.IterateUpperBound = _toIndex.To[x];
                range = M.SlotsInRange(family, _toIndex.From[x], true, _toIndex.To[x], true);
                break;
            }
            case 1:
                family = MosaikSoak.FQueue;
                range = M.SlotsInRange(family, [], false, [], false);
                break;
            case 2:
            {
                (byte[] from, byte[] to) = _vectors.Range(Rng.Next(VectorWriter.Indexes));
                family = MosaikSoak.FVectors;
                ro.IterateLowerBound = from;
                ro.IterateUpperBound = to;
                range = M.SlotsInRange(family, from, true, to, true);
                break;
            }
            default:
                family = MosaikSoak.FLuceneCount;
                range = M.SlotsInRange(family, [], false, [], false);
                break;
        }
        Status = "scan against point reads, " + MosaikProfile.Families[family].Name;
        using Snapshot snap = S.Db.GetSnapshot();
        ro.Snapshot = snap;
        var scanned = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
        using (Iterator it = S.Cf[family].NewIterator(ro))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) scanned[it.Key.ToArray()] = it.Value.ToArray();
        }
        var get = new ReadOptions { Snapshot = snap };
        int matched = 0;
        foreach (int slot in range)
        {
            byte[]? v = S.Cf[family].Get(get, M.Key[slot]);
            bool inScan = scanned.TryGetValue(M.Key[slot], out byte[]? sv);
            if (inScan != (v is not null) || (inScan && !sv.AsSpan().SequenceEqual(v)))
                S.Violation(Name + " under one snapshot the scan of " + MosaikProfile.Families[family].Name + (inScan ? " returned " : " did not return ") + Convert.ToHexString(M.Key[slot]) +
                    " and Get " + (v is null ? "found nothing" : inScan ? "returned other bytes" : "found it"));
            if (inScan) matched++;
        }
        if (matched != scanned.Count) S.Violation(Name + " under one snapshot the scan of " + MosaikProfile.Families[family].Name + " returned " + (scanned.Count - matched) + " keys outside the model's range");
    }
}

/// <summary>
/// RocksDbBase.FlushRocksDBMemory (RocksDbBase.cs:962-1145, 1150-1250, 1305-1340) with its waits shortened: Flush(wait) of
/// the default family, MemoryUtil.GetApproximateMemoryUsageByType, CleanOldWalArchive deleting archived WALs behind the
/// engine's back, replicas registering (DisableFileDeletions) and leaving (EnableFileDeletions), and twelve rounds of
/// UpdateStats (the ~30 properties on the default family and every listed family).
/// </summary>
internal sealed class FlushThreadWorker : SoakWorker
{
    private static readonly string[] s_properties =
    [
        "rocksdb.num-immutable-mem-table", "rocksdb.mem-table-flush-pending", "rocksdb.num-running-flushes", "rocksdb.compaction-pending",
        "rocksdb.num-running-compactions", "rocksdb.background-errors", "rocksdb.cur-size-active-mem-table", "rocksdb.cur-size-all-mem-tables",
        "rocksdb.size-all-mem-tables", "rocksdb.num-entries-active-mem-table", "rocksdb.num-entries-imm-mem-tables",
        "rocksdb.num-deletes-active-mem-table", "rocksdb.num-deletes-imm-mem-tables", "rocksdb.estimate-num-keys",
        "rocksdb.estimate-table-readers-mem", "rocksdb.num-snapshots", "rocksdb.oldest-snapshot-time", "rocksdb.num-live-versions",
        "rocksdb.current-super-version-number", "rocksdb.estimate-live-data-size", "rocksdb.live-sst-files-size", "rocksdb.base-level",
        "rocksdb.estimate-pending-compaction-bytes", "rocksdb.actual-delayed-write-rate", "rocksdb.is-write-stopped",
    ];

    private bool _fileDeletionsDisabled;
    private long _keepArchiveUntil;
    private long _lastArchiveClean;
    private int _pass;
    private readonly HashSet<string> _reportedProperties = [];

    public FlushThreadWorker(MosaikSoak s) : base(s, "flush-thread", 3000) => KeepArchive();

    private void KeepArchive() => Volatile.Write(ref _keepArchiveUntil, Stopwatch.GetTimestamp() + (long)(S.Settings.WalArchiveStaleTimeout.TotalSeconds * Stopwatch.Frequency));

    public override void Step()
    {
        Status = "Flush(default family, wait)";
        S.Db.Flush(new FlushOptions { Wait = true });
        Status = "MemoryUtil.GetApproximateMemoryUsageByType";
        MemoryUsageByType usage = MemoryUtil.GetApproximateMemoryUsageByType([S.Db], []);
        if (usage.MemTableUnflushed > usage.MemTableTotal) S.Violation(Name + " memtable unflushed " + usage.MemTableUnflushed + " > total " + usage.MemTableTotal);
        long now = Stopwatch.GetTimestamp();
        if (now > Volatile.Read(ref _keepArchiveUntil) && now - _lastArchiveClean > Stopwatch.Frequency / 4)
        {
            _lastArchiveClean = now;
            CleanOldWalArchive();
        }
        Replicas();
        for (int i = 0; i < 12; i++)
        {
            if (Wait(TimeSpan.FromMilliseconds(40))) return;
            UpdateStats(expensive: i == 0 && _pass % 4 == 0);
        }
        _pass++;
    }

    private void Replicas()
    {
        int r = Rng.Next(10);
        if (r < 3 && !_fileDeletionsDisabled)
        {
            // RegisterReplica: file deletions off while a replica copies the checkpoint and the WALs.
            Status = "DisableFileDeletions (replica registered)";
            KeepArchive();
            S.Db.DisableFileDeletions();
            _fileDeletionsDisabled = true;
            S.Count("replica registrations");
        }
        else if (r < 6 && _fileDeletionsDisabled)
        {
            // DeregisterReplica / PruneStaleReplicasAndMaybeReenableFileDeletions.
            Status = "EnableFileDeletions (no replica left)";
            S.Db.EnableFileDeletions();
            _fileDeletionsDisabled = false;
        }
        else if (_fileDeletionsDisabled)
        {
            KeepArchive(); // ReportReplicaVersion / TouchReplica
        }
    }

    /// <summary>RocksDbBase.CleanOldWalArchive: by age (the stale timeout), then by size, oldest first, behind the engine's back.</summary>
    private void CleanOldWalArchive()
    {
        string archive = Path.Combine(S.WalDir, "archive");
        Status = "CleanOldWalArchive";
        if (!S.Fs.DirectoryExists(archive)) return;
        var files = new List<(ulong Number, string Path, long Size, DateTimeOffset Modified)>();
        foreach (string name in S.Fs.GetChildren(archive))
        {
            if (!name.EndsWith(".log", StringComparison.Ordinal)) continue;
            ulong number = ulong.TryParse(name.AsSpan(0, name.Length - 4), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out ulong n) ? n : ulong.MaxValue;
            string path = Path.Combine(archive, name);
            try
            {
                files.Add((number, path, S.Fs.GetFileSize(path), S.Fs.GetFileModificationTime(path)));
            }
            catch (RocksDbException)
            {
                // gone already
            }
        }
        files.Sort((a, b) => a.Number.CompareTo(b.Number));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        long kept = 0;
        var left = new List<(string Path, long Size)>();
        foreach ((ulong _, string path, long size, DateTimeOffset modified) in files)
        {
            if (size == 0 || now - modified > S.Settings.WalArchiveStaleTimeout) TryDelete(path);
            else
            {
                kept += size;
                left.Add((path, size));
            }
        }
        foreach ((string path, long size) in left)
        {
            if (kept <= S.Profile.WalArchiveSizeLimitBytes) break;
            if (TryDelete(path)) kept -= size;
        }
    }

    private bool TryDelete(string path)
    {
        try
        {
            S.Fs.DeleteFile(path);
            S.Count("archived WALs deleted behind the engine");
            return true;
        }
        catch (RocksDbException)
        {
            return false;
        }
    }

    private void UpdateStats(bool expensive)
    {
        Status = "UpdateStats";
        foreach (string p in s_properties) Property(p, includeFamilies: true);
        Property("rocksdb.obsolete-sst-files-size", includeFamilies: false);
        if (expensive) Property("rocksdb.total-sst-files-size", includeFamilies: true);
        S.Count("UpdateStats");
    }

    private void Property(string name, bool includeFamilies)
    {
        Parse(name, "default", S.Db.GetProperty(name));
        if (!includeFamilies) return;
        for (int f = 0; f < S.Cf.Length; f++) Parse(name, MosaikProfile.Families[f].Name, S.Cf[f].GetProperty(name));
    }

    private void Parse(string name, string family, string? value)
    {
        if (!long.TryParse(value, out long v))
        {
            if (_reportedProperties.Add(name + "@" + family)) S.Violation(Name + " " + name + " on " + family + " is " + (value is null ? "null" : "\"" + value + "\"") + ", not a number");
        }
        else if (name == "rocksdb.background-errors" && v != 0 && !S.Crashed)
        {
            if (_reportedProperties.Add(name + "@" + family)) S.Violation(Name + " " + family + " reports " + v + " background errors");
        }
        else if (family == "default" && v != 0 && name is "rocksdb.is-write-stopped" or "rocksdb.actual-delayed-write-rate")
        {
            S.Count("UpdateStats saw " + name);
        }
    }
}

/// <summary>
/// The Compact() methods of RocksDbStorage, RocksDbIndexStorage, RocksDbDataStorage and RocksDbBlobStorage: CompactRange
/// (null, null) on each of their families in turn, from two threads.
/// </summary>
internal sealed class MaintenanceWorker : SoakWorker
{
    private static readonly string[][] s_groups =
    [
        ["types", "nodes", "content", "edges", "nodeschemas", "edgeschemas", "changelog", "nodeversion", "graphsettings", "docmetadata"],
        ["lucene-file-length", "lucene-file-size", "lucene-file-pages-count", "lucene-file-pages", "simple-text-index-keys", "simple-text-index-entries",
         "file-extraction-queue", "file-extraction-queue-stats", "command-score-index-entries", "to-index-queue"],
        ["data-length", "data-pages", "data-paths", "data-vectors", "data-vectors-hashes"],
        ["blob-length", "blob-pages"],
    ];

    public MaintenanceWorker(MosaikSoak s, int index) : base(s, "maintenance-" + index, 4000 + index)
    {
    }

    public override void Step()
    {
        foreach (string name in s_groups[Rng.Next(s_groups.Length)])
        {
            if (S.Stop.IsCancellationRequested) return;
            int f = SoakModel.IndexOf(name);
            long start = Stopwatch.GetTimestamp();
            Status = "CompactRange(" + name + ", null, null)";
            S.Cf[f].CompactRange(null, null);
            S.NoteHorizon(f, start);
            S.Count("full-range compactions");
        }
    }
}

/// <summary>
/// The checkpoint (CreateCheckpoint(dir, 0), as ReplicationSource takes it) and RocksDbBase.Backup (BackupEngine.Open,
/// CreateNewBackup(flush_before_backup), PurgeOldBackups(2), GetBackupInfo) at fixed points of the run.
/// </summary>
internal sealed class SnapshotWorker : SoakWorker
{
    private readonly List<(double At, bool Backup)> _plan = [];
    private int _next, _checkpoints;

    public SnapshotWorker(MosaikSoak s) : base(s, "checkpoint-backup", 5000)
    {
        for (int i = 0; i < s.Settings.Checkpoints; i++) _plan.Add(((i + 1.0) / (s.Settings.Checkpoints + 1) + 0.05, false));
        for (int i = 0; i < s.Settings.Backups; i++) _plan.Add(((i + 1.0) / (s.Settings.Backups + 1) - 0.05, true));
        _plan.Sort((a, b) => a.At.CompareTo(b.At));
    }

    public override void Step()
    {
        if (_next >= _plan.Count)
        {
            Status = "idle";
            Wait(TimeSpan.FromMilliseconds(250));
            return;
        }
        (double at, bool backup) = _plan[_next];
        long due = S.StartTicks + (long)(at * S.Settings.Duration.TotalSeconds * Stopwatch.Frequency);
        long wait = due - Stopwatch.GetTimestamp();
        Status = "waiting for the next " + (backup ? "backup" : "checkpoint");
        if (wait > 0 && Wait(TimeSpan.FromSeconds(Math.Min(wait / (double)Stopwatch.Frequency, 0.25)))) return;
        if (Stopwatch.GetTimestamp() < due) return;
        _next++;
        if (backup) Backup();
        else Checkpoint();
    }

    private (int[] MaxOps, long Issued, long Removing) Marks()
    {
        var maxOps = new int[M.Writers.Count];
        for (int i = 0; i < maxOps.Length; i++) maxOps[i] = M.Writers[i].OpCount;
        long issued;
        lock (M.TrackerLock) issued = M.TrackerIssued;
        return (maxOps, issued, Volatile.Read(ref M.TrackerRemoving));
    }

    private void Checkpoint()
    {
        string dir = Path.Combine(S.Settings.Root, "checkpoint-" + _checkpoints++);
        long start = Stopwatch.GetTimestamp();
        Status = "CreateCheckpoint(" + Path.GetFileName(dir) + ", 0)";
        S.Db.CreateCheckpoint(dir, 0);
        (int[] maxOps, long issued, long removing) = Marks();
        S.NoteHorizonAll(start);
        lock (S.Snapshots) S.Snapshots.Add(new SoakSnapshotRecord("checkpoint", dir, 0, maxOps, issued, removing, start));
        S.Count("checkpoints");
    }

    private void Backup()
    {
        long start = Stopwatch.GetTimestamp();
        Status = "BackupEngine.Open";
        using BackupEngine engine = S.Settings.FileSystem is null
            ? BackupEngine.Open(new BackupEngineOptions(S.BackupDir))
            : BackupEngine.Open(new BackupEngineOptions(S.BackupDir), S.Fs);
        Status = "CreateNewBackup(flushBeforeBackup: true)";
        uint id = engine.CreateNewBackup(S.Db, flushBeforeBackup: true);
        (int[] maxOps, long issued, long removing) = Marks();
        S.NoteHorizonAll(start);
        lock (S.Snapshots) S.Snapshots.Add(new SoakSnapshotRecord("backup", S.BackupDir, id, maxOps, issued, removing, start));
        Status = "PurgeOldBackups(2)";
        engine.PurgeOldBackups(2);
        Status = "GetBackupInfo";
        List<BackupInfo> infos = engine.GetBackupInfo();
        if (infos.Count > 2 || !infos.Exists(i => i.BackupId == id)) S.Violation(Name + " after PurgeOldBackups(2) the engine lists " + infos.Count + " backups, the new one " + id + (infos.Exists(i => i.BackupId == id) ? "" : " missing"));
        S.Count("backups");
    }
}

/// <summary>Every family's entries as an engine returned them, in its iteration order.</summary>
internal sealed class SoakDump
{
    public readonly List<(byte[] Key, byte[] Value)>[] Families = new List<(byte[], byte[])>[SoakModel.FamilyCount];

    public SoakDump()
    {
        for (int f = 0; f < Families.Length; f++) Families[f] = [];
    }

    public static SoakDump Of(RocksDatabase db)
    {
        var d = new SoakDump();
        for (int f = 0; f < SoakModel.FamilyCount; f++)
        {
            using Iterator it = db.GetColumnFamily(MosaikProfile.Families[f].Name).NewIterator(new ReadOptions { TotalOrderSeek = true });
            for (it.SeekToFirst(); it.Valid; it.Next()) d.Families[f].Add((it.Key.ToArray(), it.Value.ToArray()));
        }
        return d;
    }

    public long Count => Families.Sum(f => (long)f.Count);
}

/// <summary>
/// The checks after a run: the exact final state, a point in time for checkpoints and backups (every writer's visible
/// writes are a prefix of its operations, and a write is never visible without the writes acknowledged before it began),
/// and what upstream guarantees after a power loss for writes that were not synced.
/// </summary>
internal static class SoakVerifier
{
    private const int MaxErrors = 25;

    private sealed class Observed
    {
        public byte[]?[] Value = [];
        /// <summary>Index into the slot's history of the write the value came from; -1 absent; -2 present without identity.</summary>
        public int[] Event = [];
        public readonly Dictionary<long, (int Writer, int Op)> Tracker = [];
        public long? VersionKey;
    }

    private static void Add(List<string> errors, string e)
    {
        if (errors.Count < MaxErrors) errors.Add(e);
    }

    private static int CompareKeys(int family, byte[] a, byte[] b) => family == SoakModel.TrackerFamily
        ? BinaryPrimitives.ReadUInt64LittleEndian(a).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b))
        : a.AsSpan().SequenceCompareTo(b);

    /// <summary>Order, keys and values of a dump; every value must be one some operation wrote (no phantoms).</summary>
    private static Observed Observe(SoakModel m, SoakDump d, string what, List<string> errors)
    {
        var o = new Observed { Value = new byte[]?[m.SlotCount], Event = new int[m.SlotCount] };
        Array.Fill(o.Event, -1);
        for (int f = 0; f < SoakModel.FamilyCount; f++)
        {
            string family = MosaikProfile.Families[f].Name;
            List<(byte[] Key, byte[] Value)> entries = d.Families[f];
            for (int i = 0; i < entries.Count; i++)
            {
                (byte[] k, byte[] v) = entries[i];
                if (i > 0 && CompareKeys(f, entries[i - 1].Key, k) >= 0) Add(errors, what + ": " + family + " is not in comparator order at " + Convert.ToHexString(k));
                if (f == SoakModel.TrackerFamily)
                {
                    if (k.Length != 8)
                    {
                        Add(errors, what + ": tracker key of " + k.Length + " bytes");
                        continue;
                    }
                    long version = (long)BinaryPrimitives.ReadUInt64LittleEndian(k);
                    if (SoakModel.VerifyTrackerValue(version, v, out int w, out int op) is string e) Add(errors, what + ": " + e);
                    else if (version < 1 || version > m.TrackerIssued) Add(errors, what + ": tracker v" + version + " was never issued");
                    else
                    {
                        long owner = m.TrackerOwner[version];
                        if (owner != (((long)w << 32) | (uint)op)) Add(errors, what + ": tracker v" + version + " claims writer " + w + " op " + op + ", it was written by writer " + (owner >> 32) + " op " + (int)owner);
                        o.Tracker[version] = (w, op);
                    }
                    continue;
                }
                if (f == SoakModel.SettingsFamily && k.AsSpan().SequenceEqual(SoakModel.VersionKey))
                {
                    long version = v.Length == 8 ? BinaryPrimitives.ReadInt64LittleEndian(v) : -1;
                    if (version < 1 || version > m.TrackerIssued) Add(errors, what + ": graphsettings version key holds " + Convert.ToHexString(v) + ", issued " + m.TrackerIssued);
                    o.VersionKey = version;
                    continue;
                }
                if (!m.TryGetSlot(f, k, out int slot))
                {
                    Add(errors, what + ": " + family + " holds key " + Convert.ToHexString(k) + " nobody wrote");
                    continue;
                }
                o.Value[slot] = v;
                o.Event[slot] = Identify(m, slot, v, what, errors);
            }
        }
        return o;
    }

    private static int Identify(SoakModel m, int slot, byte[] v, string what, List<string> errors)
    {
        List<SlotEvent> h = m.History[slot] ?? [];
        string family = MosaikProfile.Families[m.Family[slot]].Name;
        switch (m.Kind[slot])
        {
            case SlotKind.Empty:
                if (v.Length != 0) Add(errors, what + ": " + family + " slot " + slot + " holds a " + v.Length + "-byte value, only empty ones are written");
                if (!h.Exists(e => !e.Delete)) Add(errors, what + ": " + family + " slot " + slot + " is present but was never put");
                return -2;
            case SlotKind.RawCounter:
            {
                long raw = v.Length == 8 ? BinaryPrimitives.ReadInt64LittleEndian(v) : long.MinValue;
                int i = h.FindIndex(e => !e.Delete && e.Raw == raw);
                if (i < 0) Add(errors, what + ": " + family + " slot " + slot + " holds counter " + Convert.ToHexString(v) + ", which was never written");
                return i < 0 ? -2 : i;
            }
            default:
            {
                if (SoakValue.Verify(v, out int s, out int version, out int w, out int op) is string e)
                {
                    Add(errors, what + ": " + family + " slot " + slot + ": " + e);
                    return -2;
                }
                if (s != slot || w != m.Writer[slot])
                {
                    Add(errors, what + ": " + family + " slot " + slot + " holds the value of slot " + s + " (writer " + w + ")");
                    return -2;
                }
                int i = h.FindIndex(x => x.Version == version);
                if (i < 0 || h[i].Delete || h[i].Op != op || h[i].Raw != v.Length)
                {
                    Add(errors, what + ": " + family + " slot " + slot + " holds v" + version + " op " + op + ", which its writer never put");
                    return -2;
                }
                return i;
            }
        }
    }

    /// <summary>The dump is exactly the model's final state (every operation acknowledged).</summary>
    public static void CheckFinal(SoakModel m, SoakDump d, string what, List<string> errors, out bool versionKeyIsLatest)
    {
        Observed o = Observe(m, d, what, errors);
        for (int slot = 0; slot < m.SlotCount; slot++)
        {
            SlotEvent? last = m.LastEvent(slot);
            bool present = last is { Delete: false };
            string family = MosaikProfile.Families[m.Family[slot]].Name;
            if (present != (o.Value[slot] is not null))
            {
                Add(errors, what + ": " + family + " slot " + slot + " (" + Convert.ToHexString(m.Key[slot]) + ") is " + (present ? "missing" : "present") +
                    ", its last write " + (last is { } e ? (e.Delete ? "deleted" : "put") + " v" + e.Version + " in op " + e.Op : "never happened"));
            }
            else if (present && !o.Value[slot].AsSpan().SequenceEqual(m.ValueOf(slot, last!.Value)))
            {
                Add(errors, what + ": " + family + " slot " + slot + " does not hold its last write (v" + last!.Value.Version + ")");
            }
        }
        for (long v = Math.Max(1, m.TrackerAck); v <= m.TrackerIssued; v++)
        {
            if (!o.Tracker.ContainsKey(v)) Add(errors, what + ": tracker v" + v + " is missing (acknowledged up to " + m.TrackerAck + ")");
        }
        foreach (long v in o.Tracker.Keys)
        {
            if (v < m.TrackerAck) Add(errors, what + ": tracker v" + v + " is below the acknowledged v" + m.TrackerAck + " and was removed");
        }
        if (m.TrackerIssued > 0 && o.VersionKey is null) Add(errors, what + ": graphsettings version key is missing");
        versionKeyIsLatest = o.VersionKey == m.TrackerIssued;
    }

    private static List<(int Lo, int Hi)> Intersect(List<(int Lo, int Hi)> a, List<(int Lo, int Hi)> b)
    {
        var r = new List<(int, int)>();
        int i = 0, j = 0;
        while (i < a.Count && j < b.Count)
        {
            int lo = Math.Max(a[i].Lo, b[j].Lo), hi = Math.Min(a[i].Hi, b[j].Hi);
            if (lo <= hi) r.Add((lo, hi));
            if (a[i].Hi < b[j].Hi) i++;
            else j++;
        }
        return r;
    }

    /// <summary>The cuts (number of the writer's first operations applied) that explain a slot's observed state.</summary>
    private static List<(int Lo, int Hi)> AllowedCuts(SoakModel m, int slot, Observed o, int maxOp)
    {
        List<SlotEvent> h = m.History[slot] ?? [];
        int n = 0;
        while (n < h.Count && h[n].Op <= maxOp) n++;
        (int, int) Interval(int i) => (h[i].Op, i + 1 < n ? h[i + 1].Op - 1 : maxOp);
        var r = new List<(int, int)>();
        int e = o.Event[slot];
        if (e >= 0)
        {
            if (e < n) r.Add(Interval(e));
            return r;
        }
        bool present = e == -2;
        if (!present) r.Add((0, n > 0 ? h[0].Op - 1 : maxOp));
        for (int i = 0; i < n; i++)
        {
            if (h[i].Delete != present) r.Add(Interval(i));
        }
        r.RemoveAll(x => x.Item1 > x.Item2);
        return r;
    }

    private static string DescribeSlot(SoakModel m, int slot, Observed o, int maxOp)
    {
        List<SlotEvent> h = m.History[slot] ?? [];
        var sb = new System.Text.StringBuilder();
        sb.Append(MosaikProfile.Families[m.Family[slot]].Name).Append(" slot ").Append(slot).Append(' ').Append(Convert.ToHexString(m.Key[slot])).Append(": observed ");
        sb.Append(o.Event[slot] switch { -1 => "absent", -2 => "present", int i => "v" + h[i].Version + " (op " + h[i].Op + ")" });
        sb.Append(", writes up to op ").Append(maxOp).Append(':');
        int shown = 0;
        for (int i = h.Count - 1; i >= 0 && shown < 8; i--)
        {
            if (h[i].Op > maxOp) continue;
            sb.Append(' ').Append(h[i].Delete ? "del" : "put").Append(" v").Append(h[i].Version).Append("@op").Append(h[i].Op);
            shown++;
        }
        return sb.ToString();
    }

    private static int WriterOf(SoakModel m, string name) => m.Writers.FindIndex(w => w.Name == name);

    /// <summary>
    /// A checkpoint or restored backup is the database at one point in time: for every writer, its visible writes are its
    /// first c operations for some c (the "cut"), and a cut never includes an operation begun after another writer's
    /// operation that it excludes had been acknowledged. Families cleared with DeleteFilesInRange are only checked for
    /// valid values (upstream: DeleteFilesInRange is not atomic with the DeleteRange after it).
    /// </summary>
    public static string CheckCut(SoakModel m, SoakDump d, SoakSnapshotRecord rec, string what, List<string> errors)
    {
        int before = errors.Count;
        Observed o = Observe(m, d, what, errors);
        int writers = m.Writers.Count;
        var feasible = new List<(int Lo, int Hi)>[writers];
        for (int w = 0; w < writers; w++) feasible[w] = [(0, rec.MaxOps[w])];
        var broken = new bool[writers];
        void Narrow(int w, List<(int Lo, int Hi)> allowed, Func<string> why)
        {
            if (broken[w]) return;
            feasible[w] = Intersect(feasible[w], allowed);
            if (feasible[w].Count == 0)
            {
                broken[w] = true;
                Add(errors, what + ": no point in time of writer " + m.Writers[w].Name + " explains " + why());
            }
        }
        for (int slot = 0; slot < m.SlotCount; slot++)
        {
            if (m.Relaxed[m.Family[slot]]) continue;
            int w = m.Writer[slot];
            int maxOp = rec.MaxOps[w];
            if (o.Event[slot] >= 0 && m.History[slot]![o.Event[slot]].Op > maxOp)
            {
                Add(errors, what + ": " + DescribeSlot(m, slot, o, maxOp) + " - a write begun after the snapshot returned is visible");
                broken[w] = true;
                continue;
            }
            int s = slot;
            Narrow(w, AllowedCuts(m, slot, o, maxOp), () => DescribeSlot(m, s, o, maxOp) + " together with its other writes");
        }
        int ackWriter = WriterOf(m, "tracker-ack");
        foreach ((long v, (int w, int op)) in o.Tracker)
        {
            if (op > rec.MaxOps[w]) Add(errors, what + ": tracker v" + v + " from op " + op + " of " + m.Writers[w].Name + ", begun after the snapshot returned");
            else Narrow(w, [(op, rec.MaxOps[w])], () => "the tracker entry v" + v + " (op " + op + ")");
            long removal = m.TrackerRemovalOp[v];
            if (removal != 0 && removal <= rec.MaxOps[ackWriter]) Narrow(ackWriter, [(0, (int)removal - 1)], () => "the tracker entry v" + v + " still present (removed by op " + removal + ")");
        }
        for (long v = Math.Max(1, rec.TrackerRemoving); v <= rec.TrackerIssued; v++)
        {
            if (o.Tracker.ContainsKey(v)) continue;
            long owner = m.TrackerOwner[v];
            if (owner == 0) continue;
            int w = (int)(owner >> 32), op = (int)owner;
            if (op <= rec.MaxOps[w]) Narrow(w, [(0, op - 1)], () => "the tracker entry v" + v + " (op " + op + ") missing, not removed before the snapshot returned");
        }
        if (o.VersionKey is long vk && vk >= 1 && vk <= m.TrackerIssued)
        {
            long owner = m.TrackerOwner[vk];
            int w = (int)(owner >> 32), op = (int)owner - 1; // the version key Put is the operation before the tracker Put
            if (owner != 0) Narrow(w, [(op, rec.MaxOps[w])], () => "the graphsettings version key v" + vk + " (op " + op + ")");
        }
        if (Array.Exists(broken, b => b)) return "cut check failed";

        // Real-time order: an operation visible at the point in time implies every operation acknowledged before it began.
        var c = new int[writers];
        for (int w = 0; w < writers; w++) c[w] = feasible[w][0].Lo;
        bool changed = true;
        for (int round = 0; changed && round < 100_000; round++)
        {
            changed = false;
            for (int b = 0; b < writers; b++)
            {
                if (c[b] < 1) continue;
                long invoked = m.Writers[b].Ops[c[b]].Invoke;
                for (int a = 0; a < writers; a++)
                {
                    if (a == b) continue;
                    int need = LastAckedBefore(m.Writers[a], invoked, rec.MaxOps[a]);
                    if (c[a] >= need) continue;
                    int? next = NextAtLeast(feasible[a], need);
                    if (next is null)
                    {
                        Add(errors, what + ": " + m.Writers[b].Name + " op " + c[b] + " is visible, but " + m.Writers[a].Name + " op " + need +
                            ", acknowledged before it began, is not (" + m.Writers[a].Name + " is at most at op " + feasible[a][^1].Hi + ")");
                        return "real-time order violated";
                    }
                    c[a] = next.Value;
                    changed = true;
                }
            }
        }
        var sb = new System.Text.StringBuilder(what).Append(" cuts:");
        for (int w = 0; w < writers; w++) sb.Append(' ').Append(m.Writers[w].Name).Append('=').Append(c[w]).Append('/').Append(rec.MaxOps[w]);
        return errors.Count > before ? "errors" : sb.ToString();
    }

    private static int LastAckedBefore(SoakWriterLog w, long ticks, int maxOp)
    {
        int lo = 0, hi = Math.Min(maxOp, w.Ops.Count - 1);
        while (lo < hi)
        {
            int mid = (lo + hi + 1) >>> 1;
            OpRecord r = w.Ops[mid];
            if (r.Ack != 0 && r.Ack < ticks) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    private static int? NextAtLeast(List<(int Lo, int Hi)> set, int value)
    {
        foreach ((int lo, int hi) in set)
        {
            if (hi >= value) return Math.Max(lo, value);
        }
        return null;
    }

    private static bool HasStateFromOpAtLeast(SoakModel m, int slot, Observed o, int op)
    {
        List<SlotEvent> h = m.History[slot] ?? [];
        int e = o.Event[slot];
        if (e >= 0) return h[e].Op >= op;
        bool present = e == -2;
        for (int i = h.Count - 1; i >= 0 && h[i].Op >= op; i--)
        {
            if (h[i].Delete != present) return true;
        }
        return false;
    }

    /// <summary>
    /// After a power loss with writes that were never synced (Mosaik's WriteOptions) and
    /// WALRecoveryMode::kSkipAnyCorruptedRecords, upstream guarantees (db_impl_open.cc: corruption is ignored and replay goes
    /// on; log_reader.cc: a record is checksummed, so a batch is applied whole or not at all):
    /// <list type="bullet">
    /// <item>no phantoms: every value is one some operation wrote;</item>
    /// <item>batches stay atomic across families: a flush syncs the closed WALs before its MANIFEST commit
    /// (FlushMemTableToOutputFile, needs_to_sync_closed_wals), so a batch half in an SST and half in a lost WAL cannot happen;</item>
    /// <item>what a successful flush or full-range compaction of a family wrote survives: every write to the family
    /// acknowledged before that flush began (or a later write of the slot).</item>
    /// </list>
    /// Not guaranteed, so only reported: that each writer's surviving writes are a prefix of its operations. Without
    /// syncs, replay skips over what a WAL lost (kSkipAnyCorruptedRecords does not stop at a gap) and a later WAL that
    /// was range-synced may survive while an earlier one did not.
    /// </summary>
    public static string CheckPowerLoss(SoakModel m, SoakDump d, long[] horizon, string what, List<string> errors)
    {
        Observed o = Observe(m, d, what, errors);
        int atomic = 0, durable = 0;
        for (int slot = 0; slot < m.SlotCount; slot++)
        {
            if (m.Relaxed[m.Family[slot]] || o.Event[slot] < 0) continue;
            SoakWriterLog w = m.Writers[m.Writer[slot]];
            int op = m.History[slot]![o.Event[slot]].Op;
            OpRecord r = w.Ops[op];
            for (int i = r.FirstChange; i < r.FirstChange + r.ChangeCount; i++)
            {
                int other = w.ChangeSlots[i];
                if (other == slot || m.Relaxed[m.Family[other]]) continue;
                atomic++;
                if (!HasStateFromOpAtLeast(m, other, o, op))
                    Add(errors, what + ": batch op " + op + " of " + w.Name + " is torn: " + DescribeSlot(m, slot, o, w.Ops.Count - 1) + " but " + DescribeSlot(m, other, o, w.Ops.Count - 1));
            }
        }
        for (int slot = 0; slot < m.SlotCount; slot++)
        {
            long h = horizon[m.Family[slot]];
            if (m.Relaxed[m.Family[slot]] || h == 0 || m.History[slot] is not { Count: > 0 } events) continue;
            SoakWriterLog w = m.Writers[m.Writer[slot]];
            for (int i = events.Count - 1; i >= 0; i--)
            {
                OpRecord r = w.Ops[events[i].Op];
                if (r.Ack == 0 || r.Ack >= h) continue;
                durable++;
                if (!HasStateFromOpAtLeast(m, slot, o, events[i].Op))
                    Add(errors, what + ": " + DescribeSlot(m, slot, o, w.Ops.Count - 1) + " - op " + events[i].Op + " was acknowledged before a flush of the family that completed, and is lost");
                break;
            }
        }
        // Report only: does a prefix of each writer's operations explain what survived?
        var holes = new List<string>();
        for (int wi = 0; wi < m.Writers.Count; wi++)
        {
            int maxOp = m.Writers[wi].Ops.Count - 1;
            List<(int Lo, int Hi)> set = [(0, maxOp)];
            for (int slot = 0; slot < m.SlotCount && set.Count > 0; slot++)
            {
                if (m.Writer[slot] == wi && !m.Relaxed[m.Family[slot]]) set = Intersect(set, AllowedCuts(m, slot, o, maxOp));
            }
            if (set.Count == 0) holes.Add(m.Writers[wi].Name);
        }
        return what + ": " + d.Count + " entries; " + atomic + " batch relations and " + durable + " flushed writes checked; per-writer prefix " +
            (holes.Count == 0 ? "held for every writer" : "did not hold for " + string.Join(", ", holes)) + " (reported, not required)";
    }

    /// <summary>Two dumps of the same files hold the same entries in the same order.</summary>
    public static void CompareDumps(SoakDump a, SoakDump b, string what, List<string> errors)
    {
        for (int f = 0; f < SoakModel.FamilyCount; f++)
        {
            List<(byte[] Key, byte[] Value)> x = a.Families[f], y = b.Families[f];
            int n = Math.Min(x.Count, y.Count);
            for (int i = 0; i < n; i++)
            {
                if (!x[i].Key.AsSpan().SequenceEqual(y[i].Key) || !x[i].Value.AsSpan().SequenceEqual(y[i].Value))
                {
                    Add(errors, what + ": " + MosaikProfile.Families[f].Name + " entry " + i + " differs: " + Convert.ToHexString(x[i].Key) + " / " + Convert.ToHexString(y[i].Key));
                    break;
                }
            }
            if (x.Count != y.Count) Add(errors, what + ": " + MosaikProfile.Families[f].Name + " has " + x.Count + " entries on one engine and " + y.Count + " on the other");
        }
    }
}
