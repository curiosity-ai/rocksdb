using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>Which engine writes a WAL matrix image.</summary>
internal enum WalMatrixWriter
{
    Managed,
    Native,
}

/// <summary>
/// The options a WAL matrix image is written with (and opened with again, except the recovery mode and paranoid_checks
/// of <see cref="WalOpenConfig"/>). Writers always use kPointInTimeRecovery, so that recycling is not sanitized away
/// while writing; an opener with kTolerateCorruptedTailRecords / kAbsoluteConsistency reads recycled WALs with recycling
/// sanitized off, like upstream.
/// </summary>
internal sealed record WalWriteConfig(
    WalMatrixWriter Writer,
    bool Recycle,
    bool TrackWals,
    bool TrackInManifest,
    bool Zstd,
    bool ManualWalFlush,
    bool SeparateWalDir,
    bool AvoidFlushDuringRecovery)
{
    public override string ToString() =>
        Writer + (Recycle ? "+recycle" : "") + (TrackWals ? "+trackWals" : "") + (TrackInManifest ? "+trackManifest" : "") + (Zstd ? "+zstd" : "") +
        (ManualWalFlush ? "+manualFlush" : "") + (SeparateWalDir ? "+walDir" : "") + (AvoidFlushDuringRecovery ? "+avoidFlushRecovery" : "");
}

/// <summary>How a damaged image is opened.</summary>
internal sealed record WalOpenConfig(WalRecoveryMode Mode, bool Paranoid, WalMatrixFilter Filter)
{
    public override string ToString() => Mode + (Paranoid ? "" : "+noParanoid") + (Filter.Kind == WalMatrixFilterKind.None ? "" : "+" + Filter);
}

internal enum WalMatrixFilterKind
{
    None,
    Ignore,
    Stop,
    Corrupted,
    /// <summary>A value outside WalProcessingOption.</summary>
    Invalid,
}

/// <summary>A WAL filter that returns <see cref="Kind"/> for the record at <see cref="At"/> (0-based) and continues otherwise.</summary>
internal readonly record struct WalMatrixFilter(WalMatrixFilterKind Kind, int At)
{
    public static readonly WalMatrixFilter None = new(WalMatrixFilterKind.None, 0);

    public int OptionFor(int index) => Kind == WalMatrixFilterKind.None || index != At
        ? (int)WalProcessingOption.ContinueProcessing
        : Kind switch
        {
            WalMatrixFilterKind.Ignore => (int)WalProcessingOption.IgnoreCurrentRecord,
            WalMatrixFilterKind.Stop => (int)WalProcessingOption.StopReplay,
            WalMatrixFilterKind.Corrupted => (int)WalProcessingOption.CorruptedRecord,
            _ => 99,
        };

    public override string ToString() => Kind + "@" + At;
}

/// <summary>Damage applied to a crash image before it is opened.</summary>
internal enum WalFault
{
    /// <summary>The crash image as is (a recycled WAL keeps the records of its previous use past the new data).</summary>
    None,
    /// <summary>The last record of the latest WAL cut in its payload.</summary>
    TornLastRecord,
    /// <summary>The latest WAL cut 3 bytes into the header of its last record.</summary>
    TruncatedLastHeader,
    /// <summary>A payload byte of a record in the middle of the latest WAL flipped (checksum mismatch).</summary>
    CorruptMiddleChecksum,
    /// <summary>The length field of a record in the middle of the latest WAL set to 0xFFFF (bad record length).</summary>
    CorruptMiddleLength,
    /// <summary>A payload byte of a record in the middle of the oldest WAL flipped.</summary>
    CorruptOldestWalChecksum,
    /// <summary>The latest WAL zeroed from its last record to its end (size kept).</summary>
    ZeroedTail,
    /// <summary>The bytes written after the last sync write dropped (power loss after the last fsync).</summary>
    DropUnsyncedTail,
    /// <summary>The oldest of at least two WALs deleted.</summary>
    MissingOldestWal,
    /// <summary>The latest of at least two WALs deleted.</summary>
    MissingLatestWal,
    /// <summary>A copy of the oldest WAL added as 000001.log (an obsolete WAL that was never deleted).</summary>
    ExtraStaleWalBelow,
    /// <summary>A copy of the oldest WAL added with the next free number (old sequence numbers in a newer WAL).</summary>
    StaleWalAbove,
    /// <summary>An empty WAL with the next free number (crash right after a WAL switch created it).</summary>
    EmptyNewWal,
    /// <summary>The two latest WALs swap names (out-of-order log numbers).</summary>
    SwappedLatestWals,
    /// <summary>The last record of the second-latest WAL dropped at a record boundary (mismatched predecessor info).</summary>
    DroppedPredecessorRecord,
}

/// <summary>A physical WAL record found by <see cref="WalImage.PhysicalRecords"/>.</summary>
internal readonly record struct PhysicalRecord(int Offset, int HeaderSize, int Length, byte Type)
{
    public int End => Offset + HeaderSize + Length;
}

/// <summary>A crash image of a DB written by one engine: the DB directory (and WAL directory) copied while it was open.</summary>
internal sealed class WalImage : IDisposable
{
    public required string Root { get; init; }
    public string DbDir => Path.Combine(Root, "db");
    public string WalDir => Path.Combine(Root, "wal");
    public required WalWriteConfig Config { get; init; }
    /// <summary>Latest WAL and its size right after the last sync write (DropUnsyncedTail), or (0, 0).</summary>
    public ulong SyncedWal { get; set; }
    public long SyncedWalSize { get; set; }
    /// <summary>Key and value of the last acknowledged sync write (must survive the faults that keep synced data).</summary>
    public string LastSyncedKey { get; set; } = "";
    public string LastSyncedValue { get; set; } = "";

    public string WalPath(ulong n) => FileNames.LogFileName(Config.SeparateWalDir ? WalDir : DbDir, n);

    public List<ulong> Wals() => WalMatrixUtil.Wals(Config.SeparateWalDir ? WalDir : DbDir);

    /// <summary>The valid physical records of WAL <paramref name="number"/> (crc checked; stops at a record of another log number).</summary>
    public static List<PhysicalRecord> PhysicalRecords(byte[] wal, ulong number)
    {
        var r = new List<PhysicalRecord>();
        int pos = 0;
        while (pos + LogFormat.HeaderSize <= wal.Length)
        {
            int blockLeft = LogFormat.BlockSize - pos % LogFormat.BlockSize;
            if (blockLeft < LogFormat.HeaderSize) { pos += blockLeft; continue; }
            int len = wal[pos + 4] | wal[pos + 5] << 8;
            byte type = wal[pos + 6];
            if (type == 0 && len == 0) break;
            bool recyclable = LogFormat.IsRecyclableType(type);
            int header = recyclable ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;
            if (pos + header + len > wal.Length) break;
            if (recyclable && BitConverter.ToUInt32(wal, pos + 7) != (uint)number) break;
            uint expected = Util.Crc32C.Unmask(BitConverter.ToUInt32(wal, pos));
            if (Util.Crc32C.Value(wal.AsSpan(pos + 6, header - 6 + len)) != expected) break;
            r.Add(new PhysicalRecord(pos, header, len, type));
            pos += header + len;
        }
        return r;
    }

    private static bool IsDataRecord(byte t) => t is (byte)LogRecordType.Full or (byte)LogRecordType.First or (byte)LogRecordType.Middle or (byte)LogRecordType.Last
        or (byte)LogRecordType.RecyclableFull or (byte)LogRecordType.RecyclableFirst or (byte)LogRecordType.RecyclableMiddle or (byte)LogRecordType.RecyclableLast;

    /// <summary>Copies the image into <paramref name="target"/> and applies <paramref name="fault"/>. Returns false when it does not apply.</summary>
    public bool CopyWithFault(string target, WalFault fault)
    {
        WalMatrixUtil.CopyTree(Root, target);
        var copy = new WalImage { Root = target, Config = Config, SyncedWal = SyncedWal, SyncedWalSize = SyncedWalSize };
        return copy.Apply(fault);
    }

    private bool Apply(WalFault fault)
    {
        List<ulong> wals = Wals();
        if (wals.Count == 0) return fault == WalFault.None;
        ulong latest = wals[^1];
        string latestPath = WalPath(latest);
        switch (fault)
        {
            case WalFault.None:
                return true;
            case WalFault.TornLastRecord:
            {
                List<PhysicalRecord> recs = DataRecords(latest);
                if (recs.Count == 0) return false;
                PhysicalRecord last = recs[^1];
                if (last.Length < 2) return false;
                Truncate(latestPath, last.Offset + last.HeaderSize + last.Length / 2);
                return true;
            }
            case WalFault.TruncatedLastHeader:
            {
                List<PhysicalRecord> recs = DataRecords(latest);
                if (recs.Count == 0) return false;
                Truncate(latestPath, recs[^1].Offset + 3);
                return true;
            }
            case WalFault.CorruptMiddleChecksum:
                return FlipMiddlePayload(latest);
            case WalFault.CorruptOldestWalChecksum:
                return wals.Count >= 2 && FlipMiddlePayload(wals[0]);
            case WalFault.CorruptMiddleLength:
            {
                List<PhysicalRecord> recs = DataRecords(latest);
                if (recs.Count < 3) return false;
                PhysicalRecord mid = recs[recs.Count / 2];
                byte[] b = File.ReadAllBytes(latestPath);
                b[mid.Offset + 4] = 0xFF;
                b[mid.Offset + 5] = 0xFF;
                File.WriteAllBytes(latestPath, b);
                return true;
            }
            case WalFault.ZeroedTail:
            {
                List<PhysicalRecord> recs = DataRecords(latest);
                if (recs.Count == 0) return false;
                byte[] b = File.ReadAllBytes(latestPath);
                Array.Clear(b, recs[^1].Offset, b.Length - recs[^1].Offset);
                File.WriteAllBytes(latestPath, b);
                return true;
            }
            case WalFault.DropUnsyncedTail:
                if (SyncedWal != latest || new FileInfo(latestPath).Length <= SyncedWalSize) return false;
                Truncate(latestPath, SyncedWalSize);
                return true;
            case WalFault.MissingOldestWal:
                if (wals.Count < 2) return false;
                File.Delete(WalPath(wals[0]));
                return true;
            case WalFault.MissingLatestWal:
                if (wals.Count < 2) return false;
                File.Delete(latestPath);
                return true;
            case WalFault.ExtraStaleWalBelow:
                if (wals[0] <= 1 || File.Exists(WalPath(1))) return false;
                File.Copy(WalPath(wals[0]), WalPath(1));
                return true;
            case WalFault.StaleWalAbove:
                File.Copy(WalPath(wals[0]), WalPath(NextFreeNumber()));
                return true;
            case WalFault.EmptyNewWal:
                File.WriteAllBytes(WalPath(NextFreeNumber()), []);
                return true;
            case WalFault.SwappedLatestWals:
            {
                if (wals.Count < 2) return false;
                string a = WalPath(wals[^2]), tmp = a + ".tmp";
                File.Move(a, tmp);
                File.Move(latestPath, a);
                File.Move(tmp, latestPath);
                return true;
            }
            case WalFault.DroppedPredecessorRecord:
            {
                if (wals.Count < 2) return false;
                List<PhysicalRecord> recs = DataRecords(wals[^2]);
                if (recs.Count < 2) return false;
                Truncate(WalPath(wals[^2]), recs[^1].Offset);
                return true;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(fault));
        }
    }

    private List<PhysicalRecord> DataRecords(ulong wal)
    {
        var r = new List<PhysicalRecord>();
        foreach (PhysicalRecord p in PhysicalRecords(File.ReadAllBytes(WalPath(wal)), wal))
        {
            if (IsDataRecord(p.Type)) r.Add(p);
        }
        return r;
    }

    private bool FlipMiddlePayload(ulong wal)
    {
        List<PhysicalRecord> recs = DataRecords(wal);
        if (recs.Count < 3) return false;
        PhysicalRecord mid = recs[recs.Count / 2];
        if (mid.Length == 0) return false;
        string path = WalPath(wal);
        byte[] b = File.ReadAllBytes(path);
        b[mid.Offset + mid.HeaderSize + mid.Length / 2] ^= 0x5A;
        File.WriteAllBytes(path, b);
        return true;
    }

    private ulong NextFreeNumber()
    {
        ulong max = 0;
        foreach (string dir in new[] { DbDir, WalDir })
        {
            if (!Directory.Exists(dir)) continue;
            foreach (string f in Directory.GetFiles(dir))
            {
                if (FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out _)) max = Math.Max(max, n);
            }
        }
        return max + 1;
    }

    private static void Truncate(string path, long length)
    {
        using FileStream f = File.Open(path, FileMode.Open, FileAccess.Write);
        f.SetLength(length);
    }

    public void Dispose()
    {
        try { Directory.Delete(Root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}

/// <summary>The result of opening an image: the failure (code + normalized message) or the recovered contents.</summary>
internal sealed record WalOpenOutcome(StatusCode? Code, string Message, string Contents, ulong LastSequence)
{
    public bool Ok => Code is null;

    public override string ToString() => Ok ? "OK seq=" + LastSequence : Code + ": " + Message;
}

internal static class WalMatrixUtil
{
    public static readonly string[] ColumnFamilies = ["default", "cf1"];

    public static List<ulong> Wals(string dir)
    {
        var r = new List<ulong>();
        if (!Directory.Exists(dir)) return r;
        foreach (string f in Directory.GetFiles(dir, "*.log"))
        {
            if (FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    /// <summary>Copies a directory tree, skipping LOCK files, info LOGs and files that vanish while copying.</summary>
    public static void CopyTree(string source, string target)
    {
        Directory.CreateDirectory(target);
        foreach (string d in Directory.GetDirectories(source)) CopyTree(d, Path.Combine(target, Path.GetFileName(d)));
        foreach (string f in Directory.GetFiles(source))
        {
            string name = Path.GetFileName(f);
            if (name == "LOCK" || name.StartsWith("LOG", StringComparison.Ordinal)) continue;
            try { File.Copy(f, Path.Combine(target, name)); }
            catch (FileNotFoundException) { }
        }
    }

    public static string Normalize(string message, string root) =>
        message.Replace(root, "<root>", StringComparison.Ordinal);

    /// <summary>Maps a native status string ("Corruption: ...") to a code.</summary>
    public static StatusCode NativeCode(string message)
    {
        if (message.StartsWith("Corruption", StringComparison.Ordinal)) return StatusCode.Corruption;
        if (message.StartsWith("IO error", StringComparison.Ordinal)) return StatusCode.IOError;
        if (message.StartsWith("Invalid argument", StringComparison.Ordinal)) return StatusCode.InvalidArgument;
        if (message.StartsWith("Not implemented", StringComparison.Ordinal)) return StatusCode.NotSupported;
        if (message.StartsWith("NotFound", StringComparison.Ordinal)) return StatusCode.NotFound;
        if (message.StartsWith("Operation aborted", StringComparison.Ordinal)) return StatusCode.Aborted;
        return StatusCode.Incomplete;
    }

    public static string Dump(Func<int, IEnumerable<(byte[] Key, byte[] Value)>> cf, int cfs)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < cfs; i++)
        {
            sb.Append('[').Append(ColumnFamilies[i]).Append(']');
            foreach ((byte[] k, byte[] v) in cf(i)) sb.Append(' ').Append(Encoding.UTF8.GetString(k)).Append('=').Append(Encoding.UTF8.GetString(v));
            sb.Append('\n');
        }
        return sb.ToString();
    }

    /// <summary>
    /// A strength-2 covering array over <paramref name="factors"/> binary factors: every pair of factors takes all four
    /// value combinations in some row (greedy construction, deterministic). Row 0 is all false.
    /// </summary>
    public static List<bool[]> Pairwise(int factors)
    {
        var rows = new List<bool[]> { new bool[factors] };
        var uncovered = new HashSet<(int, int, bool, bool)>();
        for (int a = 0; a < factors; a++)
        {
            for (int b = a + 1; b < factors; b++)
            {
                foreach (bool x in new[] { false, true })
                {
                    foreach (bool y in new[] { false, true })
                    {
                        if (x || y) uncovered.Add((a, b, x, y));
                    }
                }
            }
        }
        while (uncovered.Count > 0)
        {
            bool[]? best = null;
            int bestCount = -1;
            // Candidates: all 2^factors rows when small, else the complement of each existing row and single flips.
            for (int mask = 0; mask < 1 << factors; mask++)
            {
                var row = new bool[factors];
                for (int i = 0; i < factors; i++) row[i] = (mask >> i & 1) != 0;
                int count = 0;
                foreach ((int a, int b, bool x, bool y) in uncovered)
                {
                    if (row[a] == x && row[b] == y) count++;
                }
                if (count > bestCount)
                {
                    best = row;
                    bestCount = count;
                }
            }
            rows.Add(best!);
            uncovered.RemoveWhere(p => best![p.Item1] == p.Item3 && best![p.Item2] == p.Item4);
        }
        return rows;
    }
}
