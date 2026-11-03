using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Db;

/// <summary>
/// Cumulative per-level compaction statistics of one column family. Port of InternalStats::CompactionStats
/// (db/internal_stats.h), restricted to the fields the managed port measures.
/// </summary>
internal sealed class CompactionLevelStats
{
    public long Micros;
    public long CpuMicros;
    public ulong BytesReadNonOutputLevels;
    public ulong BytesReadOutputLevel;
    public ulong BytesWritten;
    public ulong BytesMoved;
    public int NumInputFilesInNonOutputLevels;
    public int NumInputFilesInOutputLevel;
    public int NumOutputFiles;
    public ulong NumInputRecords;
    public ulong NumDroppedRecords;
    public int Count;

    /// <summary>CompactionStats::Subtract (interval stats = cumulative - snapshot).</summary>
    public void Subtract(CompactionLevelStats o)
    {
        Micros -= o.Micros;
        CpuMicros -= o.CpuMicros;
        BytesReadNonOutputLevels -= o.BytesReadNonOutputLevels;
        BytesReadOutputLevel -= o.BytesReadOutputLevel;
        BytesWritten -= o.BytesWritten;
        BytesMoved -= o.BytesMoved;
        NumInputFilesInNonOutputLevels -= o.NumInputFilesInNonOutputLevels;
        NumInputFilesInOutputLevel -= o.NumInputFilesInOutputLevel;
        NumOutputFiles -= o.NumOutputFiles;
        NumInputRecords -= o.NumInputRecords;
        NumDroppedRecords -= o.NumDroppedRecords;
        Count -= o.Count;
    }

    public CompactionLevelStats Clone()
    {
        var c = new CompactionLevelStats();
        c.Add(this);
        return c;
    }

    public void Add(CompactionLevelStats o)
    {
        Micros += o.Micros;
        CpuMicros += o.CpuMicros;
        BytesReadNonOutputLevels += o.BytesReadNonOutputLevels;
        BytesReadOutputLevel += o.BytesReadOutputLevel;
        BytesWritten += o.BytesWritten;
        BytesMoved += o.BytesMoved;
        NumInputFilesInNonOutputLevels += o.NumInputFilesInNonOutputLevels;
        NumInputFilesInOutputLevel += o.NumInputFilesInOutputLevel;
        NumOutputFiles += o.NumOutputFiles;
        NumInputRecords += o.NumInputRecords;
        NumDroppedRecords += o.NumDroppedRecords;
        Count += o.Count;
    }
}

/// <summary>
/// Per column family cumulative statistics behind the "rocksdb.cfstats" property. Port of the column family part of
/// InternalStats (db/internal_stats.{h,cc}: comp_stats_, cf_stats_value_[BYTES_FLUSHED / BYTES_INGESTED_ADD_FILE],
/// DumpCFStats / DumpCFMapStats, PrintLevelStatsHeader / PrintLevelStats). Updated by flushes and compactions
/// (rare), so a lock is enough.
/// </summary>
internal sealed class InternalStats
{
    private readonly Lock _lock = new();
    private readonly CompactionLevelStats[] _levels;
    private ulong _bytesFlushed;
    private ulong _bytesIngestedAddFile;
    private ulong _flushCount;
    private ulong _ingestedFiles;
    private readonly long _startTicks = Environment.TickCount64;

    /// <summary>cf_stats_snapshot_ (CFStatsSnapshot): values at the last periodic dump, for the interval columns.</summary>
    private sealed class CfStatsSnapshot
    {
        public double SecondsUp;
        public ulong IngestBytesFlush;
        public ulong IngestBytesAddFile;
        public ulong IngestFilesAddFile;
        public ulong CompactBytesWrite;
        public ulong CompactBytesRead;
        public long CompactMicros;
        public CompactionLevelStats CompStats = new();
    }

    private readonly CfStatsSnapshot _snapshot = new();

    public InternalStats(int numLevels)
    {
        _levels = new CompactionLevelStats[Math.Max(1, numLevels)];
        for (int i = 0; i < _levels.Length; i++) _levels[i] = new CompactionLevelStats();
    }

    public void AddFlush(ulong bytes, long micros)
    {
        lock (_lock)
        {
            _bytesFlushed += bytes;
            _flushCount++;
            CompactionLevelStats l0 = _levels[0];
            l0.BytesWritten += bytes;
            l0.Micros += micros;
            l0.Count++;
            if (bytes > 0) l0.NumOutputFiles++;
        }
    }

    public void AddIngest(ulong bytes, int files)
    {
        lock (_lock)
        {
            _bytesIngestedAddFile += bytes;
            _ingestedFiles += (ulong)files;
        }
    }

    public void AddCompaction(int outputLevel, CompactionLevelStats stats)
    {
        lock (_lock) _levels[Math.Min(outputLevel, _levels.Length - 1)].Add(stats);
    }

    public ulong BytesFlushed
    {
        get
        {
            lock (_lock) return _bytesFlushed;
        }
    }

    public ulong FlushCount
    {
        get
        {
            lock (_lock) return _flushCount;
        }
    }

    /// <summary>Total compaction bytes read / written over all levels.</summary>
    public (ulong Read, ulong Written, long Micros, int Count) CompactionTotals()
    {
        lock (_lock)
        {
            ulong r = 0, w = 0;
            long us = 0;
            int n = 0;
            for (int l = 1; l < _levels.Length; l++)
            {
                r += _levels[l].BytesReadNonOutputLevels + _levels[l].BytesReadOutputLevel;
                w += _levels[l].BytesWritten;
                us += _levels[l].Micros;
                n += _levels[l].Count;
            }
            return (r, w, us, n);
        }
    }

    // ---- formatting ---------------------------------------------------------------------------------

    private const double KB = 1024.0, MB = KB * KB, GB = MB * KB, MicrosInSec = 1000000.0;

    private enum Stat
    {
        NumFiles, CompactedFiles, SizeBytes, Score, ReadGb, RnGb, Rnp1Gb, WriteGb, WritePreCompGb, WNewGb, MovedGb, WriteAmp,
        ReadMBps, WriteMBps, CompSec, CompCpuSec, CompCount, AvgSec, KeyIn, KeyDrop, RBlobGb, WBlobGb,
    }

    // InternalStats::compaction_level_stats property names (map keys), in LevelStatType order.
    private static readonly string[] s_propertyNames =
    [
        "NumFiles", "CompactedFiles", "SizeBytes", "Score", "ReadGB", "RnGB", "Rnp1GB", "WriteGB", "WPreCompGB", "WnewGB", "MovedGB",
        "WriteAmp", "ReadMBps", "WriteMBps", "CompSec", "CompMergeCPU", "CompCount", "AvgSec", "KeyIn", "KeyDrop", "RblobGB", "WblobGB",
    ];

    /// <summary>PrepareLevelStats.</summary>
    private static double[] PrepareLevelStats(int numFiles, int beingCompacted, double totalFileSize, double score, double wAmp,
        CompactionLevelStats s)
    {
        ulong bytesRead = s.BytesReadNonOutputLevels + s.BytesReadOutputLevel;
        ulong bytesWritten = s.BytesWritten;
        long bytesNew = (long)s.BytesWritten - (long)s.BytesReadOutputLevel;
        double elapsed = (s.Micros + 1) / MicrosInSec;
        var v = new double[s_propertyNames.Length];
        v[(int)Stat.NumFiles] = numFiles;
        v[(int)Stat.CompactedFiles] = beingCompacted;
        v[(int)Stat.SizeBytes] = totalFileSize;
        v[(int)Stat.Score] = score;
        v[(int)Stat.ReadGb] = bytesRead / GB;
        v[(int)Stat.RnGb] = s.BytesReadNonOutputLevels / GB;
        v[(int)Stat.Rnp1Gb] = s.BytesReadOutputLevel / GB;
        v[(int)Stat.WriteGb] = s.BytesWritten / GB;
        v[(int)Stat.WritePreCompGb] = s.BytesWritten / GB;
        v[(int)Stat.WNewGb] = bytesNew / GB;
        v[(int)Stat.MovedGb] = s.BytesMoved / GB;
        v[(int)Stat.WriteAmp] = wAmp;
        v[(int)Stat.ReadMBps] = bytesRead / MB / elapsed;
        v[(int)Stat.WriteMBps] = bytesWritten / MB / elapsed;
        v[(int)Stat.CompSec] = s.Micros / MicrosInSec;
        v[(int)Stat.CompCpuSec] = s.CpuMicros / MicrosInSec;
        v[(int)Stat.CompCount] = s.Count;
        v[(int)Stat.AvgSec] = s.Count == 0 ? 0 : s.Micros / MicrosInSec / s.Count;
        v[(int)Stat.KeyIn] = s.NumInputRecords;
        v[(int)Stat.KeyDrop] = s.NumDroppedRecords;
        return v;
    }

    /// <summary>
    /// Per-level statistics rows (level -1 = "Sum"), port of InternalStats::DumpCFMapStats(vstorage, ...): only levels
    /// with files or compaction activity are listed.
    /// </summary>
    private List<(int Level, double[] Values)> LevelRows(VersionStorageInfo vstorage) => LevelRows(vstorage, out _);

    private List<(int Level, double[] Values)> LevelRows(VersionStorageInfo vstorage, out CompactionLevelStats sum)
    {
        var rows = new List<(int, double[])>();
        sum = new CompactionLevelStats();
        int totalFiles = 0;
        double totalSize = 0;
        lock (_lock)
        {
            ulong currIngest = _bytesFlushed + _bytesIngestedAddFile;
            for (int level = 0; level < vstorage.NumLevels; level++)
            {
                CompactionLevelStats s = level < _levels.Length ? _levels[level] : new CompactionLevelStats();
                int files = vstorage.NumLevelFiles(level);
                totalFiles += files;
                if (s.Micros > 0 || s.CpuMicros > 0 || files > 0)
                {
                    sum.Add(s);
                    double size = vstorage.NumLevelBytes(level);
                    totalSize += size;
                    ulong inputBytes = level == 0 ? currIngest : s.BytesReadNonOutputLevels;
                    double wAmp = inputBytes == 0 ? 0 : (double)s.BytesWritten / inputBytes;
                    rows.Add((level, PrepareLevelStats(files, 0, size, vstorage.ScoreOfLevel(level), wAmp, s)));
                }
            }
            double sumWAmp = currIngest == 0 ? 0 : (double)(sum.BytesWritten) / currIngest;
            rows.Add((-1, PrepareLevelStats(totalFiles, 0, totalSize, 0, sumWAmp, sum)));
        }
        return rows;
    }

    /// <summary>The "rocksdb.cfstats" map form ("compaction.L0.NumFiles" = "1.000000", ...; values as std::to_string(double)).</summary>
    public Dictionary<string, string> ToMap(VersionStorageInfo vstorage)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((int level, double[] v) in LevelRows(vstorage))
        {
            string l = level == -1 ? "Sum" : "L" + level.ToString(CultureInfo.InvariantCulture);
            for (int i = 0; i < v.Length; i++) map["compaction." + l + "." + s_propertyNames[i]] = Statistics.Fixed6(v[i]);
        }
        return map;
    }

    /// <summary>
    /// The "rocksdb.cfstats" text (InternalStats::DumpCFStatsNoFileHistogram): the level table with the "Int" (interval)
    /// row, then the flush / add-file / compaction summary with cumulative and interval values. A periodic dump
    /// (<paramref name="isPeriodic"/>: rocksdb.cfstats, rocksdb.stats and the stats_dump_period_sec dump) starts a new
    /// interval; rocksdb.cfstats-no-file-histogram does not.
    /// </summary>
    public string Dump(string cfName, VersionStorageInfo vstorage, bool isPeriodic)
    {
        var sb = new StringBuilder();
        sb.Append("\n** Compaction Stats [").Append(cfName).Append("] **\n");
        string header = string.Format(CultureInfo.InvariantCulture,
            "{0}    {1}   {2}     {3} {4}  {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16} {17} {18} {19} {20} {21}\n",
            "Level", "Files", "Size", "Score", "Read(GB)", "Rn(GB)", "Rnp1(GB)", "Write(GB)", "Wnew(GB)", "Moved(GB)", "W-Amp",
            "Rd(MB/s)", "Wr(MB/s)", "Comp(sec)", "CompMergeCPU(sec)", "Comp(cnt)", "Avg(sec)", "KeyIn", "KeyDrop", "Rblob(GB)", "Wblob(GB)", "");
        header = header.TrimEnd(' ', '\n') + "\n";
        sb.Append(header);
        sb.Append('-', header.Length - 1).Append('\n');
        List<(int Level, double[] Values)> rows = LevelRows(vstorage, out CompactionLevelStats sumStats);
        foreach ((int level, double[] v) in rows) AppendRow(sb, level == -1 ? "Sum" : "L" + level.ToString(CultureInfo.InvariantCulture), v);

        double uptime = (Environment.TickCount64 - _startTicks) / 1000.0;
        ulong flushed, ingested, ingestedFiles;
        lock (_lock)
        {
            flushed = _bytesFlushed;
            ingested = _bytesIngestedAddFile;
            ingestedFiles = _ingestedFiles;
        }
        CfStatsSnapshot snap = _snapshot;
        // Interval summary: PrintLevelStats(..., "Int", 0, 0, 0, 0, w_amp, interval_stats)
        ulong intervalFlushIngest = flushed - snap.IngestBytesFlush;
        ulong intervalAddFileIngest = ingested - snap.IngestBytesAddFile;
        ulong intervalIngest = intervalFlushIngest + intervalAddFileIngest + 1;
        CompactionLevelStats intervalStats = sumStats.Clone();
        intervalStats.Subtract(snap.CompStats);
        double intervalWAmp = intervalStats.BytesWritten / (double)intervalIngest;
        AppendRow(sb, "Int", PrepareLevelStats(0, 0, 0, 0, intervalWAmp, intervalStats));

        (ulong read, ulong written, long micros, _) = CompactionTotals();
        double intervalUp = uptime - snap.SecondsUp;
        sb.Append(string.Format(CultureInfo.InvariantCulture, "\nUptime(secs): {0:F1} total, {1:F1} interval\n", uptime, intervalUp));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Flush(GB): cumulative {0:F3}, interval {1:F3}\n", flushed / GB, intervalFlushIngest / GB));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "AddFile(GB): cumulative {0:F3}, interval {1:F3}\n", ingested / GB, intervalAddFileIngest / GB));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "AddFile(Total Files): cumulative {0}, interval {1}\n", ingestedFiles,
            ingestedFiles - snap.IngestFilesAddFile));
        // Compact: the flush (L0) bytes count as compaction writes in the port's per-level stats (level 0 row).
        ulong compactWrite = written + flushed;
        double w = compactWrite / GB, r = read / GB, secs = micros / MicrosInSec;
        sb.Append(string.Format(CultureInfo.InvariantCulture,
            "Cumulative compaction: {0:F2} GB write, {1:F2} MB/s write, {2:F2} GB read, {3:F2} MB/s read, {4:F1} seconds\n",
            w, compactWrite / MB / Math.Max(uptime, 0.001), r, read / MB / Math.Max(uptime, 0.001), secs));
        ulong iWrite = compactWrite - snap.CompactBytesWrite, iRead = read - snap.CompactBytesRead;
        long iMicros = micros - snap.CompactMicros;
        sb.Append(string.Format(CultureInfo.InvariantCulture,
            "Interval compaction: {0:F2} GB write, {1:F2} MB/s write, {2:F2} GB read, {3:F2} MB/s read, {4:F1} seconds\n",
            iWrite / GB, iWrite / MB / Math.Max(intervalUp, 0.001), iRead / GB, iRead / MB / Math.Max(intervalUp, 0.001), iMicros / MicrosInSec));
        if (isPeriodic)
        {
            snap.SecondsUp = uptime;
            snap.IngestBytesFlush = flushed;
            snap.IngestBytesAddFile = ingested;
            snap.IngestFilesAddFile = ingestedFiles;
            snap.CompactBytesWrite = compactWrite;
            snap.CompactBytesRead = read;
            snap.CompactMicros = micros;
            snap.CompStats = sumStats;
        }
        return sb.ToString();
    }

    /// <summary>PrintLevelStats.</summary>
    private static void AppendRow(StringBuilder sb, string name, double[] v) =>
        sb.Append(string.Format(CultureInfo.InvariantCulture,
            "{0,4} {1,6}/{2,-3} {3,10} {4,5:F1} {5,8:F1} {6,7:F1} {7,8:F1} {8,9:F1} {9,8:F1} {10,9:F1} {11,5:F1} {12,8:F1} {13,8:F1} {14,9:F2} {15,17:F2} {16,9} {17,8:F3} {18,7} {19,6} {20,9:F1} {21,9:F1}\n",
            name, (int)v[(int)Stat.NumFiles], (int)v[(int)Stat.CompactedFiles], BytesToHumanString((ulong)v[(int)Stat.SizeBytes]),
            v[(int)Stat.Score], v[(int)Stat.ReadGb], v[(int)Stat.RnGb], v[(int)Stat.Rnp1Gb], v[(int)Stat.WriteGb], v[(int)Stat.WNewGb],
            v[(int)Stat.MovedGb], v[(int)Stat.WriteAmp], v[(int)Stat.ReadMBps], v[(int)Stat.WriteMBps], v[(int)Stat.CompSec],
            v[(int)Stat.CompCpuSec], (int)v[(int)Stat.CompCount], v[(int)Stat.AvgSec], NumberToHumanString((long)v[(int)Stat.KeyIn]),
            NumberToHumanString((long)v[(int)Stat.KeyDrop]), v[(int)Stat.RBlobGb], v[(int)Stat.WBlobGb]));

    /// <summary>BytesToHumanString (util/string_util.cc).</summary>
    internal static string BytesToHumanString(ulong bytes)
    {
        string[] sizeName = ["KB", "MB", "GB", "TB"];
        double finalSize = bytes;
        int sizeIdx;
        // always start with KB
        finalSize /= 1024;
        sizeIdx = 0;
        while (sizeIdx < 3 && finalSize >= 1024)
        {
            finalSize /= 1024;
            sizeIdx++;
        }
        return finalSize.ToString("F2", CultureInfo.InvariantCulture) + " " + sizeName[sizeIdx];
    }

    /// <summary>NumberToHumanString (util/string_util.cc).</summary>
    internal static string NumberToHumanString(long num)
    {
        long absnum = num < 0 ? -num : num;
        if (absnum < 10000) return num.ToString(CultureInfo.InvariantCulture);
        if (absnum < 10000000) return (num / 1000).ToString(CultureInfo.InvariantCulture) + "K";
        if (absnum < 10000000000L) return (num / 1000000).ToString(CultureInfo.InvariantCulture) + "M";
        return (num / 1000000000).ToString(CultureInfo.InvariantCulture) + "G";
    }
}

/// <summary>
/// DB-wide cumulative write statistics behind "rocksdb.dbstats" (InternalStats db_stats_: WAL_FILE_BYTES,
/// WAL_FILE_SYNCED, BYTES_WRITTEN, NUMBER_KEYS_WRITTEN, WRITE_DONE_BY_SELF / OTHER, WRITE_WITH_WAL, write stall
/// micros). Updated once per write group with <see cref="Interlocked"/>.
/// </summary>
internal sealed class DbWriteStats
{
    public long WalFileBytes;
    public long WalFileSynced;
    public long BytesWritten;
    public long NumKeysWritten;
    public long WriteDoneBySelf;
    public long WriteDoneByOther;
    public long WriteWithWal;
    public long WriteStallMicros;
    private readonly long _startTicks = Environment.TickCount64;

    /// <summary>db_stats_snapshot_ (DBStatsSnapshot): values at the previous dump.</summary>
    private double _snapSecondsUp;
    private long _snapIngestBytes, _snapWriteOther, _snapWriteSelf, _snapNumKeys, _snapWalBytes, _snapWalSynced, _snapWriteWithWal, _snapStall;
    private readonly Lock _dumpLock = new();

    /// <summary>
    /// InternalStats::DumpDBStats: cumulative and interval (since the previous dump, which every call starts) writes,
    /// WAL and stall lines.
    /// </summary>
    public string Dump()
    {
        const double GB = 1024.0 * 1024 * 1024, MB = 1024.0 * 1024;
        lock (_dumpLock)
        {
            double uptime = (Environment.TickCount64 - _startTicks) / 1000.0;
            double intervalUp = uptime - _snapSecondsUp;
            long self = Interlocked.Read(ref WriteDoneBySelf), other = Interlocked.Read(ref WriteDoneByOther);
            long keys = Interlocked.Read(ref NumKeysWritten), bytes = Interlocked.Read(ref BytesWritten);
            long walBytes = Interlocked.Read(ref WalFileBytes), synced = Interlocked.Read(ref WalFileSynced), withWal = Interlocked.Read(ref WriteWithWal);
            long stall = Interlocked.Read(ref WriteStallMicros);
            var sb = new StringBuilder();
            sb.Append(string.Format(CultureInfo.InvariantCulture, "\n** DB Stats **\nUptime(secs): {0:F1} total, {1:F1} interval\n", uptime, intervalUp));
            sb.Append(string.Format(CultureInfo.InvariantCulture,
                "Cumulative writes: {0} writes, {1} keys, {2} commit groups, {3:F1} writes per commit group, ingest: {4:F2} GB, {5:F2} MB/s\n",
                InternalStats.NumberToHumanString(other + self), InternalStats.NumberToHumanString(keys), InternalStats.NumberToHumanString(self),
                (other + self) / Math.Max(1.0, self), bytes / GB, bytes / MB / Math.Max(uptime, 0.001)));
            sb.Append(string.Format(CultureInfo.InvariantCulture,
                "Cumulative WAL: {0} writes, {1} syncs, {2:F2} writes per sync, written: {3:F2} GB, {4:F2} MB/s\n",
                InternalStats.NumberToHumanString(withWal), InternalStats.NumberToHumanString(synced), withWal / Math.Max(1.0, synced),
                walBytes / GB, walBytes / MB / Math.Max(uptime, 0.001)));
            sb.Append("Cumulative stall: ").Append(HumanMicrosFixed(stall))
                .Append(string.Format(CultureInfo.InvariantCulture, ", {0:F1} percent\n", stall / 10000.0 / Math.Max(uptime, 0.001)));

            long iOther = other - _snapWriteOther, iSelf = self - _snapWriteSelf, iKeys = keys - _snapNumKeys, iBytes = bytes - _snapIngestBytes;
            sb.Append(string.Format(CultureInfo.InvariantCulture,
                "Interval writes: {0} writes, {1} keys, {2} commit groups, {3:F1} writes per commit group, ingest: {4:F2} MB, {5:F2} MB/s\n",
                InternalStats.NumberToHumanString(iOther + iSelf), InternalStats.NumberToHumanString(iKeys), InternalStats.NumberToHumanString(iSelf),
                (iOther + iSelf) / Math.Max(1.0, iSelf), iBytes / MB, iBytes / MB / Math.Max(intervalUp, 0.001)));
            long iWithWal = withWal - _snapWriteWithWal, iSynced = synced - _snapWalSynced, iWalBytes = walBytes - _snapWalBytes;
            sb.Append(string.Format(CultureInfo.InvariantCulture,
                "Interval WAL: {0} writes, {1} syncs, {2:F2} writes per sync, written: {3:F2} GB, {4:F2} MB/s\n",
                InternalStats.NumberToHumanString(iWithWal), InternalStats.NumberToHumanString(iSynced), iWithWal / Math.Max(1.0, iSynced),
                iWalBytes / GB, iWalBytes / MB / Math.Max(intervalUp, 0.001)));
            long iStall = stall - _snapStall;
            sb.Append("Interval stall: ").Append(HumanMicrosFixed(iStall))
                .Append(string.Format(CultureInfo.InvariantCulture, ", {0:F1} percent\n", iStall / 10000.0 / Math.Max(intervalUp, 0.001)));

            _snapSecondsUp = uptime;
            _snapIngestBytes = bytes;
            _snapWriteOther = other;
            _snapWriteSelf = self;
            _snapNumKeys = keys;
            _snapWalBytes = walBytes;
            _snapWalSynced = synced;
            _snapWriteWithWal = withWal;
            _snapStall = stall;
            return sb.ToString();
        }
    }

    /// <summary>AppendHumanMicros(micros, fixed_format = true): "%02d:%02d:%05.3f H:M:S".</summary>
    internal static string HumanMicrosFixed(long micros)
    {
        ulong m = (ulong)Math.Max(micros, 0);
        double seconds = (m % 60000000) / 1000000.0;
        return string.Format(CultureInfo.InvariantCulture, "{0:D2}:{1:D2}:{2} H:M:S", m / 1000000 / 3600, (m / 1000000 / 60) % 60,
            seconds.ToString("F3", CultureInfo.InvariantCulture).PadLeft(5, '0'));
    }
}
