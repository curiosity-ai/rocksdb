using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>
/// The type of an entry passed to <see cref="TablePropertiesCollector.AddUserKey"/>. Port of rocksdb::EntryType
/// (include/rocksdb/types.h); values match upstream.
/// </summary>
public enum TableEntryType
{
    Put = 0,
    Delete = 1,
    SingleDelete = 2,
    Merge = 3,
    RangeDeletion = 4,
    BlobIndex = 5,
    DeleteWithTimestamp = 6,
    WideColumnEntity = 7,
    /// <summary>A TimedPut (value with preferred sequence number) that hasn't yet been converted to a Put.</summary>
    TimedPut = 8,
    Other = 9,
}

/// <summary>
/// Collects user-defined properties of a table file while it is built. Port of rocksdb::TablePropertiesCollector
/// (include/rocksdb/table_properties.h). One instance is created per table file by a
/// <see cref="TablePropertiesCollectorFactory"/>, used by a single thread, and discarded after <see cref="Finish"/>.
/// The properties it returns are stored in the file's properties block ("user collected properties") and are readable
/// by native RocksDB, e.g. through <see cref="ColumnFamily.GetPropertiesOfAllTables"/> /
/// <see cref="Table.TableProperties.UserCollectedProperties"/>.
/// <para>
/// Errors: like upstream (where a non-OK Status is logged and ignored), an exception thrown by <see cref="AddUserKey"/>
/// or <see cref="BlockAdd"/> does not fail the table build; an exception from <see cref="Finish"/> drops this collector's
/// properties.
/// </para>
/// </summary>
public abstract class TablePropertiesCollector
{
    /// <summary>Name, used for debugging (upstream Name()).</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Called for every entry added to the table, in key order, including range deletions (whose value is the end key).
    /// <paramref name="fileSize"/> is the size of the file written so far (data blocks finished before the current one).
    /// </summary>
    public virtual void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
    {
    }

    /// <summary>
    /// Called after each data block is cut with its uncompressed size. The compressed-size estimates
    /// (sample_for_compression) are always 0 in this port.
    /// </summary>
    public virtual void BlockAdd(ulong blockUncompressedBytes, ulong blockCompressedBytesFast, ulong blockCompressedBytesSlow)
    {
    }

    /// <summary>
    /// Called once when the table is complete: add the collected properties to <paramref name="properties"/> (names
    /// should not collide with other collectors or the built-in "rocksdb.*" properties).
    /// </summary>
    public abstract void Finish(IDictionary<string, byte[]> properties);

    /// <summary>Human-readable form of the properties (for logging); called after <see cref="Finish"/>.</summary>
    public virtual IReadOnlyDictionary<string, string> GetReadableProperties() => s_empty;

    /// <summary>
    /// Whether the output file should be further compacted: the file is then marked for compaction
    /// (FileMetaData::marked_for_compaction) by flushes and compactions.
    /// </summary>
    public virtual bool NeedCompact => false;

    private static readonly Dictionary<string, string> s_empty = new();
}

/// <summary>Port of TablePropertiesCollectorFactory::Context: where the table is being created.</summary>
/// <param name="ColumnFamilyId">Column family id (<see cref="Table.TableProperties.UnknownColumnFamily"/> when unknown).</param>
/// <param name="LevelAtCreation">Level of the file (-1 unknown, e.g. external files).</param>
/// <param name="NumLevels">Number of levels of the column family (-1 unknown).</param>
/// <param name="LastLevelInclusiveMaxSeqnoThreshold">
/// Tiering: data with sequence numbers at or below this are eligible for the last level (max value: tiering disabled).
/// </param>
public readonly record struct TablePropertiesCollectorContext(uint ColumnFamilyId, int LevelAtCreation, int NumLevels, ulong LastLevelInclusiveMaxSeqnoThreshold)
{
    /// <summary>kUnknownLevelAtCreation / kUnknownNumLevels.</summary>
    public const int UnknownLevel = -1;
}

/// <summary>
/// Creates a <see cref="TablePropertiesCollector"/> for each table file of a column family
/// (<see cref="ColumnFamilyOptions.TablePropertiesCollectorFactories"/>). Port of rocksdb::TablePropertiesCollectorFactory.
/// Must be thread-safe: flushes and compactions create collectors concurrently.
/// </summary>
public abstract class TablePropertiesCollectorFactory
{
    /// <summary>Name; the names of all configured factories are recorded in "rocksdb.property.collectors".</summary>
    public abstract string Name { get; }

    /// <summary>Returns a new collector for one table, or null to decline collecting for this file.</summary>
    public abstract TablePropertiesCollector? CreateTablePropertiesCollector(TablePropertiesCollectorContext context);

    /// <summary>Name followed by configuration info (logged when the DB is opened).</summary>
    public override string ToString() => Name;
}

/// <summary>
/// Marks a table file for compaction when it contains many deletions: at least <c>deletionTrigger</c> deletions within
/// any window of <c>slidingWindowSize</c> consecutive entries, or (when <c>deletionRatio</c> is in (0, 1]) a deletion
/// ratio at or above it; only files of at least <c>minFileSize</c> bytes. Port of CompactOnDeletionCollectorFactory
/// (utilities/table_properties_collectors/compact_on_deletion_collector.cc).
/// </summary>
public sealed class CompactOnDeletionCollectorFactory : TablePropertiesCollectorFactory
{
    /// <summary>kClassName.</summary>
    public const string ClassName = "CompactOnDeletionCollector";

    private long _slidingWindowSize;
    private long _deletionTrigger;
    private double _deletionRatio;
    private ulong _minFileSize;

    public CompactOnDeletionCollectorFactory(ulong slidingWindowSize, ulong deletionTrigger, double deletionRatio, ulong minFileSize)
    {
        WindowSize = slidingWindowSize;
        DeletionTrigger = deletionTrigger;
        DeletionRatio = deletionRatio;
        MinFileSize = minFileSize;
    }

    public override string Name => ClassName;

    /// <summary>Sliding window size "N" (0 disables the window trigger). Can be changed while the DB runs.</summary>
    public ulong WindowSize
    {
        get => (ulong)Interlocked.Read(ref _slidingWindowSize);
        set => Interlocked.Exchange(ref _slidingWindowSize, (long)value);
    }

    /// <summary>Deletion trigger "D": deletions within the window that mark the file.</summary>
    public ulong DeletionTrigger
    {
        get => (ulong)Interlocked.Read(ref _deletionTrigger);
        set => Interlocked.Exchange(ref _deletionTrigger, (long)value);
    }

    /// <summary>Deletion ratio threshold; values outside (0, 1] disable the ratio trigger.</summary>
    public double DeletionRatio
    {
        get => Volatile.Read(ref _deletionRatio);
        set => Volatile.Write(ref _deletionRatio, value);
    }

    /// <summary>Minimum file size for either trigger.</summary>
    public ulong MinFileSize
    {
        get => Volatile.Read(ref _minFileSize);
        set => Volatile.Write(ref _minFileSize, value);
    }

    public override TablePropertiesCollector? CreateTablePropertiesCollector(TablePropertiesCollectorContext context) =>
        new Collector(WindowSize, DeletionTrigger, DeletionRatio, MinFileSize);

    public override string ToString() =>
        Name + " (Sliding window size = " + WindowSize + " Deletion trigger = " + DeletionTrigger + " Deletion ratio = "
        + DeletionRatio.ToString(System.Globalization.CultureInfo.InvariantCulture) + ")";

    /// <summary>Port of CompactOnDeletionCollector.</summary>
    private sealed class Collector : TablePropertiesCollector
    {
        private const int NumBuckets = 128;
        private readonly ulong[] _numDeletionsInBuckets = new ulong[NumBuckets];
        private readonly ulong _bucketSize;
        private int _currentBucket;
        private ulong _numKeysInCurrentBucket;
        private ulong _numDeletionsInObservationWindow;
        private readonly ulong _deletionTrigger;
        private readonly double _deletionRatio;
        private readonly ulong _minFileSize;
        private ulong _curFileSize;
        private ulong _maxDeletionInWindow;
        private readonly bool _deletionRatioEnabled;
        private ulong _totalEntries;
        private ulong _deletionEntries;
        private bool _needCompaction;

        public Collector(ulong slidingWindowSize, ulong deletionTrigger, double deletionRatio, ulong minFileSize)
        {
            _bucketSize = (slidingWindowSize + NumBuckets - 1) / NumBuckets;
            _deletionTrigger = deletionTrigger;
            _deletionRatio = deletionRatio;
            _minFileSize = minFileSize;
            _deletionRatioEnabled = deletionRatio > 0 && deletionRatio <= 1;
        }

        public override string Name => ClassName;
        public override bool NeedCompact => _needCompaction;

        public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
        {
            // This collector is effectively disabled
            if (_bucketSize == 0 && !_deletionRatioEnabled) return;
            // If the output file already needs to be compacted, skip the check.
            if (_needCompaction) return;
            bool isDelete = type is TableEntryType.Delete or TableEntryType.SingleDelete or TableEntryType.DeleteWithTimestamp;
            if (_deletionRatioEnabled)
            {
                _totalEntries++;
                if (isDelete) _deletionEntries++;
                _curFileSize = fileSize;
            }
            if (_bucketSize != 0)
            {
                if (_numKeysInCurrentBucket == _bucketSize)
                {
                    // When the current bucket is full, advance the cursor of the ring buffer to the next bucket and drop
                    // the deletions of the oldest bucket from the observation window.
                    _currentBucket = (_currentBucket + 1) % NumBuckets;
                    _numDeletionsInObservationWindow -= _numDeletionsInBuckets[_currentBucket];
                    _numDeletionsInBuckets[_currentBucket] = 0;
                    _numKeysInCurrentBucket = 0;
                }
                _numKeysInCurrentBucket++;
                if (isDelete)
                {
                    _numDeletionsInObservationWindow++;
                    _numDeletionsInBuckets[_currentBucket]++;
                    if (_numDeletionsInObservationWindow >= _maxDeletionInWindow) _maxDeletionInWindow = _numDeletionsInObservationWindow;
                }
                // The file may qualify for compaction based on file size constraints, even if max_deletion_in_window_ is
                // not updated.
                if (_maxDeletionInWindow >= _deletionTrigger && fileSize >= _minFileSize) _needCompaction = true;
            }
        }

        public override void Finish(IDictionary<string, byte[]> properties)
        {
            if (!_needCompaction && _deletionRatioEnabled && _totalEntries > 0 && _curFileSize >= _minFileSize)
            {
                double ratio = (double)_deletionEntries / _totalEntries;
                _needCompaction = ratio >= _deletionRatio;
            }
        }
    }
}

/// <summary>
/// Marks files for compaction when enough of their entries are eligible for the last level under the tiering feature
/// (preclude_last_level_data_seconds): an entry is eligible when its sequence number (a TimedPut's preferred seqno) is
/// below the compaction's last-level threshold. Files with at least <c>compactionTriggerRatio</c> x entries eligible are
/// marked, and files with eligible entries record "rocksdb.eligible.last.level.entries". Disabled (no collector) for
/// ratios &lt;= 0, files created on the last level, and column families without tiering (flushes, ingestion). Port of
/// CompactForTieringCollectorFactory (utilities/table_properties_collectors/compact_for_tiering_collector.cc); like
/// upstream, data age stats are not collected.
/// </summary>
public sealed class CompactForTieringCollectorFactory : TablePropertiesCollectorFactory
{
    /// <summary>kClassName.</summary>
    public const string ClassName = "CompactForTieringCollector";
    /// <summary>kNumEligibleLastLevelEntriesPropertyName.</summary>
    public const string NumEligibleLastLevelEntriesPropertyName = "rocksdb.eligible.last.level.entries";

    private double _compactionTriggerRatio;

    /// <param name="compactionTriggerRatio">Threshold for the ratio of eligible entries to all entries (in (0, 1]).</param>
    public CompactForTieringCollectorFactory(double compactionTriggerRatio) => CompactionTriggerRatio = compactionTriggerRatio;

    public override string Name => ClassName;

    /// <summary>compaction_trigger_ratio; can be changed while the DB runs (affects files created afterwards).</summary>
    public double CompactionTriggerRatio
    {
        get => Volatile.Read(ref _compactionTriggerRatio);
        set => Volatile.Write(ref _compactionTriggerRatio, value);
    }

    public override TablePropertiesCollector? CreateTablePropertiesCollector(TablePropertiesCollectorContext context)
    {
        double ratio = CompactionTriggerRatio;
        if (ratio <= 0 || context.LevelAtCreation == context.NumLevels - 1
            || context.LastLevelInclusiveMaxSeqnoThreshold == DbFormat.MaxSequenceNumber)
        {
            return null;
        }
        return new Collector(context.LastLevelInclusiveMaxSeqnoThreshold, ratio);
    }

    public override string ToString() =>
        Name + ", compaction trigger ratio:" + CompactionTriggerRatio.ToString(System.Globalization.CultureInfo.InvariantCulture) + "\n";

    /// <summary>Port of CompactForTieringCollector.</summary>
    private sealed class Collector : TablePropertiesCollector
    {
        private readonly ulong _lastLevelInclusiveMaxSeqnoThreshold;
        private readonly double _compactionTriggerRatio;
        private ulong _lastLevelEligibleEntries;
        private ulong _totalEntries;
        private bool _needCompaction;

        public Collector(ulong lastLevelInclusiveMaxSeqnoThreshold, double compactionTriggerRatio)
        {
            _lastLevelInclusiveMaxSeqnoThreshold = lastLevelInclusiveMaxSeqnoThreshold;
            _compactionTriggerRatio = compactionTriggerRatio;
        }

        public override string Name => ClassName;
        public override bool NeedCompact => _needCompaction;

        public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
        {
            ulong seqForCheck = sequence;
            // ParsePackedValueForSeqno: a TimedPut value ends with its fixed64 preferred seqno.
            if (type == TableEntryType.TimedPut && value.Length >= sizeof(ulong)) seqForCheck = Util.Coding.DecodeFixed64(value[^sizeof(ulong)..]);
            if (seqForCheck < _lastLevelInclusiveMaxSeqnoThreshold) _lastLevelEligibleEntries++;
            _totalEntries++;
        }

        public override void Finish(IDictionary<string, byte[]> properties)
        {
            if (_lastLevelEligibleEntries >= _compactionTriggerRatio * _totalEntries) _needCompaction = true;
            if (_lastLevelEligibleEntries > 0)
            {
                properties[NumEligibleLastLevelEntriesPropertyName] =
                    System.Text.Encoding.ASCII.GetBytes(_lastLevelEligibleEntries.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
        }

        public override IReadOnlyDictionary<string, string> GetReadableProperties() => new Dictionary<string, string>
        {
            [NumEligibleLastLevelEntriesPropertyName] = _lastLevelEligibleEntries.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
    }
}

/// <summary>Builder-side driver of the user collectors (UserKeyTablePropertiesCollector + NotifyCollectTableCollectorsOn*, table/meta_blocks.cc).</summary>
internal sealed class TablePropertiesCollectors
{
    private readonly TablePropertiesCollector[] _collectors;
    private readonly bool[] _failed;

    private TablePropertiesCollectors(TablePropertiesCollector[] collectors)
    {
        _collectors = collectors;
        _failed = new bool[collectors.Length];
    }

    /// <summary>Creates the collectors of <paramref name="factories"/> for one table (null when there are none).</summary>
    public static TablePropertiesCollectors? Create(IReadOnlyList<TablePropertiesCollectorFactory> factories, in TablePropertiesCollectorContext context)
    {
        if (factories.Count == 0) return null;
        var list = new List<TablePropertiesCollector>(factories.Count);
        foreach (TablePropertiesCollectorFactory f in factories)
        {
            TablePropertiesCollector? c = f.CreateTablePropertiesCollector(context);
            if (c is not null) list.Add(c);
        }
        return list.Count == 0 ? null : new TablePropertiesCollectors([.. list]);
    }

    /// <summary>"rocksdb.property.collectors": "[" + factory names joined by "," + "]" (all configured factories).</summary>
    public static string Names(IReadOnlyList<TablePropertiesCollectorFactory> factories)
    {
        var sb = new System.Text.StringBuilder("[");
        for (int i = 0; i < factories.Count; i++)
        {
            if (i != 0) sb.Append(',');
            sb.Append(factories[i].Name);
        }
        return sb.Append(']').ToString();
    }

    /// <summary>UserKeyTablePropertiesCollector::InternalAdd for every collector.</summary>
    public void Add(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value, ulong fileSize)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out ulong seq, out EntryType t);
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        TableEntryType type = GetEntryType(t);
        TablePropertiesCollector[] cs = _collectors;
        for (int i = 0; i < cs.Length; i++)
        {
            try
            {
                cs[i].AddUserKey(userKey, value, type, seq, fileSize);
            }
            catch (Exception)
            {
                // Upstream logs the error (LogPropertiesCollectionError) and continues.
            }
        }
    }

    public void BlockAdd(ulong uncompressedBytes)
    {
        foreach (TablePropertiesCollector c in _collectors)
        {
            try
            {
                c.BlockAdd(uncompressedBytes, 0, 0);
            }
            catch (Exception)
            {
                // Ignored like a failed Status.
            }
        }
    }

    /// <summary>NotifyCollectTableCollectorsOnFinish: the properties of every collector whose Finish succeeded.</summary>
    public void Finish(Dictionary<string, byte[]> properties)
    {
        for (int i = 0; i < _collectors.Length; i++)
        {
            var user = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            try
            {
                _collectors[i].Finish(user);
            }
            catch (Exception)
            {
                _failed[i] = true;
                continue;
            }
            // std::map::merge keeps existing entries.
            foreach (KeyValuePair<string, byte[]> kv in user) properties.TryAdd(kv.Key, kv.Value);
        }
    }

    /// <summary>BlockBasedTableBuilder::NeedCompact: any collector asks for compaction.</summary>
    public bool NeedCompact
    {
        get
        {
            foreach (TablePropertiesCollector c in _collectors)
                if (c.NeedCompact) return true;
            return false;
        }
    }

    /// <summary>Port of GetEntryType (db/dbformat.cc).</summary>
    internal static TableEntryType GetEntryType(EntryType t) => t switch
    {
        EntryType.Value => TableEntryType.Put,
        EntryType.Deletion => TableEntryType.Delete,
        EntryType.DeletionWithTimestamp => TableEntryType.DeleteWithTimestamp,
        EntryType.SingleDeletion => TableEntryType.SingleDelete,
        EntryType.Merge => TableEntryType.Merge,
        EntryType.RangeDeletion => TableEntryType.RangeDeletion,
        EntryType.BlobIndex => TableEntryType.BlobIndex,
        EntryType.WideColumnEntity => TableEntryType.WideColumnEntity,
        EntryType.ValuePreferredSeqno => TableEntryType.TimedPut,
        _ => TableEntryType.Other,
    };
}
