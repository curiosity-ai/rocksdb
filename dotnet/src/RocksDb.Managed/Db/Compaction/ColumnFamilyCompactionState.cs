namespace RocksDb.Managed.Db;

/// <summary>
/// Per column family compaction bookkeeping: the compaction picker for the CF's compaction style, the write-stall
/// token and the compaction-queue flag (ColumnFamilyData::compaction_picker_, write_controller_token_,
/// queued_for_compaction_). Guarded by the DB mutex.
/// </summary>
internal sealed class ColumnFamilyCompactionState
{
    public ColumnFamilyCompactionState(ColumnFamilyOptions options, InternalKeyComparator icmp)
    {
        Picker = options.CompactionStyle switch
        {
            CompactionStyle.Level => new LevelCompactionPicker(options, icmp),
            CompactionStyle.Fifo => new FifoCompactionPicker(options, icmp),
            CompactionStyle.Universal => new UniversalCompactionPicker(options, icmp),
            _ => new NullCompactionPicker(options, icmp),
        };
    }

    public CompactionPicker Picker { get; }
    public ColumnFamilyStallState Stall { get; } = new();
    public bool QueuedForCompaction { get; set; }
}

/// <summary>Picker for kCompactionStyleNone: never compacts automatically (port of NullCompactionPicker).</summary>
internal sealed class NullCompactionPicker : CompactionPicker
{
    public NullCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    public override bool NeedsCompaction(VersionStorageInfo vstorage) => false;

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots) => null;
}
