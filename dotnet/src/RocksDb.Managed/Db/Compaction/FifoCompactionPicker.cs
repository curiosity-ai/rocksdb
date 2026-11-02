namespace RocksDb.Managed.Db;

/// <summary>FIFO compaction picker (placeholder, replaced below).</summary>
internal sealed class FifoCompactionPicker : CompactionPicker
{
    public FifoCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    public override bool NeedsCompaction(VersionStorageInfo vstorage) => false;

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots) => null;
}
