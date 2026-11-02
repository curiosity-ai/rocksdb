namespace RocksDb.Managed.Db;

/// <summary>Universal compaction picker (placeholder, replaced below).</summary>
internal sealed class UniversalCompactionPicker : CompactionPicker
{
    public UniversalCompactionPicker(ColumnFamilyOptions options, InternalKeyComparator icmp) : base(options, icmp) { }

    public override bool NeedsCompaction(VersionStorageInfo vstorage) => false;

    public override Compaction? PickCompaction(VersionStorageInfo vstorage, IReadOnlyList<ulong> snapshots) => null;
}
