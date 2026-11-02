using RocksDb.Managed.Db.Wide;

namespace RocksDb.Managed;

/// <summary>
/// Chooses the blob file partition of each blob written by blob direct write
/// (<see cref="ColumnFamilyOptions.EnableBlobDirectWrite"/>, <see cref="ColumnFamilyOptions.BlobDirectWritePartitions"/>).
/// Port of rocksdb::BlobFilePartitionStrategy (include/rocksdb/blob_file_partition_strategy.h). Called on the write
/// path by the write group leader; implementations must be thread-safe when shared between column families or DBs.
/// The returned value may be any number: the caller applies <c>% numPartitions</c>.
/// </summary>
public abstract class BlobDirectWritePartitionStrategy
{
    /// <summary>Name of the strategy (debugging and logging).</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Selects a partition for one plain value. <paramref name="value"/> is the original (uncompressed) user value.
    /// </summary>
    public abstract uint SelectPartition(uint numPartitions, uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>
    /// Selects the partition used for all blob-backed columns of one wide-column entity (called once per PutEntity).
    /// <paramref name="serializedEntity"/> is the entity (all columns inline; parse it with
    /// <see cref="WideColumns.SetSerialized"/> if needed). The default delegates to
    /// <see cref="SelectPartition(uint, uint, ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> with the default column's value,
    /// otherwise the first column's value, or an empty value for an empty entity (like upstream).
    /// </summary>
    public virtual uint SelectEntityPartition(uint numPartitions, uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> serializedEntity)
    {
        List<WideColumnSlot> slots = t_slots ??= new List<WideColumnSlot>(4);
        WideColumnSerialization.Deserialize(serializedEntity, slots);
        ReadOnlySpan<byte> value = slots.Count == 0 ? default : serializedEntity.Slice(slots[0].ValueOffset, slots[0].ValueLength);
        return SelectPartition(numPartitions, columnFamilyId, key, value);
    }

    [ThreadStatic] private static List<WideColumnSlot>? t_slots;
}

/// <summary>The default strategy: round robin over the partitions (RoundRobinBlobFilePartitionStrategy).</summary>
internal sealed class RoundRobinBlobDirectWritePartitionStrategy : BlobDirectWritePartitionStrategy
{
    private long _next;

    public override string Name => "RoundRobinBlobFilePartitionStrategy";

    public override uint SelectPartition(uint numPartitions, uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        (uint)((ulong)(Interlocked.Increment(ref _next) - 1) % numPartitions);
}
