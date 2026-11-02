using System.Buffers;

namespace RocksDb.Managed;

/// <summary>Result of an <see cref="InplaceUpdateCallback"/>. Port of rocksdb::UpdateStatus (include/rocksdb/advanced_options.h).</summary>
public enum InplaceUpdateStatus
{
    /// <summary>Nothing to update (despite the upstream name UPDATE_FAILED): the write is dropped.</summary>
    UpdateFailed = 0,
    /// <summary>The existing value was modified in place; its new size was stored in the size argument.</summary>
    UpdatedInplace = 1,
    /// <summary>A new value was written to the merged-value writer and is added as a new entry.</summary>
    Updated = 2,
}

/// <summary>
/// inplace_callback (ColumnFamilyOptions, used with <see cref="ColumnFamilyOptions.InplaceUpdateSupport"/>): combines the
/// existing value of a key with the value of a Put (the "delta"). Port of the upstream function pointer
/// <c>UpdateStatus (*)(char* existing_value, uint32_t* existing_value_size, Slice delta_value, std::string* merged_value)</c>.
/// </summary>
/// <param name="existingValue">
/// The current value in the memtable (writable; modify it in place for <see cref="InplaceUpdateStatus.UpdatedInplace"/>),
/// or, when the key is not in the memtable, a private copy of the value read from the DB. Empty when <paramref name="hasExistingValue"/> is false.
/// </param>
/// <param name="hasExistingValue">False when the key has no value at all (upstream passes nullptr).</param>
/// <param name="existingValueSize">In: the existing value size. For UpdatedInplace set it to the new size (&lt;= the old one).</param>
/// <param name="delta">The value being written.</param>
/// <param name="mergedValue">For <see cref="InplaceUpdateStatus.Updated"/>: receives the new value.</param>
public delegate InplaceUpdateStatus InplaceUpdateCallback(Span<byte> existingValue, bool hasExistingValue, ref int existingValueSize, ReadOnlySpan<byte> delta,
    IBufferWriter<byte> mergedValue);
