using System.Buffers;

namespace RocksDb.Managed;

/// <summary>User-defined timestamp operations on the default column family (see <see cref="ColumnFamily"/>).</summary>
public sealed partial class RocksDatabase
{
    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) => _default.Put(key, timestamp, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) =>
        _default.Put(options, key, timestamp, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(key, timestamp, value, cancellationToken);

    /// <summary>Deletes <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Delete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.Delete(key, timestamp);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.Delete(options, key, timestamp);

    /// <summary>SingleDelete of <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void SingleDelete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.SingleDelete(key, timestamp);

    /// <summary>Merge operand for <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> operand) => _default.Merge(key, timestamp, operand);

    /// <inheritdoc cref="ColumnFamily.TryGet(ReadOptions, ReadOnlySpan{byte}, IBufferWriter{byte}, IBufferWriter{byte})"/>
    public bool TryGet(ReadOptions options, ReadOnlySpan<byte> key, IBufferWriter<byte> value, IBufferWriter<byte> timestamp) =>
        _default.TryGet(options, key, value, timestamp);

    /// <inheritdoc cref="ColumnFamily.GetFullHistoryTsLow"/>
    public byte[]? GetFullHistoryTsLow() => _default.GetFullHistoryTsLow();

    /// <inheritdoc cref="ColumnFamily.IncreaseFullHistoryTsLow"/>
    public void IncreaseFullHistoryTsLow(ReadOnlySpan<byte> tsLow) => _default.IncreaseFullHistoryTsLow(tsLow);

    /// <inheritdoc cref="ColumnFamily.GetNewestUserDefinedTimestamp"/>
    public byte[] GetNewestUserDefinedTimestamp() => _default.GetNewestUserDefinedTimestamp();
}
