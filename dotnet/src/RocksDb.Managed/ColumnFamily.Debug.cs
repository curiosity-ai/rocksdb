namespace RocksDb.Managed;

public sealed partial class ColumnFamily
{
    /// <summary>
    /// Returns every internal entry (each version, merge operand and tombstone) of the keys in
    /// [<paramref name="beginKey"/>, <paramref name="endKey"/>] in internal-key order, at most
    /// <paramref name="maxNumIkeys"/> of them. Port of GetAllKeyVersions (include/rocksdb/utilities/debug.h) for
    /// debugging and tests; it reads all memtables and table files, so it is not meant for production reads.
    /// </summary>
    /// <param name="beginKey">First user key (null = from the first key). An empty array is the empty key, not "unbounded".</param>
    /// <param name="endKey">Last user key, inclusive (null = to the last key). With user-defined timestamps the bound
    /// is the key with the maximum timestamp, as upstream, so versions of this key itself are not returned.</param>
    /// <param name="maxNumIkeys">Maximum entries (checked after adding one, so 0 returns one entry like upstream); use
    /// <see cref="int.MaxValue"/> for all.</param>
    /// <remarks>Keys covered by a range tombstone are skipped; range tombstones themselves are not returned.</remarks>
    public KeyVersionCollection GetAllKeyVersions(byte[]? beginKey, byte[]? endKey, int maxNumIkeys)
    {
        var result = new KeyVersionCollection();
        try
        {
            GetAllKeyVersions(beginKey, endKey, maxNumIkeys, result);
            return result;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    /// <summary>
    /// <see cref="GetAllKeyVersions(byte[], byte[], int)"/> into a reusable <paramref name="result"/> (cleared first).
    /// </summary>
    public void GetAllKeyVersions(byte[]? beginKey, byte[]? endKey, int maxNumIkeys, KeyVersionCollection result) =>
        _db.GetAllKeyVersions(_cfd, beginKey, endKey, maxNumIkeys, result);
}

public sealed partial class RocksDatabase
{
    /// <inheritdoc cref="ColumnFamily.GetAllKeyVersions(byte[], byte[], int)"/>
    public KeyVersionCollection GetAllKeyVersions(byte[]? beginKey, byte[]? endKey, int maxNumIkeys) =>
        _default.GetAllKeyVersions(beginKey, endKey, maxNumIkeys);

    /// <inheritdoc cref="ColumnFamily.GetAllKeyVersions(byte[], byte[], int, KeyVersionCollection)"/>
    public void GetAllKeyVersions(byte[]? beginKey, byte[]? endKey, int maxNumIkeys, KeyVersionCollection result) =>
        _default.GetAllKeyVersions(beginKey, endKey, maxNumIkeys, result);
}
