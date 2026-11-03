using System.Buffers;

namespace RocksDb.Managed;

/// <summary>
/// A value returned by <see cref="ColumnFamily.GetPooled(ReadOnlySpan{byte})"/>: the bytes live in a buffer rented
/// from <see cref="ArrayPool{T}.Shared"/> and the object itself is pooled. Dispose exactly once; neither the object
/// nor <see cref="Memory"/> may be used afterwards.
/// </summary>
public sealed class PooledValue : IDisposable
{
    private static readonly PooledValue?[] s_pool = new PooledValue?[Math.Min(256, Environment.ProcessorCount * 8)];

    private byte[] _buffer = [];
    private int _length;
    private bool _rented;

    private PooledValue() { }

    /// <summary>The value bytes.</summary>
    public ReadOnlyMemory<byte> Memory => new(_buffer, 0, _length);
    /// <summary>The value bytes.</summary>
    public ReadOnlySpan<byte> Span => new(_buffer, 0, _length);
    /// <summary>Length of the value in bytes.</summary>
    public int Length => _length;

    /// <summary>Copies the value into a new array.</summary>
    public byte[] ToArray() => Span.ToArray();

    internal static PooledValue Create(ReadOnlySpan<byte> value)
    {
        PooledValue? v = null;
        PooledValue?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            PooledValue? c = pool[i];
            if (c is not null && Interlocked.CompareExchange(ref pool[i], null, c) == c)
            {
                v = c;
                break;
            }
        }
        v ??= new PooledValue();
        v._buffer = ArrayPool<byte>.Shared.Rent(Math.Max(value.Length, 1));
        v._length = value.Length;
        v._rented = true;
        value.CopyTo(v._buffer);
        return v;
    }

    /// <summary>Returns the buffer and this object to their pools.</summary>
    public void Dispose()
    {
        if (!_rented) return;
        _rented = false;
        byte[] b = _buffer;
        _buffer = [];
        _length = 0;
        ArrayPool<byte>.Shared.Return(b);
        PooledValue?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], this, null) is null) return;
        }
    }
}

/// <summary>Reads a value in place (zero-copy): <paramref name="value"/> is only valid during the call.</summary>
public delegate TResult ReadOnlySpanFunc<in TState, out TResult>(ReadOnlySpan<byte> value, TState state);

/// <summary>Metadata of a live table file. Port of rocksdb::LiveFileMetaData (include/rocksdb/metadata.h).</summary>
/// <param name="Name">File name relative to <paramref name="Directory"/>, with a leading '/', e.g. "/000012.sst".</param>
/// <param name="Directory">Directory holding the file.</param>
/// <param name="FileNumber">Table file number.</param>
/// <param name="ColumnFamilyName">Column family the file belongs to.</param>
/// <param name="Level">LSM level.</param>
/// <param name="Size">File size in bytes.</param>
/// <param name="SmallestKey">Smallest user key.</param>
/// <param name="LargestKey">Largest user key.</param>
/// <param name="SmallestSeqno">Smallest sequence number in the file.</param>
/// <param name="LargestSeqno">Largest sequence number in the file.</param>
/// <param name="EpochNumber">Epoch number (L0 ordering).</param>
public sealed record LiveFileMetadata(string Name, string Directory, ulong FileNumber, string ColumnFamilyName, int Level, ulong Size,
    byte[] SmallestKey, byte[] LargestKey, ulong SmallestSeqno, ulong LargestSeqno, ulong EpochNumber)
{
    /// <summary>File temperature recorded in the MANIFEST (SstFileMetaData::temperature).</summary>
    public Temperature Temperature { get; init; }
    /// <summary>Oldest ancestor time (oldest_ancester_time), unix seconds; 0 = unknown.</summary>
    public ulong OldestAncestorTime { get; init; }
    /// <summary>File creation time, unix seconds; 0 = unknown.</summary>
    public ulong FileCreationTime { get; init; }
    /// <summary>Whole-file checksum recorded in the MANIFEST (SstFileMetaData::file_checksum); empty when unknown.</summary>
    public byte[] FileChecksum { get; init; } = [];
    /// <summary>Its function name (file_checksum_func_name), "Unknown" when none was recorded.</summary>
    public string FileChecksumFuncName { get; init; } = "Unknown";
    /// <summary>
    /// Estimated number of user reads of the file since it was added to the LSM (SstFileMetaData::num_reads_sampled):
    /// 1 in 1024 point lookups / seeks (1 in 65536 iterator steps) is counted, as 1024 reads. In memory only.
    /// </summary>
    public ulong NumReadsSampled { get; init; }
    /// <summary>
    /// Smallest internal key (SstFileMetaData::smallest: user key + 8-byte footer); empty when unknown (e.g. metadata
    /// built by hand or received from the C API), in which case <see cref="RocksDatabase.CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, ExportImportFilesMetadata)"/>
    /// reads the bounds from the file.
    /// </summary>
    public byte[] SmallestInternalKey { get; init; } = [];
    /// <summary>Largest internal key (SstFileMetaData::largest); empty when unknown.</summary>
    public byte[] LargestInternalKey { get; init; } = [];
    /// <summary>Oldest blob file referenced by the table (SstFileMetaData::oldest_blob_file_number); 0 = none.</summary>
    public ulong OldestBlobFileNumber { get; init; }
}
