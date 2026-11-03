using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// The blob files one memtable generation wrote with blob direct write: files still open when the memtable became
/// immutable (sealed by the flush) and files already sealed by rollover. Port of
/// BlobFilePartitionManager::GenerationBatch. Attached to the <see cref="ImmutableMemTable"/>; sealing is idempotent so
/// a failed flush can be retried with the same files.
/// </summary>
internal sealed class BlobDirectWriteGeneration
{
    internal readonly List<BlobDirectWriteManager.OpenFile> Deferred = new();
    internal readonly List<BlobFileAddition> Sealed = new();

    /// <summary>Every file of the generation (protected from deletion while the memtable that references them lives).</summary>
    internal readonly List<ulong> FileNumbers = new();

    public bool IsEmpty => FileNumbers.Count == 0;
}

/// <summary>
/// Blob direct write of one column family: values of at least <c>min_blob_size</c> bytes are appended to partitioned
/// blob files on the write path (before the WAL), the batch carries blob indexes instead, and the files are registered
/// in the MANIFEST by the flush of the memtable that references them. Port of rocksdb::BlobFilePartitionManager
/// (db/blob/blob_file_partition_manager.{h,cc}): WriteBlob (rollover at blob_file_size, a new file when the column
/// family or the compression changes), RotateCurrentGeneration at memtable switch, PrepareFlushAdditions (sealing:
/// footer + sync), SyncAllOpenFiles for sync writes, and ResolveBlobDirectWriteIndex (reads of files not in the version
/// yet, without a footer).
/// <para>
/// Threading: the write-path state (partitions, the current generation) is only touched by the write group leader (blob
/// writes, memtable switches and close all run as the leader), like upstream's DB-mutex/write-thread contract; the
/// readers of unregistered files are thread-safe. File numbers come from the DB's pending outputs: a file is protected
/// from obsolete-file deletion from its creation until the memtable referencing it is gone (DbImpl releases it).
/// </para>
/// <para>
/// Records are flushed to the OS one by one like upstream (kDoFlushEachRecord). With a file_checksum_gen_factory the
/// files are checksummed while written (WritableFileWriter with the factory) and the checksum is recorded in their
/// MANIFEST addition when sealed (BlobLogWriter::AppendFooter). Statistics: BLOB_DB_BLOB_FILE_WRITE_MICROS per append,
/// BLOB_DB_BLOB_FILE_SYNC_MICROS per sync (BlobLogWriter::Sync), BLOB_DB_COMPRESSION_MICROS per compressed value.
/// Deviations: blob cache prepopulation is not implemented; failed transformed writes are accounted as garbage by the
/// flush (blobs the flush output does not reference) instead of MarkBlobWriteAsGarbage.
/// </para>
/// </summary>
internal sealed class BlobDirectWriteManager : IDisposable
{
    internal sealed class OpenFile
    {
        public required ulong FileNumber;
        public required IWritableFile File;
        public required CompressionType Compression;
        public required uint ColumnFamilyId;
        public ulong BlobCount;
        public ulong TotalBlobBytes;
        public bool SyncRequired;
        /// <summary>Checksum method / value of the sealed file (empty without a checksum factory).</summary>
        public string ChecksumMethod = "";
        public byte[] ChecksumValue = [];
    }

    private readonly IFileSystem _fs;
    private readonly string _blobDir;
    private readonly Func<ulong> _newFileNumber;
    private readonly uint _numPartitions;
    private readonly BlobDirectWritePartitionStrategy _strategy;
    private readonly Statistics? _stats;
    private readonly OpenFile?[] _partitions;
    private BlobDirectWriteGeneration _current = new();
    private readonly byte[] _header = new byte[Math.Max(BlobLogFormat.HeaderSize, BlobLogFormat.FooterSize)];
    private readonly byte[] _recordHeader = new byte[BlobLogFormat.RecordHeaderSize];
    private readonly PooledBufferWriter _compressed = new(256);
    /// <summary>Files without footer yet (all generations), closed by <see cref="Dispose"/>.</summary>
    private readonly HashSet<OpenFile> _unsealed = new();

    /// <summary>Readers of direct-write files (also after sealing, for memtables still referencing them).</summary>
    private readonly ConcurrentDictionary<ulong, ReaderEntry> _readers = new();
    private readonly Lock _readerLock = new();
    private bool _disposed;

    private sealed class ReaderEntry
    {
        public BlobFileReader? Reader;
        public ulong VisibleSize;
    }

    /// <param name="fs">File system.</param>
    /// <param name="blobDir">Directory of the blob files (the first DB path).</param>
    /// <param name="newFileNumber">Allocates a file number registered as a pending output.</param>
    /// <param name="options">Sanitized column family options.</param>
    /// <param name="stats">Statistics or null.</param>
    public BlobDirectWriteManager(IFileSystem fs, string blobDir, Func<ulong> newFileNumber, ColumnFamilyOptions options, Statistics? stats)
    {
        _fs = fs;
        _blobDir = blobDir;
        _newFileNumber = newFileNumber;
        _numPartitions = Math.Max(1, options.BlobDirectWritePartitions);
        _strategy = options.BlobDirectWritePartitionStrategy ?? new RoundRobinBlobDirectWritePartitionStrategy();
        _stats = stats;
        _partitions = new OpenFile?[_numPartitions];
    }

    /// <summary>
    /// InternalStats::GetBlobFileReadHist(): the column family's blob file read latency histogram for the readers of
    /// unsealed files.
    /// </summary>
    public HistogramImpl? BlobFileReadHistogram { get; init; }

    /// <summary>
    /// Write buffering of the blob files, and the DB's file checksum factory (set from the mutable options before each
    /// write).
    /// </summary>
    public WritableFileOptions FileOptions { get; set; } = WritableFileOptions.Default;

    /// <summary>Blob file size limit (blob_file_size, mutable).</summary>
    public ulong BlobFileSize { get; set; } = 1UL << 28;

    /// <summary>True while a partition has an open file or the current generation has files (HasInFlightBlobDirectWriteFiles).</summary>
    public bool HasCurrentFiles => !_current.IsEmpty;

    /// <summary>SelectWideColumnPartition: the partition for all blob-backed columns of one entity.</summary>
    public uint SelectEntityPartition(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
        _strategy.SelectEntityPartition(_numPartitions, columnFamilyId, key, entity) % _numPartitions;

    /// <summary>
    /// BlobFilePartitionManager::WriteBlob: appends one blob record to a partition file (compressing the value with
    /// <paramref name="compression"/>, even when larger) and writes its blob index to <paramref name="blobIndex"/>.
    /// <paramref name="partition"/> &lt; 0 = select with the partition strategy. REQUIRES: write group leader.
    /// </summary>
    public void WriteBlob(uint columnFamilyId, CompressionType compression, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, long partition,
        IBufferWriter<byte> blobIndex)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ReadOnlySpan<byte> blob = value;
        if (compression != CompressionType.None)
        {
            if (!BlockCompression.IsSupported(compression)) ThrowHelper.NotSupported("Blob direct write compression type not supported");
            _compressed.Clear();
            long tc = _stats?.StartTimer() ?? 0;
            BlockCompression.CompressForced(compression, value, BlockCompression.DefaultCompressionLevel, _compressed);
            _stats?.RecordElapsedMicros(Histogram.BlobDbCompressionMicros, tc);
            blob = _compressed.WrittenSpan;
        }
        uint idx = partition >= 0
            ? (uint)((ulong)partition % _numPartitions)
            : _strategy.SelectPartition(_numPartitions, columnFamilyId, key, value) % _numPartitions;
        OpenFile? f = _partitions[idx];
        ulong recordSize = BlobLogFormat.RecordHeaderSize + (ulong)key.Length + (ulong)blob.Length;
        if (f is not null && (f.ColumnFamilyId != columnFamilyId || f.Compression != compression
                              || (f.BlobCount > 0 && BlobLogFormat.HeaderSize + f.TotalBlobBytes + recordSize + BlobLogFormat.FooterSize > BlobFileSize)))
        {
            // Rollover: the sealed file stays in the current generation.
            Seal(f);
            _current.Sealed.Add(Addition(f));
            _current.Deferred.Remove(f);
            _partitions[idx] = f = null;
        }
        f ??= _partitions[idx] = OpenNewFile(columnFamilyId, compression);

        ulong recordOffset = BlobLogFormat.HeaderSize + f.TotalBlobBytes;
        BlobLogFormat.EncodeRecordHeader(_recordHeader, key, blob, expiration: 0);
        Append(f.File, _recordHeader);
        Append(f.File, key);
        Append(f.File, blob);
        // kDoFlushEachRecord: readers of the memtable see the record right away. Durability comes from SyncAllOpenFiles
        // (sync writes) or the footer.
        f.File.Flush();
        f.SyncRequired = true;
        f.BlobCount++;
        f.TotalBlobBytes += recordSize;
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, recordSize);
        if (_readers.TryGetValue(f.FileNumber, out ReaderEntry? e))
        {
            ulong visible = BlobLogFormat.HeaderSize + f.TotalBlobBytes;
            lock (_readerLock)
            {
                e.VisibleSize = visible;
                e.Reader?.ExtendVisibleSize(visible);
            }
        }
        ulong valueOffset = recordOffset + BlobLogFormat.RecordHeaderSize + (ulong)key.Length;
        BlobIndex.EncodeBlob(blobIndex, f.FileNumber, valueOffset, (ulong)blob.Length, compression);
    }

    private OpenFile OpenNewFile(uint columnFamilyId, CompressionType compression)
    {
        ulong number = _newFileNumber();
        _current.FileNumbers.Add(number);
        _readers[number] = new ReaderEntry { VisibleSize = BlobLogFormat.HeaderSize };
        // NewWritableFile + WritableFileWriter with the DB's file_checksum_gen_factory.
        IWritableFile file = FileChecksumHelper.CreateWritable(_fs, FileNames.BlobFileName(_blobDir, number), FileOptions);
        BlobLogFormat.EncodeHeader(_header, columnFamilyId, compression, hasTtl: false, 0, 0);
        Append(file, _header.AsSpan(0, BlobLogFormat.HeaderSize));
        file.Flush();
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, BlobLogFormat.HeaderSize);
        var f = new OpenFile { FileNumber = number, File = file, Compression = compression, ColumnFamilyId = columnFamilyId };
        _current.Deferred.Add(f);
        lock (_unsealed) _unsealed.Add(f);
        return f;
    }

    /// <summary>FinalizeBlobFile: footer, sync, close.</summary>
    private void Seal(OpenFile f)
    {
        BlobLogFormat.EncodeFooter(_header, f.BlobCount, 0, 0);
        Append(f.File, _header.AsSpan(0, BlobLogFormat.FooterSize));
        Sync(f.File);
        // BlobLogWriter::AppendFooter: checksum method/value from the writer (empty without a checksum factory).
        (byte[] checksum, string method) = FileChecksumHelper.GetChecksum(f.File);
        if (checksum.Length == 0) method = "";
        f.ChecksumMethod = method;
        f.ChecksumValue = checksum;
        f.File.Dispose();
        lock (_unsealed) _unsealed.Remove(f);
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, BlobLogFormat.FooterSize);
        _stats?.RecordTick(Ticker.BlobDbBlobFileSynced);
        if (_readers.TryGetValue(f.FileNumber, out ReaderEntry? e))
        {
            lock (_readerLock)
            {
                e.VisibleSize = BlobLogFormat.HeaderSize + f.TotalBlobBytes;
                e.Reader?.ExtendVisibleSize(e.VisibleSize);
            }
        }
    }

    private static BlobFileAddition Addition(OpenFile f) => new(f.FileNumber, f.BlobCount, f.TotalBlobBytes, f.ChecksumMethod, f.ChecksumValue);

    /// <summary>WritableFileWriter::Append with its StopWatch(BLOB_DB_BLOB_FILE_WRITE_MICROS).</summary>
    private void Append(IWritableFile file, ReadOnlySpan<byte> data)
    {
        long t0 = _stats?.StartTimer() ?? 0;
        file.Append(data);
        _stats?.RecordElapsedMicros(Histogram.BlobDbBlobFileWriteMicros, t0);
    }

    /// <summary>BlobLogWriter::Sync: StopWatch(BLOB_DB_BLOB_FILE_SYNC_MICROS) around the sync.</summary>
    private void Sync(IWritableFile file)
    {
        long t0 = _stats?.StartTimer() ?? 0;
        file.Sync();
        _stats?.RecordElapsedMicros(Histogram.BlobDbBlobFileSyncMicros, t0);
    }

    /// <summary>
    /// RotateCurrentGeneration: the open files and the files sealed while the memtable was mutable become the
    /// generation of the memtable that just became immutable (returned; null when it wrote no blob). No I/O.
    /// REQUIRES: write group leader.
    /// </summary>
    public BlobDirectWriteGeneration? RotateGeneration()
    {
        if (_current.IsEmpty) return null;
        BlobDirectWriteGeneration g = _current;
        _current = new BlobDirectWriteGeneration();
        Array.Clear(_partitions);
        return g;
    }

    /// <summary>
    /// PrepareFlushAdditions: seals the files of <paramref name="generations"/> still open (idempotent: a retried flush
    /// reuses the sealed files) and appends their MANIFEST additions.
    /// </summary>
    public static void PrepareFlushAdditions(IReadOnlyList<BlobDirectWriteGeneration> generations, BlobDirectWriteManager manager,
        List<BlobFileAddition> additions)
    {
        foreach (BlobDirectWriteGeneration g in generations)
        {
            while (g.Deferred.Count > 0)
            {
                OpenFile f = g.Deferred[0];
                manager.Seal(f);
                g.Sealed.Add(Addition(f));
                g.Deferred.RemoveAt(0);
            }
            additions.AddRange(g.Sealed);
        }
    }

    /// <summary>SyncAllOpenFiles: makes the open files durable before a sync WAL write. REQUIRES: write group leader.</summary>
    public void SyncAllOpenFiles()
    {
        foreach (OpenFile? f in _partitions)
        {
            if (f is null || !f.SyncRequired) continue;
            Sync(f.File);
            f.SyncRequired = false;
            _stats?.RecordTick(Ticker.BlobDbBlobFileSynced);
        }
    }

    /// <summary>True if <paramref name="fileNumber"/> was written by this manager and is still readable through it.</summary>
    public bool IsDirectWriteFile(ulong fileNumber) => _readers.ContainsKey(fileNumber);

    /// <summary>The file was deleted: closes its reader.</summary>
    public void Forget(ulong fileNumber)
    {
        if (!_readers.TryRemove(fileNumber, out ReaderEntry? e)) return;
        lock (_readerLock)
        {
            e.Reader?.Dispose();
            e.Reader = null;
        }
    }

    /// <summary>
    /// ResolveBlobDirectWriteIndex for a file that is not part of the reading version: reads the blob directly from the
    /// (possibly unsealed) file. Returns false if the file is unknown to this manager.
    /// </summary>
    public bool TryGetBlob(ReadOptions? options, ReadOnlySpan<byte> userKey, ulong fileNumber, ulong offset, ulong size, CompressionType compression,
        IBufferWriter<byte> output)
    {
        if (!_readers.TryGetValue(fileNumber, out ReaderEntry? e)) return false;
        if (options?.ReadTier == ReadTier.BlockCacheTier)
        {
            throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Cannot read blob(s): no disk I/O allowed");
        }
        BlobFileReader reader;
        lock (_readerLock)
        {
            if (e.Reader is null)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                e.Reader = BlobFileReader.OpenUnsealed(_fs, FileNames.BlobFileName(_blobDir, fileNumber), ColumnFamilyId, _stats, BlobFileReadHistogram);
                e.Reader.ExtendVisibleSize(e.VisibleSize);
            }
            reader = e.Reader;
        }
        // Readers stay open until Forget / Dispose, which only happen once no memtable references the file.
        reader.GetBlob(userKey, offset, size, compression, options?.VerifyChecksums ?? true, output);
        return true;
    }

    /// <summary>Id of the owning column family (written in the blob file headers).</summary>
    public required uint ColumnFamilyId { get; init; }

    /// <summary>Description for errors.</summary>
    public static string InvalidFile(ulong n) => "Invalid blob file number " + n.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Closes the open files without a footer (DB close after the shutdown flush, or a dropped column family); their
    /// blobs are only referenced from WALs whose data was not flushed (like upstream v1, not recovered).
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_unsealed)
        {
            foreach (OpenFile f in _unsealed)
            {
                try { f.File.Dispose(); }
                catch (Exception ex) when (ex is IOException or RocksDbException) { }
            }
            _unsealed.Clear();
        }
        Array.Clear(_partitions);
        lock (_readerLock)
        {
            foreach (ReaderEntry e in _readers.Values)
            {
                e.Reader?.Dispose();
                e.Reader = null;
            }
            _readers.Clear();
        }
        _compressed.Dispose();
    }
}
