using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Reads blobs from one blob file. Port of rocksdb::BlobFileReader (db/blob/blob_file_reader.cc): Create validates
/// the header (magic, version, no TTL, matching column family) and the footer (magic, CRC, no TTL); GetBlob checks the
/// offset (IsValidBlobOffset) and compression, reads the value (or, with verify_checksums, the whole record and checks
/// the record header, key and blob CRC) and decompresses it. Thread-safe (positional reads).
/// </summary>
internal sealed class BlobFileReader : IDisposable
{
    private readonly IRandomAccessFile _file;
    private readonly string _path;
    private ulong _fileSize;
    private readonly bool _hasFooter;
    private readonly Statistics? _stats;

    private BlobFileReader(IRandomAccessFile file, string path, ulong fileSize, CompressionType compression, Statistics? stats, bool hasFooter)
    {
        _file = file;
        _path = path;
        _fileSize = fileSize;
        Compression = compression;
        _stats = stats;
        _hasFooter = hasFooter;
    }

    /// <summary>
    /// BlobFileReader::Create with skip_footer_validation (the footer-skip retry of BlobFileCache for blob direct write
    /// files still being written): validates the header only. Readable bytes are those made visible with
    /// <see cref="ExtendVisibleSize"/> (no footer is assumed: IsValidBlobOffset with has_footer = false).
    /// </summary>
    public static BlobFileReader OpenUnsealed(IFileSystem fs, string path, uint columnFamilyId, Statistics? stats)
    {
        IRandomAccessFile file = fs.OpenRandomAccess(path);
        try
        {
            if ((ulong)file.Length < BlobLogFormat.HeaderSize) ThrowHelper.Corruption("Malformed blob file");
            Span<byte> buf = stackalloc byte[BlobLogFormat.HeaderSize];
            ReadExactly(file, path, 0, buf, stats);
            BlobLogFormat.Header header = BlobLogFormat.DecodeHeader(buf);
            if (header.HasTtl || header.ExpirationFirst != 0 || header.ExpirationSecond != 0) ThrowHelper.Corruption("Unexpected TTL blob file");
            if (header.ColumnFamilyId != columnFamilyId) ThrowHelper.Corruption("Column family ID mismatch");
            if (header.Compression != CompressionType.None && !BlockCompression.IsSupported(header.Compression))
            {
                ThrowHelper.NotSupported("Blob compression type " + header.Compression.ToString() + " is not supported");
            }
            return new BlobFileReader(file, path, (ulong)file.Length, header.Compression, stats, hasFooter: false);
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    /// <summary>For an unsealed reader: the file holds at least <paramref name="size"/> bytes (only grows).</summary>
    public void ExtendVisibleSize(ulong size)
    {
        ulong cur = Volatile.Read(ref _fileSize);
        while (size > cur)
        {
            ulong prev = Interlocked.CompareExchange(ref _fileSize, size, cur);
            if (prev == cur) return;
            cur = prev;
        }
    }

    /// <summary>The compression type of every blob of the file (from the header).</summary>
    public CompressionType Compression { get; }

    public ulong FileSize => _fileSize;

    /// <summary>BlobFileReader::Create (OpenFile + ReadHeader + ReadFooter). Throws Corruption / IOError / PathNotFound.</summary>
    public static BlobFileReader Open(IFileSystem fs, string path, uint columnFamilyId, Statistics? stats)
    {
        IRandomAccessFile file = fs.OpenRandomAccess(path);
        try
        {
            ulong fileSize = (ulong)file.Length;
            if (fileSize < BlobLogFormat.HeaderSize + BlobLogFormat.FooterSize) ThrowHelper.Corruption("Malformed blob file");
            Span<byte> buf = stackalloc byte[BlobLogFormat.FooterSize];
            ReadExactly(file, path, 0, buf[..BlobLogFormat.HeaderSize], stats);
            BlobLogFormat.Header header = BlobLogFormat.DecodeHeader(buf[..BlobLogFormat.HeaderSize]);
            if (header.HasTtl || header.ExpirationFirst != 0 || header.ExpirationSecond != 0) ThrowHelper.Corruption("Unexpected TTL blob file");
            if (header.ColumnFamilyId != columnFamilyId) ThrowHelper.Corruption("Column family ID mismatch");
            ReadExactly(file, path, (long)(fileSize - BlobLogFormat.FooterSize), buf, stats);
            BlobLogFormat.Footer footer = BlobLogFormat.DecodeFooter(buf);
            if (footer.ExpirationFirst != 0 || footer.ExpirationSecond != 0) ThrowHelper.Corruption("Unexpected TTL blob file");
            if (header.Compression != CompressionType.None && !BlockCompression.IsSupported(header.Compression))
            {
                ThrowHelper.NotSupported("Blob compression type " + header.Compression.ToString() + " is not supported");
            }
            return new BlobFileReader(file, path, fileSize, header.Compression, stats, hasFooter: true);
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    private static void ReadExactly(IRandomAccessFile file, string path, long offset, Span<byte> dst, Statistics? stats)
    {
        stats?.RecordTick(Ticker.BlobDbBlobFileBytesRead, (ulong)dst.Length);
        int n = file.Read(offset, dst);
        if (n != dst.Length) ThrowIncomplete(path, (ulong)offset, dst.Length, n);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowIncomplete(string path, ulong offset, int expected, int got) =>
        ThrowHelper.Corruption("Incomplete blob read from " + path + " at offset " + offset.ToString(CultureInfo.InvariantCulture) +
                               ": expected " + expected.ToString(CultureInfo.InvariantCulture) + " bytes, got " + got.ToString(CultureInfo.InvariantCulture));

    /// <summary>Validates the request; returns the record offset/size to read (GetBlob prologue).</summary>
    private void Prepare(int keySize, ulong offset, ulong valueSize, CompressionType compression, bool verifyChecksums,
        out long recordOffset, out int recordSize, out int adjustment)
    {
        ulong fileSize = Volatile.Read(ref _fileSize);
        if (!BlobLogFormat.IsValidBlobOffset(offset, (ulong)keySize, valueSize, _hasFooter ? fileSize : fileSize + BlobLogFormat.FooterSize))
        {
            ThrowHelper.Corruption("Invalid blob offset " + offset.ToString(CultureInfo.InvariantCulture) + " for value size " +
                                   valueSize.ToString(CultureInfo.InvariantCulture) + " in " + _path + " (file size " +
                                   fileSize.ToString(CultureInfo.InvariantCulture) + ")");
        }
        if (compression != Compression) ThrowHelper.Corruption("Compression type mismatch when reading blob");
        // With verify_checksums the whole record is read (the index offset points at the value).
        adjustment = verifyChecksums ? (int)BlobLogFormat.RecordHeaderAdjustment((ulong)keySize) : 0;
        recordOffset = (long)(offset - (ulong)adjustment);
        ulong size = valueSize + (ulong)adjustment;
        if (size > int.MaxValue) ThrowHelper.NotSupported("Blob larger than 2 GiB");
        recordSize = (int)size;
    }

    /// <summary>
    /// BlobFileReader::GetBlob: appends the uncompressed blob to <paramref name="output"/>; returns the number of bytes
    /// read from the file.
    /// </summary>
    public ulong GetBlob(ReadOnlySpan<byte> userKey, ulong offset, ulong valueSize, CompressionType compression, bool verifyChecksums,
        IBufferWriter<byte> output)
    {
        Prepare(userKey.Length, offset, valueSize, compression, verifyChecksums, out long recordOffset, out int recordSize, out int adjustment);
        byte[] rented = ArrayPool<byte>.Shared.Rent(recordSize);
        try
        {
            Span<byte> record = rented.AsSpan(0, recordSize);
            _stats?.RecordTick(Ticker.BlobDbBlobFileBytesRead, (ulong)recordSize);
            int n = _file.Read(recordOffset, record);
            if (n != recordSize) ThrowIncomplete(_path, (ulong)recordOffset, recordSize, n);
            Finish(record, userKey, valueSize, adjustment, verifyChecksums, output);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
        return (ulong)recordSize;
    }

    /// <summary>Async <see cref="GetBlob"/>. <paramref name="userKey"/> must stay valid until completion.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<ulong> GetBlobAsync(ReadOnlyMemory<byte> userKey, ulong offset, ulong valueSize, CompressionType compression, bool verifyChecksums,
        IBufferWriter<byte> output, CancellationToken cancellationToken)
    {
        Prepare(userKey.Length, offset, valueSize, compression, verifyChecksums, out long recordOffset, out int recordSize, out int adjustment);
        byte[] rented = ArrayPool<byte>.Shared.Rent(recordSize);
        try
        {
            _stats?.RecordTick(Ticker.BlobDbBlobFileBytesRead, (ulong)recordSize);
            int n = await _file.ReadAsync(recordOffset, rented.AsMemory(0, recordSize), cancellationToken).ConfigureAwait(false);
            if (n != recordSize) ThrowIncomplete(_path, (ulong)recordOffset, recordSize, n);
            Finish(rented.AsSpan(0, recordSize), userKey.Span, valueSize, adjustment, verifyChecksums, output);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
        return (ulong)recordSize;
    }

    private void Finish(ReadOnlySpan<byte> record, ReadOnlySpan<byte> userKey, ulong valueSize, int adjustment, bool verifyChecksums, IBufferWriter<byte> output)
    {
        if (verifyChecksums) VerifyBlob(record, userKey, valueSize);
        ReadOnlySpan<byte> value = record.Slice(adjustment, (int)valueSize);
        if (Compression == CompressionType.None)
        {
            output.Write(value);
            return;
        }
        try
        {
            // The blob format has always used compression format 2.
            BlockCompression.Decompress(Compression, value, 2, output);
        }
        catch (RocksDbException e) when (e.Code != StatusCode.Corruption)
        {
            ThrowHelper.Corruption(e.Message);
        }
    }

    /// <summary>BlobFileReader::VerifyBlob.</summary>
    private static void VerifyBlob(ReadOnlySpan<byte> record, ReadOnlySpan<byte> userKey, ulong valueSize)
    {
        BlobLogFormat.RecordHeader h = BlobLogFormat.DecodeRecordHeader(record[..BlobLogFormat.RecordHeaderSize]);
        if (h.KeySize != (ulong)userKey.Length) ThrowHelper.Corruption("Key size mismatch when reading blob");
        if (h.ValueSize != valueSize) ThrowHelper.Corruption("Value size mismatch when reading blob");
        ReadOnlySpan<byte> key = record.Slice(BlobLogFormat.RecordHeaderSize, userKey.Length);
        if (!key.SequenceEqual(userKey)) ThrowHelper.Corruption("Key mismatch when reading blob");
        ReadOnlySpan<byte> value = record.Slice(BlobLogFormat.RecordHeaderSize + userKey.Length, (int)valueSize);
        if (!BlobLogFormat.CheckBlobCrc(h.BlobCrc, key, value)) ThrowHelper.Corruption("Blob CRC mismatch");
    }

    public void Dispose() => _file.Dispose();
}
