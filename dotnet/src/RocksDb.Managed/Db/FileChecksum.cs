using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>What a <see cref="FileChecksumGenerator"/> is created for (FileChecksumGenContext, include/rocksdb/file_checksum.h).</summary>
/// <param name="FileName">Path of the file whose checksum is computed.</param>
/// <param name="RequestedChecksumFuncName">
/// The checksum function the caller needs (e.g. the one recorded for the file in the MANIFEST); empty for the factory's
/// default.
/// </param>
public readonly record struct FileChecksumGenContext(string FileName, string RequestedChecksumFuncName);

/// <summary>
/// Computes the checksum of a whole file from its bytes in order (FileChecksumGenerator, include/rocksdb/file_checksum.h).
/// One instance per file; not thread-safe.
/// </summary>
public abstract class FileChecksumGenerator
{
    /// <summary>The checksum function name, recorded in the MANIFEST with the checksum (e.g. "FileChecksumCrc32c").</summary>
    public abstract string Name { get; }

    /// <summary>Adds the next bytes of the file.</summary>
    public abstract void Update(ReadOnlySpan<byte> data);

    /// <summary>Completes the computation (Finalize); <see cref="GetChecksum"/> is valid afterwards.</summary>
    public abstract void Finish();

    /// <summary>The finalized checksum bytes, as stored in the MANIFEST.</summary>
    public abstract byte[] GetChecksum();
}

/// <summary>
/// Creates the generators used for every table and blob file the DB writes or ingests when set as
/// <see cref="DbOptions.FileChecksumGenFactory"/> (FileChecksumGenFactory, include/rocksdb/file_checksum.h). The
/// checksums are stored in the MANIFEST (readable by native RocksDB) and checked by
/// <see cref="RocksDatabase.VerifyFileChecksums()"/> and ingestion. Must be thread-safe.
/// </summary>
public abstract class FileChecksumGenFactory
{
    /// <summary>The factory name (persisted as file_checksum_gen_factory in the OPTIONS file).</summary>
    public abstract string Name { get; }

    /// <summary>
    /// A new generator for <paramref name="context"/>, or null when the requested checksum function is not supported.
    /// </summary>
    public abstract FileChecksumGenerator? CreateFileChecksumGenerator(FileChecksumGenContext context);

    public override string ToString() => Name;
}

/// <summary>
/// The built-in factory (GetFileChecksumGenCrc32cFactory / FileChecksumGenCrc32cFactory, util/file_checksum_helper.h):
/// crc32c of the whole file (unmasked), stored as 4 big-endian bytes under the function name "FileChecksumCrc32c"
/// (kStandardDbFileChecksumFuncName), identical to native RocksDB.
/// </summary>
public sealed class FileChecksumGenCrc32cFactory : FileChecksumGenFactory
{
    /// <summary>kClassName().</summary>
    public const string ClassName = "FileChecksumGenCrc32cFactory";
    /// <summary>kStandardDbFileChecksumFuncName.</summary>
    public const string Crc32cFuncName = "FileChecksumCrc32c";

    /// <summary>Shared instance (the factory is stateless).</summary>
    public static FileChecksumGenCrc32cFactory Instance { get; } = new();

    public override string Name => ClassName;

    public override FileChecksumGenerator? CreateFileChecksumGenerator(FileChecksumGenContext context) =>
        context.RequestedChecksumFuncName.Length == 0 || context.RequestedChecksumFuncName == Crc32cFuncName ? new Crc32cGenerator() : null;

    /// <summary>Port of FileChecksumGenCrc32c.</summary>
    private sealed class Crc32cGenerator : FileChecksumGenerator
    {
        private uint _checksum;
        private byte[]? _value;

        public override string Name => Crc32cFuncName;

        public override void Update(ReadOnlySpan<byte> data) => _checksum = Crc32C.Extend(_checksum, data);

        public override void Finish()
        {
            // Store as big endian raw bytes.
            var v = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(v, _checksum);
            _value = v;
        }

        public override byte[] GetChecksum() => _value ?? throw new InvalidOperationException("Finish() not called");
    }
}

/// <summary>
/// Whole-file checksum helpers: GenerateOneFileChecksum (file/file_util.cc) and the checksum generation of
/// WritableFileWriter (a writer decorator that feeds every appended byte to the generator).
/// </summary>
internal static class FileChecksumHelper
{
    /// <summary>
    /// Opens <paramref name="path"/> for writing; with <see cref="WritableFileOptions.FileChecksumGenFactory"/> the file
    /// computes its checksum while written (WritableFileWriter::checksum_generator_). The file keeps a sticky error
    /// state like WritableFileWriter (see <see cref="WriterFile"/>).
    /// </summary>
    public static IWritableFile CreateWritable(IFileSystem fs, string path, WritableFileOptions options)
    {
        IWritableFile file = fs.CreateWritable(path, truncate: true, options);
        FileChecksumGenerator? gen = options.FileChecksumGenFactory?.CreateFileChecksumGenerator(new FileChecksumGenContext(path, ""));
        return new WriterFile(file, gen);
    }

    /// <summary>Gives <paramref name="file"/> the sticky error state of WritableFileWriter (no checksum).</summary>
    public static IWritableFile WithStickyError(IWritableFile file) => new WriterFile(file, null);

    /// <summary>
    /// The finalized checksum of a file created by <see cref="CreateWritable"/> after its last append, or the unknown
    /// checksum (kUnknownFileChecksum, kUnknownFileChecksumFuncName) when it was not computed.
    /// </summary>
    public static (byte[] Checksum, string FuncName) GetChecksum(IWritableFile file)
    {
        if (file is not WriterFile { HasChecksum: true } c) return ([], Db.FileMetaData.UnknownFileChecksumFuncName);
        return c.FinalizeChecksum();
    }

    /// <summary>
    /// Port of GenerateOneFileChecksum: reads <paramref name="path"/> sequentially and computes its checksum with the
    /// generator <paramref name="factory"/> creates for <paramref name="requestedFuncName"/> (empty: the default).
    /// InvalidArgument when the factory does not support the requested function. With <paramref name="statistics"/> the
    /// reads are timed like table reads.
    /// </summary>
    public static (byte[] Checksum, string FuncName) GenerateOneFileChecksum(IFileSystem fs, string path, FileChecksumGenFactory factory,
        string requestedFuncName, Statistics? statistics)
    {
        FileChecksumGenerator? gen = factory.CreateFileChecksumGenerator(new FileChecksumGenContext(path, requestedFuncName));
        if (gen is null)
        {
            ThrowHelper.InvalidArgument("Cannot get the file checksum generator based on the requested checksum function name: "
                + requestedFuncName + " from checksum factory: " + factory.Name);
        }
        const int ChunkSize = 256 << 10;
        byte[] buffer = RocksArrayPool<byte>.Shared.Rent(ChunkSize);
        try
        {
            IRandomAccessFile raw = fs.OpenRandomAccess(path);
            // RandomAccessFileReader with the DB statistics: SST_READ_MICROS + the current activity's histogram.
            using IRandomAccessFile file = statistics is null ? raw : new Monitoring.StatisticsRandomAccessFile(raw, statistics);
            long size = file.Length;
            long offset = 0;
            while (offset < size)
            {
                int n = file.Read(offset, buffer.AsSpan(0, (int)Math.Min(ChunkSize, size - offset)));
                if (n <= 0) ThrowHelper.Corruption("File checksum: unexpected end of file " + path);
                gen!.Update(buffer.AsSpan(0, n));
                offset += n;
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buffer);
        }
        gen!.Finish();
        return (gen.GetChecksum(), gen.Name);
    }

    /// <summary>Slice::ToString(hex = true): upper-case hex.</summary>
    public static string Hex(ReadOnlySpan<byte> data) => Convert.ToHexString(data);

    /// <summary>
    /// The part of WritableFileWriter (file/writable_file_writer.cc) the engine's table and blob file writers rely on:
    /// the optional checksum generator (checksum_generator_) and the sticky error state (seen_error_). After a failed
    /// Append / Flush / Sync the contents and durability of the file are unknown, so every later Append, Flush and Sync
    /// fails with IOError "Writer has previous error." instead of reaching the file: such a file can never be reported
    /// finished or synced by a retry. Dispose still closes the file (WritableFileWriter::Close closes it too).
    /// Not thread-safe, like the files it wraps (one writer).
    /// </summary>
    internal sealed class WriterFile(IWritableFile inner, FileChecksumGenerator? generator) : IWritableFile
    {
        private (byte[], string)? _result;
        private bool _seenError;

        /// <summary>An append, flush or sync of this file failed (WritableFileWriter::seen_error).</summary>
        public bool SeenError => _seenError;

        public bool HasChecksum => generator is not null;

        public long Size => inner.Size;

        private void ThrowIfSeenError()
        {
            if (_seenError) throw new RocksDbException(StatusCode.IOError, "IO error: Writer has previous error.");
        }

        public void Append(ReadOnlySpan<byte> data)
        {
            ThrowIfSeenError();
            generator?.Update(data);
            try
            {
                inner.Append(data);
            }
            catch
            {
                _seenError = true;
                throw;
            }
        }

        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            ThrowIfSeenError();
            generator?.Update(data.Span);
            return Guard(inner, static (f, d, ct) => f.AppendAsync(d, ct), data, cancellationToken);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfSeenError();
            return Guard(inner, static (f, _, ct) => f.FlushAsync(ct), default, cancellationToken);
        }

        public ValueTask SyncAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfSeenError();
            return Guard(inner, static (f, _, ct) => f.SyncAsync(ct), default, cancellationToken);
        }

        public void Flush()
        {
            ThrowIfSeenError();
            try
            {
                inner.Flush();
            }
            catch
            {
                _seenError = true;
                throw;
            }
        }

        public void Sync()
        {
            ThrowIfSeenError();
            try
            {
                inner.Sync();
            }
            catch
            {
                _seenError = true;
                throw;
            }
        }

        /// <summary>Runs the operation; a failure (synchronous or not) sets the sticky error. No allocation when it completes synchronously.</summary>
        private ValueTask Guard(IWritableFile file, Func<IWritableFile, ReadOnlyMemory<byte>, CancellationToken, ValueTask> op,
            ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            ValueTask t;
            try
            {
                t = op(file, data, cancellationToken);
            }
            catch
            {
                _seenError = true;
                throw;
            }
            return t.IsCompletedSuccessfully ? t : Awaited(t);
        }

        private async ValueTask Awaited(ValueTask t)
        {
            try
            {
                await t.ConfigureAwait(false);
            }
            catch
            {
                _seenError = true;
                throw;
            }
        }

        public void Dispose() => inner.Dispose();
        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public (byte[], string) FinalizeChecksum()
        {
            if (_result is null)
            {
                generator!.Finish();
                _result = (generator.GetChecksum(), generator.Name);
            }
            return _result.Value;
        }
    }
}
