using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using RocksDb.Managed.Env;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication;

/// <summary>Result of comparing a source checkpoint manifest against a consumer's local inventory.</summary>
public sealed class ReplicationDeltaPlan
{
    /// <summary>Files the consumer already holds byte-identically; not transferred.</summary>
    public List<string> FilesToReuse { get; } = new();

    /// <summary>Files that are new or changed and must be transferred in full.</summary>
    public List<string> FilesToTransfer { get; } = new();
}

/// <summary>
/// Per-file delta computation for snapshot transfers (port of rocksdb-sharp <c>ReplicationDelta</c>): a consumer offers
/// an inventory of its local immutable files, the source compares it against its checkpoint and only streams the files
/// that are new or changed; the consumer deletes everything else before restoring.
/// <para>
/// Granularity is whole files. Only immutable files (.sst / .blob) are ever reused. Matching requires name + size +
/// identity signature, because two RocksDB instances allocate file numbers independently once they stop sharing a
/// lineage - files with equal names (and even equal sizes) on different nodes are not guaranteed to hold the same
/// content.
/// </para>
/// Every method takes the <see cref="IFileSystem"/> the files live on; overloads without one use
/// <see cref="LocalFileSystem"/>.
/// </summary>
public static class ReplicationDelta
{
    /// <summary>
    /// How much of the end of a file the signature covers. The tail of an SST holds the footer, the (top-level) index and
    /// the properties block, which embeds RocksDB's identity material (DB id, DB session id, original file number,
    /// creation times): two SSTs from different creation events always differ inside this window, while byte-copies are
    /// identical everywhere.
    /// </summary>
    public const int SignatureTailBytes = 1024 * 1024;

    public static bool IsImmutableFile(string fileName) =>
        fileName.EndsWith(".sst", StringComparison.OrdinalIgnoreCase) ||
        fileName.EndsWith(".blob", StringComparison.OrdinalIgnoreCase);

    /// <summary>True for a non-empty name without directory separators or "..".</summary>
    internal static bool IsPlainFileName(string? fileName) =>
        !string.IsNullOrEmpty(fileName) && fileName != "." && fileName != ".." &&
        fileName.IndexOfAny(['/', '\\']) < 0 && Path.GetFileName(fileName) == fileName;

    public static ValueTask<string> ComputeFileSignatureAsync(string path, CancellationToken cancellationToken) =>
        ComputeFileSignatureAsync(LocalFileSystem.Instance, path, SignatureTailBytes, cancellationToken);

    public static ValueTask<string> ComputeFileSignatureAsync(IFileSystem fileSystem, string path, CancellationToken cancellationToken) =>
        ComputeFileSignatureAsync(fileSystem, path, SignatureTailBytes, cancellationToken);

    /// <summary>
    /// Identity signature of an immutable file: SHA-256 over the file length (8 bytes, little-endian) and the last
    /// <paramref name="tailBytes"/> bytes, as upper-case hex.
    /// <para>
    /// Hashing the head instead would not be safe: a replica's locally flushed SST can carry a byte-identical data prefix
    /// (same keys, values and sequence numbers ingested from the WAL stream) and only differ near the end, where the
    /// identity properties live. Block CRCs still guard reused files against corruption at read time.
    /// </para>
    /// Managed difference: the length prefix is always little-endian (rocksdb-sharp used the machine's
    /// <c>BitConverter</c> order), so signatures agree across architectures.
    /// </summary>
    public static async ValueTask<string> ComputeFileSignatureAsync(IFileSystem fileSystem, string path, int tailBytes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentOutOfRangeException.ThrowIfNegative(tailBytes);
        using IncrementalHash sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using IRandomAccessFile file = fileSystem.OpenRandomAccess(path);
        long length = file.Length;
        byte[] header = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(header, length);
        sha.AppendData(header);
        long offset = Math.Max(0, length - tailBytes);
        byte[] buffer = RocksArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (offset < length)
            {
                int n = await file.ReadAsync(offset, buffer.AsMemory(0, (int)Math.Min(buffer.Length, length - offset)), cancellationToken).ConfigureAwait(false);
                if (n <= 0) break;
                sha.AppendData(buffer, 0, n);
                offset += n;
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buffer);
        }
        return Convert.ToHexString(sha.GetHashAndReset());
    }

    public static List<ReplicationFileInfo> ScanImmutableFiles(string directory) => ScanImmutableFiles(LocalFileSystem.Instance, directory);

    /// <summary>
    /// Lists the top level of a database directory's immutable files (candidates for reuse) by name and size. Hashes are
    /// not computed: they are only worth computing for files whose name and size match on both sides.
    /// </summary>
    public static List<ReplicationFileInfo> ScanImmutableFiles(IFileSystem fileSystem, string directory)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        var result = new List<ReplicationFileInfo>();
        if (!FileSystemHelpers.DirectoryExists(fileSystem, directory)) return result;
        foreach (string name in fileSystem.GetChildren(directory))
        {
            if (!IsImmutableFile(name)) continue;
            string path = FileSystemHelpers.Combine(directory, name);
            if (!fileSystem.FileExists(path)) continue;
            result.Add(new ReplicationFileInfo { FileName = name, Size = fileSystem.GetFileSize(path) });
        }
        result.Sort(static (a, b) => string.CompareOrdinal(a.FileName, b.FileName));
        return result;
    }

    public static ValueTask<string?> TryHashCandidateAsync(string directory, string fileName, long expectedSize, CancellationToken cancellationToken) =>
        TryHashCandidateAsync(LocalFileSystem.Instance, directory, fileName, expectedSize, cancellationToken);

    /// <summary>
    /// Hash-request helper for the source side: the signature of the named file only when it exists in
    /// <paramref name="directory"/>, is immutable and has <paramref name="expectedSize"/> bytes - otherwise null, without
    /// reading anything. Immutability is what makes hashing the live file (rather than a checkpoint copy) valid.
    /// </summary>
    public static async ValueTask<string?> TryHashCandidateAsync(IFileSystem fileSystem, string directory, string fileName, long expectedSize,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        if (!IsPlainFileName(fileName) || !IsImmutableFile(fileName)) return null;
        string path = FileSystemHelpers.Combine(directory, fileName);
        if (!fileSystem.FileExists(path) || fileSystem.GetFileSize(path) != expectedSize) return null;
        try
        {
            return await ComputeFileSignatureAsync(fileSystem, path, SignatureTailBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (RocksDbException e) when (e.Code is StatusCode.NotFound or StatusCode.IOError)
        {
            return null; // deleted in between (e.g. compacted away)
        }
    }

    /// <summary>
    /// Consumer side of the hash exchange: given the source's answers (<paramref name="sourceSignatures"/>: file name to
    /// signature, for the candidates the source holds at the same name and size), hashes the local copies of exactly
    /// those files and returns the ones whose signatures agree - the inventory to send with the snapshot request.
    /// </summary>
    public static async ValueTask<List<ReplicationFileInfo>> VerifyCandidatesAsync(IFileSystem fileSystem, string directory,
        IReadOnlyList<ReplicationFileInfo> localCandidates, IReadOnlyDictionary<string, string> sourceSignatures, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(localCandidates);
        ArgumentNullException.ThrowIfNull(sourceSignatures);
        var verified = new List<ReplicationFileInfo>();
        foreach (ReplicationFileInfo local in localCandidates)
        {
            if (!sourceSignatures.TryGetValue(local.FileName, out string? sourceHash) || string.IsNullOrEmpty(sourceHash)) continue;
            string localHash = await ComputeFileSignatureAsync(fileSystem, FileSystemHelpers.Combine(directory, local.FileName), SignatureTailBytes,
                cancellationToken).ConfigureAwait(false);
            if (string.Equals(localHash, sourceHash, StringComparison.OrdinalIgnoreCase))
                verified.Add(new ReplicationFileInfo { FileName = local.FileName, Size = local.Size, Hash = localHash });
        }
        return verified;
    }

    /// <summary>
    /// Splits the source manifest into files the consumer can keep and files that must be transferred. Reuse requires
    /// a name + size match and, when both sides supply a hash, agreeing hashes. When hashes are omitted the consumer
    /// must only list files it already verified through the hash exchange.
    /// </summary>
    public static ReplicationDeltaPlan Compute(IEnumerable<ReplicationFileInfo> sourceManifest, IEnumerable<ReplicationFileInfo>? consumerInventory)
    {
        ArgumentNullException.ThrowIfNull(sourceManifest);
        var byName = new Dictionary<string, ReplicationFileInfo>(StringComparer.Ordinal);
        if (consumerInventory != null)
        {
            foreach (ReplicationFileInfo f in consumerInventory)
            {
                if (f?.FileName != null) byName[f.FileName] = f;
            }
        }
        var plan = new ReplicationDeltaPlan();
        foreach (ReplicationFileInfo src in sourceManifest)
        {
            bool reusable = IsImmutableFile(src.FileName)
                            && byName.TryGetValue(src.FileName, out ReplicationFileInfo? local)
                            && local.Size == src.Size
                            && (string.IsNullOrEmpty(src.Hash)
                                || string.IsNullOrEmpty(local.Hash)
                                || string.Equals(local.Hash, src.Hash, StringComparison.OrdinalIgnoreCase));
            if (reusable) plan.FilesToReuse.Add(src.FileName);
            else plan.FilesToTransfer.Add(src.FileName);
        }
        return plan;
    }

    public static void PrepareForRestore(string dbPath, IEnumerable<string> filesToReuse, IEnumerable<string> preserveNames) =>
        PrepareForRestore(LocalFileSystem.Instance, dbPath, filesToReuse, preserveNames);

    /// <summary>
    /// Brings a database directory into the state a restore expects: every subdirectory (most importantly a WAL
    /// directory) and every top-level file that is not reused is deleted, so files absent from the source - stale SSTs,
    /// the old MANIFEST/CURRENT, divergent WALs - cannot leak into the restored database. Entries named in
    /// <paramref name="preserveNames"/> (e.g. the directory holding Raft or role state) are kept regardless.
    /// Creates <paramref name="dbPath"/> if missing.
    /// </summary>
    public static void PrepareForRestore(IFileSystem fileSystem, string dbPath, IEnumerable<string> filesToReuse, IEnumerable<string> preserveNames)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        var keep = new HashSet<string>(filesToReuse ?? [], StringComparer.Ordinal);
        var preserve = new HashSet<string>(preserveNames ?? [], StringComparer.OrdinalIgnoreCase);
        fileSystem.CreateDirectoryIfMissing(dbPath);
        foreach (string name in fileSystem.GetChildren(dbPath))
        {
            if (preserve.Contains(name)) continue;
            string path = FileSystemHelpers.Combine(dbPath, name);
            if (fileSystem.FileExists(path))
            {
                if (!keep.Contains(name)) fileSystem.DeleteFile(path);
            }
            else
            {
                FileSystemHelpers.DeleteDirectoryRecursive(fileSystem, path);
            }
        }
    }
}
