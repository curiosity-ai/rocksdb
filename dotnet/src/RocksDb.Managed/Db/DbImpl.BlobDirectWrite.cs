using System.Collections.Concurrent;
using RocksDb.Managed.Db.Blob;

namespace RocksDb.Managed.Db;

/// <summary>
/// Blob direct write at the DB level (the BlobFilePartitionManager plumbing of db/db_impl/db_impl*.cc): flush
/// registration of the generation files, their protection from obsolete-file deletion while a memtable references
/// them, and the shutdown flush.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>Generations whose memtable was disposed (MemTable.OnDisposed may run on any thread).</summary>
    private readonly ConcurrentQueue<(ColumnFamilyData Cfd, BlobDirectWriteGeneration Generation)> _releasedBlobGenerations = new();

    /// <summary>
    /// ProtectSealedBlobFileNumbers: the generation's files stay pending outputs (never deleted as obsolete) until the
    /// memtable referencing them is disposed, so readers of old super versions can still read them. REQUIRES: mutex.
    /// </summary>
    private void ProtectBlobDirectWriteFiles(ColumnFamilyData cfd, MemTable mem, BlobDirectWriteGeneration generation) =>
        mem.OnDisposed = () => _releasedBlobGenerations.Enqueue((cfd, generation));

    /// <summary>
    /// UnprotectSealedBlobFileNumbers: releases the files of disposed memtables (pending outputs) and deletes those no
    /// version references (all-garbage files, dropped column families, files dropped from the version while protected).
    /// Returns the files to delete. REQUIRES: mutex.
    /// </summary>
    private List<(ColumnFamilyData Cfd, ulong Number)>? TakeReleasedBlobDirectWriteFiles()
    {
        List<(ColumnFamilyData, ulong)>? delete = null;
        while (_releasedBlobGenerations.TryDequeue(out var r))
        {
            foreach (ulong n in r.Generation.FileNumbers)
            {
                _pendingOutputs.Remove(n);
                if (!Versions.IsLiveBlobFile(n)) (delete ??= new()).Add((r.Cfd, n));
            }
        }
        return delete;
    }

    /// <summary>
    /// PrepareFlushAdditions for the picked memtables: seals their direct-write files and returns the MANIFEST additions
    /// plus a meter whose outflow (the flush output) yields their garbage. Null when no picked memtable wrote blobs.
    /// </summary>
    private static (List<BlobFileAddition> Additions, BlobGarbageMeter Meter)? PrepareBlobDirectWriteFlush(ColumnFamilyData cfd,
        List<ImmutableMemTable> picked)
    {
        List<BlobDirectWriteGeneration>? generations = null;
        foreach (ImmutableMemTable m in picked)
        {
            if (m.BlobGeneration is { IsEmpty: false } g) (generations ??= new()).Add(g);
        }
        if (generations is null) return null;
        var additions = new List<BlobFileAddition>();
        BlobDirectWriteManager.PrepareFlushAdditions(generations, cfd.BlobDirectWrite!, additions);
        var meter = new BlobGarbageMeter();
        foreach (BlobFileAddition a in additions) meter.AddInFlow(a.BlobFileNumber, a.TotalBlobCount, a.TotalBlobBytes);
        return (additions, meter);
    }

    /// <summary>
    /// CancelAllBackgroundWork's force_flush_for_blob_direct_write: some column family has blob direct write files that a
    /// flush has not registered yet (the shutdown flush runs even with avoid_flush_during_shutdown).
    /// </summary>
    private bool HasInFlightBlobDirectWriteFiles()
    {
        if (!_anyBlobDirectWrite) return false;
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            if (cfd.BlobDirectWrite is { HasCurrentFiles: true }) return true;
            lock (_mutex)
            {
                foreach (ImmutableMemTable m in cfd.Imm)
                {
                    // In the list until its flush is committed (a completed flush may wait for an older one).
                    if (m.BlobGeneration is { IsEmpty: false }) return true;
                }
            }
        }
        return false;
    }
}
