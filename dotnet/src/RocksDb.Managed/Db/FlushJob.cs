using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>Identity of the DB written into table properties and unique ids.</summary>
internal readonly record struct DbIdentity(string DbId, string SessionId, string HostId);

/// <summary>
/// Writes memtables to a level-0 table file. Port of FlushJob::WriteLevel0Table (db/flush_job.cc) and BuildTable
/// (db/builder.cc): merges the point entries of the memtables in internal-key order, writes the fragmented range
/// tombstones, finishes and syncs the file and fills in its <see cref="FileMetaData"/>.
/// <para>
/// No entries are dropped: the output keeps every version (upstream's flush-time CompactionIterator only drops
/// versions invisible to all snapshots; keeping them is always correct). Also used for recovery flushes
/// (WriteLevel0TableForRecovery).
/// </para>
/// </summary>
internal static class FlushJob
{
    /// <summary>
    /// Builds table file #<paramref name="fileNumber"/> from <paramref name="mems"/> (any order). Returns null (and
    /// deletes the file) if the memtables contain nothing. With <paramref name="synchronous"/>, all I/O is blocking.
    /// With a <paramref name="blobBuilder"/> (enable_blob_files), plain values of at least min_blob_size bytes are written
    /// to blob files and replaced by blob indexes (BuildTable with a BlobFileBuilder); the builder is finished here and
    /// its <see cref="Blob.BlobFileBuilder.Additions"/> belong in the same version edit as the table file. On failure
    /// the caller deletes the builder's files.
    /// </summary>
    public static async ValueTask<FileMetaData?> WriteLevel0TableAsync(IFileSystem fs, string path, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems,
        ulong fileNumber, ulong epochNumber, DbIdentity identity, Blob.BlobFileBuilder? blobBuilder, bool synchronous, CancellationToken cancellationToken)
    {
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        TableBuilderOptions tbo = TableFactory.CreateBuilderOptions(cfd.Options, TableFileLevel.Flush) with
        {
            ColumnFamilyId = cfd.Id,
            ColumnFamilyName = cfd.Name,
            FileNumber = fileNumber,
            DbId = identity.DbId,
            DbSessionId = identity.SessionId,
            DbHostId = identity.HostId,
            CreationTime = now,
            OldestKeyTime = now,
            FileCreationTime = now,
        };

        var meta = new FileMetaData
        {
            FileNumber = fileNumber,
            EpochNumber = epochNumber,
            FileCreationTime = now,
            OldestAncestorTime = now,
        };
        IWritableFile file = fs.CreateWritable(path, truncate: true);
        bool keep = false;
        try
        {
            using (var builder = new BlockBasedTableBuilder(file, cfd.Format, tbo))
            {
                AddPointEntries(builder, cfd.Icmp, mems, meta, blobBuilder);
                blobBuilder?.Finish();
                AddRangeTombstones(builder, cfd, mems, meta);
                if (builder.IsEmpty && meta.Smallest.Length == 0)
                {
                    builder.Abandon();
                }
                else
                {
                    if (synchronous) builder.Finish();
                    else await builder.FinishAsync(cancellationToken).ConfigureAwait(false);
                    if (synchronous) file.Sync();
                    else await file.SyncAsync(cancellationToken).ConfigureAwait(false);
                    meta.FileSize = builder.FileSize;
                    meta.TailSize = builder.FileSize - builder.Properties.TailStartOffset;
                    // ExtractTimestampFromTableProperties (db/builder.cc).
                    meta.MinTimestamp = builder.TimestampMin;
                    meta.MaxTimestamp = builder.TimestampMax;
                    keep = true;
                }
            }
        }
        finally
        {
            if (synchronous) file.Dispose();
            else await file.DisposeAsync().ConfigureAwait(false);
            if (!keep)
            {
                try { fs.DeleteFile(path); }
                catch (IOException) { }
                catch (RocksDbException) { }
            }
        }
        if (!keep) return null;
        // BuildTable: GetSstInternalUniqueId(db_id, db_session_id, file number) (force = false).
        if (identity.DbId.Length > 0 && identity.SessionId.Length > 0)
        {
            OffsetableCacheKey.GetSstInternalUniqueId(identity.DbId, identity.SessionId, fileNumber, out ulong id0, out ulong id1);
            meta.UniqueId = new UniqueId64x2(id0, id1);
        }
        return meta;
    }

    /// <summary>K-way merge of the memtables' point entries (few inputs: linear minimum selection).</summary>
    private static void AddPointEntries(BlockBasedTableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        Blob.BlobFileBuilder? blobBuilder)
    {
        Util.PooledBufferWriter? blobIndex = blobBuilder is null ? null : new Util.PooledBufferWriter(Blob.BlobIndex.MaxEncodedLength);
        Util.PooledBufferWriter? blobKey = blobBuilder is null ? null : new Util.PooledBufferWriter(64);
        try
        {
            AddPointEntries(builder, icmp, mems, meta, blobBuilder, blobIndex, blobKey);
        }
        finally
        {
            blobIndex?.Dispose();
            blobKey?.Dispose();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void AddPointEntries(BlockBasedTableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        Blob.BlobFileBuilder? blobBuilder, Util.PooledBufferWriter? blobIndex, Util.PooledBufferWriter? blobKey)
    {
        int n = mems.Count;
        var its = new MemTableIterator[n];
        for (int i = 0; i < n; i++)
        {
            its[i] = mems[i].NewIterator();
            its[i].SeekToFirst();
        }
        bool first = true;
        byte[]? lastKey = null;
        int lastLen = 0;
        while (true)
        {
            int best = -1;
            for (int i = 0; i < n; i++)
            {
                if (!its[i].Valid) continue;
                if (best < 0 || icmp.Compare(its[i].Key, its[best].Key) < 0) best = i;
            }
            if (best < 0) break;
            ReadOnlySpan<byte> key = its[best].Key;
            ulong footer = DbFormat.ExtractFooter(key);
            var type = (EntryType)(byte)footer;
            if (blobBuilder is not null && type == EntryType.Value)
            {
                // CompactionIterator::ExtractLargeValueIfNeeded (flush runs through a CompactionIterator upstream).
                blobIndex!.Clear();
                if (blobBuilder.Add(DbFormat.ExtractUserKey(key), its[best].Value, blobIndex))
                {
                    blobKey!.Clear();
                    blobKey.Write(key);
                    Util.Coding.EncodeFixed64(blobKey.WrittenSpanMutable[^DbFormat.InternalKeyFooterSize..], (footer & ~0xFFUL) | (byte)EntryType.BlobIndex);
                    builder.Add(blobKey.WrittenSpan, blobIndex.WrittenSpan);
                    UpdateOldestBlobFileNumber(meta, blobIndex.WrittenSpan);
                }
                else
                {
                    builder.Add(key, its[best].Value);
                }
            }
            else
            {
                builder.Add(key, its[best].Value);
                if (type == EntryType.BlobIndex) UpdateOldestBlobFileNumber(meta, its[best].Value);
            }
            ulong seq = footer >> 8;
            if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
            if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
            if (first)
            {
                meta.Smallest = key.ToArray();
                first = false;
            }
            if (lastKey is null || lastKey.Length < key.Length) lastKey = new byte[Math.Max(key.Length, 64)];
            key.CopyTo(lastKey);
            lastLen = key.Length;
            its[best].Next();
        }
        if (!first) meta.Largest = lastKey.AsSpan(0, lastLen).ToArray();
    }

    /// <summary>
    /// The blob part of FileMetaData::UpdateBoundaries: tracks the oldest blob file referenced by the table. Inlined and
    /// TTL indexes are not tracked (stacked BlobDB). Throws Corruption for a malformed index.
    /// </summary>
    internal static void UpdateOldestBlobFileNumber(FileMetaData meta, ReadOnlySpan<byte> value)
    {
        Blob.BlobIndex index = Blob.BlobIndex.Decode(value);
        if (index.IsInlined || index.HasTtl) return;
        if (index.FileNumber == FileMetaData.InvalidBlobFileNumber) ThrowHelper.Corruption("Invalid blob file number");
        if (meta.OldestBlobFileNumber == FileMetaData.InvalidBlobFileNumber || index.FileNumber < meta.OldestBlobFileNumber)
        {
            meta.OldestBlobFileNumber = index.FileNumber;
        }
    }

    /// <summary>
    /// Writes the fragmented range tombstones of all memtables (every fragment with every covering sequence number,
    /// in internal key order) and extends the file boundaries like FileMetaData::UpdateBoundariesForRange.
    /// </summary>
    private static void AddRangeTombstones(BlockBasedTableBuilder builder, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems, FileMetaData meta)
    {
        List<FragmentedRangeTombstoneList.Tombstone>? raw = null;
        foreach (MemTable m in mems)
        {
            if (m.NumRangeDeletes == 0) continue;
            MemTableIterator it = m.NewRangeTombstoneEntryIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                (raw ??= new()).Add(new FragmentedRangeTombstoneList.Tombstone(it.UserKey.ToArray(), it.Value.ToArray(), it.Sequence));
            }
        }
        if (raw is null) return;
        FragmentedRangeTombstoneList frag = FragmentedRangeTombstoneList.Build(cfd.UserComparator, raw);
        InternalKeyComparator icmp = cfd.Icmp;
        for (int i = 0; i < frag.Count; i++)
        {
            ReadOnlySpan<byte> start = frag.GetStart(i);
            ReadOnlySpan<byte> end = frag.GetEnd(i);
            foreach (ulong seq in frag.GetSequences(i))
            {
                var startKey = new InternalKey(start, seq, EntryType.RangeDeletion);
                builder.Add(startKey.Encoded, end);
                // UpdateBoundariesForRange(start = tombstone.SerializeKey(), end = tombstone.SerializeEndKey()).
                if (meta.Smallest.Length == 0 || icmp.Compare(startKey.Encoded, meta.Smallest) < 0) meta.Smallest = startKey.Encoded.ToArray();
                var endKey = new InternalKey(end, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                if (meta.Largest.Length == 0 || icmp.Compare(meta.Largest, endKey.Encoded) < 0) meta.Largest = endKey.Encoded.ToArray();
                if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
                if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
            }
        }
    }
}
