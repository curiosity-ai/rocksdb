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
/// Versions hidden by a newer version of the same user key in the same snapshot stripe are dropped (rule (A) of
/// upstream's flush-time CompactionIterator, see AddPointEntries); everything else is kept. Also used for recovery
/// flushes (WriteLevel0TableForRecovery).
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
    /// the caller deletes the builder's files. <paramref name="snapshots"/> are the live snapshot sequence numbers
    /// (ascending) when the flush job was created (FlushJob's existing_snapshots_).
    /// </summary>
    public static async ValueTask<FileMetaData?> WriteLevel0TableAsync(IFileSystem fs, string path, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems,
        ulong fileNumber, ulong epochNumber, DbIdentity identity, ulong[] snapshots, Blob.BlobFileBuilder? blobBuilder, bool synchronous,
        CancellationToken cancellationToken)
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
                // Collapsing versions of a key compares user keys without their timestamp and needs full_history_ts_low;
                // with user-defined timestamps every version is kept (always correct).
                AddPointEntries(builder, cfd.Icmp, mems, meta, snapshots, dropHidden: cfd.TimestampSize == 0, blobBuilder);
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

    /// <summary>
    /// K-way merge of the memtables' point entries (few inputs: linear minimum selection). With
    /// <paramref name="dropHidden"/>, an entry is dropped when the last entry written has the same user key, is in the
    /// same snapshot stripe (no snapshot in between) and is a Put, Delete, blob index or entity: rule (A) of
    /// CompactionIterator::NextFromInput ("hidden by a newer entry for the same user key"), which upstream's flush
    /// applies through BuildTable's CompactionIterator. Merge operands, single deletes and TimedPut values never hide
    /// older entries here (upstream resolves them with the merge helper / single-delete logic; keeping the older
    /// entries is always correct and leaves that work to compaction), single deletes are never dropped (they have their
    /// own branch before rule (A) upstream), and keys covered by range tombstones of the same flush are kept.
    /// </summary>
    private static void AddPointEntries(BlockBasedTableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder)
    {
        Util.PooledBufferWriter? blobIndex = blobBuilder is null ? null : new Util.PooledBufferWriter(Blob.BlobIndex.MaxEncodedLength);
        Util.PooledBufferWriter? blobKey = blobBuilder is null ? null : new Util.PooledBufferWriter(64);
        try
        {
            AddPointEntries(builder, icmp, mems, meta, snapshots, dropHidden, blobBuilder, blobIndex, blobKey);
        }
        finally
        {
            blobIndex?.Dispose();
            blobKey?.Dispose();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void AddPointEntries(BlockBasedTableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder, Util.PooledBufferWriter? blobIndex, Util.PooledBufferWriter? blobKey)
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
        bool lastHides = false;
        ulong lastStripe = 0;
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
            ulong seq = footer >> 8;
            // A single delete is never dropped this way (CompactionIterator handles it in its own branch before rule (A)):
            // dropping one hidden by a newer Put would let a later SingleDelete cancel that Put and resurface an older one.
            if (lastHides && type != EntryType.SingleDeletion && EarliestVisibleSnapshot(snapshots, seq) == lastStripe
                && icmp.CompareUserKey(DbFormat.ExtractUserKey(key), DbFormat.ExtractUserKey(lastKey.AsSpan(0, lastLen))) == 0)
            {
                // Hidden by the newer entry written last (same user key, same snapshot stripe).
                its[best].Next();
                continue;
            }
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
            lastHides = dropHidden && type is EntryType.Value or EntryType.Deletion or EntryType.BlobIndex or EntryType.WideColumnEntity;
            lastStripe = EarliestVisibleSnapshot(snapshots, seq);
            its[best].Next();
        }
        if (!first) meta.Largest = lastKey.AsSpan(0, lastLen).ToArray();
    }

    /// <summary>
    /// CompactionIterator::findEarliestVisibleSnapshot without a snapshot checker: the smallest snapshot that can see
    /// <paramref name="seq"/> (kMaxSequenceNumber if none). <paramref name="snapshots"/> is ascending.
    /// </summary>
    internal static ulong EarliestVisibleSnapshot(ulong[] snapshots, ulong seq)
    {
        if (snapshots.Length == 0) return DbFormat.MaxSequenceNumber;
        int lo = 0, hi = snapshots.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (snapshots[mid] < seq) lo = mid + 1;
            else hi = mid;
        }
        return lo < snapshots.Length ? snapshots[lo] : DbFormat.MaxSequenceNumber;
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
        int tsSize = frag.TimestampSize;
        for (int i = 0; i < frag.Count; i++)
        {
            ReadOnlySpan<byte> start = frag.GetStart(i);
            ReadOnlySpan<byte> end = frag.GetEnd(i);
            ReadOnlySpan<ulong> seqs = frag.GetSequences(i);
            for (int k = 0; k < seqs.Length; k++)
            {
                ulong seq = seqs[k];
                ReadOnlySpan<byte> tStart = start, tEnd = end;
                if (tsSize != 0)
                {
                    // RangeTombstone(start, end, seq, ts): both keys carry the tombstone's timestamp; the end key used
                    // for the file boundary keeps the maximum timestamp (SerializeEndKey).
                    tStart = ReplaceTimestamp(start, frag.GetTimestamp(i, k));
                    tEnd = ReplaceTimestamp(end, frag.GetTimestamp(i, k));
                }
                var startKey = new InternalKey(tStart, seq, EntryType.RangeDeletion);
                builder.Add(startKey.Encoded, tEnd);
                // UpdateBoundariesForRange(start = tombstone.SerializeKey(), end = tombstone.SerializeEndKey()).
                if (meta.Smallest.Length == 0 || icmp.Compare(startKey.Encoded, meta.Smallest) < 0) meta.Smallest = startKey.Encoded.ToArray();
                var endKey = new InternalKey(end, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                if (meta.Largest.Length == 0 || icmp.Compare(meta.Largest, endKey.Encoded) < 0) meta.Largest = endKey.Encoded.ToArray();
                if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
                if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
            }
        }
    }

    /// <summary>
    /// <paramref name="keyWithTs"/> with its trailing timestamp replaced by <paramref name="ts"/>
    /// (AppendUserKeyWithDifferentTimestamp). Cold path (range tombstones only).
    /// </summary>
    internal static byte[] ReplaceTimestamp(ReadOnlySpan<byte> keyWithTs, ReadOnlySpan<byte> ts)
    {
        byte[] r = keyWithTs.ToArray();
        ts.CopyTo(r.AsSpan(r.Length - ts.Length));
        return r;
    }
}
