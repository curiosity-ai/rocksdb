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
/// <para>
/// With user-defined timestamps in memtable only (persist_user_defined_timestamps = false) the memtables are read
/// through upstream's TimestampStrippingIterator view: every timestamp is replaced by the minimum timestamp, the table
/// builder strips it from the persisted keys, and versions of a key that collapse onto one key are dropped like the
/// flush-time CompactionIterator does (rule A: a newer version in the same snapshot stripe hides older ones; merge
/// operands and single deletes keep what follows them).
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
        ulong fileNumber, ulong epochNumber, DbIdentity identity, ulong[] snapshots, Blob.BlobFileBuilder? blobBuilder, Blob.BlobGarbageMeter? blobOutflow,
        bool synchronous, CancellationToken cancellationToken)
    {
        bool stripTimestamps = cfd.TimestampsInMemtableOnly;
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
            // FileMetaData::user_defined_timestamps_persisted: the MANIFEST then stores the boundaries without timestamps.
            UserDefinedTimestampsPersisted = !stripTimestamps,
        };
        IWritableFile file = fs.CreateWritable(path, truncate: true);
        bool keep = false;
        try
        {
            using (var builder = new BlockBasedTableBuilder(file, cfd.Format, tbo))
            {
                FragmentedRangeTombstoneList? tombstones = FragmentRangeTombstones(cfd, mems, stripTimestamps);
                if (stripTimestamps)
                {
                    AddPointEntriesWithMinTimestamp(builder, cfd, mems, meta, blobBuilder, snapshots, tombstones);
                }
                else
                {
                    // Collapsing versions of a key compares user keys without their timestamp and needs full_history_ts_low;
                    // with persisted user-defined timestamps every version is kept (always correct).
                    AddPointEntries(builder, cfd.Icmp, mems, meta, snapshots, dropHidden: cfd.TimestampSize == 0, blobBuilder, blobOutflow);
                }
                blobBuilder?.Finish();
                if (tombstones is not null) AddRangeTombstones(builder, cfd, tombstones, meta);
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
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder, Blob.BlobGarbageMeter? blobOutflow)
    {
        Util.PooledBufferWriter? blobIndex = blobBuilder is null ? null : new Util.PooledBufferWriter(Blob.BlobIndex.MaxEncodedLength);
        Util.PooledBufferWriter? blobKey = blobBuilder is null ? null : new Util.PooledBufferWriter(64);
        Blob.EntityBlobRewriter? entities = blobBuilder is null ? null : new Blob.EntityBlobRewriter();
        try
        {
            AddPointEntries(builder, icmp, mems, meta, snapshots, dropHidden, blobBuilder, blobIndex, blobKey, entities, blobOutflow);
        }
        finally
        {
            blobIndex?.Dispose();
            blobKey?.Dispose();
            entities?.Dispose();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void AddPointEntries(BlockBasedTableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder, Util.PooledBufferWriter? blobIndex, Util.PooledBufferWriter? blobKey,
        Blob.EntityBlobRewriter? entities, Blob.BlobGarbageMeter? blobOutflow)
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
            else if (type == EntryType.WideColumnEntity)
            {
                AddEntity(builder, meta, key, its[best].Value, blobBuilder, entities);
                blobOutflow?.ProcessOutFlow(key, its[best].Value);
            }
            else
            {
                builder.Add(key, its[best].Value);
                if (type == EntryType.BlobIndex)
                {
                    UpdateOldestBlobFileNumber(meta, its[best].Value);
                    // Blob direct write: the flush output's references to the files of the flushed generations.
                    blobOutflow?.ProcessOutFlow(key, its[best].Value);
                }
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
    /// Flush with timestamps in memtable only: the k-way merge of <see cref="AddPointEntries(BlockBasedTableBuilder, InternalKeyComparator, IReadOnlyList{MemTable}, FileMetaData, Blob.BlobFileBuilder?)"/>
    /// over upstream's TimestampStrippingIterator (each key's timestamp replaced by the minimum timestamp,
    /// ReplaceInternalKeyWithMinTimestamp) followed by the hidden-version rule of the flush-time CompactionIterator.
    /// <paramref name="snapshots"/> are the live snapshot sequence numbers, ascending. Cold path (allocates per flush).
    /// </summary>
    private static void AddPointEntriesWithMinTimestamp(BlockBasedTableBuilder builder, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems,
        FileMetaData meta, Blob.BlobFileBuilder? blobBuilder, ulong[] snapshots, FragmentedRangeTombstoneList? tombstones)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        int ts = cfd.TimestampSize;
        int n = mems.Count;
        var its = new MemTableIterator[n];
        for (int i = 0; i < n; i++)
        {
            its[i] = mems[i].NewIterator();
            its[i].SeekToFirst();
        }
        using var key = new Util.PooledBufferWriter(64);
        using var lastUserKey = new Util.PooledBufferWriter(64);
        using var blobIndex = new Util.PooledBufferWriter(Blob.BlobIndex.MaxEncodedLength);
        using var entities = new Blob.EntityBlobRewriter();
        bool haveLast = false;
        ulong lastStripe = 0;
        EntryType lastType = EntryType.Value;
        bool first = true;
        while (true)
        {
            int best = -1;
            for (int i = 0; i < n; i++)
            {
                if (!its[i].Valid) continue;
                if (best < 0 || icmp.Compare(its[i].Key, its[best].Key) < 0) best = i;
            }
            if (best < 0) break;
            ReadOnlySpan<byte> original = its[best].Key;
            ReadOnlySpan<byte> value = its[best].Value;
            ulong footer = DbFormat.ExtractFooter(original);
            var type = (EntryType)(byte)footer;
            ulong seq = footer >> 8;
            int userLen = original.Length - DbFormat.InternalKeyFooterSize;
            if (userLen < ts) ThrowHelper.Corruption("user key too short for its timestamp");
            ReadOnlySpan<byte> userKeyWithoutTs = original[..(userLen - ts)];
            ulong stripe = EarliestVisibleSnapshot(snapshots, seq);
            bool sameKey = haveLast && cfd.UserComparator.CompareWithoutTimestamp(userKeyWithoutTs, false, lastUserKey.WrittenSpan, false) == 0;
            if (sameKey && lastType is not (EntryType.Merge or EntryType.SingleDeletion) && stripe == lastStripe)
            {
                // CompactionIterator rule (A): hidden by a newer version visible to the same snapshots.
                its[best].Next();
                continue;
            }
            if (!sameKey)
            {
                lastUserKey.Clear();
                lastUserKey.Write(userKeyWithoutTs);
                haveLast = true;
            }
            lastStripe = stripe;
            lastType = type;

            // ReplaceInternalKeyWithMinTimestamp (the minimum u64 timestamp is all zeros).
            key.Clear();
            key.Write(userKeyWithoutTs);
            key.GetSpan(ts)[..ts].Clear();
            key.Advance(ts);
            key.Write(original[userLen..]);
            if (tombstones is not null && CoveredInSameStripe(tombstones, DbFormat.ExtractUserKey(key.WrittenSpan), seq, stripe, snapshots))
            {
                // CompactionRangeDelAggregator::ShouldDelete: covered by a newer tombstone of the same snapshot stripe.
                its[best].Next();
                continue;
            }
            if (blobBuilder is not null && type == EntryType.Value)
            {
                blobIndex.Clear();
                if (blobBuilder.Add(DbFormat.ExtractUserKey(key.WrittenSpan), value, blobIndex))
                {
                    Util.Coding.EncodeFixed64(key.WrittenSpanMutable[^DbFormat.InternalKeyFooterSize..], (footer & ~0xFFUL) | (byte)EntryType.BlobIndex);
                    builder.Add(key.WrittenSpan, blobIndex.WrittenSpan);
                    UpdateOldestBlobFileNumber(meta, blobIndex.WrittenSpan);
                }
                else
                {
                    builder.Add(key.WrittenSpan, value);
                }
            }
            else if (type == EntryType.WideColumnEntity)
            {
                AddEntity(builder, meta, key.WrittenSpan, value, blobBuilder, entities);
            }
            else
            {
                builder.Add(key.WrittenSpan, value);
                if (type == EntryType.BlobIndex) UpdateOldestBlobFileNumber(meta, value);
            }
            if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
            if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
            if (first)
            {
                meta.Smallest = key.WrittenSpan.ToArray();
                first = false;
            }
            meta.Largest = key.WrittenSpan.ToArray();
            its[best].Next();
        }
    }

    private static bool CoveredInSameStripe(FragmentedRangeTombstoneList tombstones, ReadOnlySpan<byte> userKey, ulong seq, ulong stripe,
        ulong[] snapshots)
    {
        int f = tombstones.FindCovering(userKey);
        if (f < 0) return false;
        foreach (ulong s in tombstones.GetSequences(f))
        {
            if (s <= seq) break; // newest first
            if (EarliestVisibleSnapshot(snapshots, s) == stripe) return true;
        }
        return false;
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

    [ThreadStatic] private static List<Wide.WideColumnSlot>? t_entitySlots;

    /// <summary>
    /// Adds an entity to the flush output: CompactionIterator::ExtractLargeColumnValuesIfNeeded when the output gets
    /// blob files (flush runs through a CompactionIterator upstream), then the blob part of UpdateBoundaries.
    /// </summary>
    private static void AddEntity(BlockBasedTableBuilder builder, FileMetaData meta, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity,
        Blob.BlobFileBuilder? blobBuilder, Blob.EntityBlobRewriter? entities)
    {
        if (blobBuilder is not null && entities!.TryExtract(blobBuilder, DbFormat.ExtractUserKey(key), entity)) entity = entities.Output;
        builder.Add(key, entity);
        UpdateOldestBlobFileNumberForEntity(meta, entity);
    }

    /// <summary>
    /// The entity part of FileMetaData::UpdateBoundaries (WideColumnSerialization::ForEachBlobFileNumber): tracks the
    /// oldest blob file referenced by the blob-backed columns of <paramref name="entity"/>.
    /// </summary>
    internal static void UpdateOldestBlobFileNumberForEntity(FileMetaData meta, ReadOnlySpan<byte> entity)
    {
        if (!Wide.WideColumnSerialization.HasBlobColumns(entity)) return;
        List<Wide.WideColumnSlot> slots = t_entitySlots ??= new List<Wide.WideColumnSlot>(8);
        Wide.WideColumnSerialization.GetBlobColumnIndexes(entity, slots);
        foreach (Wide.WideColumnSlot s in slots) UpdateOldestBlobFileNumber(meta, entity.Slice(s.ValueOffset, s.ValueLength));
    }

    /// <summary>
    /// The fragmented range tombstones of all memtables (null when there are none); with <paramref name="minTimestamps"/>
    /// read through NewTimestampStrippingRangeTombstoneIterator.
    /// </summary>
    private static FragmentedRangeTombstoneList? FragmentRangeTombstones(ColumnFamilyData cfd, IReadOnlyList<MemTable> mems, bool minTimestamps)
    {
        List<FragmentedRangeTombstoneList.Tombstone>? raw = null;
        int tsSz = cfd.TimestampSize;
        foreach (MemTable m in mems)
        {
            if (m.NumRangeDeletes == 0) continue;
            MemTableIterator it = m.NewRangeTombstoneEntryIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                byte[] start = it.UserKey.ToArray();
                byte[] end = it.Value.ToArray();
                if (minTimestamps)
                {
                    // NewTimestampStrippingRangeTombstoneIterator: start and end keys get the minimum timestamp.
                    if (start.Length < tsSz || end.Length < tsSz) ThrowHelper.Corruption("range tombstone key too short for its timestamp");
                    start.AsSpan(start.Length - tsSz).Clear();
                    end.AsSpan(end.Length - tsSz).Clear();
                }
                (raw ??= new()).Add(new FragmentedRangeTombstoneList.Tombstone(start, end, it.Sequence));
            }
        }
        if (raw is null) return null;
        FragmentedRangeTombstoneList frag = FragmentedRangeTombstoneList.Build(cfd.UserComparator, raw);
        return frag.IsEmpty ? null : frag;
    }

    /// <summary>
    /// Writes the fragmented range tombstones (every fragment with every covering sequence number, in internal key order)
    /// and extends the file boundaries like FileMetaData::UpdateBoundariesForRange.
    /// </summary>
    private static void AddRangeTombstones(BlockBasedTableBuilder builder, ColumnFamilyData cfd, FragmentedRangeTombstoneList frag, FileMetaData meta)
    {
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
