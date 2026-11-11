using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

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
    /// Test hook: when it returns true the flush behaves as if every entry had been dropped (BuildTable with an empty
    /// output: the file is abandoned and the flush installs no table).
    /// </summary>
    internal static Func<bool>? TestForceEmptyOutput;

    /// <summary>
    /// Builds table file #<paramref name="fileNumber"/> from <paramref name="mems"/> (any order). Returns null (and
    /// deletes the file) if the memtables contain nothing. With <paramref name="synchronous"/>, all I/O is blocking.
    /// With a <paramref name="blobBuilder"/> (enable_blob_files), plain values of at least min_blob_size bytes are written
    /// to blob files and replaced by blob indexes (BuildTable with a BlobFileBuilder); the builder is finished here and
    /// its <see cref="Blob.BlobFileBuilder.Additions"/> belong in the same version edit as the table file. On failure
    /// the caller deletes the builder's files. <paramref name="snapshots"/> are the live snapshot sequence numbers
    /// (ascending) when the flush job was created (FlushJob's existing_snapshots_). <paramref name="baseVersion"/> is the
    /// version the flush is based on (FlushJob's base_, which the caller keeps referenced; null for a recovery flush):
    /// with it the file's compensated_range_deletion_size is the approximate size of the data in that version its range
    /// tombstones cover. <paramref name="flushStatistics"/>: where the memtable payload and garbage bytes of the flush
    /// are recorded (TableFileCreationReason::kFlush only), or null.
    /// </summary>
    /// <remarks>
    /// <paramref name="snapshotChecker"/> is the DB's snapshot checker (WritePrepared / WriteUnprepared, or
    /// DisableGCSnapshotChecker for a sequence-per-batch DB being recovered). With a checker the memtables may hold
    /// uncommitted data, so no version is dropped as hidden (the managed flush does not evaluate the checker; keeping
    /// every version is always correct and leaves the garbage collection to compaction, which does).
    /// </remarks>
    public static async ValueTask<FileMetaData?> WriteLevel0TableAsync(IFileSystem fs, string path, WritableFileOptions fileOptions, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems,
        ulong fileNumber, ulong epochNumber, DbIdentity identity, ulong[] snapshots, SnapshotChecker? snapshotChecker, Blob.BlobFileBuilder? blobBuilder,
        Blob.BlobGarbageMeter? blobOutflow, SeqnoToTimeMapping? seqnoToTimeMapping, VersionStorageInfo? baseVersion,
        Statistics? flushStatistics, bool synchronous, CancellationToken cancellationToken)
    {
        bool stripTimestamps = cfd.TimestampsInMemtableOnly;
        ValidateFlushCompactionFilter(cfd);
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        // FlushJob::WriteLevel0Table: oldest_key_time = mems_.front()->ApproximateOldestKeyTime() (the oldest memtable,
        // whose first insert precedes every insert into the later ones: the minimum over all of them). It is not
        // always available (no insert), in which case the oldest ancestor time is the current time.
        ulong oldestKeyTime = ulong.MaxValue;
        foreach (MemTable m in mems) oldestKeyTime = Math.Min(oldestKeyTime, m.ApproximateOldestKeyTime);
        ulong oldestAncestorTime = Math.Min(now, oldestKeyTime);
        TableBuilderOptions tbo = TableFactory.CreateBuilderOptions(cfd.Options, TableFileLevel.Flush) with
        {
            ColumnFamilyId = cfd.Id,
            Statistics = cfd.TableCache.Statistics,
            ColumnFamilyName = cfd.Name,
            FileNumber = fileNumber,
            DbId = identity.DbId,
            DbSessionId = identity.SessionId,
            DbHostId = identity.HostId,
            // BuildTable -> SetSeqnoTimeTableProperties: rocksdb.creation.time is the oldest ancestor time (the file
            // creation time with FIFO compaction).
            CreationTime = cfd.Options.CompactionStyle == CompactionStyle.Fifo ? now : oldestAncestorTime,
            OldestKeyTime = oldestKeyTime,
            NewestKeyTime = now, // FlushJob::WriteLevel0Table: current_time /* newest_key_time */
            FileCreationTime = now,
        };

        var meta = new FileMetaData
        {
            FileNumber = fileNumber,
            EpochNumber = epochNumber,
            FileCreationTime = now,
            OldestAncestorTime = oldestAncestorTime,
            NewestKeyTime = now,
            // FlushJob::WriteLevel0Table: meta_.temperature = default_write_temperature.
            Temperature = cfd.Options.DefaultWriteTemperature,
            // FileMetaData::user_defined_timestamps_persisted: the MANIFEST then stores the boundaries without timestamps.
            UserDefinedTimestampsPersisted = !stripTimestamps,
            // BuildTable opens the new table with internal_stats->GetFileReadHist(0).
            LevelHint = 0,
        };
        IWritableFile file = FileChecksumHelper.CreateWritable(fs, path, fileOptions);
        bool keep = false;
        try
        {
            using (TableBuilder builder = TableBuilder.Create(file, cfd.Format, tbo))
            {
                ulong smallestPreferredSeqno = DbFormat.MaxSequenceNumber;
                // BuildTable: total_input_raw_key_bytes + total_input_raw_value_bytes of the point entries read, plus
                // the key and value bytes of the range tombstones (total_tombstone_payload_bytes).
                ulong inputPayloadBytes = RangeTombstonePayloadBytes(mems);
                FragmentedRangeTombstoneList? tombstones = FragmentRangeTombstones(cfd, mems, stripTimestamps);
                if (stripTimestamps)
                {
                    AddPointEntriesWithMinTimestamp(builder, cfd, mems, meta, blobBuilder, snapshots, tombstones, ref inputPayloadBytes);
                }
                else
                {
                    // Collapsing versions of a key compares user keys without their timestamp and needs full_history_ts_low;
                    // with persisted user-defined timestamps every version is kept (always correct).
                    smallestPreferredSeqno = AddPointEntries(builder, cfd.Icmp, mems, meta, snapshots,
                        dropHidden: cfd.TimestampSize == 0 && snapshotChecker is null, blobBuilder, blobOutflow, seqnoToTimeMapping, ref inputPayloadBytes);
                }
                blobBuilder?.Finish();
                // BuildTable writes CompactionRangeDelAggregator::NewIterator(): only the newest tombstone of each snapshot
                // stripe (every one with user-defined timestamps; and every one when a snapshot checker decides visibility,
                // like the point entries above).
                if (tombstones is not null)
                {
                    AddRangeTombstones(builder, cfd, tombstones, meta, cfd.TimestampSize == 0 && snapshotChecker is null ? snapshots : null, baseVersion);
                }
                if ((builder.IsEmpty && meta.Smallest.Length == 0) || TestForceEmptyOutput is { } force && force())
                {
                    builder.Abandon();
                }
                else
                {
                    // BuildTable -> SetSeqnoTimeTableProperties: the part of the DB's seqno-to-time mapping relevant to
                    // the file's seqno range (and the preferred seqnos of its TimedPut entries), at most
                    // kMaxSeqnoTimePairsPerSST pairs.
                    if (seqnoToTimeMapping is not null)
                    {
                        builder.Properties.SequenceNumberTimeMapping = RelevantSeqnoToTimeMapping(seqnoToTimeMapping,
                            Math.Min(meta.SmallestSeqno, smallestPreferredSeqno), meta.LargestSeqno, now);
                    }
                    if (synchronous) builder.Finish();
                    else await builder.FinishAsync(cancellationToken).ConfigureAwait(false);
                    if (synchronous) file.Sync();
                    else await file.SyncAsync(cancellationToken).ConfigureAwait(false);
                    meta.FileSize = builder.FileSize;
                    meta.TailSize = builder.FileSize - builder.Properties.TailStartOffset;
                    // BuildTable: file_writer->GetFileChecksum() / GetFileChecksumFuncName().
                    (meta.FileChecksum, meta.FileChecksumFuncName) = FileChecksumHelper.GetChecksum(file);
                    // ExtractTimestampFromTableProperties (db/builder.cc).
                    meta.MinTimestamp = builder.TimestampMin;
                    meta.MaxTimestamp = builder.TimestampMax;
                    // BuildTable: meta->marked_for_compaction = builder->NeedCompact() (table properties collectors).
                    meta.MarkedForCompaction = builder.NeedCompact;
                    // Entry stats as the version would load them from the table properties (no table read needed).
                    meta.SetEntryStats(builder.Properties);
                    if (flushStatistics is not null)
                    {
                        // BuildTable / FlushJob::WriteLevel0Table: MEMTABLE_PAYLOAD_BYTES_AT_FLUSH and
                        // MEMTABLE_GARBAGE_BYTES_AT_FLUSH (payload read minus payload written; "Prevent underflow").
                        ulong written = builder.Properties.RawKeySize + builder.Properties.RawValueSize;
                        bool consistent = written <= inputPayloadBytes;
                        flushStatistics.RecordTick(Ticker.MemtablePayloadBytesAtFlush, consistent ? inputPayloadBytes : 0);
                        flushStatistics.RecordTick(Ticker.MemtableGarbageBytesAtFlush, consistent ? inputPayloadBytes - written : 0);
                    }
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
    /// BuildTable (db/builder.cc): a filter the CF's compaction filter factory creates for the flush
    /// (ShouldFilterTableFileCreation(kFlush)) must ignore snapshots, else the flush fails with NotSupported. Flush-time
    /// filtering itself is not ported: such a filter is not applied (see README "Not ported yet").
    /// </summary>
    private static void ValidateFlushCompactionFilter(ColumnFamilyData cfd)
    {
        if (cfd.Options.CompactionFilterFactory is not { } factory || !factory.ShouldFilterTableFileCreation(isFlush: true)) return;
        CompactionFilter? filter = factory.CreateCompactionFilter(new CompactionFilterContext(false, false, cfd.Id, IsFlush: true));
        if (filter is not null && !filter.IgnoreSnapshots)
        {
            ThrowHelper.NotSupported(CompactionFilter.IgnoreSnapshotsFalseMessage);
        }
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
    /// <summary>
    /// The table property value of SetSeqnoTimeTableProperties: <paramref name="mapping"/> restricted to
    /// [<paramref name="fromSeqno"/>, <paramref name="toSeqno"/>], capped at kMaxSeqnoTimePairsPerSST pairs and
    /// enforced at <paramref name="fileCreationTime"/> (empty when nothing is relevant).
    /// </summary>
    internal static byte[] RelevantSeqnoToTimeMapping(SeqnoToTimeMapping mapping, ulong fromSeqno, ulong toSeqno, ulong fileCreationTime)
    {
        var relevant = new SeqnoToTimeMapping();
        relevant.CopyFromSeqnoRange(mapping, fromSeqno, toSeqno);
        relevant.SetCapacity(SeqnoToTimeMapping.MaxSeqnoTimePairsPerSst);
        relevant.Enforce(fileCreationTime);
        return relevant.Encode();
    }

    /// <summary>Returns the smallest preferred seqno assigned to a TimedPut entry (kMaxSequenceNumber if none).</summary>
    private static ulong AddPointEntries(TableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder, Blob.BlobGarbageMeter? blobOutflow, SeqnoToTimeMapping? seqnoToTimeMapping,
        ref ulong inputPayloadBytes)
    {
        Util.PooledBufferWriter? blobIndex = blobBuilder is null ? null : new Util.PooledBufferWriter(Blob.BlobIndex.MaxEncodedLength);
        Util.PooledBufferWriter? blobKey = blobBuilder is null ? null : new Util.PooledBufferWriter(64);
        Blob.EntityBlobRewriter? entities = blobBuilder is null ? null : new Blob.EntityBlobRewriter();
        var timed = new TimedPutRewriter(seqnoToTimeMapping);
        try
        {
            AddPointEntries(builder, icmp, mems, meta, snapshots, dropHidden, blobBuilder, blobIndex, blobKey, entities, blobOutflow, ref timed, ref inputPayloadBytes);
            return timed.SmallestPreferredSeqno;
        }
        finally
        {
            blobIndex?.Dispose();
            blobKey?.Dispose();
            entities?.Dispose();
            timed.Dispose();
        }
    }

    /// <summary>
    /// The TimedPut conversion of BuildTable (db/builder.cc): a kTypeValuePreferredSeqno entry carries its write time
    /// in the memtable; the table gets the preferred seqno estimated from the seqno-to-time mapping
    /// (GetProximalSeqnoBeforeTime) when it is older than the entry's seqno, else a plain kTypeValue.
    /// </summary>
    private struct TimedPutRewriter(SeqnoToTimeMapping? mapping) : IDisposable
    {
        public ulong SmallestPreferredSeqno = DbFormat.MaxSequenceNumber;
        private Util.PooledBufferWriter? _key;
        private Util.PooledBufferWriter? _value;

        public void Add(TableBuilder builder, ReadOnlySpan<byte> key, ulong seq, ReadOnlySpan<byte> packedValue)
        {
            _key ??= new Util.PooledBufferWriter(64);
            _value ??= new Util.PooledBufferWriter(64);
            ReadOnlySpan<byte> unpacked = packedValue[..^8];
            ulong writeTime = Util.Coding.DecodeFixed64(packedValue[^8..]);
            ulong preferred = mapping is not null ? mapping.GetProximalSeqnoBeforeTime(writeTime) : DbFormat.MaxSequenceNumber;
            _value.Clear();
            _value.Write(unpacked);
            if (preferred < seq)
            {
                // PackValueAndSeqno
                Span<byte> tail = _value.GetSpan(8);
                Util.Coding.EncodeFixed64(tail, preferred);
                _value.Advance(8);
                SmallestPreferredSeqno = Math.Min(SmallestPreferredSeqno, preferred);
                builder.Add(key, _value.WrittenSpan);
            }
            else
            {
                // Cannot get a useful preferred seqno, convert it to a kTypeValue.
                _key.Clear();
                _key.Write(key);
                Util.Coding.EncodeFixed64(_key.WrittenSpanMutable[^DbFormat.InternalKeyFooterSize..], (seq << 8) | (byte)EntryType.Value);
                builder.Add(_key.WrittenSpan, _value.WrittenSpan);
            }
        }

        public void Dispose()
        {
            _key?.Dispose();
            _value?.Dispose();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void AddPointEntries(TableBuilder builder, InternalKeyComparator icmp, IReadOnlyList<MemTable> mems, FileMetaData meta,
        ulong[] snapshots, bool dropHidden, Blob.BlobFileBuilder? blobBuilder, Util.PooledBufferWriter? blobIndex, Util.PooledBufferWriter? blobKey,
        Blob.EntityBlobRewriter? entities, Blob.BlobGarbageMeter? blobOutflow, ref TimedPutRewriter timed, ref ulong inputPayloadBytes)
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
            inputPayloadBytes += (ulong)(key.Length + its[best].Value.Length);
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
            else if (type == EntryType.ValuePreferredSeqno)
            {
                timed.Add(builder, key, seq, its[best].Value);
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
    /// Flush with timestamps in memtable only: the k-way merge of <see cref="AddPointEntries(TableBuilder, InternalKeyComparator, IReadOnlyList{MemTable}, FileMetaData, Blob.BlobFileBuilder?)"/>
    /// over upstream's TimestampStrippingIterator (each key's timestamp replaced by the minimum timestamp,
    /// ReplaceInternalKeyWithMinTimestamp) followed by the hidden-version rule of the flush-time CompactionIterator.
    /// <paramref name="snapshots"/> are the live snapshot sequence numbers, ascending. Cold path (allocates per flush).
    /// </summary>
    private static void AddPointEntriesWithMinTimestamp(TableBuilder builder, ColumnFamilyData cfd, IReadOnlyList<MemTable> mems,
        FileMetaData meta, Blob.BlobFileBuilder? blobBuilder, ulong[] snapshots, FragmentedRangeTombstoneList? tombstones, ref ulong inputPayloadBytes)
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
            inputPayloadBytes += (ulong)(original.Length + value.Length);
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
    private static void AddEntity(TableBuilder builder, FileMetaData meta, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity,
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
    /// <summary>
    /// FragmentedRangeTombstoneList::total_tombstone_payload_bytes summed over the memtables: internal key plus value
    /// (end key) bytes of every range tombstone entry.
    /// </summary>
    private static ulong RangeTombstonePayloadBytes(IReadOnlyList<MemTable> mems)
    {
        ulong bytes = 0;
        foreach (MemTable m in mems)
        {
            if (m.NumRangeDeletes == 0) continue;
            MemTableIterator it = m.NewRangeTombstoneEntryIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) bytes += (ulong)(it.UserKey.Length + DbFormat.InternalKeyFooterSize + it.Value.Length);
        }
        return bytes;
    }

    private static FragmentedRangeTombstoneList? FragmentRangeTombstones(ColumnFamilyData cfd, IReadOnlyList<MemTable> mems, bool minTimestamps)
    {
        int withRangeDels = 0;
        MemTable? single = null;
        foreach (MemTable m in mems)
        {
            if (m.NumRangeDeletes == 0) continue;
            withRangeDels++;
            single = m;
        }
        if (withRangeDels == 0) return null;
        if (withRangeDels == 1 && !minTimestamps)
        {
            // One memtable: its cached fragmented list (MemTable::NewRangeTombstoneIterator of an immutable memtable) is
            // what fragmenting its tombstones again would build.
            FragmentedRangeTombstoneList cached = single!.GetFragmentedRangeTombstones();
            return cached.IsEmpty ? null : cached;
        }
        int tsSz = cfd.TimestampSize;
        // NewTimestampStrippingRangeTombstoneIterator: start and end keys get the minimum timestamp (all zero bytes); the
        // fragmenter takes the key parts and this timestamp separately, so nothing is copied per tombstone.
        Span<byte> minTs = minTimestamps ? stackalloc byte[tsSz] : default;
        minTs.Clear();
        FragmentedRangeTombstoneList frag;
        RangeTombstoneFragmenter fragmenter = RangeTombstoneFragmenter.Rent(cfd.UserComparator, endIncludesTimestamp: true);
        try
        {
            foreach (MemTable m in mems)
            {
                if (m.NumRangeDeletes == 0) continue;
                MemTableIterator it = m.NewRangeTombstoneEntryIterator();
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    ReadOnlySpan<byte> start = it.UserKey, end = it.Value;
                    if (minTimestamps)
                    {
                        if (start.Length < tsSz || end.Length < tsSz) ThrowHelper.Corruption("range tombstone key too short for its timestamp");
                        fragmenter.Add(start, end, it.Sequence, minTs);
                    }
                    else
                    {
                        fragmenter.Add(start, end, it.Sequence);
                    }
                }
            }
            frag = fragmenter.Finish();
        }
        finally
        {
            RangeTombstoneFragmenter.Return(fragmenter);
        }
        return frag.IsEmpty ? null : frag;
    }

    /// <summary>
    /// Writes the fragmented range tombstones (every fragment with its covering sequence numbers, in internal key order)
    /// and extends the file boundaries like FileMetaData::UpdateBoundariesForRange. With <paramref name="stripeSnapshots"/>
    /// only the newest sequence number of each snapshot stripe is written (CompactionRangeDelAggregator::NewIterator
    /// fragments for_compaction with the job's snapshots); null writes all of them.
    /// </summary>
    private static void AddRangeTombstones(TableBuilder builder, ColumnFamilyData cfd, FragmentedRangeTombstoneList frag, FileMetaData meta,
        ulong[]? stripeSnapshots, VersionStorageInfo? baseVersion)
    {
        InternalKeyComparator icmp = cfd.Icmp;
        int tsSize = frag.TimestampSize;
        // The tombstone keys are encoded into scratch buffers; the file boundaries are tracked there too and copied into
        // meta once at the end (only when a tombstone extends them).
        PooledBufferWriter startKey = ObjectPools.RentWriter();
        var endKey = new PooledBufferWriter(64);
        var tEndBuf = new PooledBufferWriter(tsSize != 0 ? 64 : 0);
        var smallest = new PooledBufferWriter(64);
        var largest = new PooledBufferWriter(64);
        try
        {
            smallest.Write(meta.Smallest);
            largest.Write(meta.Largest);
            bool smallestChanged = false, largestChanged = false;
            for (int i = 0; i < frag.Count; i++)
            {
                ReadOnlySpan<byte> start = frag.GetStart(i);
                ReadOnlySpan<byte> end = frag.GetEnd(i);
                ReadOnlySpan<ulong> seqs = frag.GetSequences(i);
                // SerializeEndKey: the end key at kMaxSequenceNumber (with the maximum timestamp), the same for every seq.
                endKey.Clear();
                AppendInternalKey(endKey, end, default, DbFormat.MaxSequenceNumber);
                // UpdateBoundariesForRange(start = tombstone.SerializeKey(), end = tombstone.SerializeEndKey()).
                if (largest.WrittenCount == 0 || icmp.Compare(largest.WrittenSpan, endKey.WrittenSpan) < 0)
                {
                    largest.Clear();
                    largest.Write(endKey.WrittenSpan);
                    largestChanged = true;
                }
                ulong prevStripe = 0;
                bool compensated = false;
                for (int k = 0; k < seqs.Length; k++)
                {
                    ulong seq = seqs[k];
                    if (stripeSnapshots is not null)
                    {
                        // Sequence numbers are descending: an older one in the same stripe is hidden by the one kept.
                        ulong stripe = EarliestVisibleSnapshot(stripeSnapshots, seq);
                        if (k > 0 && stripe == prevStripe) continue;
                        prevStripe = stripe;
                    }
                    ReadOnlySpan<byte> tEnd = end;
                    startKey.Clear();
                    if (tsSize != 0)
                    {
                        // RangeTombstone(start, end, seq, ts): both keys carry the tombstone's timestamp; the end key used
                        // for the file boundary keeps the maximum timestamp (SerializeEndKey).
                        ReadOnlySpan<byte> ts = frag.GetTimestamp(i, k);
                        AppendInternalKey(startKey, start[..^tsSize], ts, seq);
                        tEndBuf.Clear();
                        tEndBuf.Write(end[..^tsSize]);
                        tEndBuf.Write(ts);
                        tEnd = tEndBuf.WrittenSpan;
                    }
                    else
                    {
                        AppendInternalKey(startKey, start, default, seq);
                    }
                    builder.Add(startKey.WrittenSpan, tEnd);
                    // BuildTable: the first tombstone of each start user key (the newest of the fragment) compensates the
                    // file by the approximate size of the data it covers in the base version (all levels, error margin
                    // 0.1). Upstream's "last start key is empty" test also re-counts a fragment starting at the empty key.
                    if (baseVersion is not null && (!compensated || start.Length == 0))
                    {
                        compensated = true;
                        meta.CompensatedRangeDeletionSize += DbImpl.ApproximateSize(cfd, baseVersion, startKey.WrittenSpan, endKey.WrittenSpan, 0, 0.1);
                    }
                    if (smallest.WrittenCount == 0 || icmp.Compare(startKey.WrittenSpan, smallest.WrittenSpan) < 0)
                    {
                        smallest.Clear();
                        smallest.Write(startKey.WrittenSpan);
                        smallestChanged = true;
                    }
                    if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
                    if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
                }
            }
            if (smallestChanged) meta.Smallest = smallest.WrittenSpan.ToArray();
            if (largestChanged) meta.Largest = largest.WrittenSpan.ToArray();
        }
        finally
        {
            ObjectPools.ReturnWriter(startKey);
            endKey.Dispose();
            tEndBuf.Dispose();
            smallest.Dispose();
            largest.Dispose();
        }
    }

    /// <summary>InternalKey(userKey | timestamp, seq, kTypeRangeDeletion) appended to <paramref name="dst"/>.</summary>
    internal static void AppendInternalKey(PooledBufferWriter dst, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> timestamp, ulong seq)
    {
        int n = userKey.Length + timestamp.Length + DbFormat.InternalKeyFooterSize;
        dst.Advance(DbFormat.AppendInternalKey(dst.GetSpan(n), userKey, timestamp, seq, EntryType.RangeDeletion));
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
