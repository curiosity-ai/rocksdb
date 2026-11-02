using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>Result statistics of one compaction (subset of CompactionJobStats / InternalStats::CompactionStats).</summary>
internal sealed class CompactionJobStats
{
    public int NumInputFiles;
    public int NumOutputFiles;
    public ulong BytesRead;
    public ulong BytesWritten;
    public long NumInputRecords;
    public long NumOutputRecords;
    public long NumOutputRangeDeletions;
    public CompactionIterationStats IterStats;
    public TimeSpan Elapsed;
}

/// <summary>
/// Runs a picked compaction: merges the input files through a <see cref="CompactionIterator"/>, writes the outputs
/// (split at the target file size and at grandparent boundaries, with range tombstones assigned to the overlapping
/// outputs and file boundaries extended for them) and returns the outputs' <see cref="FileMetaData"/>. Port of
/// CompactionJob::ProcessKeyValueCompaction / FinishCompactionOutputFile (db/compaction/compaction_job.cc),
/// CompactionOutputs::ShouldStopBefore / AddRangeDels / UpdateGrandparentBoundaryInfo
/// (db/compaction/compaction_outputs.cc). One subcompaction (no parallel key-range split).
/// <para>
/// I/O: input blocks are read synchronously through the table cache; outputs are written through
/// <see cref="BlockBasedTableBuilder"/> (staged output is appended synchronously; finish and fsync are awaited unless
/// <c>synchronous</c>). Per key the loop is allocation-free; the output files' key copies use pooled buffers.
/// </para>
/// </summary>
internal sealed class CompactionJob
{
    private readonly DbImpl _db;
    private readonly ColumnFamilyData _cfd;
    private readonly Compaction _c;
    private readonly ulong[] _snapshots;
    private readonly ulong _earliestSnapshot;
    private readonly Func<ulong> _newFileNumber;
    private readonly CancellationToken _cancellation;
    private readonly InternalKeyComparator _icmp;
    private readonly NoFillTableCache _noFillCache;

    // Range tombstones to write (fragmented over all inputs, truncated to input file boundaries at user-key level).
    private FragmentedRangeTombstoneList? _outputTombstones;
    private byte[]? _fullHistoryTsLow;
    private readonly int[] _rangeDelLevelPtrs;

    // Current output.
    private IWritableFile? _file;
    private BlockBasedTableBuilder? _builder;
    private FileMetaData? _meta;
    private string? _path;
    private readonly PooledBufferWriter _largestKey = new(64);
    private bool _hasPointKey;

    // Grandparent tracking (CompactionOutputs).
    private int _grandparentIndex;
    private bool _beingGrandparentGap = true;
    private ulong _grandparentOverlappedBytes;
    private int _grandparentBoundarySwitchedNum;
    private bool _seenKey;

    private bool _firstOutput = true;

    public CompactionJob(DbImpl db, ColumnFamilyData cfd, Compaction compaction, ulong[] snapshots, Func<ulong> newFileNumber,
        CancellationToken cancellation)
    {
        _db = db;
        _cfd = cfd;
        _c = compaction;
        _snapshots = snapshots;
        _earliestSnapshot = snapshots.Length == 0 ? DbFormat.MaxSequenceNumber : snapshots[0];
        _newFileNumber = newFileNumber;
        _cancellation = cancellation;
        _icmp = cfd.Icmp;
        _rangeDelLevelPtrs = new int[compaction.NumberLevels];
        _noFillCache = new NoFillTableCache(cfd.TableCache);
    }

    /// <summary>
    /// Table cache view whose iterators don't insert data blocks into the block cache (compaction input reads use
    /// fill_cache = false, like CompactionJob's ReadOptions): avoids polluting the cache and lets the iterator reuse
    /// its block buffers.
    /// </summary>
    private sealed class NoFillTableCache(TableCache inner) : ITableCache
    {
        public ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default) =>
            inner.GetReaderAsync(file, cancellationToken);

        public TableReaderLease GetReader(FileMetaData file) => inner.GetReader(file);

        public InternalIterator NewIterator(FileMetaData file) => new TableInternalIterator(inner.GetReader(file), TableReadFlags.DontFillCache);

        public InternalIterator NewIterator(FileMetaData file, InternalIterator? reuse) =>
            TableCache.Recycle(reuse, inner.GetReader(file), TableReadFlags.DontFillCache);

        public void Evict(ulong fileNumber) => inner.Evict(fileNumber);
    }

    public List<FileMetaData> Outputs { get; } = new();
    public CompactionJobStats Stats { get; } = new();

    /// <summary>File numbers of outputs created so far (also failed ones, for cleanup).</summary>
    public List<ulong> CreatedFileNumbers { get; } = new();

    // Blob files (integrated BlobDB): output builder, GC/extraction and the garbage meter over the input.
    private Blob.CompactionBlobProcessor? _blob;
    private Blob.BlobGarbageMeter? _blobGarbageMeter;

    /// <summary>The blob file builder of this job (null when the output level gets no blob files). Released by the DB.</summary>
    public Blob.BlobFileBuilder? BlobBuilder { get; private set; }

    /// <summary>Blob files written by the compaction (for the version edit).</summary>
    public List<BlobFileAddition> BlobFileAdditions { get; } = new();

    /// <summary>Additional garbage in preexisting blob files (for the version edit).</summary>
    public List<BlobFileGarbage> BlobFileGarbages { get; } = new();

    // ---------------------------------------------------------------------------------------------------------
    // input

    /// <summary>Builds the merged input iterator and the compaction range-del aggregator (MakeInputIterator).</summary>
    private InternalIterator MakeInputIterator(RangeDelAggregator rangeDel, List<FragmentedRangeTombstoneList.Tombstone> rawTombstones)
    {
        var children = new List<InternalIterator>();
        TableCache tc = _cfd.TableCache;
        try
        {
            for (int which = 0; which < _c.NumInputLevels; which++)
            {
                CompactionInputFiles input = _c.Inputs(which);
                if (input.IsEmpty) continue;
                if (input.Level == 0)
                {
                    foreach (FileMetaData f in input.Files) children.Add(_noFillCache.NewIterator(f));
                }
                else
                {
                    children.Add(new LevelIterator(_noFillCache, _icmp, input.Files, null));
                }
                foreach (FileMetaData f in input.Files)
                {
                    FragmentedRangeTombstoneList? list = tc.GetRangeTombstones(f);
                    if (list is null) continue;
                    rangeDel.AddTombstones(list, f.Smallest, f.Largest);
                    CollectTruncated(list, f, rawTombstones);
                }
            }
        }
        catch
        {
            foreach (InternalIterator it in children) it.Dispose();
            throw;
        }
        return children.Count == 1 ? children[0] : new MergingIterator(_icmp, children.ToArray());
    }

    /// <summary>Adds the tombstones of an input file clipped to the file's user-key range.</summary>
    private void CollectTruncated(FragmentedRangeTombstoneList list, FileMetaData f, List<FragmentedRangeTombstoneList.Tombstone> into)
    {
        ReadOnlySpan<byte> lo = DbFormat.ExtractUserKey(f.Smallest);
        ReadOnlySpan<byte> hi = DbFormat.ExtractUserKey(f.Largest);
        for (int i = 0; i < list.Count; i++)
        {
            ReadOnlySpan<byte> s = list.GetStart(i), e = list.GetEnd(i);
            if (f.Smallest.Length > 0 && _icmp.CompareUserKey(s, lo) < 0) s = lo;
            if (f.Largest.Length > 0 && _icmp.CompareUserKey(e, hi) > 0) e = hi;
            if (_icmp.CompareUserKey(s, e) >= 0) continue;
            byte[] sa = s.ToArray(), ea = e.ToArray();
            ReadOnlySpan<ulong> seqs = list.GetSequences(i);
            for (int k = 0; k < seqs.Length; k++)
            {
                // With timestamps the clipped keys may carry another timestamp: pass the tombstone's explicitly.
                byte[]? ts = list.TimestampSize == 0 ? null : list.GetTimestamp(i, k).ToArray();
                into.Add(new FragmentedRangeTombstoneList.Tombstone(sa, ea, seqs[k], ts));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // run

    /// <summary>
    /// Runs the compaction. With <paramref name="synchronous"/> all I/O is blocking and the returned task is complete.
    /// On failure the created output files are deleted by the caller (<see cref="CreatedFileNumbers"/>).
    /// </summary>
    public async ValueTask RunAsync(bool synchronous)
    {
        long start = System.Diagnostics.Stopwatch.GetTimestamp();
        Stats.NumInputFiles = _c.TotalInputFiles;
        Stats.BytesRead = _c.TotalInputBytes;
        // full_history_ts_low of the column family at the time the job runs (CompactionJob: cfd->GetFullHistoryTsLow()).
        byte[]? fullHistoryTsLow = _cfd.TimestampSize != 0 ? _cfd.FullHistoryTsLow : null;
        _fullHistoryTsLow = fullHistoryTsLow;
        // With user-defined timestamps only tombstones at or below full_history_ts_low delete keys.
        RangeDelAggregator rangeDel = RangeDelAggregator.ForCompaction(_icmp, _snapshots, fullHistoryTsLow);
        var raw = new List<FragmentedRangeTombstoneList.Tombstone>();
        InternalIterator input = MakeInputIterator(rangeDel, raw);
        // CompactionJob::ProcessKeyValueCompaction: a BlobCountingIterator measures the blob inflow when the input
        // references blob files (DoesInputReferenceBlobFiles).
        if (_c.DoesInputReferenceBlobFiles())
        {
            _blobGarbageMeter = new Blob.BlobGarbageMeter();
            input = new Blob.BlobCountingIterator(input, _blobGarbageMeter);
        }
        BlobBuilder = _db.CreateBlobFileBuilder(_cfd, _c.OutputLevel, Blob.BlobFileCreationReason.Compaction);
        bool enableGc = _c.EnableBlobGarbageCollection;
        if (BlobBuilder is not null || enableGc || _c.InputStorage.BlobFiles.Length > 0)
        {
            _blob = new Blob.CompactionBlobProcessor(BlobBuilder, _cfd.BlobSource, _c.InputStorage, enableGc, _c.BlobGarbageCollectionAgeCutoff);
        }
        if (raw.Count > 0)
        {
            FragmentedRangeTombstoneList frag = FragmentedRangeTombstoneList.Build(_cfd.UserComparator, raw);
            _outputTombstones = frag.IsEmpty ? null : frag;
        }
        CompactionFilter? filter = _c.CreateCompactionFilter(_cfd.Id);
        MergeOperator? mergeOperator = _cfd.MergeOperator;
        ulong latestSnapshot = _snapshots.Length == 0 ? 0 : _snapshots[^1];
        MergeHelper? merge = mergeOperator is null ? null : new MergeHelper(_icmp, mergeOperator, filter, latestSnapshot, _c.StartLevel);
        ulong earliestWriteConflict = DbFormat.MaxSequenceNumber;
        if (merge is not null) merge.FullHistoryTsLow = fullHistoryTsLow;
        var iter = new CompactionIterator(input, _icmp, merge, _snapshots, earliestWriteConflict, rangeDel, _c, filter,
            _db.Options.EnforceSingleDelContracts, _cancellation)
        {
            FullHistoryTsLow = fullHistoryTsLow,
            BlobProcessor = _blob,
        };
        if (merge is not null) merge.BlobProcessor = _blob;
        try
        {
            input.SeekToFirst();
            for (iter.SeekToFirst(); iter.Valid; iter.Next())
            {
                // (The iterator is not moved across the await, so iter.Key stays valid.)
                if (ShouldStopBefore(iter.Key) && _builder is not null)
                {
                    await FinishOutputAsync(iter.Key, synchronous).ConfigureAwait(false);
                    // reset grandparent information
                    _grandparentBoundarySwitchedNum = 0;
                    _grandparentOverlappedBytes = GetCurrentKeyGrandparentOverlappedBytes(iter.Key);
                }
                AddToOutput(iter);
            }
            Stats.IterStats = iter.Stats;
            Stats.NumInputRecords = iter.Stats.NumInputRecords;
            if (_builder is null && _outputTombstones is not null && !(_c.BottommostLevel && AllTombstonesDroppable()))
            {
                // Only range tombstones: they still need an output file.
                OpenOutput();
            }
            if (_builder is not null) await FinishOutputAsync(ReadOnlySpan<byte>.Empty, synchronous).ConfigureAwait(false);
            if (BlobBuilder is not null)
            {
                BlobBuilder.Finish();
                BlobFileAdditions.AddRange(BlobBuilder.Additions);
                // bytes_written_blob is part of the compaction's write volume (write amplification).
                foreach (BlobFileAddition a in BlobBuilder.Additions)
                {
                    Stats.BytesWritten += a.TotalBlobBytes + Blob.BlobLogFormat.HeaderSize + Blob.BlobLogFormat.FooterSize;
                }
            }
            _blobGarbageMeter?.AddGarbageTo(BlobFileGarbages);
        }
        finally
        {
            iter.Dispose();
            merge?.Dispose();
            _blob?.Dispose();
            BlobBuilder?.Abandon();
            input.Dispose();
            _largestKey.Dispose();
            if (_builder is not null)
            {
                _builder.Abandon();
                _builder.Dispose();
                _builder = null;
            }
            if (_file is not null)
            {
                _file.Dispose();
                _file = null;
            }
            Stats.Elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(start);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void AddToOutput(CompactionIterator iter)
    {
        if (_builder is null) OpenOutput();
        ReadOnlySpan<byte> key = iter.Key;
        ReadOnlySpan<byte> value = iter.Value;
        _builder!.Add(key, value);
        Stats.NumOutputRecords++;
        UpdateBoundaries(key, iter.Sequence);
        if (iter.Type == EntryType.BlobIndex)
        {
            FlushJob.UpdateOldestBlobFileNumber(_meta!, value);
            _blobGarbageMeter?.ProcessOutFlow(key, value);
        }
    }

    private bool AllTombstonesDroppable()
    {
        FragmentedRangeTombstoneList t = _outputTombstones!;
        for (int i = 0; i < t.Count; i++)
        {
            ReadOnlySpan<ulong> seqs = t.GetSequences(i);
            for (int k = 0; k < seqs.Length; k++)
            {
                if (!TombstoneConsiderDrop(t, i, k)) return false;
            }
        }
        return true;
    }

    /// <summary>
    /// consider_drop of AddRangeDels: the tombstone is visible to no snapshot and, with user-defined timestamps, its
    /// timestamp is below full_history_ts_low.
    /// </summary>
    private bool TombstoneConsiderDrop(FragmentedRangeTombstoneList t, int i, int k)
    {
        if (t.GetSequences(i)[k] > _earliestSnapshot) return false;
        if (t.TimestampSize == 0) return true;
        return _fullHistoryTsLow is not null && _cfd.UserComparator.CompareTimestamp(t.GetTimestamp(i, k), _fullHistoryTsLow) < 0;
    }

    private void OpenOutput()
    {
        ulong fileNumber = _newFileNumber();
        CreatedFileNumbers.Add(fileNumber);
        _path = FileNames.TableFileName(_db.DbPaths, fileNumber, _c.OutputPathId);
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        ulong oldestAncestor = _c.MinInputFileOldestAncestorTime();
        if (oldestAncestor == ulong.MaxValue) oldestAncestor = now;
        VersionStorageInfo vs = _c.InputStorage;
        bool bottommostCompression = _c.OutputLevel >= vs.NumNonEmptyLevels - 1;
        TableBuilderOptions tbo = TableFactory.CreateBuilderOptions(_cfd.Options,
            new TableFileLevel(_c.OutputLevel, Math.Max(1, vs.BaseLevel), IsFlush: false, IsBottommost: bottommostCompression)) with
        {
            ColumnFamilyId = _cfd.Id,
            ColumnFamilyName = _cfd.Name,
            FileNumber = fileNumber,
            DbId = _db.Identity.DbId,
            DbSessionId = _db.Identity.SessionId,
            DbHostId = _db.Identity.HostId,
            CreationTime = oldestAncestor,
            TargetFileSize = _c.MaxOutputFileSize,
            FileCreationTime = now,
        };
        _meta = new FileMetaData
        {
            FileNumber = fileNumber,
            PathId = _c.OutputPathId,
            EpochNumber = _c.MinInputFileEpochNumber(),
            OldestAncestorTime = oldestAncestor,
            FileCreationTime = now,
        };
        var fileOptions = new WritableFileOptions
        {
            BytesPerSync = (long)Math.Min(_db.Options.BytesPerSync, long.MaxValue),
            MaxBufferSize = (int)Math.Clamp(_db.Options.WritableFileMaxBufferSize, 64 * 1024, int.MaxValue),
        };
        _file = _db.FileSystem.CreateWritable(_path, truncate: true, fileOptions);
        _builder = new BlockBasedTableBuilder(_file, _cfd.Format, tbo);
        _hasPointKey = false;
        _largestKey.Clear();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void UpdateBoundaries(ReadOnlySpan<byte> key, ulong seq)
    {
        FileMetaData meta = _meta!;
        if (!_hasPointKey)
        {
            if (meta.Smallest.Length == 0 || _icmp.Compare(key, meta.Smallest) < 0) meta.Smallest = key.ToArray();
            _hasPointKey = true;
        }
        _largestKey.Clear();
        _largestKey.Write(key);
        if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
        if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
    }

    /// <summary>
    /// Port of FinishCompactionOutputFile: adds the range tombstones overlapping this output, finishes and syncs the
    /// file and records its metadata. <paramref name="nextTableMinKey"/> is the first key of the next output (empty
    /// for the last output).
    /// </summary>
    private ValueTask FinishOutputAsync(ReadOnlySpan<byte> nextTableMinKey, bool synchronous)
    {
        // Copy the (once per output file) cut key so it can cross the await.
        byte[] copy = nextTableMinKey.ToArray();
        return FinishOutputCoreAsync(copy, synchronous);
    }

    private async ValueTask FinishOutputCoreAsync(ReadOnlyMemory<byte> nextTableMinKey, bool synchronous)
    {
        FileMetaData meta = _meta!;
        if (_hasPointKey) meta.Largest = _largestKey.WrittenSpan.ToArray();
        if (_outputTombstones is not null) AddRangeDels(nextTableMinKey.Span);
        BlockBasedTableBuilder builder = _builder!;
        IWritableFile file = _file!;
        _builder = null;
        _file = null;
        bool keep = false;
        try
        {
            if (builder.IsEmpty && meta.Smallest.Length == 0)
            {
                builder.Abandon();
            }
            else
            {
                if (synchronous) builder.Finish();
                else await builder.FinishAsync(_cancellation).ConfigureAwait(false);
                if (synchronous) file.Sync();
                else await file.SyncAsync(_cancellation).ConfigureAwait(false);
                meta.FileSize = builder.FileSize;
                meta.TailSize = builder.FileSize - builder.Properties.TailStartOffset;
                // ExtractTimestampFromTableProperties (db/builder.cc).
                meta.MinTimestamp = builder.TimestampMin;
                meta.MaxTimestamp = builder.TimestampMax;
                Stats.NumOutputRangeDeletions += (long)builder.Properties.NumRangeDeletions;
                keep = true;
            }
        }
        finally
        {
            builder.Dispose();
            if (synchronous) file.Dispose();
            else await file.DisposeAsync().ConfigureAwait(false);
            if (!keep)
            {
                try { _db.FileSystem.DeleteFile(_path!); }
                catch (IOException) { }
                catch (RocksDbException) { }
            }
        }
        _firstOutput = false;
        if (!keep) return;
        DbIdentity id = _db.Identity;
        if (id.DbId.Length > 0 && id.SessionId.Length > 0)
        {
            OffsetableCacheKey.GetSstInternalUniqueId(id.DbId, id.SessionId, meta.FileNumber, out ulong id0, out ulong id1);
            meta.UniqueId = new UniqueId64x2(id0, id1);
        }
        Outputs.Add(meta);
        Stats.NumOutputFiles++;
        Stats.BytesWritten += meta.FileSize;
    }

    // ---------------------------------------------------------------------------------------------------------
    // range tombstones (CompactionOutputs::AddRangeDels)

    private static byte[] MakeInternalKey(ReadOnlySpan<byte> userKey, ulong seq, EntryType type)
    {
        var k = new byte[userKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    private void AddRangeDels(ReadOnlySpan<byte> nextTableMinKey)
    {
        FragmentedRangeTombstoneList tombstones = _outputTombstones!;
        FileMetaData meta = _meta!;
        BlockBasedTableBuilder builder = _builder!;
        bool bottommost = _c.BottommostLevel;

        // Lower bound: none for the first output; otherwise the previous output's upper bound or this file's smallest.
        byte[]? lowerBound = _firstOutput || meta.Smallest.Length == 0 ? null : meta.Smallest;
        byte[]? upperBound;
        if (nextTableMinKey.Length == 0)
        {
            upperBound = null; // last file of the (single) subcompaction
        }
        else
        {
            // There is another file coming whose coverage will begin at next_table_min_key. The current file needs to
            // extend range tombstone coverage through its own keys and through user keys preceding next_table_min_key's.
            ReadOnlySpan<byte> nextUser = DbFormat.ExtractUserKey(nextTableMinKey);
            if (meta.Largest.Length > 0 && _cfd.UserComparator.EqualWithoutTimestamp(DbFormat.ExtractUserKey(meta.Largest), nextUser)) upperBound = meta.Largest;
            else
            {
                // SetMaxSeqAndTs: next user key with the maximum timestamp, kMaxSequenceNumber, kTypeRangeDeletion.
                upperBound = MakeInternalKey(nextUser, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                int ts = _cfd.TimestampSize;
                if (ts != 0) upperBound.AsSpan(nextUser.Length - ts, ts).Fill(0xff);
            }
        }
        if (lowerBound is not null && upperBound is not null && _icmp.Compare(lowerBound, upperBound) > 0) return;
        int tsSize = tombstones.TimestampSize;

        for (int i = 0; i < tombstones.Count; i++)
        {
            ReadOnlySpan<byte> fStart = tombstones.GetStart(i);
            ReadOnlySpan<byte> fEnd = tombstones.GetEnd(i);
            // tombstone_end = end_user_key@kMaxSequenceNumber,kTypeRangeDeletion
            if (lowerBound is not null && _icmp.Compare(lowerBound, fEnd, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion) >= 0) continue;
            ulong prevKeptStripe = ulong.MaxValue;
            bool stop = false;
            ReadOnlySpan<ulong> seqs = tombstones.GetSequences(i);
            for (int k = 0; k < seqs.Length; k++)
            {
                ulong seq = seqs[k];
                // Only the newest tombstone of each snapshot stripe matters (CompactionRangeDelAggregator). With
                // user-defined timestamps every tombstone is kept (FragmentTombstones drops none for compaction).
                if (tsSize == 0)
                {
                    ulong stripe = StripeOf(seq);
                    if (stripe == prevKeptStripe) continue;
                    prevKeptStripe = stripe;
                }
                ReadOnlySpan<byte> ts = tsSize == 0 ? default : tombstones.GetTimestamp(i, k);

                bool considerDrop = TombstoneConsiderDrop(tombstones, i, k);
                if (considerDrop && bottommost)
                {
                    Stats.IterStats.NumRangeDelDropObsolete++;
                    continue;
                }
                // tombstone_start (user key raised to the lower bound's) / truncated to [lower, upper]. With timestamps
                // the start key carries the tombstone's timestamp, also when raised (the timestamp goes with the seqno).
                ReadOnlySpan<byte> startUser = tsSize == 0 ? fStart : FlushJob.ReplaceTimestamp(fStart, ts);
                if (lowerBound is not null && _cfd.UserComparator.CompareWithoutTimestamp(startUser, DbFormat.ExtractUserKey(lowerBound)) < 0)
                {
                    startUser = tsSize == 0 ? DbFormat.ExtractUserKey(lowerBound) : FlushJob.ReplaceTimestamp(DbFormat.ExtractUserKey(lowerBound), ts);
                }
                if (upperBound is not null && _icmp.Compare(upperBound, startUser, seq, EntryType.RangeDeletion) < 0)
                {
                    stop = true;
                    break;
                }
                byte[] tStart = MakeInternalKey(startUser, seq, EntryType.RangeDeletion);
                if (lowerBound is not null && _icmp.Compare(tStart, lowerBound) < 0) tStart = lowerBound;
                byte[] endBuf = MakeInternalKey(fEnd, DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
                byte[] tEnd = upperBound is not null && _icmp.Compare(upperBound, endBuf) < 0 ? upperBound : endBuf;
                if (considerDrop && _c.KeyRangeNotExistsBeyondOutputLevel(DbFormat.ExtractUserKey(tStart), DbFormat.ExtractUserKey(tEnd), _rangeDelLevelPtrs))
                {
                    Stats.IterStats.NumRangeDelDropObsolete++;
                    continue;
                }
                // The file stores the untruncated fragment; its boundaries do the truncation on read.
                if (tsSize == 0)
                {
                    builder.Add(MakeInternalKey(fStart, seq, EntryType.RangeDeletion), fEnd);
                }
                else
                {
                    byte[] fragKey = MakeInternalKey(FlushJob.ReplaceTimestamp(fStart, ts), seq, EntryType.RangeDeletion);
                    builder.Add(fragKey, FlushJob.ReplaceTimestamp(fEnd, ts));
                }
                // FileMetaData::UpdateBoundariesForRange
                if (meta.Smallest.Length == 0 || _icmp.Compare(tStart, meta.Smallest) < 0) meta.Smallest = tStart;
                if (meta.Largest.Length == 0 || _icmp.Compare(meta.Largest, tEnd) < 0) meta.Largest = tEnd;
                if (seq < meta.SmallestSeqno) meta.SmallestSeqno = seq;
                if (seq > meta.LargestSeqno) meta.LargestSeqno = seq;
            }
            if (stop) break;
        }
    }

    private ulong StripeOf(ulong seq)
    {
        ulong[] s = _snapshots;
        int lo = 0, hi = s.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (s[mid] < seq) lo = mid + 1;
            else hi = mid;
        }
        return (ulong)lo;
    }

    // ---------------------------------------------------------------------------------------------------------
    // file cutting (CompactionOutputs::ShouldStopBefore)

    /// <summary>Port of UpdateGrandparentBoundaryInfo.</summary>
    private int UpdateGrandparentBoundaryInfo(ReadOnlySpan<byte> internalKey)
    {
        int switched = 0;
        List<FileMetaData> grandparents = _c.Grandparents;
        if (grandparents.Count == 0) return 0;
        // Move the grandparent_index_ to the file containing the current user_key. If there are multiple files
        // containing the same user_key, make sure the index points to the last file containing the key.
        while (_grandparentIndex < grandparents.Count)
        {
            if (_beingGrandparentGap)
            {
                if (VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_grandparentIndex].Smallest) < 0) break;
                if (_seenKey)
                {
                    switched++;
                    _grandparentOverlappedBytes += grandparents[_grandparentIndex].FileSize;
                    _grandparentBoundarySwitchedNum++;
                }
                _beingGrandparentGap = false;
            }
            else
            {
                int cmp = VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_grandparentIndex].Largest);
                if (cmp < 0 || (cmp == 0 && (_grandparentIndex == grandparents.Count - 1
                                             || VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_grandparentIndex + 1].Smallest) < 0)))
                {
                    break;
                }
                if (_seenKey)
                {
                    switched++;
                    _grandparentBoundarySwitchedNum++;
                }
                _beingGrandparentGap = true;
                _grandparentIndex++;
            }
        }
        // If the first key is in the middle of a grandparent file, adding it to the overlap.
        if (!_seenKey && !_beingGrandparentGap) _grandparentOverlappedBytes = GetCurrentKeyGrandparentOverlappedBytes(internalKey);
        _seenKey = true;
        return switched;
    }

    /// <summary>Port of GetCurrentKeyGrandparentOverlappedBytes.</summary>
    private ulong GetCurrentKeyGrandparentOverlappedBytes(ReadOnlySpan<byte> internalKey)
    {
        if (_beingGrandparentGap) return 0;
        List<FileMetaData> grandparents = _c.Grandparents;
        if (_grandparentIndex >= grandparents.Count) return 0;
        ulong overlapped = grandparents[_grandparentIndex].FileSize;
        // go backwards to find all overlapped files, one key can overlap multiple files.
        for (int i = _grandparentIndex - 1; i >= 0 && VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[i].Largest) == 0; i--)
        {
            overlapped += grandparents[i].FileSize;
        }
        return overlapped;
    }

    /// <summary>Port of CompactionOutputs::ShouldStopBefore (no TTL cuts, partitioner or round-robin split key).</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool ShouldStopBefore(ReadOnlySpan<byte> internalKey)
    {
        ulong previousOverlappedBytes = _grandparentOverlappedBytes;
        int numGrandparentBoundariesCrossed = 0;
        // Always update grandparent information like overlapped file number, size etc.
        if (_c.OutputLevel > 0) numGrandparentBoundariesCrossed = UpdateGrandparentBoundaryInfo(internalKey);
        if (_builder is null) return false;
        // files output to Level 0 won't be split
        if (_c.OutputLevel == 0) return false;
        ulong currentOutputFileSize = _builder.FileSize;
        // reach the max file size
        if (currentOutputFileSize >= _c.MaxOutputFileSize) return true;
        // only check if the current key is going to cross the grandparents file boundary (either the file beginning or ending).
        if (numGrandparentBoundariesCrossed > 0)
        {
            // Cut the file before the current key if the size of the current output file + its overlapped grandparent
            // files is bigger than max_compaction_bytes.
            if (_grandparentOverlappedBytes + currentOutputFileSize > _c.MaxCompactionBytes) return true;
            // Cut the file if including the key is going to add a skippable file on the grandparent level AND its size is
            // reasonably big (1/8 of target file size).
            int numSkippableBoundariesCrossed = _beingGrandparentGap ? 2 : 3;
            if (_c.Options.CompactionStyle == CompactionStyle.Level && numGrandparentBoundariesCrossed >= numSkippableBoundariesCrossed
                && _grandparentOverlappedBytes - previousOverlappedBytes > _c.TargetOutputFileSize / 8)
            {
                return true;
            }
            // Pre-cut the output file if it's reaching a certain size AND it's at the boundary of a grandparent file.
            if (_c.Options.CompactionStyle == CompactionStyle.Level
                && currentOutputFileSize >= (_c.TargetOutputFileSize + 99) / 100 * (ulong)(50 + Math.Min(_grandparentBoundarySwitchedNum * 5, 40)))
            {
                return true;
            }
        }
        return false;
    }
}
