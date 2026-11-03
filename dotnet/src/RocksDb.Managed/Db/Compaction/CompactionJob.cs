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
    /// <summary>Output files written to the proximal level (per-key placement).</summary>
    public int NumProximalOutputFiles;
    public CompactionIterationStats IterStats;
    public TimeSpan Elapsed;
}

/// <summary>
/// Runs a picked compaction: merges the input files through a <see cref="CompactionIterator"/>, writes the outputs
/// (cut by CompactionOutputs::ShouldStopBefore: max output size, optionally including the estimated tail, grandparent
/// boundaries, TTL cuts, the SST partitioner and the round-robin split key; with range tombstones assigned to the overlapping
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

    // The outputs to the output level and, with per-key placement, to the proximal level (CompactionOutputs of
    // SubcompactionState); _out is the group the current key goes to (current_outputs_).
    private readonly OutputGroup _lastLevelOutputs;
    private readonly OutputGroup? _proximalOutputs;
    private OutputGroup _out;

    // CompactionJob::PrepareTimes: the seqno-to-time mapping of the inputs, the seqno up to which write times need not be
    // preserved, and the seqno above which entries go to the proximal level.
    private SeqnoToTimeMapping? _seqnoToTimeMapping;
    private ulong _preserveSeqnoAfter;
    private ulong _proximalAfterSeqno = DbFormat.MaxSequenceNumber;

    private readonly bool _targetFileSizeIsUpperBound;

    /// <summary>The DB's snapshot checker (WritePrepared / WriteUnprepared), or null.</summary>
    internal SnapshotChecker? SnapshotChecker { get; init; }

    /// <summary>The snapshot taken for this job when there is a snapshot checker (job_snapshot).</summary>
    internal ulong JobSnapshot { get; init; }

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
        _noFillCache = new NoFillTableCache(cfd.TableCache);
        _lastLevelOutputs = new OutputGroup(compaction, isProximalLevel: false);
        _proximalOutputs = compaction.SupportsPerKeyPlacement ? new OutputGroup(compaction, isProximalLevel: true) : null;
        _out = _lastLevelOutputs;
        _targetFileSizeIsUpperBound = compaction.Options.TargetFileSizeIsUpperBound;
        _preserveSeqnoAfter = _earliestSnapshot;
    }

    /// <summary>
    /// The output files of one output level (CompactionOutputs, db/compaction/compaction_outputs.{h,cc}): the open file
    /// and builder, the grandparent / TTL / partitioner / split-key state of ShouldStopBefore, the range-del level
    /// pointers and the finished outputs.
    /// </summary>
    private sealed class OutputGroup
    {
        public OutputGroup(Compaction compaction, bool isProximalLevel)
        {
            IsProximalLevel = isProximalLevel;
            Level = isProximalLevel ? compaction.ProximalLevel : compaction.OutputLevel;
            RangeDelLevelPtrs = new int[compaction.NumberLevels];
            // CompactionOutputs::CompactionOutputs: no partitioner and no TTL cuts for outputs to level 0.
            if (compaction.OutputLevel != 0)
            {
                Partitioner = compaction.CreateSstPartitioner();
                if (Partitioner is not null) LastKeyForPartitioner = new PooledBufferWriter(64);
                FilesToCutForTtl = FillFilesToCutForTtl(compaction);
            }
            // SetOutputSlitKey with a single subcompaction (no boundaries): the compaction's split key as is.
            OutputSplitKey = compaction.OutputSplitKey;
        }

        public readonly bool IsProximalLevel;
        /// <summary>The level the files are added to.</summary>
        public readonly int Level;
        public readonly List<FileMetaData> Outputs = new();

        // Current output.
        public IWritableFile? File;
        public BlockBasedTableBuilder? Builder;
        public FileMetaData? Meta;
        public string? Path;
        public readonly PooledBufferWriter LargestKey = new(64);
        public bool HasPointKey;
        public ulong SmallestPreferredSeqno = DbFormat.MaxSequenceNumber;

        // Grandparent tracking.
        public int GrandparentIndex;
        public bool BeingGrandparentGap = true;
        public ulong GrandparentOverlappedBytes;
        public int GrandparentBoundarySwitchedNum;
        public bool SeenKey;

        public bool FirstOutput = true;

        // SST partitioner (CompactionOutputs::partitioner_): the prefix fast path (ShouldPartitionByPrefix) or the
        // per-key ShouldPartition fallback with the previous output key.
        public readonly SstPartitioner? Partitioner;
        public byte[] PartitionerPrefix = [];
        public int PartitionerPrefixLength;
        public bool PartitionerPrefixValid;
        public readonly PooledBufferWriter? LastKeyForPartitioner;

        // TTL-based cuts (CompactionOutputs::FillFilesToCutForTtl / UpdateFilesToCutForTTLStates).
        public readonly List<FileMetaData>? FilesToCutForTtl;
        public int CurFilesToCutForTtl = -1;
        public int NextFilesToCutForTtl;

        // Round-robin split key (CompactionOutputs::local_output_split_key_ / is_split_).
        public readonly byte[]? OutputSplitKey;
        public bool IsSplit;

        // Level pointers of KeyRangeNotExistsBeyondOutputLevel in AddRangeDels.
        public readonly int[] RangeDelLevelPtrs;

        /// <summary>Releases the open output (on failure) and the pooled buffers.</summary>
        public void Release()
        {
            LargestKey.Dispose();
            LastKeyForPartitioner?.Dispose();
            if (Builder is not null)
            {
                Builder.Abandon();
                Builder.Dispose();
                Builder = null;
            }
            if (File is not null)
            {
                File.Dispose();
                File = null;
            }
        }
    }

    /// <summary>
    /// Port of CompactionOutputs::FillFilesToCutForTtl: with leveled kMinOverlappingRatio compaction and a TTL, the
    /// non-small output-level inputs older than ttl / 2 are listed so that the outputs are cut at their boundaries (the
    /// new data of a mostly-old range then lands in separate files that TTL compaction can pick without the old data).
    /// Null when there is nothing to cut.
    /// </summary>
    internal static List<FileMetaData>? FillFilesToCutForTtl(Compaction c)
    {
        ColumnFamilyOptions o = c.Options;
        if (o.CompactionStyle != CompactionStyle.Level || o.CompactionPri != CompactionPri.MinOverlappingRatio || o.Ttl == 0
            || c.NumInputLevels < 2 || c.BottommostLevel)
        {
            return null;
        }
        // We define new file with the oldest ancestor time to be younger than 1/4 TTL, and an old one to be older than
        // 1/2 TTL time.
        ulong currentTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (currentTime < o.Ttl) return null;
        ulong oldAgeThres = currentTime - o.Ttl / 2;
        List<FileMetaData>? result = null;
        foreach (FileMetaData file in c.Inputs(c.NumInputLevels - 1).Files)
        {
            // We put old files if they are not too small to prevent a flood of small files.
            if (file.OldestAncestorTime < oldAgeThres && file.FileSize > o.TargetFileSizeBase / 2)
            {
                (result ??= new List<FileMetaData>()).Add(file);
            }
        }
        return result;
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

    /// <summary>The output files of the output level.</summary>
    public List<FileMetaData> Outputs => _lastLevelOutputs.Outputs;

    /// <summary>With per-key placement: the output files of the proximal level (<see cref="Compaction.ProximalLevel"/>).</summary>
    public List<FileMetaData> ProximalOutputs => _proximalOutputs?.Outputs ?? s_noOutputs;

    private static readonly List<FileMetaData> s_noOutputs = new();

    /// <summary>The output files of both levels (output level first).</summary>
    public IEnumerable<FileMetaData> AllOutputs
    {
        get
        {
            foreach (FileMetaData f in Outputs) yield return f;
            foreach (FileMetaData f in ProximalOutputs) yield return f;
        }
    }

    /// <summary>The seqno above which entries were placed on the proximal level (proximal_after_seqno_).</summary>
    internal ulong ProximalAfterSeqno => _proximalAfterSeqno;
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
        if (merge is not null)
        {
            merge.FullHistoryTsLow = fullHistoryTsLow;
            merge.SnapshotChecker = SnapshotChecker;
        }
        PrepareTimes();
        var iter = new CompactionIterator(input, _icmp, merge, _snapshots, earliestWriteConflict, rangeDel, _c, filter,
            _db.Options.EnforceSingleDelContracts, _cancellation)
        {
            FullHistoryTsLow = fullHistoryTsLow,
            BlobProcessor = _blob,
            SnapshotChecker = SnapshotChecker,
            JobSnapshot = JobSnapshot,
            PreserveSeqnoAfter = _preserveSeqnoAfter,
        };
        if (merge is not null) merge.BlobProcessor = _blob;
        try
        {
            input.SeekToFirst();
            for (iter.SeekToFirst(); iter.Valid; iter.Next())
            {
                // Per-key placement: entries newer than proximal_after_seqno_ go to the proximal level
                // (SubcompactionState::AddToOutput switches current_outputs_).
                if (_proximalOutputs is not null) _out = iter.Sequence > _proximalAfterSeqno ? _proximalOutputs : _lastLevelOutputs;
                // (The iterator is not moved across the await, so iter.Key stays valid.)
                if (ShouldStopBefore(iter.Key) && _out.Builder is not null)
                {
                    await FinishOutputAsync(iter.Key, synchronous).ConfigureAwait(false);
                    // reset grandparent information
                    _out.GrandparentBoundarySwitchedNum = 0;
                    _out.GrandparentOverlappedBytes = GetCurrentKeyGrandparentOverlappedBytes(iter.Key);
                }
                AddToOutput(iter);
            }
            Stats.IterStats = iter.Stats;
            Stats.NumInputRecords = iter.Stats.NumInputRecords;
            // SubcompactionState::CloseCompactionFiles: the output level's outputs, then the proximal level's.
            for (int g = 0; g < 2; g++)
            {
                OutputGroup? group = g == 0 ? _lastLevelOutputs : _proximalOutputs;
                if (group is null) continue;
                _out = group;
                if (_out.Builder is null && _outputTombstones is not null && !(_c.BottommostLevel && AllTombstonesDroppable()))
                {
                    // Only range tombstones: they still need an output file.
                    OpenOutput();
                }
                if (_out.Builder is not null) await FinishOutputAsync(ReadOnlySpan<byte>.Empty, synchronous).ConfigureAwait(false);
            }
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
            _lastLevelOutputs.Release();
            _proximalOutputs?.Release();
            Stats.Elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(start);
        }
    }

    /// <summary>
    /// Port of CompactionJob::PrepareTimes (single subcompaction): with preserve_internal_time_seconds /
    /// preclude_last_level_data_seconds the seqno-to-time mappings of the inputs (table property rocksdb.seqno.time.map)
    /// are merged and give the cutoffs: sequence numbers above <see cref="_preserveSeqnoAfter"/> keep their value at the
    /// bottommost level, and entries above <see cref="_proximalAfterSeqno"/> (also any visible to a snapshot when
    /// precluding) are placed on the proximal level.
    /// </summary>
    private void PrepareTimes()
    {
        ulong preserveTimeMinSeqno = DbFormat.MaxSequenceNumber;
        ulong precludeLastLevelMinSeqno = DbFormat.MaxSequenceNumber;
        ColumnFamilyOptions o = _c.Options;
        ulong preserveTimeDuration = new MinAndMaxPreserveSeconds(o).MaxPreserveSeconds;
        if (preserveTimeDuration > 0)
        {
            var mapping = new SeqnoToTimeMapping();
            // Setup seqno_to_time_mapping_ with relevant time range.
            mapping.SetMaxTimeSpan(preserveTimeDuration);
            for (int which = 0; which < _c.NumInputLevels; which++)
            {
                foreach (FileMetaData f in _c.Inputs(which).Files)
                {
                    try
                    {
                        using TableReaderLease lease = _cfd.TableCache.GetReader(f);
                        string? error = mapping.DecodeFrom(lease.Reader.Properties.SequenceNumberTimeMapping);
                        if (error is not null) _db.LogWarn("Problem reading or processing seqno-to-time mapping: Corruption: " + error);
                    }
                    catch (RocksDbException e)
                    {
                        _db.LogWarn("Problem reading or processing seqno-to-time mapping: " + e.Message);
                    }
                }
            }
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            mapping.Enforce(now);
            mapping.GetCurrentTieringCutoffSeqnos(now, o.PreserveInternalTimeSeconds, o.PrecludeLastLevelDataSeconds,
                ref preserveTimeMinSeqno, ref precludeLastLevelMinSeqno);
            // For accuracy of the GetProximalSeqnoBeforeTime queries above, we only limit the capacity after them.
            mapping.SetCapacity(SeqnoToTimeMapping.MaxSeqnoToTimeEntries);
            _seqnoToTimeMapping = mapping;
        }
        if (preserveTimeDuration > 0 && TestPrecludeLastLevelMinSeqno is { } forced) precludeLastLevelMinSeqno = forced(precludeLastLevelMinSeqno);
        // Restore the invariant preserve_time_min_seqno <= preclude_last_level_min_seqno (test override).
        preserveTimeMinSeqno = Math.Min(precludeLastLevelMinSeqno, preserveTimeMinSeqno);
        // Preserve sequence numbers for preserved write times and snapshots, though the specific sequence number of the
        // earliest snapshot can be zeroed.
        _preserveSeqnoAfter = Math.Min(Math.Max(preserveTimeMinSeqno, 1UL) - 1, _earliestSnapshot);
        // If using preclude feature, also preclude snapshots from last level, just because they are heuristically more
        // likely to be accessed than non-snapshot data.
        if (precludeLastLevelMinSeqno < DbFormat.MaxSequenceNumber && _earliestSnapshot < precludeLastLevelMinSeqno)
        {
            precludeLastLevelMinSeqno = _earliestSnapshot;
        }
        // Combine what we would like to preclude from last level with what we can safely support without moving data
        // back up the LSM tree (entries already in the last level outside the proximal output range stay there).
        _proximalAfterSeqno = _c.SupportsPerKeyPlacement
            ? Math.Max(precludeLastLevelMinSeqno, _c.KeepInLastLevelThroughSeqno)
            : DbFormat.MaxSequenceNumber;
    }

    /// <summary>Test hook (sync point CompactionJob::PrepareTimes():preclude_last_level_min_seqno): overrides the cutoff.</summary>
    internal static Func<ulong, ulong>? TestPrecludeLastLevelMinSeqno;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void AddToOutput(CompactionIterator iter)
    {
        ReadOnlySpan<byte> key = iter.Key;
        if (_out.Builder is null)
        {
            OpenOutput();
            if (_out.Partitioner is not null) PrepareNewOutputForFirstKey(DbFormat.ExtractUserKey(key));
        }
        ReadOnlySpan<byte> value = iter.Value;
        _out.Builder!.Add(key, value);
        if (_out.Partitioner is not null && !_out.PartitionerPrefixValid)
        {
            _out.LastKeyForPartitioner!.Clear();
            _out.LastKeyForPartitioner.Write(DbFormat.ExtractUserKey(key));
        }
        Stats.NumOutputRecords++;
        UpdateBoundaries(key, iter.Sequence);
        if (iter.Type == EntryType.ValuePreferredSeqno && value.Length >= 8)
        {
            // CompactionOutputs::AddToOutput: the mapping written with the output must cover the preferred seqnos.
            _out.SmallestPreferredSeqno = Math.Min(_out.SmallestPreferredSeqno, Coding.DecodeFixed64(value[^8..]));
        }
        if (iter.Type == EntryType.BlobIndex)
        {
            FlushJob.UpdateOldestBlobFileNumber(_out.Meta!, value);
            _blobGarbageMeter?.ProcessOutFlow(key, value);
        }
        else if (iter.Type == EntryType.WideColumnEntity)
        {
            // FileMetaData::UpdateBoundaries / BlobGarbageMeter::ProcessEntityBlobReferences: blob-backed entity columns.
            FlushJob.UpdateOldestBlobFileNumberForEntity(_out.Meta!, value);
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
        _out.Path = FileNames.TableFileName(_db.DbPaths, fileNumber, _c.OutputPathId);
        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        ulong oldestAncestor = _c.MinInputFileOldestAncestorTime();
        if (oldestAncestor == ulong.MaxValue) oldestAncestor = now;
        ulong newestKeyTime = _c.MaxInputFileNewestKeyTime();
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
            NewestKeyTime = newestKeyTime,
        };
        if (!_c.EnableCompression)
        {
            // GetCompressionType / GetCompressionOptions with enable_compression = false.
            tbo = tbo with { CompressionType = Compression.CompressionType.None, CompressionOptions = _cfd.Options.CompressionOpts };
        }
        _out.Meta = new FileMetaData
        {
            FileNumber = fileNumber,
            PathId = _c.OutputPathId,
            EpochNumber = _c.MinInputFileEpochNumber(),
            OldestAncestorTime = oldestAncestor,
            NewestKeyTime = newestKeyTime,
            FileCreationTime = now,
            Temperature = _c.GetOutputTemperature(_out.IsProximalLevel),
            // user_defined_timestamps_persisted (persist_user_defined_timestamps): the outputs strip timestamps too.
            UserDefinedTimestampsPersisted = !_cfd.TimestampsInMemtableOnly,
        };
        var fileOptions = new WritableFileOptions
        {
            BytesPerSync = (long)Math.Min(_db.Options.BytesPerSync, long.MaxValue),
            MaxBufferSize = (int)Math.Clamp(_db.Options.WritableFileMaxBufferSize, 64 * 1024, int.MaxValue),
        };
        _out.File = _db.FileSystem.CreateWritable(_out.Path, truncate: true, fileOptions);
        _out.Builder = new BlockBasedTableBuilder(_out.File, _cfd.Format, tbo);
        _out.HasPointKey = false;
        _out.LargestKey.Clear();
    }

    /// <summary>
    /// Port of CompactionOutputs::PrepareNewOutputForFirstKey: a new output file starts a fresh partition window; cache
    /// the partitioner's prefix for this file's first key when it can express its next boundary as one.
    /// </summary>
    private void PrepareNewOutputForFirstKey(ReadOnlySpan<byte> userKey)
    {
        int len = _out.Partitioner!.ShouldPartitionByPrefix(userKey);
        _out.PartitionerPrefixValid = len >= 0;
        if (_out.PartitionerPrefixValid)
        {
            if (len > userKey.Length) throw new InvalidOperationException("SstPartitioner.ShouldPartitionByPrefix returned a length beyond the key");
            if (_out.PartitionerPrefix.Length < len) _out.PartitionerPrefix = new byte[Math.Max(len, 16)];
            userKey[..len].CopyTo(_out.PartitionerPrefix);
            _out.PartitionerPrefixLength = len;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void UpdateBoundaries(ReadOnlySpan<byte> key, ulong seq)
    {
        FileMetaData meta = _out.Meta!;
        if (!_out.HasPointKey)
        {
            if (meta.Smallest.Length == 0 || _icmp.Compare(key, meta.Smallest) < 0) meta.Smallest = key.ToArray();
            _out.HasPointKey = true;
        }
        _out.LargestKey.Clear();
        _out.LargestKey.Write(key);
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
        FileMetaData meta = _out.Meta!;
        if (_out.HasPointKey) meta.Largest = _out.LargestKey.WrittenSpan.ToArray();
        if (_outputTombstones is not null) AddRangeDels(nextTableMinKey.Span);
        BlockBasedTableBuilder builder = _out.Builder!;
        IWritableFile file = _out.File!;
        _out.Builder = null;
        _out.File = null;
        bool keep = false;
        try
        {
            if (builder.IsEmpty && meta.Smallest.Length == 0)
            {
                builder.Abandon();
            }
            else
            {
                // CompactionOutputs::Finish -> SetSeqnoTimeTableProperties: the part of the inputs' seqno-to-time mapping
                // relevant to this output.
                if (_seqnoToTimeMapping is not null)
                {
                    builder.Properties.SequenceNumberTimeMapping = FlushJob.RelevantSeqnoToTimeMapping(_seqnoToTimeMapping,
                        Math.Min(_out.SmallestPreferredSeqno, meta.SmallestSeqno), meta.LargestSeqno, 0);
                }
                if (synchronous) builder.Finish();
                else await builder.FinishAsync(_cancellation).ConfigureAwait(false);
                if (synchronous) file.Sync();
                else await file.SyncAsync(_cancellation).ConfigureAwait(false);
                meta.FileSize = builder.FileSize;
                meta.TailSize = builder.FileSize - builder.Properties.TailStartOffset;
                // ExtractTimestampFromTableProperties (db/builder.cc).
                meta.MinTimestamp = builder.TimestampMin;
                meta.MaxTimestamp = builder.TimestampMax;
                // CompactionOutputs::Finish: meta->marked_for_compaction = builder_->NeedCompact().
                meta.MarkedForCompaction = builder.NeedCompact;
                // Entry stats as the version would load them from the table properties (no table read needed).
                meta.SetEntryStats(builder.Properties);
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
                try { _db.FileSystem.DeleteFile(_out.Path!); }
                catch (IOException) { }
                catch (RocksDbException) { }
            }
        }
        _out.FirstOutput = false;
        if (!keep) return;
        if (_out.IsProximalLevel) Stats.NumProximalOutputFiles++;
        DbIdentity id = _db.Identity;
        if (id.DbId.Length > 0 && id.SessionId.Length > 0)
        {
            OffsetableCacheKey.GetSstInternalUniqueId(id.DbId, id.SessionId, meta.FileNumber, out ulong id0, out ulong id1);
            meta.UniqueId = new UniqueId64x2(id0, id1);
        }
        _out.Outputs.Add(meta);
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
        FileMetaData meta = _out.Meta!;
        BlockBasedTableBuilder builder = _out.Builder!;
        bool bottommost = _c.BottommostLevel;

        // Lower bound: none for the first output; otherwise the previous output's upper bound or this file's smallest.
        byte[]? lowerBound = _out.FirstOutput || meta.Smallest.Length == 0 ? null : meta.Smallest;
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
                // Filter out by seqno for per-key placement (keep_seqno_range): range tombstones are split at the next
                // seqno after proximal_after_seqno_, like the point entries.
                if (_proximalOutputs is not null)
                {
                    ulong split = _proximalAfterSeqno < DbFormat.MaxSequenceNumber ? _proximalAfterSeqno + 1 : _proximalAfterSeqno;
                    if (_out.IsProximalLevel ? seq < split : seq >= split) continue;
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
                if (considerDrop && _c.KeyRangeNotExistsBeyondOutputLevel(DbFormat.ExtractUserKey(tStart), DbFormat.ExtractUserKey(tEnd), _out.RangeDelLevelPtrs))
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
        while (_out.GrandparentIndex < grandparents.Count)
        {
            if (_out.BeingGrandparentGap)
            {
                if (VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_out.GrandparentIndex].Smallest) < 0) break;
                if (_out.SeenKey)
                {
                    switched++;
                    _out.GrandparentOverlappedBytes += grandparents[_out.GrandparentIndex].FileSize;
                    _out.GrandparentBoundarySwitchedNum++;
                }
                _out.BeingGrandparentGap = false;
            }
            else
            {
                int cmp = VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_out.GrandparentIndex].Largest);
                if (cmp < 0 || (cmp == 0 && (_out.GrandparentIndex == grandparents.Count - 1
                                             || VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[_out.GrandparentIndex + 1].Smallest) < 0)))
                {
                    break;
                }
                if (_out.SeenKey)
                {
                    switched++;
                    _out.GrandparentBoundarySwitchedNum++;
                }
                _out.BeingGrandparentGap = true;
                _out.GrandparentIndex++;
            }
        }
        // If the first key is in the middle of a grandparent file, adding it to the overlap.
        if (!_out.SeenKey && !_out.BeingGrandparentGap) _out.GrandparentOverlappedBytes = GetCurrentKeyGrandparentOverlappedBytes(internalKey);
        _out.SeenKey = true;
        return switched;
    }

    /// <summary>Port of GetCurrentKeyGrandparentOverlappedBytes.</summary>
    private ulong GetCurrentKeyGrandparentOverlappedBytes(ReadOnlySpan<byte> internalKey)
    {
        if (_out.BeingGrandparentGap) return 0;
        List<FileMetaData> grandparents = _c.Grandparents;
        if (_out.GrandparentIndex >= grandparents.Count) return 0;
        ulong overlapped = grandparents[_out.GrandparentIndex].FileSize;
        // go backwards to find all overlapped files, one key can overlap multiple files.
        for (int i = _out.GrandparentIndex - 1; i >= 0 && VersionStorageInfo.SstableKeyCompare(_icmp, internalKey, grandparents[i].Largest) == 0; i--)
        {
            overlapped += grandparents[i].FileSize;
        }
        return overlapped;
    }

    /// <summary>Port of CompactionOutputs::UpdateFilesToCutForTTLStates: true at the boundaries of old output-level files.</summary>
    private bool UpdateFilesToCutForTtlStates(ReadOnlySpan<byte> internalKey)
    {
        List<FileMetaData> files = _out.FilesToCutForTtl!;
        if (_out.CurFilesToCutForTtl != -1)
        {
            // Previous key is inside the range of a file
            if (_icmp.Compare(internalKey, files[_out.CurFilesToCutForTtl].Largest) > 0)
            {
                _out.NextFilesToCutForTtl = _out.CurFilesToCutForTtl + 1;
                _out.CurFilesToCutForTtl = -1;
                return true;
            }
        }
        else
        {
            // Look for the key position
            while (_out.NextFilesToCutForTtl < files.Count)
            {
                if (_icmp.Compare(internalKey, files[_out.NextFilesToCutForTtl].Smallest) >= 0)
                {
                    if (_icmp.Compare(internalKey, files[_out.NextFilesToCutForTtl].Largest) <= 0)
                    {
                        // With in the current file
                        _out.CurFilesToCutForTtl = _out.NextFilesToCutForTtl;
                        return true;
                    }
                    // Beyond the current file
                    _out.NextFilesToCutForTtl++;
                }
                else
                {
                    // Still fall into the gap
                    break;
                }
            }
        }
        return false;
    }

    /// <summary>
    /// Port of CompactionOutputs::ShouldStopBefore: TTL cuts, SST partitioner (prefix fast path or ShouldPartition),
    /// max output file size (with the estimated tail under target_file_size_is_upper_bound), round-robin split key,
    /// and the grandparent rules (max_compaction_bytes, skippable grandparent files, pre-cut at grandparent boundaries).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool ShouldStopBefore(ReadOnlySpan<byte> internalKey)
    {
        ulong previousOverlappedBytes = _out.GrandparentOverlappedBytes;
        int numGrandparentBoundariesCrossed = 0;
        bool shouldStopForTtl = false;
        // Always update grandparent information like overlapped file number, size etc., and TTL states.
        if (_c.OutputLevel > 0)
        {
            numGrandparentBoundariesCrossed = UpdateGrandparentBoundaryInfo(internalKey);
            if (_out.FilesToCutForTtl is not null) shouldStopForTtl = UpdateFilesToCutForTtlStates(internalKey);
        }
        if (_out.Builder is null) return false;
        if (shouldStopForTtl) return true;
        if (_out.Partitioner is not null && ShouldPartition(internalKey)) return true;
        // files output to Level 0 won't be split
        if (_c.OutputLevel == 0) return false;
        ulong currentOutputFileSize = _out.Builder.FileSize;
        // reach the max file size
        ulong estimatedFileSize = currentOutputFileSize;
        if (_targetFileSizeIsUpperBound) estimatedFileSize += _out.Builder.EstimatedTailSize();
        if (estimatedFileSize >= _c.MaxOutputFileSize) return true;
        // Check if it needs to split for RoundRobin. A null split key means we do not need to split.
        if (_out.OutputSplitKey is not null && !_out.IsSplit && _icmp.Compare(internalKey, _out.OutputSplitKey) >= 0)
        {
            // Split occurs when the next key is larger than/equal to the cursor.
            _out.IsSplit = true;
            return true;
        }
        // only check if the current key is going to cross the grandparents file boundary (either the file beginning or ending).
        if (numGrandparentBoundariesCrossed > 0)
        {
            // Cut the file before the current key if the size of the current output file + its overlapped grandparent
            // files is bigger than max_compaction_bytes.
            if (_out.GrandparentOverlappedBytes + currentOutputFileSize > _c.MaxCompactionBytes) return true;
            // Cut the file if including the key is going to add a skippable file on the grandparent level AND its size is
            // reasonably big (1/8 of target file size).
            int numSkippableBoundariesCrossed = _out.BeingGrandparentGap ? 2 : 3;
            if (_c.Options.CompactionStyle == CompactionStyle.Level && numGrandparentBoundariesCrossed >= numSkippableBoundariesCrossed
                && _out.GrandparentOverlappedBytes - previousOverlappedBytes > _c.TargetOutputFileSize / 8)
            {
                return true;
            }
            // Pre-cut the output file if it's reaching a certain size AND it's at the boundary of a grandparent file.
            if (_c.Options.CompactionStyle == CompactionStyle.Level
                && currentOutputFileSize >= (_c.TargetOutputFileSize + 99) / 100 * (ulong)(50 + Math.Min(_out.GrandparentBoundarySwitchedNum * 5, 40)))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>The partitioner part of ShouldStopBefore (kept out of the per-key method's body).</summary>
    private bool ShouldPartition(ReadOnlySpan<byte> internalKey)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (_out.PartitionerPrefixValid)
        {
            // Fast path: a partition is requested at the first key that no longer shares the established prefix. An
            // empty prefix means no further partitions (every key "starts with" it).
            return !userKey.StartsWith(_out.PartitionerPrefix.AsSpan(0, _out.PartitionerPrefixLength));
        }
        // Slow path: per-key callback.
        return _out.Partitioner!.ShouldPartition(_out.LastKeyForPartitioner!.WrittenSpan, userKey, _out.Builder!.FileSize) == PartitionerResult.Required;
    }
}
