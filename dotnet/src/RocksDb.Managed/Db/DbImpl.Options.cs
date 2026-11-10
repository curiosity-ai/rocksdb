using System.Text;
using RocksDb.Managed.Options;

namespace RocksDb.Managed.Db;

/// <summary>
/// Dynamic options. Port of DBImpl::SetOptions / SetDBOptions (db/db_impl/db_impl.cc), ColumnFamilyData::SetOptions
/// (db/column_family.cc) and the mutable option tables cf_mutable_options_type_info (options/cf_options.cc) and
/// db_mutable_options_type_info (options/db_options.cc).
/// <para>
/// Changes are validated on a copy of the current options (including the same sanitization as at open), then copied
/// into the live options object under the DB mutex, the derived state is refreshed (memtable size, compaction score,
/// write stall, background job limits, delayed write rate, stats dump period), a new super version is installed,
/// pending flushes/compactions are scheduled and a new OPTIONS file is written. Every super version carries an
/// immutable snapshot of the options (SuperVersion::mutable_cf_options) that readers use. Block-based table options that
/// only affect how new tables are built can be changed too ("table_factory.block_size", or
/// "block_based_table_factory" = "{block_size=...;...}"; TableFactoryParseFn / CloneTableFactoryForOptionsUpdate in
/// options/cf_options.cc); they take effect for the next flush or compaction output. Known deviation: background jobs
/// read the live options object (changed under the DB mutex), not a copy taken when the job starts.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>cf_mutable_options_type_info names supported by the managed options model.</summary>
    internal static readonly HashSet<string> MutableCfOptionNames = new(StringComparer.Ordinal)
    {
        "report_bg_io_stats", "disable_auto_compactions", "paranoid_file_checks", "verify_output_flags", "soft_pending_compaction_bytes_limit",
        "hard_pending_compaction_bytes_limit", "max_compaction_bytes", "level0_file_num_compaction_trigger",
        "level0_slowdown_writes_trigger", "level0_stop_writes_trigger", "max_write_buffer_number", "target_file_size_multiplier",
        "arena_block_size", "inplace_update_num_locks", "max_successive_merges", "memtable_huge_page_size", "write_buffer_size",
        "memtable_prefix_bloom_size_ratio", "memtable_whole_key_filtering", "max_bytes_for_level_base", "max_bytes_for_level_multiplier",
        "max_bytes_for_level_multiplier_additional", "max_sequential_skip_in_iterations", "target_file_size_base", "compression",
        "prefix_extractor", "ttl", "periodic_compaction_seconds", "preclude_last_level_data_seconds", "preserve_internal_time_seconds",
        "last_level_temperature", "default_write_temperature", "enable_blob_files", "min_blob_size", "blob_file_size",
        "blob_compression_type", "enable_blob_garbage_collection", "blob_garbage_collection_age_cutoff",
        "blob_garbage_collection_force_threshold", "blob_compaction_readahead_size", "blob_file_starting_level", "prepopulate_blob_cache", "blob_file_writable_file_max_buffer_size",
        "sample_for_compression", "bottommost_compression", "compression_per_level", "experimental_mempurge_threshold",
        "memtable_protection_bytes_per_key", "block_protection_bytes_per_key", "compression_opts", "bottommost_compression_opts",
        "compaction_options_fifo", "compaction_options_universal", "target_file_size_is_upper_bound",
        "read_triggered_compaction_threshold", "bottommost_file_compaction_delay",
        "strict_max_successive_merges", "verify_output_flags", "blob_compression_opts", "uncache_aggressiveness",
        "paranoid_memory_checks", "memtable_verify_per_key_checksum_on_seek", "compression_manager",
        "memtable_max_range_deletions", "memtable_op_scan_flush_trigger", "memtable_avg_op_scan_flush_trigger",
        "min_tombstones_for_range_conversion",
        // OptionVerificationType::kDeprecated entries flagged kMutable: accepted and ignored by SetOptions.
        "filter_deletes", "check_flush_compaction_key_order", "verify_checksums_in_compaction", "hard_rate_limit", "soft_rate_limit",
        "ignore_max_compaction_bytes_for_input", "expanded_compaction_factor", "max_grandparent_overlap_factor",
        "source_compaction_factor", "memtable_prefix_bloom_huge_page_tlb_size", "memtable_prefix_bloom_bits",
        "memtable_prefix_bloom_probes", "min_partial_merge_operands", "snap_refresh_nanos", "bottommost_temperature",
    };

    /// <summary>
    /// True if <paramref name="name"/> is a mutable CF option: a name of <see cref="MutableCfOptionNames"/>, the old
    /// misspelled memtable_veirfy_per_key_checksum_on_seek, or a field of a mutable struct option ("struct.field").
    /// </summary>
    private static bool IsMutableCfOptionName(string name)
    {
        if (MutableCfOptionNames.Contains(name) || name == "memtable_veirfy_per_key_checksum_on_seek") return true;
        int dot = name.IndexOf('.', StringComparison.Ordinal);
        return dot > 0 && MutableCfOptionNames.Contains(name[..dot]);
    }

    /// <summary>db_mutable_options_type_info names supported by the managed options model.</summary>
    internal static readonly HashSet<string> MutableDbOptionNames = new(StringComparer.Ordinal)
    {
        "max_background_jobs", "max_background_compactions", "max_subcompactions", "avoid_flush_during_shutdown",
        "writable_file_max_buffer_size", "delayed_write_rate", "max_total_wal_size", "delete_obsolete_files_period_micros",
        "stats_dump_period_sec", "stats_persist_period_sec", "stats_history_buffer_size", "max_open_files", "bytes_per_sync",
        "wal_bytes_per_sync", "strict_bytes_per_sync", "compaction_readahead_size", "max_background_flushes",
        "max_manifest_file_size", "manifest_preallocation_size", "max_compaction_trigger_wakeup_seconds",
        "max_manifest_space_amp_pct", "verify_manifest_content_on_close", "optimize_manifest_for_recovery", "daily_offpeak_time_utc",
        "periodic_compaction_phase_recovery_percent", "fast_sst_open", "remote_compaction_manifest_floor",
        // OptionVerificationType::kDeprecated entries flagged kMutable: accepted and ignored by SetDBOptions.
        "allow_os_buffer", "base_background_compactions",
    };


    /// <summary>
    /// Applies a table factory option of SetOptions (TableFactoryParseFn): "table_factory.NAME" /
    /// "block_based_table_factory.NAME" set one field, "block_based_table_factory" a "{a=b;c=d}" list. Returns false when
    /// <paramref name="name"/> is not a table factory option.
    /// </summary>
    private static bool TrySetTableOption(ColumnFamilyOptions candidate, string name, string value)
    {
        const string tablePrefix = "table_factory.";
        const string blockBasedPrefix = "block_based_table_factory.";
        if (name == "table_factory")
        {
            if (value != candidate.GetOption("table_factory")) ThrowHelper.InvalidArgument("Option not changeable: table_factory");
            return true;
        }
        if (name == "block_based_table_factory")
        {
            string body = value.Trim();
            if (body.StartsWith('{') && body.EndsWith('}')) body = body[1..^1];
            foreach (string part in body.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                int eq = part.IndexOf('=', StringComparison.Ordinal);
                if (eq <= 0) ThrowHelper.InvalidArgument("Error parsing block_based_table_factory: " + value);
                SetTableOption(candidate.TableOptions, part[..eq].Trim(), part[(eq + 1)..].Trim());
            }
            return true;
        }
        if (name.StartsWith(tablePrefix, StringComparison.Ordinal))
        {
            SetTableOption(candidate.TableOptions, name[tablePrefix.Length..], value);
            return true;
        }
        if (name.StartsWith(blockBasedPrefix, StringComparison.Ordinal))
        {
            SetTableOption(candidate.TableOptions, name[blockBasedPrefix.Length..], value);
            return true;
        }
        return false;
    }

    /// <summary>
    /// One block-based table option of SetOptions. Like upstream's TableFactoryParseFn, which clones the table factory and
    /// configures any of its fields (CloneTableFactoryForOptionsUpdate), every block-based table option can be changed:
    /// builder settings apply to new tables, reader settings (block cache, filter policy, cache/pin settings, index search
    /// type, readahead) to table readers opened afterwards, while readers already open keep theirs. "block_cache" takes
    /// a capacity (Cache::CreateFromString: a new LRU cache of that many bytes, k/m/g/t suffixes allowed).
    /// </summary>
    private static void SetTableOption(BlockBasedTableOptions options, string name, string value)
    {
        if (name == "block_cache")
        {
            // Cache::CreateFromString with a plain capacity (kDontSerialize: not written to the OPTIONS file).
            ulong capacity = RocksDb.Managed.Options.OptionStrings.ParseUInt64("table_factory.block_cache", value.Trim());
            options.BlockCache = Cache.BlockCache.CreateLru(checked((long)capacity));
            return;
        }
        if (options.GetOption(name) is null) ThrowHelper.InvalidArgument("Unrecognized option: table_factory." + name);
        try
        {
            options.SetOption(name, value, preserveUnknown: false);
        }
        catch (Exception e) when (e is FormatException or ArgumentException or OverflowException)
        {
            throw RocksDbException.InvalidArgument("Error parsing table_factory." + name + ": " + e.Message);
        }
    }

    private readonly SemaphoreSlim _setOptionsLock = new(1, 1);

    private OffpeakTimeOption? _offpeakTime;

    /// <summary>
    /// VersionSet::offpeak_time_option: the daily off-peak window of this DB, shared by the options of all its column
    /// families (<see cref="ColumnFamilyOptions.DbOffpeakTime"/>); changed in place by SetDBOptions. First created by the
    /// open (single-threaded).
    /// </summary>
    internal OffpeakTimeOption OffpeakTime => _offpeakTime ??= new OffpeakTimeOption(Options.DailyOffpeakTimeUtc);

    /// <summary>
    /// Port of DBImpl::SetOptions for one column family: applies mutable CF options given as OPTIONS-file name/value pairs.
    /// Fails with InvalidArgument on empty input, unknown names ("Unrecognized option"), immutable options ("Option not
    /// changeable") and invalid values; nothing is changed then.
    /// </summary>
    public async ValueTask SetOptionsAsync(ColumnFamilyData cfd, IEnumerable<KeyValuePair<string, string>> options, bool synchronous,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        var list = new List<KeyValuePair<string, string>>(options);
        if (list.Count == 0)
        {
            LogWarn("SetOptions() on column family [" + cfd.Name + "], empty input");
            ThrowHelper.InvalidArgument("empty input");
        }
        ColumnFamilyOptions candidate = cfd.Options.Clone();
        foreach (KeyValuePair<string, string> kv in list)
        {
            if (TrySetTableOption(candidate, kv.Key, kv.Value)) continue;
            if (!IsMutableCfOptionName(kv.Key))
            {
                if (!candidate.IsKnownOption(kv.Key)) ThrowHelper.InvalidArgument("Unrecognized option: " + kv.Key);
                ThrowHelper.InvalidArgument("Option not changeable: " + kv.Key);
            }
            try
            {
                candidate.SetOption(kv.Key, kv.Value, preserveUnknown: false);
            }
            catch (Exception e) when (e is FormatException or ArgumentException or OverflowException)
            {
                throw RocksDbException.InvalidArgument("Error parsing " + kv.Key + ": " + e.Message);
            }
        }
        await ApplyColumnFamilyOptionsAsync(cfd, candidate, Describe(list), synchronous, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Typed SetOptions: applies every mutable option whose value in <paramref name="newOptions"/> differs from the
    /// current one. Fails with InvalidArgument ("Option not changeable") if an immutable option differs.
    /// </summary>
    public async ValueTask SetOptionsAsync(ColumnFamilyData cfd, ColumnFamilyOptions newOptions, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(newOptions);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        ColumnFamilyOptions current = cfd.Options;
        ColumnFamilyOptions candidate = current.Clone();
        var changed = new List<KeyValuePair<string, string>>();
        foreach (KeyValuePair<string, string> kv in newOptions.ToOptionsList())
        {
            string? old = current.GetOption(kv.Key);
            if (old == kv.Value) continue;
            // Objects resolved at open (comparator, merge operator, ...) are compared by name; derived values only differ
            // here when the caller changed them.
            if (!MutableCfOptionNames.Contains(kv.Key)) ThrowHelper.InvalidArgument("Option not changeable: " + kv.Key);
            candidate.SetOption(kv.Key, kv.Value, preserveUnknown: false);
            changed.Add(kv);
        }
        foreach (KeyValuePair<string, string> kv in newOptions.TableOptions.ToOptionsList())
        {
            if (current.TableOptions.GetOption(kv.Key) == kv.Value) continue;
            SetTableOption(candidate.TableOptions, kv.Key, kv.Value);
            changed.Add(new KeyValuePair<string, string>("table_factory." + kv.Key, kv.Value));
        }
        // Objects that are not serialized: the block cache and a filter policy object with the same id.
        if (!ReferenceEquals(newOptions.TableOptions.BlockCache, current.TableOptions.BlockCache))
        {
            candidate.TableOptions.BlockCache = newOptions.TableOptions.BlockCache;
            changed.Add(new KeyValuePair<string, string>("table_factory.block_cache", newOptions.TableOptions.BlockCache?.ToString() ?? "nullptr"));
        }
        if (newOptions.PrefixExtractor is not null && !ReferenceEquals(newOptions.PrefixExtractor, current.PrefixExtractor)
            && newOptions.PrefixExtractor.AsString() == current.PrefixExtractor?.AsString())
        {
            candidate.PrefixExtractor = newOptions.PrefixExtractor;
        }
        if (changed.Count == 0) return;
        await ApplyColumnFamilyOptionsAsync(cfd, candidate, Describe(changed), synchronous, ct).ConfigureAwait(false);
    }

    private async ValueTask ApplyColumnFamilyOptionsAsync(ColumnFamilyData cfd, ColumnFamilyOptions candidate, string description,
        bool synchronous, CancellationToken ct)
    {
        // Same invariants as at open (SanitizeCfOptions); num_levels and the compaction style are immutable.
        SanitizeCfOptions(candidate, Options.AllowIngestBehind, DbPaths.Count);
        // ColumnFamilyData::SetOptions (db/column_family.cc): the complete new option set is validated
        // (ColumnFamilyData::ValidateOptions) before anything is installed, so a rejected call changes nothing.
        ValidateMutableSizes(candidate);
        ValidateCfOptions(Options, candidate);
        if (synchronous) _setOptionsLock.Wait(ct);
        else await _setOptionsLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_mutex)
            {
                if (cfd.Dropped) ThrowDropped(cfd);
                ColumnFamilyOptions live = cfd.Options;
                foreach (string name in MutableCfOptionNames)
                {
                    string? v = candidate.GetOption(name);
                    if (v is not null && v != live.GetOption(name)) live.SetOption(name, v, preserveUnknown: false);
                }
                live.PrefixExtractor = candidate.PrefixExtractor;
                // The table options are replaced, not modified: running flushes and compactions keep the settings they
                // started with (CloneTableFactoryForOptionsUpdate).
                live.TableOptions = candidate.TableOptions.Clone();
                cfd.OnOptionsChanged(this);
                // Derived state (MutableCFOptions::RefreshDerivedOptions, InstallSuperVersionForConfigChange).
                cfd.MemOptions.WriteBufferSize = (long)Math.Min(live.WriteBufferSize, (ulong)long.MaxValue);
                cfd.MemOptions.ArenaBlockSize = (int)Math.Min(live.ArenaBlockSize, int.MaxValue / 2);
                // Memtable bloom / in-place settings take effect for the next memtable (ImmutableMemTableOptions).
                cfd.MemOptions.ApplyColumnFamilyOptions(live);
                cfd.Mem.UpdateWriteBufferSize(cfd.MemOptions.WriteBufferSize);
                RecomputeMaxTotalInMemoryState();
                cfd.Current.Storage.ComputeCompactionScore();
                RecalculateWriteStall(cfd);
                InstallSuperVersion(cfd);
                _maybeStalled = IsWriteStalled();
                // Trigger possible flush/compactions.
                EnqueueFlush(cfd);
                MaybeScheduleFlush();
                SchedulePendingCompaction(cfd);
                MaybeScheduleCompaction();
                SignalBackgroundWork();
            }
            lock (_mutex) EnsureSeqnoToTimeMapping(cfd.Options);
            RegisterRecordSeqnoTimeWorker();
            RescheduleCompactionTrigger();
            LogInfo("SetOptions() on column family [" + cfd.Name + "], inputs:" + description);
            await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
            LogInfo("[" + cfd.Name + "] SetOptions() succeeded");
        }
        finally
        {
            _setOptionsLock.Release();
        }
    }

    /// <summary>
    /// Managed guards of SetOptions (no upstream equivalent; the engine divides by these): sizes that must not be 0.
    /// </summary>
    private static void ValidateMutableSizes(ColumnFamilyOptions o)
    {
        if (o.WriteBufferSize == 0) ThrowHelper.InvalidArgument("write_buffer_size must be > 0");
        if (o.TargetFileSizeBase == 0) ThrowHelper.InvalidArgument("target_file_size_base must be > 0");
        if (o.TargetFileSizeMultiplier <= 0) ThrowHelper.InvalidArgument("target_file_size_multiplier must be > 0");
        if (o.MaxBytesForLevelBase == 0) ThrowHelper.InvalidArgument("max_bytes_for_level_base must be > 0");
    }

    /// <summary>
    /// Port of ColumnFamilyData::ValidateOptions (db/column_family.cc) against <paramref name="db"/>: run by DB::Open and
    /// CreateColumnFamily, by SetOptions on the complete new option set, and by SetDBOptions for every column family with
    /// the new DB options. Messages are copied verbatim. The managed port additionally rejects a compression_manager
    /// (no compression manager plugins). Checks done where the objects are built: the
    /// concurrent-write / unordered-write / blob direct write / memtable-only timestamp checks (ColumnFamilyData).
    /// Not ported: CheckCompressionSupported (every compression type the OPTIONS model accepts is either supported or
    /// rejected when a table is written) and CheckCFPathsSupported (no cf_paths).
    /// </summary>
    internal static void ValidateCfOptions(DbOptions db, ColumnFamilyOptions o)
    {
        if (db.UnorderedWrite && o.MaxSuccessiveMerges != 0)
            ThrowHelper.InvalidArgument("max_successive_merges > 0 is incompatible with unordered_write");
        if (o.CompressionManagerName is not null)
            ThrowHelper.NotSupported("compression_manager " + o.CompressionManagerName + " is not supported by the managed port");
        // kDefaultTtl: the managed default is the value upstream sanitizes it to for block-based tables (30 days), so that
        // value is treated as the default here (upstream sanitizes it to 0 for other table formats).
        const ulong defaultTtl = 30UL * 24 * 60 * 60;
        bool blockBased = o.TableFactory == TableFactoryType.BlockBasedTable;
        if (o.Ttl > 0 && o.Ttl != defaultTtl && o.Ttl != ColumnFamilyOptions.DefaultPeriodicCompactionSeconds && !blockBased)
        {
            ThrowHelper.NotSupported("TTL is only supported in Block-Based Table format. ");
        }
        if (o.PeriodicCompactionSeconds > 0 && o.PeriodicCompactionSeconds != ColumnFamilyOptions.DefaultPeriodicCompactionSeconds && !blockBased)
        {
            ThrowHelper.NotSupported("Periodic Compaction is only supported in Block-Based Table format. ");
        }
        if (o.EnableBlobGarbageCollection)
        {
            if (o.BlobGarbageCollectionAgeCutoff is < 0 or > 1)
                ThrowHelper.InvalidArgument("The age cutoff for blob garbage collection should be in the range [0.0, 1.0].");
            if (o.BlobGarbageCollectionForceThreshold is < 0 or > 1)
                ThrowHelper.InvalidArgument("The garbage ratio threshold for forcing blob garbage collection should be in the range [0.0, 1.0].");
        }
        ValidateReadTriggeredCompactionThreshold(o);
        if (o.CompactionStyle == CompactionStyle.Fifo && db.MaxOpenFiles != -1 && o.Ttl > 0)
            ThrowHelper.NotSupported("FIFO compaction only supported with max_open_files = -1.");
        if (db.OpenFilesAsync)
        {
            if (o.CompactionStyle == CompactionStyle.Fifo) ThrowHelper.NotSupported("FIFO compaction is not supported with open_files_async = true.");
            if (!db.SkipStatsUpdateOnDbOpen) ThrowHelper.InvalidArgument("open_files_async requires skip_stats_update_on_db_open = true.");
        }
        if (!KvChecksum.IsSupportedLength(o.MemtableProtectionBytesPerKey))
            ThrowHelper.NotSupported("Memtable per key-value checksum protection only supports 0, 1, 2, 4 or 8 bytes per key.");
        if (!KvChecksum.IsSupportedLength(o.BlockProtectionBytesPerKey))
            ThrowHelper.NotSupported("Block per key-value checksum protection only supports 0, 1, 2, 4 or 8 bytes per key.");
        List<FileTemperatureAge> ages = o.CompactionOptionsFifo.FileTemperatureAgeThresholds;
        if (ages.Count > 0)
        {
            if (o.CompactionStyle != CompactionStyle.Fifo)
                ThrowHelper.NotSupported("Option file_temperature_age_thresholds only supports FIFO compaction.");
            if (o.NumLevels > 1)
                ThrowHelper.NotSupported("Option file_temperature_age_thresholds is only supported when num_levels = 1.");
            for (int i = 0; i < ages.Count - 1; i++)
            {
                if (ages[i].Age >= ages[i + 1].Age)
                {
                    ThrowHelper.NotSupported("Option file_temperature_age_thresholds requires elements to be sorted in increasing order with respect to `age` field.");
                }
            }
        }
        if (o.CompactionStyle == CompactionStyle.Universal)
        {
            int maxReadAmp = o.CompactionOptionsUniversal.MaxReadAmp;
            if (maxReadAmp < -1)
                ThrowHelper.NotSupported("CompactionOptionsUniversal::max_read_amp should be at least -1.");
            if (maxReadAmp > 0 && maxReadAmp < o.Level0FileNumCompactionTrigger)
            {
                ThrowHelper.NotSupported("CompactionOptionsUniversal::max_read_amp limits the number of sorted runs but is smaller than the compaction trigger level0_file_num_compaction_trigger.");
            }
        }
    }

    /// <summary>
    /// The checks of DBImpl::ValidateOptions(const DBOptions&amp;) (db/db_impl/db_impl_open.cc) that apply to options
    /// SetDBOptions can change (also run at open): daily_offpeak_time_utc, background job counts.
    /// </summary>
    internal static void ValidateDbOptions(DbOptions o)
    {
        if (o.ReadIoExecutorThreads <= 0) ThrowHelper.InvalidArgument("read_io_executor_threads must be greater than zero");
        if (o.DbPaths.Count > 4) ThrowHelper.NotSupported("More than four DB paths are not supported yet. ");
        if (o.AllowMmapReads && o.UseDirectReads)
        {
            ThrowHelper.NotSupported("If memory mapped reads (allow_mmap_reads) are enabled then direct I/O reads (use_direct_reads) must be disabled. ");
        }
        if (o.AllowMmapReads && o.UseDirectIoForCompactionReads)
        {
            ThrowHelper.NotSupported("If memory mapped reads (allow_mmap_reads) are enabled then compaction-only direct I/O reads " +
                "(use_direct_io_for_compaction_reads) must be disabled. ");
        }
        if (o.AllowMmapWrites && o.UseDirectIoForFlushAndCompaction)
        {
            ThrowHelper.NotSupported("If memory mapped writes (allow_mmap_writes) are enabled then direct I/O writes " +
                "(use_direct_io_for_flush_and_compaction) must be disabled. ");
        }
        if (o.KeepLogFileNum == 0) ThrowHelper.InvalidArgument("keep_log_file_num must be greater than 0");
        if (o.UseDirectIoForFlushAndCompaction && o.WritableFileMaxBufferSize == 0)
            ThrowHelper.InvalidArgument("writes in direct IO require writable_file_max_buffer_size > 0");
        if (o.DailyOffpeakTimeUtc.Length != 0)
        {
            if (!OffpeakTimeOption.TryParseTimeRange(o.DailyOffpeakTimeUtc, out int start, out int end))
                ThrowHelper.InvalidArgument("daily_offpeak_time_utc should be set in the format HH:mm-HH:mm (e.g. 04:30-07:30)");
            if (start == end) ThrowHelper.InvalidArgument("start_time and end_time cannot be the same");
        }
        if (!o.WriteDbIdToManifest && !o.WriteIdentityFile)
            ThrowHelper.InvalidArgument("write_dbid_to_manifest and write_identity_file cannot both be false");
    }

    /// <summary>
    /// Port of the sanitization of BlockBasedTableFactory::InitializeOptions (table/block_based/
    /// block_based_table_factory.cc), in place: the factory holds (and the OPTIONS file records) the sanitized values.
    /// </summary>
    internal static void SanitizeTableOptions(BlockBasedTableOptions t)
    {
        if (t.BlockSizeDeviation is < 0 or > 100) t.BlockSizeDeviation = 0;
        if (t.BlockRestartInterval < 1) t.BlockRestartInterval = 1;
        if (t.IndexBlockRestartInterval < 1) t.IndexBlockRestartInterval = 1;
        // Currently kHashSearch is incompatible with index_block_restart_interval > 1
        if (t.IndexType == IndexType.HashSearch && t.IndexBlockRestartInterval != 1) t.IndexBlockRestartInterval = 1;
        // We do not support partitioned filters without partitioning indexes
        if (t.PartitionFilters && t.IndexType != IndexType.TwoLevelIndexSearch) t.PartitionFilters = false;
        // A too high format_version is not sanitized, so that it is rejected in validation.
        if (t.FormatVersion < MinFormatVersionForWrite) t.FormatVersion = MinFormatVersionForWrite;
    }

    /// <summary>kMinSupportedBbtFormatVersionForWrite (table/format.h).</summary>
    private const uint MinFormatVersionForWrite = Table.BlockBasedTableConstants.MinSupportedFormatVersionForWrite;

    /// <summary>
    /// Port of BlockBasedTableFactory::ValidateOptions (table/block_based/block_based_table_factory.cc), which DB::Open
    /// runs on the column family options as given (ValidateOptionsByTable, db/db_impl/db_impl_open.cc); messages copied
    /// verbatim. Checked elsewhere: interpolation search with a non-bytewise comparator and max_successive_merges with
    /// unordered_write (ColumnFamilyData). Not ported: custom compression managers and cache_usage_options
    /// (CacheEntryRoleOptions), which the options model does not have.
    /// </summary>
    internal static void ValidateTableOptions(ColumnFamilyOptions cf)
    {
        if (cf.TableFactory != TableFactoryType.BlockBasedTable) return;
        BlockBasedTableOptions t = cf.TableOptions;
        if (t.IndexType == IndexType.HashSearch && cf.PrefixExtractor is null)
            ThrowHelper.InvalidArgument("Hash index is specified for block-based table, but prefix_extractor is not given");
        BlockBasedIndexMode indexMode = t.IndexMode; // GetEffectiveIndexMode
        if (indexMode != BlockBasedIndexMode.StandardOnly && t.UserDefinedIndexFactory is not null && cf.Comparator is { TimestampSize: > 0 })
            ThrowHelper.InvalidArgument("user_defined_index_factory is incompatible with user-defined timestamps");
        if (t.CacheIndexAndFilterBlocks && t.NoBlockCache)
            ThrowHelper.InvalidArgument("Enable cache_index_and_filter_blocks, , but block cache is disabled");
        if (t.PinL0FilterAndIndexBlocksInCache && t.NoBlockCache)
            ThrowHelper.InvalidArgument("Enable pin_l0_filter_and_index_blocks_in_cache, , but block cache is disabled");
        // A format_version below the minimum for writing is sanitized (InitializeOptions), not rejected.
        if (t.FormatVersion > Table.BlockBasedTableConstants.LatestSupportedFormatVersion)
            ThrowHelper.InvalidArgument("Unsupported BlockBasedTable format_version. Please check include/rocksdb/table.h for more info");
        ValidateTableCompressionType(cf.Compression, "compression", t.BlockAlign);
        ValidateTableCompressionType(cf.BottommostCompression, "bottommost_compression", t.BlockAlign);
        foreach (Compression.CompressionType c in cf.CompressionPerLevel) ValidateTableCompressionType(c, "compression_per_level", t.BlockAlign);
        if (t.BlockAlign && (t.BlockSize & (t.BlockSize - 1)) != 0)
            ThrowHelper.InvalidArgument("Block alignment requested but block size is not a power of 2");
        if (t.BlockSize > uint.MaxValue) ThrowHelper.InvalidArgument("block size exceeds maximum number (4GiB) allowed");
        if ((t.SuperBlockAlignmentSize & (t.SuperBlockAlignmentSize - 1)) != 0)
            ThrowHelper.InvalidArgument("Super Block alignment requested but super block alignment size is not a power of 2");
        if (t.SuperBlockAlignmentSize > uint.MaxValue)
            ThrowHelper.InvalidArgument("Super block alignment size exceeds maximum number (4GiB) allowed");
        if (t.SuperBlockAlignmentSpaceOverheadRatio is > 0 and < 4)
            ThrowHelper.InvalidArgument("Super block alignment space overhead is too high");
        if (t.DataBlockIndexType == DataBlockIndexType.BinaryAndHash && t.DataBlockHashTableUtilRatio <= 0)
        {
            ThrowHelper.InvalidArgument("data_block_hash_table_util_ratio should be greater than 0 when data_block_index_type is set to " +
                "kDataBlockBinaryAndHash");
        }
        if (indexMode is BlockBasedIndexMode.CustomDefault or BlockBasedIndexMode.CustomOnly)
        {
            if (t.UserDefinedIndexFactory is null)
                ThrowHelper.InvalidArgument("index_mode kCustomDefault/kCustomOnly requires user_defined_index_factory");
            if (indexMode == BlockBasedIndexMode.CustomOnly && t.FormatVersion < 6)
            {
                ThrowHelper.InvalidArgument("index_mode kCustomOnly requires format_version >= 6; got " +
                    t.FormatVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            if (t.IndexType == IndexType.TwoLevelIndexSearch)
                ThrowHelper.InvalidArgument("index_mode kCustomDefault/kCustomOnly is incompatible with partitioned index (kTwoLevelIndexSearch).");
            if (t.PartitionFilters)
                ThrowHelper.InvalidArgument("index_mode kCustomDefault/kCustomOnly is incompatible with partitioned filters.");
        }
    }

    /// <summary>validate_compression_type_fn of BlockBasedTableFactory::ValidateOptions.</summary>
    private static void ValidateTableCompressionType(Compression.CompressionType type, string context, bool blockAlign)
    {
        if (type == Compression.CompressionType.None) return;
        if (type == Compression.CompressionType.DisableCompressionOption)
        {
            if (context == "compression") ThrowHelper.InvalidArgument("kDisableCompressionOption not permitted for option: compression");
            return;
        }
        if (blockAlign) ThrowHelper.InvalidArgument("Enable block_align, but " + context + " enabled");
    }

    /// <summary>
    /// Port of the DBOptions part of SanitizeOptions (db/db_impl/db_impl_open.cc) not covered by
    /// <see cref="SanitizeWalOptions"/>, in place: max_open_files is clipped to [20, port::GetMaxOpenFiles()] (0x400000
    /// when the OS limit is unknown) unless it is -1, and delayed_write_rate = 0 becomes the rate limiter's rate, or
    /// 16 MiB/s.
    /// </summary>
    internal static void SanitizeDbOptions(DbOptions result)
    {
        if (result.MaxOpenFiles != -1)
        {
            int maxMaxOpenFiles = Env.UnixNative.GetMaxOpenFiles();
            if (maxMaxOpenFiles == -1) maxMaxOpenFiles = 0x400000;
            result.MaxOpenFiles = Math.Clamp(result.MaxOpenFiles, 20, Math.Max(20, maxMaxOpenFiles));
        }
        if (result.RateLimiter is not null && result.BytesPerSync == 0) result.BytesPerSync = 1024 * 1024;
        if (result.DelayedWriteRate == 0)
        {
            if (result.RateLimiter is not null) result.DelayedWriteRate = (ulong)Math.Max(0, result.RateLimiter.BytesPerSecond);
            if (result.DelayedWriteRate == 0) result.DelayedWriteRate = 16 * 1024 * 1024;
        }
    }

    /// <summary>ColumnFamilyData::ValidateOptions: read_triggered_compaction_threshold must be &gt;= 0 and finite.</summary>
    internal static void ValidateReadTriggeredCompactionThreshold(ColumnFamilyOptions o)
    {
        if (!(o.ReadTriggeredCompactionThreshold >= 0.0) || double.IsInfinity(o.ReadTriggeredCompactionThreshold))
        {
            ThrowHelper.InvalidArgument("read_triggered_compaction_threshold must be >= 0.0 and finite. Use 0.0 to disable.");
        }
    }

    /// <summary>
    /// Port of DBImpl::SetDBOptions: applies mutable DB options given as OPTIONS-file name/value pairs (InvalidArgument for
    /// empty input, unknown and immutable options). bytes_per_sync = 0 is stored as 1 MiB like upstream.
    /// </summary>
    public async ValueTask SetDbOptionsAsync(IEnumerable<KeyValuePair<string, string>> options, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        var list = new List<KeyValuePair<string, string>>(options);
        if (list.Count == 0)
        {
            LogWarn("SetDBOptions(), empty input.");
            ThrowHelper.InvalidArgument("empty input");
        }
        DbOptions candidate = Options.Clone();
        foreach (KeyValuePair<string, string> kv in list)
        {
            if (!MutableDbOptionNames.Contains(kv.Key))
            {
                if (!candidate.IsKnownOption(kv.Key)) ThrowHelper.InvalidArgument("Unrecognized option: " + kv.Key);
                ThrowHelper.InvalidArgument("Option not changeable: " + kv.Key);
            }
            try
            {
                candidate.SetOption(kv.Key, kv.Value, preserveUnknown: false);
            }
            catch (Exception e) when (e is FormatException or ArgumentException or OverflowException)
            {
                throw RocksDbException.InvalidArgument("Error parsing " + kv.Key + ": " + e.Message);
            }
        }
        await ApplyDbOptionsAsync(candidate, Describe(list), synchronous, ct).ConfigureAwait(false);
    }

    /// <summary>Typed SetDBOptions: applies every mutable DB option that differs (InvalidArgument if an immutable one differs).</summary>
    public async ValueTask SetDbOptionsAsync(DbOptions newOptions, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(newOptions);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        DbOptions candidate = Options.Clone();
        var changed = new List<KeyValuePair<string, string>>();
        foreach (KeyValuePair<string, string> kv in newOptions.ToOptionsList())
        {
            if (Options.GetOption(kv.Key) == kv.Value) continue;
            if (!MutableDbOptionNames.Contains(kv.Key)) ThrowHelper.InvalidArgument("Option not changeable: " + kv.Key);
            candidate.SetOption(kv.Key, kv.Value, preserveUnknown: false);
            changed.Add(kv);
        }
        if (changed.Count == 0) return;
        await ApplyDbOptionsAsync(candidate, Describe(changed), synchronous, ct).ConfigureAwait(false);
    }

    private async ValueTask ApplyDbOptionsAsync(DbOptions candidate, string description, bool synchronous, CancellationToken ct)
    {
        if (candidate.BytesPerSync == 0) candidate.BytesPerSync = 1024 * 1024;
        if (synchronous) _setOptionsLock.Wait(ct);
        else await _setOptionsLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // MutableDBOptionsAreEqual: nothing changes (and no OPTIONS file is written) when every value is the same.
            bool anyChange = false;
            foreach (string name in MutableDbOptionNames)
            {
                if (candidate.GetOption(name) != Options.GetOption(name))
                {
                    anyChange = true;
                    break;
                }
            }
            if (!anyChange)
            {
                LogInfo("SetDBOptions(), input option value is not changed, skipping updating.");
                return;
            }
            // DBImpl::ValidateOptions(new_db_options), then ColumnFamilyData::ValidateOptions(new_db_options, cf_options)
            // for every live column family.
            if (candidate.MaxBackgroundJobs < 1 && candidate.MaxBackgroundJobs != -1) ThrowHelper.InvalidArgument("max_background_jobs must be > 0");
            if (candidate.MaxSubcompactions == 0) ThrowHelper.InvalidArgument("max_subcompactions must be > 0");
            ValidateDbOptions(candidate);
            foreach (ColumnFamilyData c in GetColumnFamilies())
            {
                if (!c.Dropped) ValidateCfOptions(candidate, c.Options);
            }
            uint oldDumpPeriod = Options.StatsDumpPeriodSec;
            uint oldPersistPeriod = Options.StatsPersistPeriodSec;
            bool walOtherOptionChanged = candidate.WalBytesPerSync != Options.WalBytesPerSync;
            bool walSizeOptionChanged = candidate.MaxTotalWalSize != Options.MaxTotalWalSize;
            lock (_mutex)
            {
                foreach (string name in MutableDbOptionNames)
                {
                    string? v = candidate.GetOption(name);
                    if (v is not null && v != Options.GetOption(name)) Options.SetOption(name, v, preserveUnknown: false);
                }
                // versions_->ChangeOffpeakTimeOption: shared by the options of every column family.
                if (OffpeakTime.DailyOffpeakTimeUtc != Options.DailyOffpeakTimeUtc) OffpeakTime.SetFromOffpeakTimeString(Options.DailyOffpeakTimeUtc);
                // GetBGJobLimits (flush part); the compaction limit is read from the options on each scheduling decision.
                int maxFlushes = Options.MaxBackgroundFlushes;
                if (maxFlushes == -1 && Options.MaxBackgroundCompactions == -1) maxFlushes = Math.Max(1, Options.MaxBackgroundJobs / 4);
                _maxBackgroundFlushes = Math.Max(1, maxFlushes);
                IncreaseBackgroundThreadsIfNeeded();
                WriteControllerInstance.SetMaxDelayedWriteRate(Options.DelayedWriteRate);
                // table_cache_->SetCapacity for a new max_open_files.
                SharedTableCache.SetCapacity(Options.MaxOpenFiles);
                MaybeScheduleFlush();
                MaybeScheduleCompaction();
                SignalBackgroundWork();
            }
            if (Options.StatsDumpPeriodSec != oldDumpPeriod)
            {
                _statsDumpTimer?.Dispose();
                _statsDumpTimer = null;
                StartStatsDump();
            }
            // periodic_task_scheduler_.Register / Unregister(kPersistStats).
            if (Options.StatsPersistPeriodSec != oldPersistPeriod) RestartStatsPersist();
            RescheduleCompactionTrigger();
            if (walOtherOptionChanged || walSizeOptionChanged)
            {
                // SwitchWAL when wal_bytes_per_sync changed or the WALs already exceed a lowered max_total_wal_size: the
                // column families holding the oldest WAL are flushed so it can be released.
                WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
                try
                {
                    if (walOtherOptionChanged || Volatile.Read(ref _walsTotalSize) > (long)GetMaxTotalWalSize())
                    {
                        try
                        {
                            await SwitchWalAsync(synchronous, ct).ConfigureAwait(false);
                        }
                        catch (Exception e) when (e is RocksDbException or IOException)
                        {
                            LogWarn("Unable to purge WAL files in SetDBOptions() -- " + e.Message);
                        }
                    }
                }
                finally
                {
                    ExitUnbatched(w, synchronous);
                }
            }
            LogInfo("SetDBOptions(), inputs:" + description);
            await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
            LogInfo("SetDBOptions() succeeded");
        }
        finally
        {
            _setOptionsLock.Release();
        }
    }

    private static string Describe(List<KeyValuePair<string, string>> list)
    {
        var sb = new StringBuilder();
        foreach (KeyValuePair<string, string> kv in list) sb.Append("\n ").Append(kv.Key).Append(": ").Append(kv.Value);
        return sb.ToString();
    }
}
