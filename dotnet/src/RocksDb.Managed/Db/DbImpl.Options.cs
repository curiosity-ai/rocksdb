using System.Text;

namespace RocksDb.Managed.Db;

/// <summary>
/// Dynamic options. Port of DBImpl::SetOptions / SetDBOptions (db/db_impl/db_impl.cc), ColumnFamilyData::SetOptions
/// (db/column_family.cc) and the mutable option tables cf_mutable_options_type_info (options/cf_options.cc) and
/// db_mutable_options_type_info (options/db_options.cc).
/// <para>
/// Changes are validated on a copy of the current options (including the same sanitization as at open), then copied
/// into the live options object under the DB mutex, the derived state is refreshed (memtable size, compaction score,
/// write stall, background job limits, delayed write rate, stats dump period), a new super version is installed,
/// pending flushes/compactions are scheduled and a new OPTIONS file is written. Known deviation: upstream readers take
/// an immutable MutableCFOptions snapshot from the super version; managed readers read the live options object, so a
/// concurrent reader may briefly observe a mix of old and new values of different options.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>cf_mutable_options_type_info names supported by the managed options model.</summary>
    internal static readonly HashSet<string> MutableCfOptionNames = new(StringComparer.Ordinal)
    {
        "report_bg_io_stats", "disable_auto_compactions", "paranoid_file_checks", "soft_pending_compaction_bytes_limit",
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
    };

    /// <summary>db_mutable_options_type_info names supported by the managed options model.</summary>
    internal static readonly HashSet<string> MutableDbOptionNames = new(StringComparer.Ordinal)
    {
        "max_background_jobs", "max_background_compactions", "max_subcompactions", "avoid_flush_during_shutdown",
        "writable_file_max_buffer_size", "delayed_write_rate", "max_total_wal_size", "delete_obsolete_files_period_micros",
        "stats_dump_period_sec", "stats_persist_period_sec", "stats_history_buffer_size", "max_open_files", "bytes_per_sync",
        "wal_bytes_per_sync", "strict_bytes_per_sync", "compaction_readahead_size", "max_background_flushes",
        "max_manifest_file_size", "manifest_preallocation_size",
    };

    private readonly SemaphoreSlim _setOptionsLock = new(1, 1);

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
            if (!MutableCfOptionNames.Contains(kv.Key))
            {
                if (candidate.GetOption(kv.Key) is null) ThrowHelper.InvalidArgument("Unrecognized option: " + kv.Key);
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
        ValidateMutableCfOptions(candidate);
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
                // Derived state (MutableCFOptions::RefreshDerivedOptions, InstallSuperVersionForConfigChange).
                cfd.MemOptions.WriteBufferSize = (long)Math.Min(live.WriteBufferSize, (ulong)long.MaxValue);
                cfd.MemOptions.ArenaBlockSize = (int)Math.Min(live.ArenaBlockSize, int.MaxValue / 2);
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
            LogInfo("SetOptions() on column family [" + cfd.Name + "], inputs:" + description);
            await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
            LogInfo("[" + cfd.Name + "] SetOptions() succeeded");
        }
        finally
        {
            _setOptionsLock.Release();
        }
    }

    /// <summary>The checks of ColumnFamilyData::ValidateOptions / CheckCompressionSupported that apply to mutable options.</summary>
    private static void ValidateMutableCfOptions(ColumnFamilyOptions o)
    {
        if (o.WriteBufferSize == 0) ThrowHelper.InvalidArgument("write_buffer_size must be > 0");
        if (o.TargetFileSizeBase == 0) ThrowHelper.InvalidArgument("target_file_size_base must be > 0");
        if (o.TargetFileSizeMultiplier <= 0) ThrowHelper.InvalidArgument("target_file_size_multiplier must be > 0");
        if (o.MaxBytesForLevelBase == 0) ThrowHelper.InvalidArgument("max_bytes_for_level_base must be > 0");
        if (o.EnableBlobGarbageCollection && o.BlobGarbageCollectionAgeCutoff is < 0 or > 1)
        {
            ThrowHelper.InvalidArgument("The age cutoff for blob garbage collection should be in the range [0.0, 1.0].");
        }
        if (o.EnableBlobGarbageCollection && o.BlobGarbageCollectionForceThreshold is < 0 or > 1)
        {
            ThrowHelper.InvalidArgument("The garbage ratio threshold for forcing blob garbage collection should be in the range [0.0, 1.0].");
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
                if (candidate.GetOption(kv.Key) is null) ThrowHelper.InvalidArgument("Unrecognized option: " + kv.Key);
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
        // DBImpl::ValidateOptions subset for mutable options.
        if (candidate.MaxBackgroundJobs < 1 && candidate.MaxBackgroundJobs != -1) ThrowHelper.InvalidArgument("max_background_jobs must be > 0");
        if (candidate.MaxSubcompactions == 0) ThrowHelper.InvalidArgument("max_subcompactions must be > 0");
        if (synchronous) _setOptionsLock.Wait(ct);
        else await _setOptionsLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            uint oldDumpPeriod = Options.StatsDumpPeriodSec;
            lock (_mutex)
            {
                foreach (string name in MutableDbOptionNames)
                {
                    string? v = candidate.GetOption(name);
                    if (v is not null && v != Options.GetOption(name)) Options.SetOption(name, v, preserveUnknown: false);
                }
                // GetBGJobLimits (flush part); the compaction limit is read from the options on each scheduling decision.
                int maxFlushes = Options.MaxBackgroundFlushes;
                if (maxFlushes == -1 && Options.MaxBackgroundCompactions == -1) maxFlushes = Math.Max(1, Options.MaxBackgroundJobs / 4);
                _maxBackgroundFlushes = Math.Max(1, maxFlushes);
                WriteControllerInstance.SetMaxDelayedWriteRate(Options.DelayedWriteRate);
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
