namespace RocksDb.Managed;

/// <summary>
/// Rewrites the files of a closed database so that it can be opened with different compaction settings
/// (compaction style, num_levels, level_compaction_dynamic_level_bytes, ...). Port of OptionChangeMigration
/// (include/rocksdb/utilities/option_change_migration.h, utilities/option_change_migration/option_change_migration.cc).
/// </summary>
/// <remarks>
/// The database is opened with the old options (compactions disabled, write stalls off), each column family is
/// compacted to the level its new options require (one full compaction; none when nothing needs to move), and the
/// database is closed (and, when the old options were kept, reopened and closed once more so the MANIFEST is
/// rewritten). The migration is done when this returns: open the database with the new options afterwards.
/// <list type="bullet">
/// <item>From FIFO: nothing to do (all files are in L0).</item>
/// <item>To universal: when num_levels shrinks, everything is compacted into level new_num_levels - 1.</item>
/// <item>To level, static level sizes: unless the old num_levels is 1, everything is compacted into L1.</item>
/// <item>To level, dynamic level bytes: unless the old num_levels is 1, everything is compacted into the last new level.</item>
/// <item>To FIFO: everything is compacted into one L0 file (bottommost compaction forced).</item>
/// </list>
/// Column families cannot be added or dropped: the old and new descriptors must name the same column families in
/// the same order. Not thread-safe with respect to other users of the database directory (it must not be open).
/// </remarks>
public static class OptionChangeMigration
{
    /// <summary>Migrates a database with only the default column family from <paramref name="oldColumnFamilyOptions"/> to <paramref name="newColumnFamilyOptions"/>.</summary>
    public static void Migrate(string dbname, DbOptions oldDbOptions, ColumnFamilyOptions oldColumnFamilyOptions, DbOptions newDbOptions,
        ColumnFamilyOptions newColumnFamilyOptions) =>
        Migrate(dbname, oldDbOptions, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, oldColumnFamilyOptions)],
            newDbOptions, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, newColumnFamilyOptions)]);

    /// <summary>
    /// Migrates every column family of the database from its old to its new options. <paramref name="oldColumnFamilies"/>
    /// and <paramref name="newColumnFamilies"/> must list the same column families (every one of the database) in
    /// the same order. Throws InvalidArgument when they don't, NotSupported for an unknown compaction style.
    /// </summary>
    public static void Migrate(string dbname, DbOptions oldDbOptions, IReadOnlyList<ColumnFamilyDescriptor> oldColumnFamilies,
        DbOptions newDbOptions, IReadOnlyList<ColumnFamilyDescriptor> newColumnFamilies)
    {
        // Step 1: Validate that old and new have same CFs in same order
        ValidateDescriptors(oldColumnFamilies, newColumnFamilies);
        ArgumentNullException.ThrowIfNull(oldDbOptions);
        ArgumentNullException.ThrowIfNull(newDbOptions);

        // Step 2: Prepare no-compaction CF descriptors
        List<ColumnFamilyDescriptor> noCompact = PrepareNoCompactionDescriptors(oldColumnFamilies, newColumnFamilies, out bool anyNeedReopen);

        // Step 3: Open DB with all CFs; Step 4: Migrate all CFs
        using (RocksDatabase db = RocksDatabase.Open(dbname, oldDbOptions, noCompact))
        {
            for (int i = 0; i < oldColumnFamilies.Count; i++)
            {
                CompactRangeOptions? cro = MigrateSingleColumnFamily(db.GetColumnFamily(oldColumnFamilies[i].Name),
                    oldColumnFamilies[i].Options, newColumnFamilies[i].Options);
                if (cro is not null) db.GetColumnFamily(oldColumnFamilies[i].Name).CompactRange(cro, null, null);
            }
        }

        // Step 6: Close and reopen DB if needed to rewrite manifest
        if (anyNeedReopen)
        {
            using RocksDatabase db = RocksDatabase.Open(dbname, oldDbOptions, noCompact);
        }
    }

    /// <summary>Async <see cref="Migrate(string, DbOptions, ColumnFamilyOptions, DbOptions, ColumnFamilyOptions)"/>.</summary>
    public static ValueTask MigrateAsync(string dbname, DbOptions oldDbOptions, ColumnFamilyOptions oldColumnFamilyOptions, DbOptions newDbOptions,
        ColumnFamilyOptions newColumnFamilyOptions, CancellationToken cancellationToken = default) =>
        MigrateAsync(dbname, oldDbOptions, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, oldColumnFamilyOptions)],
            newDbOptions, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, newColumnFamilyOptions)], cancellationToken);

    /// <summary>Async <see cref="Migrate(string, DbOptions, IReadOnlyList{ColumnFamilyDescriptor}, DbOptions, IReadOnlyList{ColumnFamilyDescriptor})"/>.</summary>
    public static async ValueTask MigrateAsync(string dbname, DbOptions oldDbOptions, IReadOnlyList<ColumnFamilyDescriptor> oldColumnFamilies,
        DbOptions newDbOptions, IReadOnlyList<ColumnFamilyDescriptor> newColumnFamilies, CancellationToken cancellationToken = default)
    {
        ValidateDescriptors(oldColumnFamilies, newColumnFamilies);
        ArgumentNullException.ThrowIfNull(oldDbOptions);
        ArgumentNullException.ThrowIfNull(newDbOptions);
        List<ColumnFamilyDescriptor> noCompact = PrepareNoCompactionDescriptors(oldColumnFamilies, newColumnFamilies, out bool anyNeedReopen);
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dbname, oldDbOptions, noCompact, cancellationToken).ConfigureAwait(false))
        {
            for (int i = 0; i < oldColumnFamilies.Count; i++)
            {
                ColumnFamily cf = db.GetColumnFamily(oldColumnFamilies[i].Name);
                CompactRangeOptions? cro = MigrateSingleColumnFamily(cf, oldColumnFamilies[i].Options, newColumnFamilies[i].Options);
                if (cro is not null) await cf.CompactRangeAsync(cro, null, null, cancellationToken).ConfigureAwait(false);
            }
        }
        if (anyNeedReopen)
        {
            await using RocksDatabase db = await RocksDatabase.OpenAsync(dbname, oldDbOptions, noCompact, cancellationToken).ConfigureAwait(false);
        }
    }

    // ---- migration of one column family (returns the compaction to run, or null) ----------------------------

    /// <summary>GetNoCompactionOptions.</summary>
    private static ColumnFamilyOptions GetNoCompactionOptions(ColumnFamilyOptions opts)
    {
        ColumnFamilyOptions ret = opts.Clone();
        ret.DisableAutoCompactions = true;
        ret.Level0SlowdownWritesTrigger = 999999;
        ret.Level0StopWritesTrigger = 999999;
        ret.SoftPendingCompactionBytesLimit = 0;
        ret.HardPendingCompactionBytesLimit = 0;
        return ret;
    }

    /// <summary>CompactToLevel: a full manual compaction moving the result to <paramref name="destLevel"/>.</summary>
    private static CompactRangeOptions CompactToLevel(int destLevel)
    {
        var cro = new CompactRangeOptions { ChangeLevel = true, TargetLevel = destLevel };
        if (destLevel == 0)
        {
            // cannot use kForceOptimized because the compaction is expected to generate one output file so to force
            // the full compaction to skip trivial move to L0
            cro.BottommostLevelCompaction = BottommostLevelCompaction.Force;
        }
        return cro;
    }

    /// <summary>MigrateToUniversal.</summary>
    private static CompactRangeOptions? MigrateToUniversal(ColumnFamily cf, int oldNumLevels, int newNumLevels)
    {
        if (oldNumLevels <= newNumLevels) return null;
        // Check if compaction is needed: ColumnFamilyMetaData::levels lists every level of the open column family,
        // so its last entry is level num_levels - 1.
        int lastLevel = cf.Data.NumLevels - 1;
        if (lastLevel >= newNumLevels)
        {
            // Need to compact to fit new num_levels
            return CompactToLevel(newNumLevels - 1);
        }
        return null;
    }

    /// <summary>MigrateToLevelBase.</summary>
    private static CompactRangeOptions? MigrateToLevelBase(int oldNumLevels, int newNumLevels, bool dynamicLevelBytes)
    {
        if (oldNumLevels == 1) return null;
        // Non-dynamic level mode: compact to L1; dynamic level mode: compact to the last level.
        return CompactToLevel(dynamicLevelBytes ? newNumLevels - 1 : 1);
    }

    /// <summary>MigrateSingleColumnFamily.</summary>
    private static CompactRangeOptions? MigrateSingleColumnFamily(ColumnFamily cf, ColumnFamilyOptions oldOpts, ColumnFamilyOptions newOpts)
    {
        if (oldOpts.CompactionStyle == CompactionStyle.Fifo) return null;
        return newOpts.CompactionStyle switch
        {
            CompactionStyle.Universal => MigrateToUniversal(cf, oldOpts.NumLevels, newOpts.NumLevels),
            CompactionStyle.Level => MigrateToLevelBase(oldOpts.NumLevels, newOpts.NumLevels, newOpts.LevelCompactionDynamicLevelBytes),
            CompactionStyle.Fifo => CompactToLevel(0), // MigrateToFIFO
            _ => throw RocksDbException.NotSupported("Do not know how to migrate to this compaction style"),
        };
    }

    // ---- descriptors -------------------------------------------------------------------------------------------

    /// <summary>ValidateCFDescriptors.</summary>
    private static void ValidateDescriptors(IReadOnlyList<ColumnFamilyDescriptor> oldCfs, IReadOnlyList<ColumnFamilyDescriptor> newCfs)
    {
        ArgumentNullException.ThrowIfNull(oldCfs);
        ArgumentNullException.ThrowIfNull(newCfs);
        if (oldCfs.Count != newCfs.Count)
        {
            ThrowHelper.InvalidArgument("old_cf_descs and new_cf_descs must have the same number of column families. Got " + oldCfs.Count +
                " old CFs and " + newCfs.Count + " new CFs. Adding or dropping CFs is not supported.");
        }
        for (int i = 0; i < oldCfs.Count; i++)
        {
            if (oldCfs[i].Name != newCfs[i].Name)
            {
                ThrowHelper.InvalidArgument("Column family mismatch at index " + i + ": old has '" + oldCfs[i].Name + "', new has '" + newCfs[i].Name +
                    "'. CF names and order must match exactly.");
            }
        }
    }

    /// <summary>
    /// DetermineBaseOptions: the options the database is opened with during the migration; <paramref name="needReopen"/>
    /// is false only when the new options themselves are used.
    /// </summary>
    private static ColumnFamilyOptions DetermineBaseOptions(ColumnFamilyOptions oldOpts, ColumnFamilyOptions newOpts, out bool needReopen)
    {
        needReopen = true;
        ColumnFamilyOptions baseOpts;
        if (newOpts.CompactionStyle == CompactionStyle.Level)
        {
            if (newOpts.LevelCompactionDynamicLevelBytes && newOpts.NumLevels > oldOpts.NumLevels)
            {
                baseOpts = newOpts.Clone();
                needReopen = false;
            }
            else
            {
                baseOpts = oldOpts.Clone();
                baseOpts.TargetFileSizeBase = newOpts.TargetFileSizeBase;
            }
        }
        else
        {
            baseOpts = oldOpts.Clone();
        }
        return baseOpts;
    }

    /// <summary>ApplySpecialSingleLevelSettings: one huge output file when the target has one level (or is FIFO).</summary>
    private static void ApplySpecialSingleLevelSettings(ColumnFamilyOptions newOpts, ColumnFamilyOptions baseOpts)
    {
        if ((newOpts.CompactionStyle is CompactionStyle.Universal or CompactionStyle.Level && newOpts.NumLevels == 1) ||
            newOpts.CompactionStyle == CompactionStyle.Fifo)
        {
            baseOpts.TargetFileSizeBase = 999999999999999;
            baseOpts.MaxCompactionBytes = 999999999999999;
        }
    }

    /// <summary>PrepareNoCompactionCFDescriptors.</summary>
    private static List<ColumnFamilyDescriptor> PrepareNoCompactionDescriptors(IReadOnlyList<ColumnFamilyDescriptor> oldCfs,
        IReadOnlyList<ColumnFamilyDescriptor> newCfs, out bool anyNeedReopen)
    {
        anyNeedReopen = false;
        var result = new List<ColumnFamilyDescriptor>(oldCfs.Count);
        for (int i = 0; i < oldCfs.Count; i++)
        {
            ColumnFamilyOptions baseOpts = DetermineBaseOptions(oldCfs[i].Options, newCfs[i].Options, out bool needReopen);
            anyNeedReopen |= needReopen;
            ApplySpecialSingleLevelSettings(newCfs[i].Options, baseOpts);
            result.Add(new ColumnFamilyDescriptor(oldCfs[i].Name, GetNoCompactionOptions(baseOpts)));
        }
        return result;
    }
}
