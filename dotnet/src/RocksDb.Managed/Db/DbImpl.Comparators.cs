using System.Collections.Concurrent;
using System.Globalization;

namespace RocksDb.Managed.Db;

/// <summary>
/// Custom comparator plumbing for open / recovery. Ports the comparator check of VersionEditHandler (a column family
/// whose MANIFEST records a comparator name different from the configured comparator fails with InvalidArgument,
/// db/version_edit_handler.cc) and VersionBuilder::CheckConsistencyForNumLevels ("db has more levels than
/// options.num_levels", db/version_builder.cc).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>Configured comparators by name (custom ones from the CF descriptors), used to order recovered files.</summary>
    private readonly ConcurrentDictionary<string, Comparator> _comparatorsByName = new(StringComparer.Ordinal);
    private Func<string, Comparator?>? _comparatorResolverDelegate;

    /// <summary>Resolves a persisted comparator name: a configured (custom) comparator first, then the built-ins.</summary>
    private Func<string, Comparator?> _comparatorResolver => _comparatorResolverDelegate ??= ResolveComparatorByName;

    private Comparator? ResolveComparatorByName(string name)
    {
        if (_comparatorsByName.TryGetValue(name, out Comparator? c)) return c;
        return Comparator.TryGetBuiltin(name, out c) ? c : null;
    }

    /// <summary>The user comparator of the live column family <paramref name="cfId"/> (bytewise when unknown).</summary>
    internal Comparator UserComparatorOf(uint cfId)
    {
        ColumnFamilyData?[] map = Volatile.Read(ref _cfById);
        return cfId < (uint)map.Length && map[cfId] is { } cfd ? cfd.UserComparator : Comparator.Bytewise;
    }

    /// <summary>The CompareComparator outcome of an accepted comparator change (util/udt_util.cc ToggleUDT).</summary>
    private enum UdtToggle
    {
        Unchanged,
        Enable,
        Disable,
    }

    /// <summary>
    /// Port of ValidateUserDefinedTimestampsOptions (util/udt_util.cc): a column family may only change comparators by
    /// enabling user-defined timestamps in memtable only ("&lt;name&gt;" -&gt; "&lt;name&gt;.u64ts" with
    /// persist_user_defined_timestamps = false) or by disabling them when they were not persisted; the persist flag of a
    /// column family with timestamps cannot change. Messages are copied verbatim.
    /// </summary>
    private static UdtToggle ValidateUserDefinedTimestampsOptions(Comparator newComparator, string oldComparatorName, bool newPersistUdt, bool oldPersistUdt)
    {
        string newName = newComparator.Name;
        string suffix = Table.BlockBasedTableReader.UdtComparatorSuffix;
        if (newName == oldComparatorName)
        {
            if (oldPersistUdt == newPersistUdt || newComparator.TimestampSize == 0) return UdtToggle.Unchanged;
            ThrowHelper.InvalidArgument("Cannot toggle the persist_user_defined_timestamps flag for a column family with user-defined timestamps feature enabled.");
        }
        if (newName.Length == oldComparatorName.Length + suffix.Length && newName.StartsWith(oldComparatorName, StringComparison.Ordinal)
            && newName.EndsWith(suffix, StringComparison.Ordinal))
        {
            if (!newPersistUdt) return UdtToggle.Enable;
            ThrowHelper.InvalidArgument("Cannot open a column family and enable user-defined timestamps feature without setting " +
                                        "persist_user_defined_timestamps flag to false.");
        }
        if (oldComparatorName.Length == newName.Length + suffix.Length && oldComparatorName.StartsWith(newName, StringComparison.Ordinal)
            && oldComparatorName.EndsWith(suffix, StringComparison.Ordinal))
        {
            if (!oldPersistUdt) return UdtToggle.Disable;
            ThrowHelper.InvalidArgument("Cannot open a column family and disable user-defined timestamps feature if its existing " +
                                        "persist_user_defined_timestamps flag is not false.");
        }
        // Status::InvalidArgument(new_comparator->Name(), "does not match existing comparator " + old_comparator_name)
        ThrowHelper.InvalidArgument(newName + ": does not match existing comparator " + oldComparatorName);
        return UdtToggle.Unchanged;
    }

    private static int TimestampSizeOfName(string comparatorName) =>
        Comparator.TryGetBuiltin(comparatorName, out Comparator? c) ? c!.TimestampSize : sizeof(ulong);

    /// <summary>
    /// File boundaries after a timestamp toggle. Enabling (VersionEditHandler cfds_to_mark_no_udt_ +
    /// MaybeHandleFileBoundariesForNewFiles): every existing file is marked as not persisting timestamps and its
    /// boundaries get the minimum timestamp. Disabling: the boundaries were padded with the recorded comparator's
    /// timestamp during replay; upstream never pads them for a comparator without timestamps, so the padding is removed.
    /// </summary>
    private static void ApplyUdtToggleToFiles(ColumnFamilyVersion cf, UdtToggle toggle, int tsSize)
    {
        if (toggle == UdtToggle.Unchanged) return;
        foreach ((_, FileMetaData f) in cf.AllFiles())
        {
            if (toggle == UdtToggle.Enable)
            {
                f.UserDefinedTimestampsPersisted = false;
                ManifestReplayer.PadFileBoundaries(f, tsSize);
            }
            else if (!f.UserDefinedTimestampsPersisted)
            {
                ManifestReplayer.StripFileBoundaries(f, tsSize);
            }
        }
    }

    /// <summary>Registers the custom comparators of <paramref name="descriptors"/> for MANIFEST replays.</summary>
    private void SetComparatorResolver(Dictionary<string, ColumnFamilyOptions> descriptors)
    {
        foreach (ColumnFamilyOptions o in descriptors.Values) RegisterComparator(o);
    }

    private void RegisterComparator(ColumnFamilyOptions o)
    {
        if (o.Comparator is { } c && c.Kind == ComparatorKind.Custom) _comparatorsByName.TryAdd(c.Name, c);
    }

    /// <summary>
    /// Checks a recovered column family against its configured options: the comparator name recorded in the MANIFEST
    /// must equal the configured comparator's name, and no file may live on a level &gt;= num_levels.
    /// </summary>
    private static void ValidateRecoveredColumnFamily(ColumnFamilyVersion cf, ColumnFamilyOptions opts)
    {
        if (cf.ComparatorName is not null
            && (cf.ComparatorName != opts.ComparatorName || cf.PersistUserDefinedTimestamps != opts.PersistUserDefinedTimestamps))
        {
            Comparator running = TableFactory.ResolveComparator(opts);
            UdtToggle toggle = ValidateUserDefinedTimestampsOptions(running, cf.ComparatorName, opts.PersistUserDefinedTimestamps,
                cf.PersistUserDefinedTimestamps);
            ApplyUdtToggleToFiles(cf, toggle, running.TimestampSize == 0 ? TimestampSizeOfName(cf.ComparatorName) : running.TimestampSize);
        }
        for (int l = Math.Max(1, opts.NumLevels); l < cf.NumLevels; l++)
        {
            if (cf.Levels[l].Count > 0)
            {
                ThrowHelper.InvalidArgument("db has more levels than options.num_levels (file on level " + l.ToString(CultureInfo.InvariantCulture) +
                                            ", num_levels = " + opts.NumLevels.ToString(CultureInfo.InvariantCulture) + ")");
            }
        }
    }
}
