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
        if (cf.ComparatorName is not null && cf.ComparatorName != opts.ComparatorName)
        {
            // VersionEditHandler: Status::InvalidArgument(cfd->user_comparator()->Name(), "does not match existing comparator " + edit.comparator_)
            ThrowHelper.InvalidArgument(opts.ComparatorName + ": does not match existing comparator " + cf.ComparatorName);
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
