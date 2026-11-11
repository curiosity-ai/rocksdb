using RocksDb.Managed.Db;

namespace RocksDb.Managed;

public sealed partial class RocksDatabase
{
    /// <summary>
    /// DB::RepairDB (db/repair.cc): rebuilds the MANIFEST of the DB at <paramref name="path"/> from its table files and
    /// WALs when the MANIFEST or CURRENT is lost or corrupt. Some data may be lost; unreadable files are moved to
    /// <c>path/lost/</c>. The DB must not be open. Tables of unknown column families are added with default options.
    /// </summary>
    public static void Repair(string path, DbOptions options)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        new Repairer(path, options, [], new ColumnFamilyOptions()).Run();
    }

    /// <summary>
    /// RepairDB with column family options: tables of column families not listed in <paramref name="columnFamilies"/>
    /// are added with <paramref name="unknownColumnFamilyOptions"/>, or are moved to <c>lost/</c> when it is null
    /// (upstream: the overload without unknown_cf_opts creates none). <paramref name="columnFamilies"/> must name the
    /// default column family (InvalidArgument otherwise).
    /// </summary>
    public static void Repair(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, ColumnFamilyOptions? unknownColumnFamilyOptions)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(columnFamilies);
        // GetDefaultCFOptions (db/repair.cc).
        bool hasDefault = false;
        foreach (ColumnFamilyDescriptor d in columnFamilies) hasDefault |= d.Name == ColumnFamilyDescriptor.DefaultColumnFamilyName;
        if (!hasDefault) throw RocksDbException.InvalidArgument("column_families: Must contain entry for default column family");
        new Repairer(path, options, columnFamilies, unknownColumnFamilyOptions).Run();
    }
}
