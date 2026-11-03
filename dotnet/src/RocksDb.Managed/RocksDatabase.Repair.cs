using RocksDb.Managed.Db;

namespace RocksDb.Managed;

public sealed partial class RocksDatabase
{
    /// <summary>
    /// DB::RepairDB (db/repair.cc): rebuilds the MANIFEST of the DB at <paramref name="path"/> from its table files and
    /// WALs when the MANIFEST or CURRENT is lost or corrupt. Some data may be lost; unreadable files are moved to
    /// <c>path/lost/</c>. The DB must not be open. Tables of unknown column families are added with default options.
    /// </summary>
    public static void Repair(string path, DbOptions options) => Repair(path, options, [], new ColumnFamilyOptions());

    /// <summary>
    /// RepairDB with column family options: tables of column families not listed in <paramref name="columnFamilies"/>
    /// are added with <paramref name="unknownColumnFamilyOptions"/>, or make the repair fail with Corruption when it is
    /// null (upstream: the overload without unknown_cf_opts creates none).
    /// </summary>
    public static void Repair(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, ColumnFamilyOptions? unknownColumnFamilyOptions)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(columnFamilies);
        new Repairer(path, options, columnFamilies, unknownColumnFamilyOptions).Run();
    }
}
