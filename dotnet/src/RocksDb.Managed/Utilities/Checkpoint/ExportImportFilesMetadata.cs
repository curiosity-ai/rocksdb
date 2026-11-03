namespace RocksDb.Managed;

/// <summary>
/// The table files of an exported column family and the comparator they are ordered by. Port of
/// rocksdb::ExportImportFilesMetaData (include/rocksdb/metadata.h). Produced by
/// <see cref="ColumnFamily.ExportColumnFamily(string)"/> (or native <c>Checkpoint::ExportColumnFamily</c>) and consumed
/// by <see cref="RocksDatabase.CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, ExportImportFilesMetadata)"/>.
/// </summary>
/// <param name="DbComparatorName">Name of the column family's user comparator; the importing column family's comparator must have the same name.</param>
/// <param name="Files">
/// The table files with their level and sequence number range (<see cref="LiveFileMetadata.Directory"/> +
/// <see cref="LiveFileMetadata.Name"/> locate the file). When the internal-key bounds
/// (<see cref="LiveFileMetadata.SmallestInternalKey"/>) are empty they are read from the files.
/// </param>
public sealed record ExportImportFilesMetadata(string DbComparatorName, IReadOnlyList<LiveFileMetadata> Files);

/// <summary>Options of a column family import. Port of rocksdb::ImportColumnFamilyOptions (include/rocksdb/options.h).</summary>
public sealed class ImportColumnFamilyOptions
{
    /// <summary>
    /// Hard-link the files into the DB and delete the originals once the import succeeded (move_files). Default false:
    /// the files are copied and the originals are left alone. Files that cannot be hard-linked (another file system) are
    /// copied.
    /// </summary>
    public bool MoveFiles { get; init; }
}
