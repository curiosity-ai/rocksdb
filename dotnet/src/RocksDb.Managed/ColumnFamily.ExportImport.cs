namespace RocksDb.Managed;

public sealed partial class ColumnFamily
{
    /// <summary>
    /// Exports this column family's table files into the new directory <paramref name="exportDirectory"/>
    /// (Checkpoint::ExportColumnFamily): the memtable is flushed, the files are hard-linked (copied across file systems)
    /// and their metadata returned, ready for
    /// <see cref="RocksDatabase.CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, ExportImportFilesMetadata)"/>
    /// in this or another (managed or native) database. Fails with InvalidArgument if the directory exists. Blob files
    /// are not exported.
    /// </summary>
    public ExportImportFilesMetadata ExportColumnFamily(string exportDirectory) =>
        _db.ExportColumnFamilyAsync(_cfd, exportDirectory, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Async <see cref="ExportColumnFamily(string)"/>.</summary>
    public ValueTask<ExportImportFilesMetadata> ExportColumnFamilyAsync(string exportDirectory, CancellationToken cancellationToken = default) =>
        _db.ExportColumnFamilyAsync(_cfd, exportDirectory, synchronous: false, cancellationToken);
}

public sealed partial class RocksDatabase
{
    /// <summary>
    /// Creates column family <paramref name="name"/> from exported table files (DB::CreateColumnFamilyWithImport). The
    /// files keep their levels and sequence numbers (the DB's latest sequence number is raised to cover them); they are
    /// copied, or hard-linked and the originals deleted with <see cref="ImportColumnFamilyOptions.MoveFiles"/>. The
    /// comparator of <paramref name="options"/> must have the exported comparator's name (else InvalidArgument). On
    /// failure the new column family is dropped again.
    /// </summary>
    public ColumnFamily CreateColumnFamilyWithImport(string name, ColumnFamilyOptions options, ImportColumnFamilyOptions importOptions,
        ExportImportFilesMetadata metadata) =>
        CreateColumnFamilyWithImport(name, options, importOptions, [metadata]);

    /// <summary>
    /// <see cref="CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, ExportImportFilesMetadata)"/>
    /// merging several exports (e.g. of different databases' column families) whose user-key ranges must not overlap
    /// (else InvalidArgument "CFs have overlapping ranges").
    /// </summary>
    public ColumnFamily CreateColumnFamilyWithImport(string name, ColumnFamilyOptions options, ImportColumnFamilyOptions importOptions,
        IReadOnlyList<ExportImportFilesMetadata> metadatas) =>
        Handle(_db.CreateColumnFamilyWithImportAsync(name, options, importOptions, metadatas, synchronous: true, CancellationToken.None)
            .GetAwaiter().GetResult());

    /// <summary>Async <see cref="CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, ExportImportFilesMetadata)"/>.</summary>
    public ValueTask<ColumnFamily> CreateColumnFamilyWithImportAsync(string name, ColumnFamilyOptions options, ImportColumnFamilyOptions importOptions,
        ExportImportFilesMetadata metadata, CancellationToken cancellationToken = default) =>
        CreateColumnFamilyWithImportAsync(name, options, importOptions, [metadata], cancellationToken);

    /// <summary>Async <see cref="CreateColumnFamilyWithImport(string, ColumnFamilyOptions, ImportColumnFamilyOptions, IReadOnlyList{ExportImportFilesMetadata})"/>.</summary>
    public async ValueTask<ColumnFamily> CreateColumnFamilyWithImportAsync(string name, ColumnFamilyOptions options, ImportColumnFamilyOptions importOptions,
        IReadOnlyList<ExportImportFilesMetadata> metadatas, CancellationToken cancellationToken = default) =>
        Handle(await _db.CreateColumnFamilyWithImportAsync(name, options, importOptions, metadatas, synchronous: false, cancellationToken)
            .ConfigureAwait(false));
}
