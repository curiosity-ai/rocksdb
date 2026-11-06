using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>
/// Reads a single SST file outside of a DB (rocksdb::SstFileReader, include/rocksdb/sst_file_reader.h,
/// table/sst_file_reader.cc): user-key iteration with DBIter semantics (newest visible version of each key, deletions
/// hidden, merges applied), point lookups, checksum verification and the table properties. The column family options
/// supply the comparator, merge operator, prefix extractor and table options the file was written with.
/// </summary>
/// <remarks>
/// Like upstream, the file's range tombstones are not applied by <see cref="NewIterator"/>, <see cref="Get"/> or
/// <see cref="MultiGet"/>, and values stored in blob files are not resolved (blob indexes are NotSupported). Files with
/// a global sequence number (ingested files) can be opened. Thread-safe for concurrent reads; dispose to close the file.
/// </remarks>
public sealed class SstFileReader : IDisposable
{
    private readonly ColumnFamilyOptions _options;
    private readonly IFileSystem _fs;
    private TableReader? _reader;
    private InternalKeyComparator _icmp = InternalKeyComparator.Bytewise;

    /// <summary>A reader for files written with <paramref name="options"/>, on the local file system.</summary>
    public SstFileReader(ColumnFamilyOptions options)
        : this(options, LocalFileSystem.Instance)
    {
    }

    /// <summary>A reader for files written with <paramref name="options"/>, read through <paramref name="fileSystem"/>.</summary>
    public SstFileReader(ColumnFamilyOptions options, IFileSystem fileSystem)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(fileSystem);
        _options = options;
        _fs = fileSystem;
    }

    /// <summary>
    /// SstFileReader::Open: opens the table at <paramref name="filePath"/> (footer, meta blocks, index and filter).
    /// Corruption / NotSupported for unreadable files. A reader can be opened once.
    /// </summary>
    public void Open(string filePath)
    {
        ArgumentNullException.ThrowIfNull(filePath);
        if (_reader is not null) throw new InvalidOperationException("The SstFileReader is already open.");
        // Allow open file with global sequence number for backward compatibility (largest_seqno = kMaxSequenceNumber).
        TableReaderOptions ro = TableFactory.CreateReaderOptions(_options, TableFactory.CreateFormatOptions(_options.TableOptions)) with
        {
            LargestSeqno = DbFormat.MaxSequenceNumber,
        };
        IRandomAccessFile f = _fs.OpenRandomAccess(filePath);
        try
        {
            _reader = TableReaderFactory.Open(f, ro);
        }
        catch
        {
            f.Dispose();
            throw;
        }
        _icmp = _reader.InternalComparator;
    }

    private TableReader Reader => _reader ?? throw new InvalidOperationException("The SstFileReader is not open.");

    /// <summary>SstFileReader::GetTableProperties.</summary>
    public TableProperties GetTableProperties() => Reader.Properties;

    /// <summary>
    /// SstFileReader::NewIterator: an iterator over the user keys of the file, as of <see cref="ReadOptions.Snapshot"/>'s
    /// sequence number (every entry otherwise), honouring bounds and prefix seek options. Dispose it before the reader.
    /// </summary>
    public Iterator NewIterator(ReadOptions? options)
    {
        TableReader r = Reader;
        ulong sequence = options?.Snapshot is { } snap ? snap.SequenceNumber : DbFormat.MaxSequenceNumber;
        InternalIterator internalIter = r.NewInternalIterator(TableReadFlagsExtensions.FromReadOptions(options), options, _options.PrefixExtractor);
        try
        {
            return new Iterator(new DbIter(_icmp, internalIter, sequence, options, _options.MergeOperator, rangeDel: null,
                _options.MaxSequentialSkipInIterations, cleanup: null, _options.PrefixExtractor));
        }
        catch
        {
            internalIter.Dispose();
            throw;
        }
    }

    /// <summary>
    /// SstFileReader::Get: the value of <paramref name="key"/> in the file (merges applied when a base value or deletion
    /// ends the chain), or null if it is absent or deleted. A merge chain without a base fails with MergeInProgress.
    /// </summary>
    public byte[]? Get(ReadOptions? options, ReadOnlySpan<byte> key)
    {
        GetContext ctx = GetContext.Rent(_options.MergeOperator);
        try
        {
            return Lookup(options, key, ctx) ? ctx.Value.WrittenSpan.ToArray() : null;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>
    /// SstFileReader::MultiGet: looks up every key of <paramref name="batch"/>; per-key outcomes (Found, NotFound, or
    /// Error with MergeInProgress / Corruption) are read from the batch.
    /// </summary>
    public void MultiGet(ReadOptions? options, MultiGetBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        Comparator ucmp = Reader.InternalComparator.UserComparator;
        batch.BeginExternalRun(ucmp);
        GetContext ctx = GetContext.Rent(_options.MergeOperator);
        try
        {
            for (int k = 0; k < batch.Count; k++)
            {
                ctx.Reset(_options.MergeOperator);
                try
                {
                    bool found = Lookup(options, batch.UserKey(k), ctx);
                    batch.SetExternalResult(k, found, found ? ctx.Value.WrittenSpan : default);
                }
                catch (RocksDbException e)
                {
                    batch.SetError(k, e);
                }
            }
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    private static readonly TableEntryVisitor<GetContext> s_saveValue =
        static (ref GetContext ctx, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => ctx.SaveValue(k, v);

    /// <summary>SstFileReader::Rep::Get: table Get at kMaxSequenceNumber, then the final GetContext state.</summary>
    [System.Runtime.CompilerServices.SkipLocalsInit]
    private bool Lookup(ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        TableReader r = Reader;
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, DbFormat.MaxSequenceNumber, scratch);
        try
        {
            r.Get(lkey.InternalKey, ref ctx, s_saveValue, TableReadFlagsExtensions.FromReadOptions(options));
        }
        finally
        {
            lkey.Dispose();
        }
        switch (ctx.State)
        {
            case GetContext.GetState.Merge:
                throw new RocksDbException(StatusCode.MergeInProgress, "Merge in progress: ");
            case GetContext.GetState.Corrupt:
                ThrowHelper.Corruption("corrupted entry for key");
                break;
        }
        // GetContext::kUnexpectedBlobIndex (no blob fetcher): Corruption.
        if (ctx.NeedsBlobResolution) ThrowHelper.Corruption("Encounter unexpected blob index.");
        if (!ctx.TryResolve(key, out _)) return false;
        if (ctx.ValueIsEntity) ctx.ReplaceEntityWithDefaultColumn();
        return true;
    }

    /// <summary>
    /// SstFileReader::VerifyChecksum: verifies the checksums of the meta blocks and every data block (block-based
    /// tables only; NotSupported for other formats, like TableReader::VerifyChecksum).
    /// </summary>
    public void VerifyChecksum()
    {
        if (Reader is not BlockBasedTableReader bbt) throw new RocksDbException(StatusCode.NotSupported, "Not implemented: VerifyChecksum() not supported");
        bbt.VerifyChecksum();
    }

    /// <summary>
    /// SstFileReader::VerifyNumEntries: Corruption unless a scan of the file returns num_entries - num_range_deletions
    /// entries (table properties).
    /// </summary>
    public void VerifyNumEntries()
    {
        TableReader r = Reader;
        ulong numRead = 0;
        using (InternalIterator it = r.NewInternalIterator(TableReadFlags.DontFillCache, null, _options.PrefixExtractor))
        {
            for (it.SeekToFirst(); it.Valid; it.Next()) numRead++;
        }
        TableProperties tp = r.Properties;
        ulong expected = tp.NumEntries - tp.NumRangeDeletions;
        if (numRead != expected)
        {
            ThrowHelper.Corruption("Table property expects " + expected.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " entries when excluding range deletions, but scanning the table returned "
                + numRead.ToString(System.Globalization.CultureInfo.InvariantCulture) + " entries");
        }
    }

    public void Dispose()
    {
        _reader?.Dispose();
        _reader = null;
    }
}
