using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// The <see cref="WideColumnBlobResolver"/> handed to <see cref="CompactionFilter.FilterV4"/> for an entity with
/// blob-backed columns. Port of CompactionBlobResolver (db/compaction/compaction_iterator.{h,cc}): blob columns are read
/// on demand through the compaction's blob fetcher and cached per entity, inline columns are returned as they are, and
/// the first resolution error is kept (sticky) so the compaction fails after the filter returns even if the filter
/// swallowed the exception. Each resolved blob lives in its own pooled array, so spans handed out stay valid until
/// <see cref="Clear"/> (the end of the FilterV4 call). One instance per compaction iterator; not thread-safe.
/// </summary>
internal sealed class CompactionBlobResolver : WideColumnBlobResolver, IDisposable
{
    private readonly CompactionBlobProcessor? _processor;
    private readonly PooledBufferWriter _userKey = new(64);
    private readonly PooledBufferWriter _fetch = new(256);
    private WideColumns? _columns;
    // Resolved cache: (column index, rented buffer, length). Typical entities have few blob columns, so a linear scan
    // is cheaper than a dictionary (like upstream's resolved_cache_ vector).
    private readonly List<(int Index, byte[] Buffer, int Length)> _resolved = new(4);

    public CompactionBlobResolver(CompactionBlobProcessor? processor) => _processor = processor;

    /// <summary>The first resolution error since the last <see cref="Reset"/> (CompactionBlobResolver::resolve_status), or null.</summary>
    public RocksDbException? ResolveError { get; private set; }

    /// <summary>Binds the resolver to the entity in <paramref name="columns"/> (blob columns hold their blob index).</summary>
    public void Reset(ReadOnlySpan<byte> userKey, WideColumns columns)
    {
        Clear();
        _userKey.Write(userKey);
        _columns = columns;
    }

    /// <summary>Releases the resolved values (end of the FilterV4 call).</summary>
    public void Clear()
    {
        foreach ((int _, byte[] buf, int _) in _resolved) ArrayPool<byte>.Shared.Return(buf);
        _resolved.Clear();
        _userKey.Clear();
        _columns = null;
        ResolveError = null;
    }

    public override int ColumnCount => _columns?.Count ?? 0;

    public override bool IsBlobColumn(int columnIndex)
    {
        WideColumns? c = _columns;
        return c is not null && (uint)columnIndex < (uint)c.Count && c.IsBlobIndex(columnIndex);
    }

    public override ReadOnlySpan<byte> ResolveColumn(int columnIndex)
    {
        WideColumns? c = _columns;
        if (c is null || (uint)columnIndex >= (uint)c.Count)
        {
            throw Track(new RocksDbException(StatusCode.InvalidArgument, "Column index out of bounds"));
        }
        if (!c.IsBlobIndex(columnIndex)) return c.GetValue(columnIndex);
        foreach ((int index, byte[] buf, int len) in _resolved)
        {
            if (index == columnIndex) return buf.AsSpan(0, len);
        }
        if (_processor is null) throw Track(new RocksDbException(StatusCode.NotSupported, "Blob fetcher not available"));
        _fetch.Clear();
        try
        {
            // BlobSource.GetBlob handles inlined blob indexes (legacy stacked BlobDB TTL entries) without I/O.
            _processor.FetchBlobCounted(_userKey.WrittenSpan, c.GetValue(columnIndex), _fetch);
        }
        catch (RocksDbException e)
        {
            throw Track(e);
        }
        int n = _fetch.WrittenCount;
        byte[] copy = ArrayPool<byte>.Shared.Rent(Math.Max(1, n));
        _fetch.WrittenSpan.CopyTo(copy);
        _resolved.Add((columnIndex, copy, n));
        return copy.AsSpan(0, n);
    }

    private RocksDbException Track(RocksDbException e)
    {
        ResolveError ??= e;
        return e;
    }

    public void Dispose()
    {
        Clear();
        _userKey.Dispose();
        _fetch.Dispose();
    }
}
