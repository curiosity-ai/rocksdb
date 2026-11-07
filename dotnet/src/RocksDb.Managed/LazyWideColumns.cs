using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// The super version reference of a lazy result (the Cleanable "pin" of LazyWideColumns::Rep / the per-column-family
/// pin of LazyWideColumnsBatch): keeps the version's blob files alive while deferred reads are possible.
/// <para>
/// A pin is owned by one result (<see cref="LazyWideColumns"/> or <see cref="LazyWideColumnsBatch"/>) and re-armed by
/// <see cref="Rent"/> for its next read once released, so repeated reads into a reused result do not allocate. Only the
/// owner and the owner's entries reference it, and they drop the reference when the pin is released.
/// </para>
/// </summary>
internal sealed class LazyReadPin
{
    // Cleared on release so a reset result that is kept for reuse does not keep the old super version (memtables,
    // version) or the database reachable.
    private DbImpl? _db;
    private SuperVersion? _sv;
    private int _released;

    private LazyReadPin(DbImpl db, SuperVersion sv)
    {
        _db = db;
        _sv = sv;
    }

    /// <summary>The referenced super version; only valid while the pin is not <see cref="Released"/>.</summary>
    public SuperVersion SuperVersion => _sv!;
    public bool Released => Volatile.Read(ref _released) != 0;

    /// <summary>
    /// A pin holding <paramref name="sv"/> (already referenced by the caller): <paramref name="cached"/> re-armed if it
    /// is released, else a new pin stored in <paramref name="cached"/>.
    /// </summary>
    public static LazyReadPin Rent(ref LazyReadPin? cached, DbImpl db, SuperVersion sv)
    {
        LazyReadPin? p = cached;
        if (p is null || !p.Released)
        {
            // First read, or the previous pin is still live (cannot happen for a reset owner): never re-arm a live pin.
            return cached = new LazyReadPin(db, sv);
        }
        p._db = db;
        p._sv = sv;
        Volatile.Write(ref p._released, 0);
        return p;
    }

    public void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0) return;
        DbImpl db = _db!;
        SuperVersion sv = _sv!;
        _db = null;
        _sv = null;
        db.ReturnSuperVersion(sv);
    }
}

/// <summary>
/// EXPERIMENTAL (like upstream). The result of a lazy wide-column read (rocksdb::LazyWideColumns,
/// include/rocksdb/lazy_wide_columns.h, db/wide/lazy_wide_columns.cc + read_path_blob_resolver.cc): the columns of an
/// entity, sorted by name, whose inline values are available immediately and whose blob-backed columns are
/// <em>references</em> read from storage only when pulled, in whole (<see cref="ResolveColumn"/>) or by byte range
/// (<see cref="ReadColumnRange(int, ulong, Span{byte})"/>). Columns that are never pulled are never read.
/// <para>
/// Unlike <see cref="WideColumns"/>, the result keeps the data it may still read alive: it holds a reference to the
/// column family's super version (like an iterator), released by <see cref="Dispose"/> / <see cref="Reset"/>. Dispose
/// it promptly and before the database. Reusable: pass the same instance to repeated
/// <see cref="ColumnFamily.TryGetEntityLazy(ReadOptions?, ReadOnlySpan{byte}, LazyWideColumns)"/> calls. Not
/// thread-safe.
/// </para>
/// <para>
/// Resolution uses the <see cref="ReadOptions.VerifyChecksums"/>, <see cref="ReadOptions.FillCache"/> and
/// <see cref="ReadOptions.ReadTier"/> of the originating read. A whole-column read is cached in the result, so a blob is
/// fetched at most once. A strict sub-range of an uncompressed blob reference reads only those bytes (no checksum
/// verification, no blob cache insertion) unless <c>forceVerify</c> is set; any other range read resolves the whole
/// column and copies the range out of it.
/// </para>
/// </summary>
public sealed partial class LazyWideColumns : IDisposable
{
    private enum ColumnKind : byte { Inline, BlobFile, InlinedBlob }

    private struct ColumnInfo
    {
        public ColumnKind Kind;
        public CompressionType Compression;
        public ulong FileNumber;
        public ulong Offset;
        public ulong Size;
        // Inlined blob index (TTL): position of its value inside the entity buffer.
        public int InlinedOffset;
        public int InlinedLength;
        public byte[]? Resolved;
        public int ResolvedLength;
    }

    private readonly PooledBufferWriter _entity = new(256);
    private readonly PooledBufferWriter _key = new(64);
    private readonly PooledBufferWriter _fetch = new(256);
    private readonly List<WideColumnSlot> _slots = new(8);
    private ColumnInfo[] _info = new ColumnInfo[8];
    private LazyReadPin? _pin;
    // The pin this result owns between reads (re-armed by TryGetEntityLazy, see LazyReadPin.Rent).
    private LazyReadPin? _cachedPin;
    private bool _ownsPin;
    private BlobSource? _source;
    private VersionStorageInfo? _version;
    private Statistics? _stats;
    private readonly ReadOptions _readOptions = new();
    private bool _disposed;

    public LazyWideColumns() { }

    /// <summary>Number of columns (0 for an empty / not found result).</summary>
    public int Count => _slots.Count;

    /// <summary>True if the result holds no columns.</summary>
    public bool IsEmpty => _slots.Count == 0;

    /// <summary>The name of column <paramref name="index"/> (columns are sorted by name). No I/O.</summary>
    public ReadOnlySpan<byte> GetName(int index)
    {
        WideColumnSlot s = Slot(index);
        return _entity.WrittenSpan.Slice(s.NameOffset, s.NameLength);
    }

    /// <summary>True if column <paramref name="index"/> is an unresolved blob reference (LazyWideColumn::is_reference). No I/O.</summary>
    public bool IsReference(int index)
    {
        Slot(index);
        return _info[index].Kind != ColumnKind.Inline;
    }

    /// <summary>The inline value of column <paramref name="index"/> (false for a blob reference). No I/O.</summary>
    public bool TryGetInlineValue(int index, out ReadOnlySpan<byte> value)
    {
        WideColumnSlot s = Slot(index);
        if (_info[index].Kind != ColumnKind.Inline)
        {
            value = default;
            return false;
        }
        value = _entity.WrittenSpan.Slice(s.ValueOffset, s.ValueLength);
        return true;
    }

    /// <summary>
    /// The logical (uncompressed) size of column <paramref name="index"/> when known without I/O
    /// (LazyWideColumn::logical_size): always for inline columns and uncompressed references, null for compressed
    /// references.
    /// </summary>
    public ulong? GetLogicalSize(int index)
    {
        WideColumnSlot s = Slot(index);
        ref ColumnInfo c = ref _info[index];
        return c.Kind switch
        {
            ColumnKind.Inline => (ulong)s.ValueLength,
            ColumnKind.InlinedBlob => (ulong)c.InlinedLength,
            _ => c.Compression == CompressionType.None ? c.Size : null,
        };
    }

    /// <summary>The index of the column named <paramref name="name"/> (binary search), or -1.</summary>
    public int IndexOf(ReadOnlySpan<byte> name)
    {
        int lo = 0, hi = _slots.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = GetName(mid).SequenceCompareTo(name);
            if (c == 0) return mid;
            if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -1;
    }

    /// <summary>
    /// The whole value of column <paramref name="index"/> (LazyWideColumns::ResolveColumn): the inline value, or the
    /// blob read once and cached in this result (verified under <see cref="ReadOptions.VerifyChecksums"/>). Valid until
    /// the result is reset, reused or disposed. Throws Incomplete for a blob not in the cache when the originating read
    /// used <see cref="ReadTier.BlockCacheTier"/>, Corruption / IOError for read failures.
    /// </summary>
    public ReadOnlySpan<byte> ResolveColumn(int index)
    {
        WideColumnSlot s = Slot(index);
        ThrowIfReleased();
        ref ColumnInfo c = ref _info[index];
        switch (c.Kind)
        {
            case ColumnKind.Inline:
                return _entity.WrittenSpan.Slice(s.ValueOffset, s.ValueLength);
            case ColumnKind.InlinedBlob:
                return _entity.WrittenSpan.Slice(c.InlinedOffset, c.InlinedLength);
        }
        if (c.Resolved is null) FetchWhole(index, s);
        return c.Resolved.AsSpan(0, c.ResolvedLength);
    }

    /// <summary>Async <see cref="ResolveColumn"/>: a blob missing from the cache is read with async I/O.</summary>
    public async ValueTask<ReadOnlyMemory<byte>> ResolveColumnAsync(int index, CancellationToken cancellationToken = default)
    {
        WideColumnSlot s = Slot(index);
        ThrowIfReleased();
        ColumnInfo c = _info[index];
        if (c.Kind == ColumnKind.Inline) return _entity.WrittenMemory.Slice(s.ValueOffset, s.ValueLength);
        if (c.Kind == ColumnKind.InlinedBlob) return _entity.WrittenMemory.Slice(c.InlinedOffset, c.InlinedLength);
        if (c.Resolved is null)
        {
            bool cached = _source!.IsInCache(c.FileNumber, c.Offset);
            _fetch.Clear();
            await _source.GetBlobAsync(_version!, _readOptions, _key.WrittenMemory, _entity.WrittenSpan.Slice(s.ValueOffset, s.ValueLength), _fetch,
                cancellationToken).ConfigureAwait(false);
            if (!cached) RecordRead(c.Size);
            Adopt(index);
        }
        return _info[index].Resolved.AsMemory(0, _info[index].ResolvedLength);
    }

    /// <summary>
    /// Copies bytes [<paramref name="offset"/>, <paramref name="offset"/> + <paramref name="destination"/>.Length) of
    /// column <paramref name="index"/>'s logical value into <paramref name="destination"/> (LazyWideColumns::ResolveColumnRange
    /// with a LazyColumnReadRequest). The range is clamped to the value; an offset at or past the end copies nothing.
    /// Returns the number of bytes copied.
    /// </summary>
    public int ReadColumnRange(int index, ulong offset, Span<byte> destination) => ReadColumnRange(index, offset, destination, forceVerify: false);

    /// <summary>
    /// <see cref="ReadColumnRange(int, ulong, Span{byte})"/>; with <paramref name="forceVerify"/> a partial read is
    /// replaced by a verified whole-column read (LazyColumnReadRequest::force_verify).
    /// </summary>
    public int ReadColumnRange(int index, ulong offset, Span<byte> destination, bool forceVerify)
    {
        WideColumnSlot s = Slot(index);
        ThrowIfReleased();
        ref ColumnInfo c = ref _info[index];
        // ClassifyColumnRange: only a strict sub-range of an unresolved, uncompressed reference to a blob file of the
        // version is read partially.
        if (c.Kind == ColumnKind.BlobFile && c.Resolved is null && !forceVerify && c.Compression == CompressionType.None
            && _version!.GetBlobFileMetaData(c.FileNumber) is not null)
        {
            if (offset >= c.Size) return 0;
            ulong remaining = c.Size - offset;
            int length = (ulong)destination.Length < remaining ? destination.Length : (int)remaining;
            if (length == 0) return 0;
            if (offset != 0 || (ulong)length < c.Size)
            {
                ulong read = _source!.ReadBlobRange(_readOptions, _key.WrittenSpan, c.FileNumber, c.Offset, c.Size, offset, destination[..length]);
                RecordRead(read);
                if (_stats is { } st)
                {
                    st.RecordTick(Ticker.BlobDbLazyPartialReadCount);
                    st.RecordTick(Ticker.BlobDbLazyPartialBytesSaved, c.Size - read);
                }
                return length;
            }
        }
        ReadOnlySpan<byte> whole = forceVerify && c.Kind == ColumnKind.BlobFile && c.Resolved is null ? ResolveVerified(index, s) : ResolveColumn(index);
        if (offset >= (ulong)whole.Length) return 0;
        ReadOnlySpan<byte> range = whole[(int)offset..];
        if (range.Length > destination.Length) range = range[..destination.Length];
        range.CopyTo(destination);
        return range.Length;
    }

    /// <summary>Releases the columns, the cached blobs and (for a standalone result) the super version reference.</summary>
    public void Reset()
    {
        if (_ownsPin) _pin?.Release();
        _pin = null;
        _ownsPin = false;
        _source = null;
        _version = null;
        _stats = null;
        for (int i = 0; i < _slots.Count; i++)
        {
            if (_info[i].Resolved is { } buf) RocksArrayPool<byte>.Shared.Return(buf);
            _info[i] = default;
        }
        _slots.Clear();
        _entity.Clear();
        _key.Clear();
    }

    public void Dispose()
    {
        if (_disposed) return;
        Reset();
        _disposed = true;
        _entity.Dispose();
        _key.Dispose();
        _fetch.Dispose();
    }

    // ---- filling (LazyWideColumnsHelper::Finalize / FinalizeInBatch) --------------------------------------

    /// <summary>A pin for the next standalone read into this (reset) result; reuses the previous one.</summary>
    internal LazyReadPin RentPin(DbImpl db, SuperVersion sv) => LazyReadPin.Rent(ref _cachedPin, db, sv);

    /// <summary>
    /// Fills the result from a finished lazy lookup. Returns false (result left empty) when the key does not exist.
    /// <paramref name="ownsPin"/>: a standalone result releases the pin; a batch entry leaves it to the batch.
    /// </summary>
    internal bool Fill(GetContext ctx, ReadOnlySpan<byte> userKey, ColumnFamilyData cfd, ReadOptions? options, Statistics? stats, LazyReadPin pin,
        bool ownsPin)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Reset();
        bool found;
        if (ctx.ValueEntityHasBlobColumns)
        {
            // Unresolved version 2 entity (no merge operand applied to it).
            _entity.Write(ctx.Value.WrittenSpan);
            WideColumnSerialization.DeserializeWithBlobColumns(_entity.WrittenSpan, _slots);
            found = true;
        }
        else
        {
            found = ctx.TryResolveEntity(userKey, _entity);
            if (found) WideColumnSerialization.Deserialize(_entity.WrittenSpan, _slots);
        }
        if (!found)
        {
            if (ownsPin) pin.Release();
            return false;
        }
        _pin = pin;
        _ownsPin = ownsPin;
        _source = cfd.BlobSource;
        _version = pin.SuperVersion.Current.Storage;
        _stats = stats;
        _readOptions.VerifyChecksums = options?.VerifyChecksums ?? true;
        _readOptions.FillCache = options?.FillCache ?? true;
        _readOptions.ReadTier = options?.ReadTier ?? ReadTier.ReadAllTier;
        _key.Write(userKey);
        if (_info.Length < _slots.Count) _info = new ColumnInfo[Math.Max(_slots.Count, _info.Length * 2)];
        ReadOnlySpan<byte> entity = _entity.WrittenSpan;
        for (int i = 0; i < _slots.Count; i++)
        {
            WideColumnSlot s = _slots[i];
            ref ColumnInfo c = ref _info[i];
            c = default;
            if (!s.IsBlobIndex) continue;
            BlobIndex bi = BlobIndex.Decode(entity.Slice(s.ValueOffset, s.ValueLength));
            if (bi.IsInlined)
            {
                c.Kind = ColumnKind.InlinedBlob;
                entity.Overlaps(bi.InlinedValue, out int off);
                c.InlinedOffset = off;
                c.InlinedLength = bi.InlinedValue.Length;
            }
            else
            {
                c.Kind = ColumnKind.BlobFile;
                c.FileNumber = bi.FileNumber;
                c.Offset = bi.Offset;
                c.Size = bi.Size;
                c.Compression = bi.Compression;
            }
        }
        return true;
    }

    // ---- resolution helpers ----------------------------------------------------------------------------

    private void FetchWhole(int index, WideColumnSlot s)
    {
        ref ColumnInfo c = ref _info[index];
        ReadOnlySpan<byte> blobIndex = _entity.WrittenSpan.Slice(s.ValueOffset, s.ValueLength);
        _fetch.Clear();
        if (_version!.GetBlobFileMetaData(c.FileNumber) is not null)
        {
            ulong read = _source!.GetBlob(_readOptions, _key.WrittenSpan, c.FileNumber, c.Offset, c.Size, c.Compression, _fetch);
            if (read != 0) RecordRead(read);
        }
        else
        {
            // Not a file of the version (e.g. a blob direct write file not yet flushed): the version-checking path.
            _source!.GetBlob(_version, _readOptions, _key.WrittenSpan, blobIndex, _fetch);
        }
        Adopt(index);
    }

    /// <summary>force_verify whole read: verifies the record checksum even when the originating read did not.</summary>
    private ReadOnlySpan<byte> ResolveVerified(int index, WideColumnSlot s)
    {
        bool verify = _readOptions.VerifyChecksums;
        _readOptions.VerifyChecksums = true;
        try
        {
            FetchWhole(index, s);
        }
        finally
        {
            _readOptions.VerifyChecksums = verify;
        }
        return _info[index].Resolved.AsSpan(0, _info[index].ResolvedLength);
    }

    private void Adopt(int index)
    {
        int n = _fetch.WrittenCount;
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, n));
        _fetch.WrittenSpan.CopyTo(buf);
        _info[index].Resolved = buf;
        _info[index].ResolvedLength = n;
    }

    private void RecordRead(ulong bytes)
    {
        if (_stats is not { } st) return;
        st.RecordTick(Ticker.BlobDbLazyReadCount);
        st.RecordTick(Ticker.BlobDbLazyReadBytes, bytes);
    }

    private WideColumnSlot Slot(int index)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((uint)index >= (uint)_slots.Count) throw new ArgumentOutOfRangeException(nameof(index));
        return _slots[index];
    }

    private void ThrowIfReleased()
    {
        if (_pin is null || _pin.Released) throw new ObjectDisposedException(nameof(LazyWideColumns), "The lazy result (or its batch) was released.");
    }
}

/// <summary>
/// EXPERIMENTAL. The results of one <see cref="ColumnFamily.MultiGetEntityLazy(ReadOptions?, MultiGetBatch, LazyWideColumnsBatch)"/>
/// (rocksdb::LazyWideColumnsBatch): one <see cref="LazyWideColumns"/> per key, in key order, all read from one super
/// version and sequence number, which the batch keeps referenced until it is reset or disposed (its entries are valid
/// only that long). Reusable; not thread-safe.
/// </summary>
public sealed partial class LazyWideColumnsBatch : IDisposable
{
    private readonly List<LazyWideColumns> _entries = new();
    private MultiGetStatus[] _status = [];
    private RocksDbException?[] _errors = [];
    private int _count;
    private LazyReadPin? _pin;
    // The pin this batch owns between reads (re-armed by MultiGetEntityLazy, see LazyReadPin.Rent).
    private LazyReadPin? _cachedPin;
    private bool _disposed;

    /// <summary>Number of results (the number of keys of the last read).</summary>
    public int Count => _count;

    /// <summary>The result of key <paramref name="index"/> (empty unless <see cref="GetStatus"/> is Found).</summary>
    public LazyWideColumns this[int index]
    {
        get
        {
            CheckIndex(index);
            return _entries[index];
        }
    }

    /// <summary>The outcome of key <paramref name="index"/>.</summary>
    public MultiGetStatus GetStatus(int index)
    {
        CheckIndex(index);
        return _status[index];
    }

    /// <summary>The failure of key <paramref name="index"/> (status Error), else null.</summary>
    public RocksDbException? GetError(int index)
    {
        CheckIndex(index);
        return _errors[index];
    }

    /// <summary>Releases all results and the super version reference.</summary>
    public void Reset()
    {
        for (int i = 0; i < _count; i++) _entries[i].Reset();
        _pin?.Release();
        _pin = null;
        _count = 0;
    }

    public void Dispose()
    {
        if (_disposed) return;
        Reset();
        foreach (LazyWideColumns e in _entries) e.Dispose();
        _entries.Clear();
        _disposed = true;
    }

    internal void Begin(int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Reset();
        while (_entries.Count < count) _entries.Add(new LazyWideColumns());
        if (_status.Length < count)
        {
            _status = new MultiGetStatus[count];
            _errors = new RocksDbException?[count];
        }
        Array.Clear(_status, 0, count);
        Array.Clear(_errors, 0, count);
        _count = count;
    }

    /// <summary>Stores the pin of the read started by <see cref="Begin"/> (reuses the previous, released one).</summary>
    internal LazyReadPin SetPin(DbImpl db, SuperVersion sv) => _pin = LazyReadPin.Rent(ref _cachedPin, db, sv);

    internal LazyWideColumns Entry(int index) => _entries[index];

    internal void SetStatus(int index, MultiGetStatus status, RocksDbException? error)
    {
        _status[index] = status;
        _errors[index] = error;
    }

    private void CheckIndex(int index)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if ((uint)index >= (uint)_count) throw new ArgumentOutOfRangeException(nameof(index));
    }
}
