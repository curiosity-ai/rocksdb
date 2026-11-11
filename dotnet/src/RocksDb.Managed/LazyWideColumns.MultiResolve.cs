using RocksDb.Managed.Compression;
using RocksDb.Managed.Db.Wide;

namespace RocksDb.Managed;

/// <summary>
/// EXPERIMENTAL. One byte-range read of a column of a lazy wide-column result (rocksdb::LazyColumnReadRequest,
/// include/rocksdb/lazy_wide_columns.h), for <see cref="LazyWideColumns.MultiResolve"/> and
/// <see cref="LazyWideColumnsBatch.MultiResolve"/>. Inputs: the target (<see cref="Entry"/> = index of the result in a
/// batch, ignored by a single result; <see cref="Column"/> = column index), the range of the column's logical value
/// (<see cref="Offset"/>, <see cref="Length"/>; <see cref="WholeColumn"/> or any negative length reads to the end; the
/// range is clamped, an offset at or past the end yields an empty result) and <see cref="ForceVerify"/>. Outputs, set by
/// the resolve call: <see cref="Result"/> and <see cref="Error"/>.
/// <para>
/// A strict sub-range of an unresolved, uncompressed blob-file reference is read partially (only those bytes, no
/// checksum verification, no blob cache insertion) unless <see cref="ForceVerify"/> is set; every other read resolves
/// the whole column (cached in its result, verified under the originating read's verify_checksums) and slices the
/// range out of it.
/// </para>
/// </summary>
public struct LazyColumnReadRequest
{
    /// <summary>kLazyWholeColumn: read from <see cref="Offset"/> to the end of the column.</summary>
    public const long WholeColumn = -1;

    /// <summary>A whole-column read of column <paramref name="column"/> of a single result.</summary>
    public LazyColumnReadRequest(int column)
    {
        Column = column;
        Length = WholeColumn;
    }

    /// <summary>A range read of column <paramref name="column"/> of a single result.</summary>
    public LazyColumnReadRequest(int column, ulong offset, long length)
    {
        Column = column;
        Offset = offset;
        Length = length;
    }

    /// <summary>A range read of column <paramref name="column"/> of result <paramref name="entry"/> of a batch.</summary>
    public LazyColumnReadRequest(int entry, int column, ulong offset, long length)
    {
        Entry = entry;
        Column = column;
        Offset = offset;
        Length = length;
    }

    /// <summary>The index of the target result in a <see cref="LazyWideColumnsBatch"/> (ignored by <see cref="LazyWideColumns.MultiResolve"/>).</summary>
    public int Entry { get; set; }

    /// <summary>The index of the target column in its result.</summary>
    public int Column { get; set; }

    /// <summary>Start of the range in the column's logical (uncompressed) value.</summary>
    public ulong Offset { get; set; }

    /// <summary>Length of the range; negative (<see cref="WholeColumn"/>) reads to the end.</summary>
    public long Length { get; set; }

    /// <summary>Verify the record checksum even for a partial read or with verify_checksums off (force_verify).</summary>
    public bool ForceVerify { get; set; }

    /// <summary>
    /// Output: the requested bytes. A view into the result's cached column (valid until the result is reset, reused or
    /// disposed) or, for a partial read, a buffer of its own. Empty when <see cref="Error"/> is set.
    /// </summary>
    public ReadOnlyMemory<byte> Result { readonly get; internal set; }

    /// <summary>
    /// Output: the read's failure, or null. InvalidArgument for a column or entry that is not part of the target;
    /// Incomplete for a blob missing from the cache when the originating read used <see cref="ReadTier.BlockCacheTier"/>;
    /// otherwise the I/O or corruption error of the read.
    /// </summary>
    public RocksDbException? Error { readonly get; internal set; }

    /// <summary>True if the read succeeded.</summary>
    public readonly bool Succeeded => Error is null;
}

public sealed partial class LazyWideColumns
{
    /// <summary>How one read is served (ReadPathBlobResolver::LazyColumnReadPlan).</summary>
    internal enum ReadPlan : byte
    {
        /// <summary>Inline, already resolved, force-verified or version-checked: resolved on its own.</summary>
        ServeIndividually,
        /// <summary>The whole blob of a version blob file (deduplicated and ordered by a batch).</summary>
        FetchWhole,
        /// <summary>A partial read of an uncompressed blob of a version blob file.</summary>
        FetchRange,
    }

    /// <summary>
    /// Resolves several reads of this result's columns (LazyWideColumns::MultiResolve). Each read's
    /// <see cref="LazyColumnReadRequest.Result"/> / <see cref="LazyColumnReadRequest.Error"/> are set independently;
    /// a column's blob is fetched at most once over the result's lifetime, so several reads of one column cost one
    /// fetch. To resolve across many keys, use <see cref="LazyWideColumnsBatch.MultiResolve"/>.
    /// </summary>
    public void MultiResolve(Span<LazyColumnReadRequest> reads)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        for (int i = 0; i < reads.Length; i++) ResolveRead(ref reads[i]);
    }

    /// <summary>LazyWideColumns::Rep::ResolveOneRead: validates the column, then serves the read (errors go to the read).</summary>
    internal void ResolveRead(ref LazyColumnReadRequest read)
    {
        read.Result = default;
        read.Error = null;
        if ((uint)read.Column >= (uint)_slots.Count)
        {
            read.Error = new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Column does not belong to this LazyWideColumns");
            return;
        }
        try
        {
            ThrowIfReleased();
            ReadPlan plan = Classify(read.Column, read.Offset, read.Length, read.ForceVerify, out ulong rangeOffset, out int rangeLength);
            if (plan == ReadPlan.FetchRange)
            {
                // A partial read: only the requested bytes, into storage of the result's own.
                byte[] buffer = new byte[rangeLength];
                int n = ReadColumnRange(read.Column, rangeOffset, buffer);
                read.Result = buffer.AsMemory(0, n);
                return;
            }
            ref ColumnInfo c = ref _info[read.Column];
            // ReadPathBlobResolver::ResolveColumnRange: an empty strict sub-range (offset at or past the end, or a zero
            // length) of a reference that would be read partially is empty without any I/O.
            if (c.Kind == ColumnKind.BlobFile && c.Resolved is null && !read.ForceVerify && c.Compression == CompressionType.None
                && (read.Offset >= c.Size || read.Length == 0) && (read.Offset > 0 || c.Size > 0)
                && _version!.GetBlobFileMetaData(c.FileNumber) is not null)
            {
                return;
            }
            WideColumnSlot s = _slots[read.Column];
            ReadOnlyMemory<byte> whole;
            if (c.Kind == ColumnKind.Inline) whole = _entity.WrittenMemory.Slice(s.ValueOffset, s.ValueLength);
            else if (c.Kind == ColumnKind.InlinedBlob) whole = _entity.WrittenMemory.Slice(c.InlinedOffset, c.InlinedLength);
            else
            {
                if (c.Resolved is null)
                {
                    if (read.ForceVerify) ResolveVerified(read.Column, s);
                    else FetchWhole(read.Column, s);
                }
                whole = _info[read.Column].Resolved.AsMemory(0, _info[read.Column].ResolvedLength);
            }
            read.Result = Clamp(whole, read.Offset, read.Length);
        }
        catch (RocksDbException e)
        {
            read.Result = default;
            read.Error = e;
        }
    }

    private static ReadOnlyMemory<byte> Clamp(ReadOnlyMemory<byte> whole, ulong offset, long length)
    {
        if (offset >= (ulong)whole.Length) return ReadOnlyMemory<byte>.Empty;
        ReadOnlyMemory<byte> range = whole[(int)offset..];
        return length >= 0 && length < range.Length ? range[..(int)length] : range;
    }

    /// <summary>
    /// ReadPathBlobResolver::ClassifyColumnRange: a strict sub-range of an unresolved, uncompressed reference to a blob
    /// file of the version is a partial read (<paramref name="rangeOffset"/>, <paramref name="rangeLength"/>); any other
    /// read of such a reference fetches the whole blob; everything else is served individually. REQUIRES: valid column.
    /// </summary>
    internal ReadPlan Classify(int index, ulong offset, long length, bool forceVerify, out ulong rangeOffset, out int rangeLength)
    {
        rangeOffset = 0;
        rangeLength = 0;
        ref ColumnInfo c = ref _info[index];
        if (c.Kind != ColumnKind.BlobFile || c.Resolved is not null || forceVerify) return ReadPlan.ServeIndividually;
        if (_version!.GetBlobFileMetaData(c.FileNumber) is null) return ReadPlan.ServeIndividually;
        if (c.Compression == CompressionType.None)
        {
            if (offset >= c.Size) return ReadPlan.ServeIndividually; // empty result, no I/O
            ulong remaining = c.Size - offset;
            ulong len = length < 0 || (ulong)length > remaining ? remaining : (ulong)length;
            if (len == 0) return ReadPlan.ServeIndividually;
            if ((offset != 0 || len < c.Size) && len <= int.MaxValue)
            {
                rangeOffset = offset;
                rangeLength = (int)len;
                return ReadPlan.FetchRange;
            }
        }
        return ReadPlan.FetchWhole;
    }

    /// <summary>The blob file and record offset of a blob-file reference column (for ordering and deduplication), else false.</summary>
    internal bool TryGetBlobLocation(int index, out ulong fileNumber, out ulong offset)
    {
        ref ColumnInfo c = ref _info[index];
        fileNumber = c.FileNumber;
        offset = c.Offset;
        return c.Kind == ColumnKind.BlobFile;
    }

    /// <summary>A coalesced whole fetch of a batch: reads and caches the column's blob; returns the failure, if any.</summary>
    internal RocksDbException? FetchWholeForBatch(int index)
    {
        try
        {
            ThrowIfReleased();
            if (_info[index].Resolved is null) FetchWhole(index, _slots[index]);
            return null;
        }
        catch (RocksDbException e)
        {
            return e;
        }
    }
}

public sealed partial class LazyWideColumnsBatch
{
    /// <summary>
    /// Cross-key batch resolution (LazyWideColumnsBatch::MultiResolve): resolves reads that may span any results of
    /// this batch (<see cref="LazyColumnReadRequest.Entry"/>) in one call. Like upstream, the reads are planned first:
    /// whole-blob fetches are deduplicated per (result, column), and the whole and partial blob reads are dispatched
    /// grouped by blob file in file-offset order; the reads of a column with a <see cref="LazyColumnReadRequest.ForceVerify"/>
    /// read anywhere in the call (or of the same blob record through another result) are resolved individually so that
    /// verification is never skipped by a shared fetch. A read targeting an entry that was not found, or a column out of
    /// range, fails with InvalidArgument; per-read outcomes are in each request.
    /// </summary>
    public void MultiResolve(Span<LazyColumnReadRequest> reads)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // Columns (and physical blob records) with a force_verify read: resolved individually.
        HashSet<(int Entry, int Column)>? forceVerifyColumns = null;
        HashSet<(ulong File, ulong Offset)>? forceVerifyBlobs = null;
        for (int i = 0; i < reads.Length; i++)
        {
            ref LazyColumnReadRequest r = ref reads[i];
            if (!r.ForceVerify || !IsValidTarget(r.Entry, r.Column)) continue;
            (forceVerifyColumns ??= new()).Add((r.Entry, r.Column));
            if (_entries[r.Entry].TryGetBlobLocation(r.Column, out ulong file, out ulong off)) (forceVerifyBlobs ??= new()).Add((file, off));
        }

        // Pass 1: reset outputs, validate, classify.
        var wholeFetches = new List<(int Entry, int Column, ulong File, ulong Offset)>();
        var wholeIndex = new Dictionary<(int, int), int>();
        var wholeBindings = new List<(int Fetch, int Read)>();
        var rangeFetches = new List<(int Read, ulong File, ulong Offset)>();
        var individual = new List<int>();
        for (int i = 0; i < reads.Length; i++)
        {
            ref LazyColumnReadRequest r = ref reads[i];
            r.Result = default;
            r.Error = null;
            if (!IsValidTarget(r.Entry, r.Column))
            {
                r.Error = new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Column does not belong to this batch");
                continue;
            }
            LazyWideColumns entry = _entries[r.Entry];
            if (forceVerifyColumns is not null && forceVerifyColumns.Contains((r.Entry, r.Column)))
            {
                individual.Add(i);
                continue;
            }
            LazyWideColumns.ReadPlan plan = entry.Classify(r.Column, r.Offset, r.Length, r.ForceVerify, out _, out _);
            entry.TryGetBlobLocation(r.Column, out ulong file, out ulong off);
            if (plan != LazyWideColumns.ReadPlan.ServeIndividually && forceVerifyBlobs is not null && forceVerifyBlobs.Contains((file, off)))
            {
                individual.Add(i);
                continue;
            }
            switch (plan)
            {
                case LazyWideColumns.ReadPlan.FetchWhole:
                    if (!wholeIndex.TryGetValue((r.Entry, r.Column), out int idx))
                    {
                        idx = wholeFetches.Count;
                        wholeIndex.Add((r.Entry, r.Column), idx);
                        wholeFetches.Add((r.Entry, r.Column, file, off));
                    }
                    wholeBindings.Add((idx, i));
                    break;
                case LazyWideColumns.ReadPlan.FetchRange:
                    rangeFetches.Add((i, file, off));
                    break;
                default:
                    individual.Add(i);
                    break;
            }
        }

        // Pass 2: dispatch the blob reads grouped by file, in offset order.
        var fetchOrder = new int[wholeFetches.Count];
        for (int i = 0; i < fetchOrder.Length; i++) fetchOrder[i] = i;
        Array.Sort(fetchOrder, (a, b) =>
        {
            int c = wholeFetches[a].File.CompareTo(wholeFetches[b].File);
            return c != 0 ? c : wholeFetches[a].Offset.CompareTo(wholeFetches[b].Offset);
        });
        var fetchErrors = new RocksDbException?[wholeFetches.Count];
        foreach (int f in fetchOrder) fetchErrors[f] = _entries[wholeFetches[f].Entry].FetchWholeForBatch(wholeFetches[f].Column);
        rangeFetches.Sort(static (a, b) =>
        {
            int c = a.File.CompareTo(b.File);
            return c != 0 ? c : a.Offset.CompareTo(b.Offset);
        });
        foreach ((int read, _, _) in rangeFetches) _entries[reads[read].Entry].ResolveRead(ref reads[read]);

        // Passes 3 + 4: the whole reads slice their range out of the (now cached) column.
        foreach ((int fetch, int read) in wholeBindings)
        {
            if (fetchErrors[fetch] is { } e)
            {
                reads[read].Error = e;
                continue;
            }
            _entries[reads[read].Entry].ResolveRead(ref reads[read]);
        }

        // Pass 5: the individual reads.
        foreach (int read in individual) _entries[reads[read].Entry].ResolveRead(ref reads[read]);
    }

    private bool IsValidTarget(int entry, int column) =>
        (uint)entry < (uint)_count && _status[entry] == MultiGetStatus.Found && (uint)column < (uint)_entries[entry].Count;
}
