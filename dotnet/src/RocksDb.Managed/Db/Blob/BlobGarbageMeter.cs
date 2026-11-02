using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Measures the blob garbage a compaction produces: for every blob file referenced by the compaction input, the blobs
/// (count and bytes) referenced by the input ("inflow") minus those still referenced by the output ("outflow"). Port of
/// rocksdb::BlobGarbageMeter (db/blob/blob_garbage_meter.{h,cc}). Bytes of a blob are its record size
/// (value size + key size + record header), matching BlobFileAddition's total_blob_bytes.
/// </summary>
internal sealed class BlobGarbageMeter
{
    private readonly SortedDictionary<ulong, Flow> _flows = new();

    private sealed class Flow
    {
        public ulong InCount, InBytes, OutCount, OutBytes;
    }

    /// <summary>
    /// Inflow of a whole blob file (all its blobs): with the outflow of a flush output, the garbage of the blob direct
    /// write files the flush registers is their blobs the output does not reference (dropped hidden versions, failed
    /// writes).
    /// </summary>
    public void AddInFlow(ulong blobFileNumber, ulong count, ulong bytes)
    {
        if (!_flows.TryGetValue(blobFileNumber, out Flow? f)) _flows[blobFileNumber] = f = new Flow();
        f.InCount += count;
        f.InBytes += bytes;
    }

    /// <summary>BlobGarbageMeter::ProcessInFlow. Throws Corruption for a malformed key or blob index.</summary>
    public void ProcessInFlow(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value) => ProcessFlow(internalKey, value, isInflow: true);

    /// <summary>BlobGarbageMeter::ProcessOutFlow.</summary>
    public void ProcessOutFlow(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value) => ProcessFlow(internalKey, value, isInflow: false);

    private void ProcessFlow(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value, bool isInflow)
    {
        if (!DbFormat.TryParseInternalKey(internalKey, out ParsedInternalKey ikey)) ThrowHelper.Corruption("Corrupted internal key in blob garbage meter");
        if (ikey.Type == EntryType.BlobIndex)
        {
            AddFlow(BlobIndex.Decode(value), ikey.UserKeyLength, isInflow);
        }
        else if (ikey.Type == EntryType.WideColumnEntity && Wide.WideColumnSerialization.HasBlobColumns(value))
        {
            // ProcessEntityBlobReferences: every blob-backed column of an entity is a blob reference.
            Wide.WideColumnSerialization.GetBlobColumnIndexes(value, _slots);
            foreach (Wide.WideColumnSlot s in _slots) AddFlow(BlobIndex.Decode(value.Slice(s.ValueOffset, s.ValueLength)), ikey.UserKeyLength, isInflow);
        }
    }

    private readonly List<Wide.WideColumnSlot> _slots = new(8);

    /// <summary>GetBlobReferenceDetails + AddFlow.</summary>
    private void AddFlow(BlobIndex index, int userKeyLength, bool isInflow)
    {
        if (index.IsInlined || index.HasTtl) ThrowHelper.Corruption("Unexpected TTL/inlined blob index");
        if (index.IsSameFile) return;
        ulong bytes = index.Size + BlobLogFormat.RecordHeaderAdjustment((ulong)userKeyLength);
        if (isInflow)
        {
            if (!_flows.TryGetValue(index.FileNumber, out Flow? f)) _flows[index.FileNumber] = f = new Flow();
            f.InCount++;
            f.InBytes += bytes;
        }
        else if (_flows.TryGetValue(index.FileNumber, out Flow? f))
        {
            // Only preexisting files (those with inflow) can gain garbage; new files only have outflow.
            f.OutCount++;
            f.OutBytes += bytes;
        }
    }

    /// <summary>
    /// The additional garbage per blob file (CompactionJob / CompactionOutputs::AddBlobFileGarbages with
    /// BlobGarbageMeter::GetFlows): files whose inflow exceeds their outflow.
    /// </summary>
    public void AddGarbageTo(List<BlobFileGarbage> into)
    {
        foreach ((ulong number, Flow f) in _flows)
        {
            if (f.InCount == f.OutCount) continue;
            into.Add(new BlobFileGarbage(number, f.InCount - f.OutCount, f.InBytes - f.OutBytes));
        }
    }
}

/// <summary>
/// Wraps the compaction input and reports every entry it is positioned on to a <see cref="BlobGarbageMeter"/>
/// (inflow). Port of rocksdb::BlobCountingIterator (db/blob/blob_counting_iterator.h). Takes ownership of the inner
/// iterator.
/// </summary>
internal sealed class BlobCountingIterator : InternalIterator
{
    private readonly InternalIterator _iter;
    private readonly BlobGarbageMeter _meter;

    public BlobCountingIterator(InternalIterator iter, BlobGarbageMeter meter)
    {
        _iter = iter;
        _meter = meter;
        UpdateAndCountBlobIfNeeded();
    }

    public override bool Valid => _iter.Valid;
    public override ReadOnlySpan<byte> Key => _iter.Key;
    public override ReadOnlySpan<byte> Value => _iter.Value;

    private void UpdateAndCountBlobIfNeeded()
    {
        if (_iter.Valid) _meter.ProcessInFlow(_iter.Key, _iter.Value);
    }

    public override void SeekToFirst()
    {
        _iter.SeekToFirst();
        UpdateAndCountBlobIfNeeded();
    }

    public override void SeekToLast()
    {
        _iter.SeekToLast();
        UpdateAndCountBlobIfNeeded();
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        _iter.Seek(target);
        UpdateAndCountBlobIfNeeded();
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        _iter.SeekForPrev(target);
        UpdateAndCountBlobIfNeeded();
    }

    public override void Next()
    {
        _iter.Next();
        UpdateAndCountBlobIfNeeded();
    }

    public override void Prev()
    {
        _iter.Prev();
        UpdateAndCountBlobIfNeeded();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) _iter.Dispose();
    }
}
