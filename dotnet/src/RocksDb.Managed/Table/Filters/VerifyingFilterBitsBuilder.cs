using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// BlockBasedTableOptions::detect_filter_construct_corruption: wraps a filter bits builder, remembers the entries added
/// since the last <see cref="Finish"/>, and after building checks each against the finished filter with a reader of
/// the same policy (XXPH3FilterBitsBuilder::MaybePostVerify, FilterBlockBuilder::MaybePostVerifyFilter). A filter that
/// does not match one of its own entries fails the table build with Corruption("Corrupted filter content").
/// </summary>
/// <remarks>
/// Upstream also checksums the buffered hash entries before building; here the entries themselves are kept, which
/// covers the same corruption (an entry the built filter does not contain). Only used with the option set, so the
/// default write path is unchanged.
/// </remarks>
internal sealed class VerifyingFilterBitsBuilder(IFilterBitsBuilder inner, FilterPolicy policy) : IFilterBitsBuilder
{
    private readonly PooledBufferWriter _entries = new(1024); // varint32 length + bytes per entry
    private readonly PooledBufferWriter _filter = new(1024);

    public int EstimatedEntries => inner.EstimatedEntries;

    public ulong ApproximateNumEntries(ulong bytes) => inner.ApproximateNumEntries(bytes);

    public void AddKey(ReadOnlySpan<byte> key)
    {
        Record(key);
        inner.AddKey(key);
    }

    public void AddKeyAndAlt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> alt)
    {
        Record(alt);
        Record(key);
        inner.AddKeyAndAlt(key, alt);
    }

    private void Record(ReadOnlySpan<byte> entry)
    {
        Coding.PutVarint32(_entries, (uint)entry.Length);
        _entries.Write(entry);
    }

    public void Finish(IBufferWriter<byte> output)
    {
        _filter.Clear();
        inner.Finish(_filter);
        ReadOnlySpan<byte> contents = _filter.WrittenSpan;
        byte[] copy = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, contents.Length));
        try
        {
            contents.CopyTo(copy);
            IFilterBitsReader reader = BlockBasedTableReader.CreateFilterReader(policy, copy, contents.Length);
            ReadOnlySpan<byte> entries = _entries.WrittenSpan;
            while (!entries.IsEmpty)
            {
                if (!Coding.GetLengthPrefixedSlice(ref entries, out ReadOnlySpan<byte> entry)) break;
                if (!reader.MayMatch(entry)) ThrowHelper.Corruption("Corrupted filter content");
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(copy);
            _entries.Clear();
        }
        output.Write(contents);
    }

    public void Dispose()
    {
        inner.Dispose();
        _entries.Dispose();
        _filter.Dispose();
    }
}
