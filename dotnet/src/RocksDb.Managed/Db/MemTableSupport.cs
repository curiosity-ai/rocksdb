using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Memtable;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Memtable configuration (the subset of MutableCFOptions/ImmutableOptions read by db/memtable.cc).
/// </summary>
public sealed class MemTableOptions
{
    /// <summary>write_buffer_size (default 64 MiB). Drives <see cref="MemTable.ShouldFlushNow"/>.</summary>
    public long WriteBufferSize { get; set; } = 64L << 20;

    /// <summary>arena_block_size; 0 means "derive from write_buffer_size" (min(1 MiB, write_buffer_size / 8), 4 KiB aligned).</summary>
    public int ArenaBlockSize { get; set; }

    /// <summary>Skiplist max height (upstream SkipListFactory default 12).</summary>
    public int SkipListMaxHeight { get; set; } = 12;

    /// <summary>Skiplist branching factor (default 4).</summary>
    public int SkipListBranchingFactor { get; set; } = 4;

    /// <summary>memtable_max_range_deletions: flush once this many range deletions were added (0 = disabled).</summary>
    public long MaxRangeDeletions { get; set; }

    /// <summary>
    /// Timestamp size of the column family when its user-defined timestamps are not persisted
    /// (persist_user_defined_timestamps = false), else 0: the memtable then tracks the newest timestamp it holds
    /// (MemTable::MaybeUpdateNewestUDT / GetNewestUDT).
    /// </summary>
    public int TrackNewestTimestampSize { get; set; }

    /// <summary>MutableCFOptions::prefix_extractor: prefixes go to the memtable bloom (with <see cref="PrefixBloomBits"/> &gt; 0).</summary>
    public SliceTransform? PrefixExtractor { get; set; }

    /// <summary>
    /// ImmutableMemTableOptions::memtable_prefix_bloom_bits = write_buffer_size * memtable_prefix_bloom_size_ratio * 8
    /// (0: no memtable bloom). See <see cref="ComputePrefixBloomBits"/>.
    /// </summary>
    public uint PrefixBloomBits { get; set; }

    /// <summary>memtable_whole_key_filtering: whole user keys (without timestamp) are added to the memtable bloom too.</summary>
    public bool WholeKeyFiltering { get; set; }

    /// <summary>inplace_update_support (see <see cref="MemTable.Update"/>).</summary>
    public bool InplaceUpdateSupport { get; set; }

    /// <summary>inplace_update_num_locks: number of striped locks guarding in-place updates.</summary>
    public int InplaceUpdateNumLocks { get; set; } = 10000;

    /// <summary>inplace_callback (see <see cref="MemTable.UpdateCallback"/>).</summary>
    public InplaceUpdateCallback? InplaceCallback { get; set; }

    /// <summary>memtable_insert_with_hint_prefix_extractor: serial point inserts use a per-prefix skiplist splice hint.</summary>
    public SliceTransform? InsertWithHintPrefixExtractor { get; set; }

    /// <summary>
    /// memtable_protection_bytes_per_key (0, 1, 2, 4 or 8): bytes of per key-value checksum stored after each entry and
    /// verified on reads, iteration and flush.
    /// </summary>
    public uint ProtectionBytesPerKey { get; set; }

    /// <summary>
    /// Port of the memtable_prefix_bloom_bits computation of ImmutableMemTableOptions (db/memtable.cc):
    /// static_cast&lt;uint32_t&gt;(write_buffer_size * memtable_prefix_bloom_size_ratio) * 8.
    /// </summary>
    public static uint ComputePrefixBloomBits(ulong writeBufferSize, double ratio)
    {
        double bytes = writeBufferSize * ratio;
        uint b = bytes <= 0 ? 0u : bytes >= uint.MaxValue ? uint.MaxValue : (uint)bytes;
        return unchecked(b * 8u);
    }

    /// <summary>Copies the memtable-related settings of a column family's options (sanitized like SanitizeCfOptions).</summary>
    public void ApplyColumnFamilyOptions(ColumnFamilyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        double ratio = Math.Clamp(options.MemtablePrefixBloomSizeRatio, 0, 0.25);
        PrefixExtractor = options.PrefixExtractor;
        PrefixBloomBits = ComputePrefixBloomBits(options.WriteBufferSize, ratio);
        WholeKeyFiltering = options.MemtableWholeKeyFiltering;
        InplaceUpdateSupport = options.InplaceUpdateSupport;
        InplaceUpdateNumLocks = (int)Math.Clamp(options.InplaceUpdateNumLocks, 1UL, 1UL << 24);
        // ColumnFamilyData::ValidateOptions (db/column_family.cc).
        if (!KvChecksum.IsSupportedLength(options.MemtableProtectionBytesPerKey))
            ThrowHelper.NotSupported("Memtable per key-value checksum protection only supports 0, 1, 2, 4 or 8 bytes per key.");
        if (!KvChecksum.IsSupportedLength(options.BlockProtectionBytesPerKey))
            ThrowHelper.NotSupported("Block per key-value checksum protection only supports 0, 1, 2, 4 or 8 bytes per key.");
        ProtectionBytesPerKey = options.MemtableProtectionBytesPerKey;
        InplaceCallback = options.InplaceCallback;
        InsertWithHintPrefixExtractor = options.MemtableInsertWithHintPrefixExtractor;
    }

    /// <summary>Port of the arena_block_size sanitization in db/column_family.cc (SanitizeCfOptions).</summary>
    public int GetEffectiveArenaBlockSize()
    {
        if (ArenaBlockSize > 0) return ArenaBlockSize;
        long wbs = Math.Clamp(WriteBufferSize, 64L << 10, 64L << 30);
        long size = Math.Min(1024 * 1024, wbs / 8);
        const long align = 4 * 1024;
        return (int)((size + align - 1) / align * align);
    }
}

/// <summary>Outcome of <see cref="MemTable.Get"/>. Port of the (found_final_value, Status) pair of MemTable::Get.</summary>
public enum MemTableGetResult : byte
{
    /// <summary>No entry for the key; continue with older sources.</summary>
    NotFound = 0,
    /// <summary>A final value was found (type in the <c>valueType</c> out parameter). If the merge context holds operands, they must be applied on top of it.</summary>
    Found = 1,
    /// <summary>The newest visible entry is a deletion (point or covering range tombstone). If the merge context holds operands, merge them with no base value.</summary>
    Deleted = 2,
    /// <summary>Only merge operands were found (collected into the merge context); continue with older sources.</summary>
    MergeInProgress = 3,
    /// <summary>An entry with an unexpected value type was found.</summary>
    Corruption = 4,
}

/// <summary>Counters accumulated by concurrent memtable inserts, applied with <see cref="MemTable.BatchPostProcess"/>. Port of MemTablePostProcessInfo.</summary>
public struct MemTablePostProcessInfo
{
    public long DataSize;
    public long NumEntries;
    public long NumDeletes;
    public long NumRangeDeletes;
}

/// <summary>
/// Collects merge operands encountered during a point lookup. Port of the operand-list part of rocksdb::MergeContext
/// (db/merge_context.h). Operands are copied into one pooled buffer so they stay valid regardless of the source's
/// lifetime. Reusable via <see cref="Clear"/>; return the buffers with <see cref="Dispose"/>. Not thread-safe.
/// </summary>
public sealed class MergeContext : IDisposable
{
    private readonly PooledBufferWriter _data = new(256);
    private int[] _ends = ArrayPool<int>.Shared.Rent(8);
    private int _count;

    /// <summary>Number of operands collected.</summary>
    public int Count => _count;

    public void Clear()
    {
        _data.Clear();
        _count = 0;
    }

    /// <summary>Adds an operand older than all operands added so far (lookups walk newest to oldest).</summary>
    public void PushOperand(ReadOnlySpan<byte> operand)
    {
        if (_count == _ends.Length)
        {
            int[] n = ArrayPool<int>.Shared.Rent(_count * 2);
            _ends.AsSpan(0, _count).CopyTo(n);
            ArrayPool<int>.Shared.Return(_ends);
            _ends = n;
        }
        _data.Write(operand);
        _ends[_count++] = _data.WrittenCount;
    }

    /// <summary>Keeps only the <paramref name="count"/> newest operands (the first pushed).</summary>
    public void Truncate(int count)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)count, (uint)_count);
        _count = count;
        _data.Truncate(count == 0 ? 0 : _ends[count - 1]);
    }

    /// <summary>Operand by push order: index 0 is the newest operand.</summary>
    public ReadOnlySpan<byte> GetOperandNewestFirst(int index)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
        int start = index == 0 ? 0 : _ends[index - 1];
        return _data.WrittenSpan[start.._ends[index]];
    }

    /// <summary>Operand in merge order: index 0 is the oldest operand (the order passed to FullMerge).</summary>
    public ReadOnlySpan<byte> GetOperand(int index) => GetOperandNewestFirst(_count - 1 - index);

    public void Dispose()
    {
        _data.Dispose();
        int[] e = _ends;
        _ends = [];
        _count = 0;
        if (e.Length > 0) ArrayPool<int>.Shared.Return(e);
    }
}

/// <summary>
/// Skiplist comparator for memtable entries <c>varint32(ikey_len) | internal_key | ...</c>: decodes the internal key and
/// compares with <see cref="InternalKeyComparator.CompareKeySeq"/> (user key asc, sequence desc, type ignored), exactly
/// like MemTable::KeyComparator (db/memtable.cc). Bytewise user comparators are compared inline.
/// </summary>
public readonly unsafe struct MemTableKeyComparer : ISkipListComparer
{
    private readonly InternalKeyComparator _icmp;
    private readonly bool _bytewise;

    public MemTableKeyComparer(InternalKeyComparator icmp)
    {
        _icmp = icmp;
        _bytewise = icmp.IsBytewise;
    }

    public InternalKeyComparator InternalComparator => _icmp;

    /// <summary>Decodes the length-prefixed internal key at <paramref name="entry"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SkipListKey DecodeEntryKey(byte* entry)
    {
        uint len = *entry;
        if (len < 0x80) return new SkipListKey(entry + 1, (int)len);
        return DecodeSlow(entry);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static SkipListKey DecodeSlow(byte* entry)
    {
        int n = Coding.DecodeVarint32(new ReadOnlySpan<byte>(entry, Coding.MaxVarint32Length), out uint len);
        return new SkipListKey(entry + n, (int)len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SkipListKey Decode(byte* encodedKey) => DecodeEntryKey(encodedKey);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(byte* encodedKey, SkipListKey decoded)
    {
        SkipListKey a = DecodeEntryKey(encodedKey);
        if (_bytewise)
        {
            int r = ByteCompare.Compare(new ReadOnlySpan<byte>(a.Ptr, a.Length - DbFormat.InternalKeyFooterSize),
                new ReadOnlySpan<byte>(decoded.Ptr, decoded.Length - DbFormat.InternalKeyFooterSize));
            if (r != 0) return r;
            // Shift the number to exclude the last byte which contains the value type.
            ulong anum = Unsafe.ReadUnaligned<ulong>(a.Ptr + a.Length - DbFormat.InternalKeyFooterSize);
            ulong bnum = Unsafe.ReadUnaligned<ulong>(decoded.Ptr + decoded.Length - DbFormat.InternalKeyFooterSize);
            if (!BitConverter.IsLittleEndian)
            {
                anum = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(anum);
                bnum = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(bnum);
            }
            anum >>= 8;
            bnum >>= 8;
            return anum > bnum ? -1 : (anum < bnum ? 1 : 0);
        }
        return _icmp.CompareKeySeq(a.Span, decoded.Span);
    }
}
