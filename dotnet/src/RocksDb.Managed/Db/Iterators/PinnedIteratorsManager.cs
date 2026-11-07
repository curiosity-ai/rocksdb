using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// Keeps the data blocks an iterator stack moves away from alive while pinning is enabled, so values handed out as
/// <see cref="PinnedSlice"/>s stay valid until <see cref="ReleasePinnedData"/>. Port of PinnedIteratorsManager
/// (db/pinned_iterators_manager.h) for the block-cleanup delegation BlockBasedTableIterator does with it
/// (block_based_table_iterator.h: <c>block_iter_.DelegateCleanupsTo(pinned_iters_mgr_)</c> when pinning).
/// </summary>
/// <remarks>
/// <para>DbIter enables pinning while it walks the entries of one user key backwards (TempPinData) and releases the
/// pinned blocks at the start of the next positioning call (ReleaseTempPinnedData); with
/// <see cref="ReadOptions.PinData"/> it pins for the iterator's whole lifetime.</para>
/// <para>A table iterator that leaves (or releases) its current block while <see cref="IsPinning"/> hands its
/// reference over with <see cref="PinBlock"/> instead of releasing it. Only block changes reach the manager, so a
/// reverse scan pins about one block per data block it crosses, not one per key.</para>
/// <para>Not thread-safe (owned by one iterator).</para>
/// </remarks>
public sealed class PinnedIteratorsManager
{
    private BlockRef[] _blocks = new BlockRef[4];
    private int _count;
    private bool _pinning;

    /// <summary>Is pinning enabled? (PinningEnabled)</summary>
    public bool IsPinning
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _pinning;
    }

    /// <summary>Number of blocks currently held (tests/diagnostics).</summary>
    public int PinnedBlockCount => _count;

    /// <summary>Enables pinning (StartPinning). REQUIRES: not pinning.</summary>
    public void StartPinning()
    {
        System.Diagnostics.Debug.Assert(!_pinning);
        _pinning = true;
    }

    /// <summary>Takes over <paramref name="block"/>'s reference until <see cref="ReleasePinnedData"/>. REQUIRES: pinning.</summary>
    internal void PinBlock(BlockRef block)
    {
        System.Diagnostics.Debug.Assert(_pinning && !block.IsEmpty);
        if (_count == _blocks.Length) Array.Resize(ref _blocks, _count * 2);
        _blocks[_count++] = block;
    }

    /// <summary>Releases every pinned block and disables pinning (ReleasePinnedData). REQUIRES: pinning.</summary>
    public void ReleasePinnedData()
    {
        System.Diagnostics.Debug.Assert(_pinning);
        _pinning = false;
        Span<BlockRef> blocks = _blocks.AsSpan(0, _count);
        _count = 0;
        for (int i = 0; i < blocks.Length; i++)
        {
            blocks[i].Release();
            blocks[i] = default;
        }
    }
}

/// <summary>
/// A value that stays valid while the memory it points into is pinned: a range of a data block buffer (kept alive by
/// a <see cref="PinnedIteratorsManager"/>) or of memtable arena memory (fixed address, alive while the memtable is
/// referenced). The storable counterpart of upstream's pinned <c>Slice</c> (DBIter::pinned_value_).
/// </summary>
public readonly unsafe struct PinnedSlice
{
    private readonly byte[]? _array;
    private readonly nint _offsetOrPointer;
    private readonly int _length;

    /// <summary>A range of a managed buffer.</summary>
    public PinnedSlice(byte[] array, int offset, int length)
    {
        _array = array;
        _offsetOrPointer = offset;
        _length = length;
    }

    /// <summary>
    /// A span over memory at a fixed address (memtable arena slabs live on the pinned object heap). The caller
    /// guarantees the memory outlives every use of the slice.
    /// </summary>
    public static PinnedSlice FromFixed(ReadOnlySpan<byte> fixedMemory) =>
        new((nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(fixedMemory)), fixedMemory.Length);

    private PinnedSlice(nint pointer, int length)
    {
        _array = null;
        _offsetOrPointer = pointer;
        _length = length;
    }

    public int Length => _length;

    public ReadOnlySpan<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _array is not null
            ? new ReadOnlySpan<byte>(_array, (int)_offsetOrPointer, _length)
            : new ReadOnlySpan<byte>((void*)_offsetOrPointer, _length);
    }

    /// <summary>The slice without its last <paramref name="n"/> bytes.</summary>
    public PinnedSlice TrimEnd(int n) => _array is not null ? new PinnedSlice(_array, (int)_offsetOrPointer, _length - n) : new PinnedSlice(_offsetOrPointer, _length - n);
}
