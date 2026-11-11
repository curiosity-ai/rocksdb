using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Cached super version references of a column family, so that a point read does not write the shared reference count
/// of the current <see cref="SuperVersion"/> twice per call (with several reader threads that cache line bounces between
/// cores on every Get). Port of ColumnFamilyData::local_sv_ (db/column_family.cc: GetThreadLocalSuperVersion,
/// ReturnThreadLocalSuperVersion, ResetThreadLocalSuperVersions) with one slot per processor instead of one per thread:
/// <list type="bullet">
/// <item>A slot holds null (empty / obsolete, upstream kSVObsolete), <see cref="InUse"/> (a reader is using the slot's
/// super version, kSVInUse) or a <see cref="SuperVersion"/> on which the slot owns one reference.</item>
/// <item>A reader swaps <see cref="InUse"/> into its processor's slot. If it got a super version that is still the
/// current one it reads with the slot's reference; otherwise it takes a fresh reference. When done it swaps the super
/// version back if the slot still says <see cref="InUse"/>; if the slot was reset meanwhile it releases its reference.</item>
/// <item>Installing a new super version resets every slot to null (under the DB mutex), releasing the cached references
/// before the column family's own reference on the old super version, so a slot never holds the last reference
/// unnoticed.</item>
/// </list>
/// Unlike thread-local slots, a processor slot can be shared by two threads (one preempted while it holds the slot):
/// a reader that finds <see cref="InUse"/> falls back to a plain reference, and a reader that finds a super version that
/// is no longer current (stored back by a reader that raced with a reset) releases it, so a stale super version is never
/// used for a read that started after the install.
/// </summary>
internal sealed class SuperVersionCache
{
    /// <summary>kSVInUse.</summary>
    internal static readonly object InUse = new();

    // One slot per 128 bytes (two cache lines, also clear of the adjacent-line prefetcher). A struct array also avoids
    // the covariance check of `ref object?[i]`.
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct Slot
    {
        [FieldOffset(0)] public object? Value;
    }

    private readonly Slot[] _slots;
    private readonly int _mask;

    public SuperVersionCache()
    {
        int n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Clamp(Environment.ProcessorCount, 1, 256));
        _slots = new Slot[n];
        _mask = n - 1;
    }

    /// <summary>
    /// Swaps <see cref="InUse"/> into the slot of the current processor. Returns the slot index and in
    /// <paramref name="previous"/> what the slot held.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Take(out object? previous)
    {
        int i = Thread.GetCurrentProcessorId() & _mask;
        previous = Interlocked.Exchange(ref _slots[i].Value, InUse);
        return i;
    }

    /// <summary>Stores <paramref name="sv"/> back into slot <paramref name="slot"/>; false if the slot was reset meanwhile (the caller must release its reference).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPut(int slot, SuperVersion sv) => Interlocked.CompareExchange(ref _slots[slot].Value, sv, InUse) == InUse;

    /// <summary>Gives up a slot taken with <see cref="Take"/> without storing a super version (error paths).</summary>
    public void Abandon(int slot) => Interlocked.CompareExchange(ref _slots[slot].Value, null, InUse);

    /// <summary>
    /// Test hook: marks every slot <see cref="InUse"/> (as if a reader held each one) and returns what the slots held,
    /// for <see cref="TestRestore"/>.
    /// </summary>
    internal object?[] TestOccupyAll()
    {
        var previous = new object?[_slots.Length];
        for (int i = 0; i < previous.Length; i++) previous[i] = Interlocked.Exchange(ref _slots[i].Value, InUse);
        return previous;
    }

    /// <summary>Test hook: undoes <see cref="TestOccupyAll"/>.</summary>
    internal void TestRestore(object?[] previous)
    {
        for (int i = 0; i < previous.Length; i++) Interlocked.Exchange(ref _slots[i].Value, previous[i]);
    }

    /// <summary>
    /// Empties every slot (ResetThreadLocalSuperVersions), cleaning up the super versions whose cached reference was the
    /// last one. REQUIRES: DB mutex.
    /// </summary>
    public void Reset(DbImpl db)
    {
        Slot[] slots = _slots;
        for (int i = 0; i < slots.Length; i++)
        {
            object? o = Interlocked.Exchange(ref slots[i].Value, null);
            if (o is SuperVersion sv && sv.Unref()) db.CleanupSuperVersion(sv);
        }
    }
}
