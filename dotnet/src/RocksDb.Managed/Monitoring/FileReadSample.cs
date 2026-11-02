using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Port of monitoring/file_read_sample.h: sampling of user reads per table file. A point lookup or an iterator seek is
/// sampled with probability 1/<see cref="SampleRate"/> (thread-local Lehmer generator, like
/// <c>Random::GetTLSInstance()-&gt;Next() % kFileReadSampleRate == 307</c>); an iterator Next/Prev with probability
/// 1/<see cref="NextSampleRate"/> (a counter, since stepping is cheaper than seeking). A sampled read adds
/// <see cref="SampleRate"/> to the file's <see cref="FileMetaData.NumReadsSampled"/> (and to
/// <see cref="FileMetaData.NumCollapsibleEntryReadsSampled"/> when it found a tombstone, a merge operand or nothing), so
/// the counters estimate the real read counts. Read-triggered compaction
/// (<see cref="ColumnFamilyOptions.ReadTriggeredCompactionThreshold"/>) compares the collapsible count per byte of the
/// file with the threshold. Allocation-free; the unsampled cost is a thread-static load and a multiply.
/// </summary>
internal static class FileReadSample
{
    /// <summary>kFileReadSampleRate.</summary>
    public const uint SampleRate = 1024;
    /// <summary>kFileReadNextSampleRate (must be kept a power of 2).</summary>
    public const uint NextSampleRate = SampleRate * 64;

    [ThreadStatic] private static uint t_seed;
    [ThreadStatic] private static uint t_nextCounter;

    /// <summary>
    /// Test override (sync point <c>should_sample_file_read:override</c>): 0 = sample randomly, 1 = sample every read,
    /// -1 = sample no read.
    /// </summary>
    internal static int TestOverride;

    /// <summary>should_sample_file_read(): a Get or an iterator seek.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSampleFileRead()
    {
        bool result = NextRandom() % SampleRate == 307;
        int o = Volatile.Read(ref TestOverride);
        return o == 0 ? result : o > 0;
    }

    /// <summary>should_sample_file_read_next(): an iterator Next / Prev.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSampleFileReadNext()
    {
        bool result = (++t_nextCounter & (NextSampleRate - 1)) == 0;
        int o = Volatile.Read(ref TestOverride);
        return o == 0 ? result : o > 0;
    }

    /// <summary>
    /// Random::Next (util/random.h): seed = seed * 16807 % (2^31 - 1). The thread-local instance is seeded from the
    /// thread id like Random::GetTLSInstance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint NextRandom()
    {
        uint seed = t_seed;
        if (seed == 0) seed = InitSeed();
        const uint M = 2147483647u; // 2^31-1
        ulong product = seed * 16807UL;
        // Compute (product % M) using the fact that ((x << 31) % M) == x.
        seed = (uint)((product >> 31) + (product & M));
        // The first reduction may overflow by 1 bit, so we may need to repeat. mod == M is not possible; using > allows
        // the faster sign-bit-based test.
        if (seed > M) seed -= M;
        t_seed = seed;
        return seed;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static uint InitSeed()
    {
        // Random(seed): seed_ = seed & 0x7fffffffu; avoid the bad seeds 0 and 2^31-1.
        uint seed = (uint)(Environment.CurrentManagedThreadId * 2654435761u) & 0x7fffffffu;
        if (seed == 0 || seed == 2147483647u) seed = 1;
        return seed;
    }

    /// <summary>sample_file_read_inc.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SampleFileReadInc(FileMetaData meta) => meta.AddReadsSampled(SampleRate);

    /// <summary>sample_collapsible_entry_file_read_inc.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SampleCollapsibleEntryFileReadInc(FileMetaData meta) => meta.AddCollapsibleEntryReadsSampled(SampleRate);
}
