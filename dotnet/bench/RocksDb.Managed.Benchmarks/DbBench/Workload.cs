using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Benchmarks.DbBench;

// Everything in this file must stay bit-for-bit identical to bench/native-c/rocksdb_bench.c so that the three engines
// (C native, rocksdb-sharp, managed) see exactly the same keys, values and random streams.

/// <summary>SplitMix64 PRNG (same constants and seed derivation as the C benchmark).</summary>
public struct SplitMix64
{
    private ulong _state;

    public SplitMix64(ulong seed) => _state = seed;

    /// <summary>Stream for thread <paramref name="tid"/> of the benchmark at position <paramref name="benchIndex"/>.</summary>
    public static SplitMix64 ForThread(long seed, int benchIndex, int tid) =>
        new(unchecked((ulong)seed * 1000003UL + (ulong)benchIndex * 1009UL + (ulong)tid));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Next()
    {
        unchecked
        {
            ulong z = _state += 0x9E3779B97F4A7C15UL;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Uniform(ulong n) => Next() % n;
}

public static class KeyFormat
{
    /// <summary>db_bench GenerateKeyFromInt (no prefix): 8-byte big-endian number padded with '0' to the key size.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Make(Span<byte> key, ulong v)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(key, v);
        if (key.Length > 8) key[8..].Fill((byte)'0');
    }
}

/// <summary>
/// db_bench RandomGenerator: at least max(1 MiB, value_size) bytes built from 100-byte pieces, each a random printable
/// string of 100*ratio bytes repeated to fill the piece (test::CompressibleString). Values are consecutive windows.
/// </summary>
public sealed class ValueBuffer
{
    public byte[] Data { get; }
    public int Length { get; }

    public ValueBuffer(long seed, int valueSize, double compressionRatio)
    {
        int target = Math.Max(1048576, valueSize);
        Data = new byte[target + 100];
        var rng = new SplitMix64(unchecked((ulong)seed));
        int rawLen = Math.Clamp((int)(100 * compressionRatio), 1, 100);
        Span<byte> raw = stackalloc byte[100];
        int len = 0;
        while (len < target)
        {
            for (int i = 0; i < rawLen; i++) raw[i] = (byte)(' ' + (int)rng.Uniform(95));
            for (int i = 0; i < 100; i++) Data[len + i] = raw[i % rawLen];
            len += 100;
        }
        Length = len;
    }
}

/// <summary>Per-worker cursor over a <see cref="ValueBuffer"/>.</summary>
public struct ValueCursor(ValueBuffer buffer)
{
    private int _pos;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> Next(int len)
    {
        if (_pos + len > buffer.Length) _pos = 0;
        var v = new ReadOnlyMemory<byte>(buffer.Data, _pos, len);
        _pos += len;
        return v;
    }
}

/// <summary>
/// Log-linear latency histogram in nanoseconds: values below 128 are exact, above that 64 sub-buckets per power of two
/// (at most 1.6% relative error). Percentiles interpolate inside the bucket like db_bench's HistogramImpl.
/// </summary>
public sealed class Histogram
{
    private const int Buckets = 3776;
    private readonly long[] _counts = new long[Buckets];
    public long Count { get; private set; }
    public long Min { get; private set; } = long.MaxValue;
    public long Max { get; private set; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Index(ulong v)
    {
        if (v < 128) return (int)v;
        int msb = 63 - BitOperations.LeadingZeroCount(v);
        int shift = msb - 6;
        return 128 + (shift - 1) * 64 + (int)((v >> shift) - 64);
    }

    private static void Bounds(int idx, out double lo, out double hi)
    {
        if (idx < 128)
        {
            lo = idx;
            hi = idx + 1;
            return;
        }
        int shift = (idx - 128) / 64 + 1;
        ulong sub = (ulong)((idx - 128) % 64 + 64);
        lo = sub << shift;
        hi = (sub + 1) << shift;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long ns)
    {
        if (ns < 0) ns = 0;
        _counts[Index((ulong)ns)]++;
        Count++;
        if (ns < Min) Min = ns;
        if (ns > Max) Max = ns;
    }

    public void Merge(Histogram other)
    {
        for (int i = 0; i < Buckets; i++) _counts[i] += other._counts[i];
        Count += other.Count;
        if (other.Min < Min) Min = other.Min;
        if (other.Max > Max) Max = other.Max;
    }

    /// <summary>Percentile in nanoseconds.</summary>
    public double Percentile(double p)
    {
        if (Count == 0) return 0;
        double threshold = Count * (p / 100.0);
        double cumulative = 0;
        for (int b = 0; b < Buckets; b++)
        {
            if (_counts[b] == 0) continue;
            double c = _counts[b];
            cumulative += c;
            if (cumulative >= threshold)
            {
                Bounds(b, out double lo, out double hi);
                double r = lo + (hi - lo) * ((threshold - (cumulative - c)) / c);
                return Math.Clamp(r, Min, Max);
            }
        }
        return Max;
    }
}

/// <summary>Monotonic nanosecond clock (Stopwatch; 1 tick = 1 ns on Linux).</summary>
public static class Clock
{
    private static readonly double NsPerTick = 1e9 / Stopwatch.Frequency;
    private static readonly bool TicksAreNs = Stopwatch.Frequency == 1_000_000_000;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Now() => Stopwatch.GetTimestamp();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ElapsedNs(long start)
    {
        long d = Stopwatch.GetTimestamp() - start;
        return TicksAreNs ? d : (long)(d * NsPerTick);
    }

    public static long ToNs(long ticks) => TicksAreNs ? ticks : (long)(ticks * NsPerTick);
}
