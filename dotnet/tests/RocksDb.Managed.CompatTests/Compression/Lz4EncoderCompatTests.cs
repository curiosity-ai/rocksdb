using System.Diagnostics;
using System.Runtime.InteropServices;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.CompatTests.Compression;

/// <summary>
/// The managed LZ4 block encoder (<see cref="Lz4BlockEncoder"/>, a port of LZ4_compress_generic's byU16 path) produces
/// exactly the bytes of native liblz4's LZ4_compress_fast (the copy linked into the packaged librocksdb), including
/// the limited-output rejections, for random inputs of every size class below 64 KiB.
/// </summary>
[TestClass]
public sealed unsafe class Lz4EncoderCompatTests
{
    private static delegate* unmanaged<byte*, byte*, int, int, int, int> s_lz4CompressFast;

    private static delegate* unmanaged<byte*, byte*, int, int, int, int> NativeCompressFast()
    {
        if (s_lz4CompressFast != null) return s_lz4CompressFast;
        _ = RocksDbSharp.Native.Instance; // loads librocksdb
        string? path = null;
        foreach (ProcessModule m in Process.GetCurrentProcess().Modules)
        {
            if (m.ModuleName.StartsWith("librocksdb", StringComparison.Ordinal)) path = m.FileName;
        }
        Assert.IsNotNull(path, "librocksdb not loaded");
        nint lib = NativeLibrary.Load(path);
        s_lz4CompressFast = (delegate* unmanaged<byte*, byte*, int, int, int, int>)NativeLibrary.GetExport(lib, "LZ4_compress_fast");
        return s_lz4CompressFast;
    }

    private static int Native(ReadOnlySpan<byte> src, Span<byte> dst)
    {
        delegate* unmanaged<byte*, byte*, int, int, int, int> f = NativeCompressFast();
        fixed (byte* s = src)
        fixed (byte* d = dst)
            return f(s, d, src.Length, dst.Length, 1);
    }

    [TestMethod]
    public void OutputIsIdenticalToNativeLz4()
    {
        if (!OperatingSystem.IsLinux() || !BitConverter.IsLittleEndian) Assert.Inconclusive("native LZ4 symbol lookup is Linux-only here");
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        int[] sizes = [0, 1, 4, 12, 13, 14, 15, 16, 31, 64, 100, 255, 256, 1000, 1024, 4095, 4096, 4097, 8192, 16384, 32768, 65535, 65546];
        for (int round = 0; round < 3; round++)
        {
            foreach (int baseSize in sizes)
            {
                int size = round == 0 ? baseSize : Math.Min(Lz4BlockEncoder.Limit64K - 1, Math.Max(0, baseSize + rnd.Next(-8, 9)));
                byte[] src = MakeInput(rnd, size, rnd.Next(4));
                int bound = K4os.Compression.LZ4.LZ4Codec.MaximumOutputSize(size);
                foreach (int capacity in (int[])[bound, size * 7 / 8, size / 2, Math.Max(1, size / 8)])
                {
                    byte[] a = new byte[Math.Max(capacity, 1)];
                    byte[] b = new byte[Math.Max(capacity, 1)];
                    int na = Native(src, a.AsSpan(0, capacity));
                    int nb = Lz4BlockEncoder.Compress(src, b.AsSpan(0, capacity));
                    string ctx = $"seed={seed} size={size} capacity={capacity}";
                    Assert.AreEqual(na, nb, ctx);
                    Assert.IsTrue(a.AsSpan(0, na).SequenceEqual(b.AsSpan(0, nb)), ctx);
                }
            }
        }
    }

    /// <summary>Random input with a given kind: 0 random, 1 db_bench-like (ratio 0.5), 2 runs of one byte, 3 small alphabet.</summary>
    internal static byte[] MakeInput(Random rnd, int size, int kind)
    {
        byte[] b = new byte[size];
        switch (kind)
        {
            case 0:
                rnd.NextBytes(b);
                break;
            case 1:
                byte[] piece = new byte[50];
                for (int i = 0; i < size; i++)
                {
                    if (i % 100 == 0) for (int j = 0; j < piece.Length; j++) piece[j] = (byte)(' ' + rnd.Next(95));
                    b[i] = piece[i % 50];
                }
                break;
            case 2:
                for (int i = 0; i < size;)
                {
                    byte v = (byte)rnd.Next(256);
                    int run = 1 + rnd.Next(600);
                    for (int j = 0; j < run && i < size; j++) b[i++] = v;
                }
                break;
            default:
                for (int i = 0; i < size; i++) b[i] = (byte)('a' + rnd.Next(4));
                break;
        }
        return b;
    }
}
