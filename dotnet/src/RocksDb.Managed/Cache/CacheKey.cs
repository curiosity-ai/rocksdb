using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;

namespace RocksDb.Managed.Cache;

/// <summary>
/// 16-byte block cache key. Port of rocksdb::CacheKey (cache/cache_key.h): two 64-bit words, never all-zero
/// except for <see cref="Empty"/>. Keys for blocks of an SST file are produced by
/// <see cref="OffsetableCacheKey.WithOffset"/>.
/// </summary>
public readonly struct CacheKey : IEquatable<CacheKey>
{
    /// <summary>First 64 bits; shared by all keys of one file ("common prefix").</summary>
    public readonly ulong FileNumEtc64;
    /// <summary>Second 64 bits; xor-ed with the block offset.</summary>
    public readonly ulong OffsetEtc64;

    public CacheKey(ulong fileNumEtc64, ulong offsetEtc64)
    {
        FileNumEtc64 = fileNumEtc64;
        OffsetEtc64 = offsetEtc64;
    }

    public static CacheKey Empty => default;

    public bool IsEmpty => (FileNumEtc64 | OffsetEtc64) == 0;

    private static long s_processCounter = -1; // counts down from UINT64_MAX (as signed -1)

    /// <summary>A key unique for the process lifetime (CacheKey::CreateUniqueForProcessLifetime).</summary>
    public static CacheKey CreateUniqueForProcessLifetime()
    {
        ulong id = unchecked((ulong)Interlocked.Decrement(ref s_processCounter) + 1);
        return new CacheKey(0, id);
    }

    /// <summary>A key unique for <paramref name="cache"/>'s lifetime (CacheKey::CreateUniqueForCacheLifetime).</summary>
    public static CacheKey CreateUniqueForCacheLifetime(BlockCache cache)
    {
        // +1 so that all zeros stay reserved for "unset"; NewId counts up from 1, below 1 << 63.
        ulong id = (ulong)cache.NewId() + 1;
        return new CacheKey(0, id);
    }

    /// <summary>
    /// 32-bit hash used for shard selection (low bits) and the per-shard table (high bits). Not persisted; any
    /// well-mixed function works. This is a 128→64 multiply/xorshift mix.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint Hash32()
    {
        ulong x = OffsetEtc64 ^ BitOperations.RotateLeft(FileNumEtc64 * 0x9E3779B97F4A7C15UL, 29);
        x ^= x >> 32;
        x *= 0xBF58476D1CE4E5B9UL;
        x ^= x >> 29;
        x *= 0x94D049BB133111EBUL;
        return (uint)(x >> 32);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(CacheKey other) => ((FileNumEtc64 ^ other.FileNumEtc64) | (OffsetEtc64 ^ other.OffsetEtc64)) == 0;

    public override bool Equals(object? obj) => obj is CacheKey k && Equals(k);

    public override int GetHashCode() => (int)Hash32();

    public static bool operator ==(CacheKey a, CacheKey b) => a.Equals(b);

    public static bool operator !=(CacheKey a, CacheKey b) => !a.Equals(b);

    /// <summary>Writes the key in native (little-endian) layout like CacheKey::AsSlice on LE machines.</summary>
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, FileNumEtc64);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], OffsetEtc64);
    }

    public override string ToString() => FileNumEtc64.ToString("x16") + OffsetEtc64.ToString("x16");

    /// <summary>Size of the key in bytes (CacheKey::AsSlice().size()).</summary>
    public const int Size = 16;

    /// <summary>Reads a key written by <see cref="WriteTo"/>; InvalidArgument unless <paramref name="source"/> is 16 bytes.</summary>
    public static CacheKey Read(ReadOnlySpan<byte> source)
    {
        if (source.Length != Size) ThrowHelper.InvalidArgument("cache keys are 16 bytes");
        return new CacheKey(BinaryPrimitives.ReadUInt64LittleEndian(source), BinaryPrimitives.ReadUInt64LittleEndian(source[8..]));
    }
}

/// <summary>
/// Per-file base cache key. Port of rocksdb::OffsetableCacheKey (cache/cache_key.{h,cc}) including the
/// SST internal unique id derivation (table/unique_id.cc GetSstInternalUniqueId with force=true), so keys are
/// stable across reopen for the same (db_id, db_session_id, file_number) and practically unique across DBs.
/// </summary>
public readonly struct OffsetableCacheKey
{
    private readonly ulong _fileNumEtc64;
    private readonly ulong _offsetEtc64;

    private OffsetableCacheKey(ulong fileNumEtc64, ulong offsetEtc64)
    {
        _fileNumEtc64 = fileNumEtc64;
        _offsetEtc64 = offsetEtc64;
    }

    public bool IsEmpty => (_fileNumEtc64 | _offsetEtc64) == 0;

    /// <summary>The 8-byte prefix shared by all keys of this file (CommonPrefixSlice).</summary>
    public ulong CommonPrefix => _fileNumEtc64;

    /// <summary>OffsetableCacheKey(db_id, db_session_id, file_number).</summary>
    public OffsetableCacheKey(string dbId, string dbSessionId, ulong fileNumber)
    {
        GetSstInternalUniqueId(dbId, dbSessionId, fileNumber, out ulong sessionLower, out ulong fileNumEtc);
        this = FromInternalUniqueId(sessionLower, fileNumEtc);
    }

    /// <summary>OffsetableCacheKey::FromInternalUniqueId (UniqueId64x2 {id0, id1}).</summary>
    public static OffsetableCacheKey FromInternalUniqueId(ulong id0, ulong id1)
    {
        ulong sessionLower = id0;
        ulong fileNumEtc = id1;
        if (sessionLower == 0) sessionLower = fileNumEtc;
        ulong a = DownwardInvolution(sessionLower) ^ ReverseBits(fileNumEtc);
        ulong b = ReverseBits(sessionLower);
        if (a == 0) (a, b) = (b, a);
        return new OffsetableCacheKey(a, b);
    }

    /// <summary>OffsetableCacheKey::ToInternalUniqueId (inverse of <see cref="FromInternalUniqueId"/>).</summary>
    public (ulong Id0, ulong Id1) ToInternalUniqueId()
    {
        ulong a = _fileNumEtc64, b = _offsetEtc64;
        if (b == 0) (a, b) = (b, a);
        ulong id0 = ReverseBits(b);
        ulong id1 = ReverseBits(a ^ DownwardInvolution(id0));
        return (id0, id1);
    }

    /// <summary>Key of the record at <paramref name="offset"/> in this file (WithOffset).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CacheKey WithOffset(ulong offset) => new(_fileNumEtc64, _offsetEtc64 ^ offset);

    /// <summary>
    /// Key for a record at <paramref name="offset"/> in a file where every cached region is at least 5 bytes long
    /// (WithOffsetForMinSizeRecord: drops the low two offset bits). This is what block-based tables use.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CacheKey WithOffsetForMinSizeRecord(ulong offset) => WithOffset(offset >> 2);

    // ---- table/unique_id.cc ----

    internal static void GetSstInternalUniqueId(string dbId, string dbSessionId, ulong fileNumber, out ulong id0, out ulong id1)
    {
        if (!TryDecodeSessionId(dbSessionId, out ulong sessionUpper, out ulong sessionLower))
        {
            // Hash2x64 fallback for malformed ids (force=true).
            Hash2x64(Encoding.UTF8.GetBytes(dbSessionId), 0, out sessionUpper, out sessionLower);
            if (sessionLower == 0) sessionLower = sessionUpper | 1;
        }
        id0 = sessionLower;
        Hash2x64(Encoding.UTF8.GetBytes(dbId), sessionUpper, out ulong dbA, out _);
        id1 = dbA ^ fileNumber;
    }

    private static void Hash2x64(byte[] data, ulong seed, out ulong high64, out ulong low64)
    {
        UInt128 h = XxHash128.HashToUInt128(data, unchecked((long)seed));
        high64 = (ulong)(h >> 64);
        low64 = (ulong)h;
    }

    /// <summary>DecodeSessionId (table/unique_id.cc): base-36, 13..24 chars, last 12 chars hold the low bits.</summary>
    public static bool TryDecodeSessionId(string sessionId, out ulong upper, out ulong lower)
    {
        upper = lower = 0;
        int len = sessionId.Length;
        if (len < 13 || len > 24) return false;
        ulong a = 0, b = 0;
        if (!ParseBase36(sessionId.AsSpan(0, len - 12), ref a) || !ParseBase36(sessionId.AsSpan(len - 12), ref b)) return false;
        upper = a >> 2;
        lower = (b & (ulong.MaxValue >> 2)) | (a << 62);
        return true;
    }

    /// <summary>EncodeSessionId (table/unique_id.cc): 20 upper-case base-36 chars.</summary>
    public static string EncodeSessionId(ulong upper, ulong lower)
    {
        Span<char> buf = stackalloc char[20];
        ulong a = (upper << 2) | (lower >> 62);
        ulong b = lower & (ulong.MaxValue >> 2);
        PutBase36(buf[..8], a);
        PutBase36(buf[8..], b);
        return new string(buf);
    }

    private static bool ParseBase36(ReadOnlySpan<char> s, ref ulong v)
    {
        foreach (char c in s)
        {
            v = unchecked(v * 36);
            if (c >= '0' && c <= '9') v += (ulong)(c - '0');
            else if (c >= 'A' && c <= 'Z') v += (ulong)(c - 'A' + 10);
            else if (c >= 'a' && c <= 'z') v += (ulong)(c - 'a' + 10);
            else return false;
        }
        return true;
    }

    // PutBaseChars<36>(buf, n, v, uppercase): most significant digit first, value taken mod 36^n.
    private static void PutBase36(Span<char> dst, ulong v)
    {
        for (int i = dst.Length - 1; i >= 0; i--)
        {
            int d = (int)(v % 36);
            v /= 36;
            dst[i] = (char)(d < 10 ? '0' + d : 'A' + d - 10);
        }
    }

    // ---- util/math.h ----

    /// <summary>ReverseBits for 64-bit values.</summary>
    internal static ulong ReverseBits(ulong v)
    {
        ulong r = BinaryPrimitives.ReverseEndianness(v);
        const ulong every = 0x0101010101010101UL;
        r = ((r & (every * 0x0f)) << 4) | ((r >> 4) & (every * 0x0f));
        r = ((r & (every * 0x33)) << 2) | ((r >> 2) & (every * 0x33));
        r = ((r & (every * 0x55)) << 1) | ((r >> 1) & (every * 0x55));
        return r;
    }

    /// <summary>DownwardInvolution for 64-bit values.</summary>
    internal static ulong DownwardInvolution(ulong r)
    {
        r ^= r >> 32;
        r ^= (r & 0xffff0000ffff0000UL) >> 16;
        r ^= (r & 0xff00ff00ff00ff00UL) >> 8;
        r ^= (r & 0xf0f0f0f0f0f0f0f0UL) >> 4;
        r ^= (r & 0xccccccccccccccccUL) >> 2;
        r ^= (r & 0xaaaaaaaaaaaaaaaaUL) >> 1;
        return r;
    }
}
