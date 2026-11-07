using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace RocksDb.Managed.Soak;

/// <summary>Sizes of the key spaces (fixed per run, stored in run.json).</summary>
internal sealed class Layout
{
    /// <summary>Register writer threads (one writer per key).</summary>
    public int Writers { get; set; } = 4;
    /// <summary>Register keys per writer (split over the reg0 / reg1 column families by parity).</summary>
    public int KeysPerWriter { get; set; } = 1500;
    /// <summary>Bank accounts.</summary>
    public int Accounts { get; set; } = 24;
    /// <summary>Initial balance of each account.</summary>
    public ulong InitialBalance { get; set; } = 1000;
    /// <summary>Merge counters.</summary>
    public int Counters { get; set; } = 16;
    /// <summary>Keys of the range-delete key space.</summary>
    public int RdelKeys { get; set; } = 1024;
    /// <summary>Largest register payload.</summary>
    public int MaxPayload { get; set; } = 4096;

    public ulong BankTotal => InitialBalance * (ulong)Accounts;
}

/// <summary>Column family names and key/value encodings of the soak workload.</summary>
internal static class Keys
{
    public const string Reg0 = "reg0";
    public const string Reg1 = "reg1";
    public const string Bank = "bank";
    public const string Ctr = "ctr";
    public const string Rdel = "rdel";
    public const string ScratchPrefix = "scratch-";

    public static readonly string[] FixedColumnFamilies = ["default", Reg0, Reg1, Bank, Ctr, Rdel];

    /// <summary>Register key "r{w:D2}-{i:D5}"; key i lives in reg{i % 2}.</summary>
    public static byte[] Register(int writer, int index) => Encoding.ASCII.GetBytes($"r{writer:D2}-{index:D5}");

    public static string RegisterCf(int index) => (index & 1) == 0 ? Reg0 : Reg1;

    public static bool TryParseRegister(ReadOnlySpan<byte> key, out int writer, out int index)
    {
        writer = index = 0;
        if (key.Length != 9 || key[0] != (byte)'r' || key[3] != (byte)'-') return false;
        return TryDigits(key.Slice(1, 2), out writer) && TryDigits(key.Slice(4, 5), out index);
    }

    public static byte[] Account(int i) => Encoding.ASCII.GetBytes($"a{i:D4}");
    public static byte[] Counter(int i) => Encoding.ASCII.GetBytes($"c{i:D3}");
    public static byte[] RdelKey(int i) => Encoding.ASCII.GetBytes($"d{i:D5}");

    public static bool TryParseIndexed(ReadOnlySpan<byte> key, byte prefix, int digits, out int index)
    {
        index = 0;
        return key.Length == digits + 1 && key[0] == prefix && TryDigits(key[1..], out index);
    }

    private static bool TryDigits(ReadOnlySpan<byte> s, out int value)
    {
        value = 0;
        foreach (byte b in s)
        {
            if (b < (byte)'0' || b > (byte)'9') return false;
            value = value * 10 + (b - '0');
        }
        return true;
    }

    public static byte[] U64(ulong v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, v);
        return b;
    }

    public static bool TryU64(ReadOnlySpan<byte> v, out ulong value)
    {
        value = 0;
        if (v.Length != 8) return false;
        value = BinaryPrimitives.ReadUInt64LittleEndian(v);
        return true;
    }

    // ---- register / rdel values ----
    // [0]   'V' (register) or 'D' (rdel)
    // [1]   writer
    // [2..6)  key index (LE)
    // [6..14) version (LE)
    // [14..18) crc32 of the payload
    // [18..22) payload length
    // [22..)  payload (deterministic from writer, index, version)
    public const int ValueHeader = 22;

    public static byte[] MakeValue(byte tag, int writer, int index, long version, int length, out uint crc)
    {
        var v = new byte[ValueHeader + length];
        Span<byte> payload = v.AsSpan(ValueHeader);
        FillPayload(payload, writer, index, version);
        crc = Crc32.HashToUInt32(payload);
        v[0] = tag;
        v[1] = (byte)writer;
        BinaryPrimitives.WriteInt32LittleEndian(v.AsSpan(2), index);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(6), version);
        BinaryPrimitives.WriteUInt32LittleEndian(v.AsSpan(14), crc);
        BinaryPrimitives.WriteInt32LittleEndian(v.AsSpan(18), length);
        return v;
    }

    public static void FillPayload(Span<byte> payload, int writer, int index, long version)
    {
        ulong x = Mix((ulong)version * 0x9E3779B97F4A7C15UL ^ ((ulong)writer << 48) ^ (ulong)index);
        for (int i = 0; i < payload.Length; i++)
        {
            if ((i & 7) == 0) x = Mix(x + 0x632BE59BD9B4E019UL);
            payload[i] = (byte)(x >> ((i & 7) * 8));
        }
    }

    public static ulong Mix(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    /// <summary>A decoded register / rdel value.</summary>
    internal readonly record struct ValueInfo(int Writer, int Index, long Version, uint Crc, int Length);

    /// <summary>
    /// Decodes and self-checks a value: header fields, payload length, payload CRC and the deterministic payload bytes.
    /// Returns an error text or null.
    /// </summary>
    public static string? Decode(byte tag, ReadOnlySpan<byte> v, out ValueInfo info)
    {
        info = default;
        if (v.Length < ValueHeader) return "value shorter than its header (" + v.Length + " bytes)";
        if (v[0] != tag) return "bad value tag " + v[0];
        int writer = v[1];
        int index = BinaryPrimitives.ReadInt32LittleEndian(v[2..]);
        long version = BinaryPrimitives.ReadInt64LittleEndian(v[6..]);
        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(v[14..]);
        int len = BinaryPrimitives.ReadInt32LittleEndian(v[18..]);
        info = new ValueInfo(writer, index, version, crc, len);
        if (len != v.Length - ValueHeader) return $"payload length {len} != {v.Length - ValueHeader}";
        ReadOnlySpan<byte> payload = v[ValueHeader..];
        if (Crc32.HashToUInt32(payload) != crc) return "payload checksum mismatch";
        byte[] expect = new byte[len];
        FillPayload(expect, writer, index, version);
        if (!payload.SequenceEqual(expect)) return "payload bytes differ from the deterministic payload of its version";
        return null;
    }
}
