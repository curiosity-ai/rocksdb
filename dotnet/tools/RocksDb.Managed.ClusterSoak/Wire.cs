using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using MagicOnion;
using MessagePack;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>
/// The client / control service each soak node serves next to the cluster's <c>IClusterService</c>. Clients reach it
/// through their proxies (faultable); the supervisor's control calls (status, dump, checkpoint, test injections) go to
/// the node's port directly, like Jepsen's control node over SSH.
/// </summary>
public interface ISoakService : IService<ISoakService>
{
    /// <summary>A write batch (see <see cref="WriteReq"/>), acknowledged once committed.</summary>
    UnaryResult<WriteResp> WriteAsync(WriteReq request);

    /// <summary>Point reads of <paramref name="keys"/> under one snapshot of this node's database (no ReadIndex).</summary>
    UnaryResult<ReadResp> ReadAsync(List<string> keys);

    UnaryResult<NodeStatusDto> StatusAsync();

    /// <summary>Every key/value of the node plus its log position and term map (VERIFY).</summary>
    UnaryResult<DumpDto> DumpAsync();

    /// <summary>Checkpoint of the node's database into <paramref name="dir"/> (native cross-check); returns an error text or "".</summary>
    UnaryResult<string> CheckpointAsync(string dir);

    /// <summary>
    /// Self-test injections: "local-put"/"local-delete" write this node's database directly (outside the Raft log);
    /// "raft-put"/"raft-merge"/"raft-delete" go through the leader's committed write path without a history record.
    /// </summary>
    UnaryResult<string> InjectAsync(string kind, string key, byte[] value);
}

/// <summary>Write operation kinds of <see cref="WriteReq"/>.</summary>
public enum WriteKind
{
    /// <summary>Register put: Keys[0] = key, Values[0] = value.</summary>
    Put = 0,
    /// <summary>Merges (UInt64Add) of Values[i] into Keys[i], in one batch (bank transfer, counter add).</summary>
    Merge = 1,
    /// <summary>Puts of Values[i] into Keys[i], in one batch (bank initialisation).</summary>
    PutMany = 2,
}

/// <summary>Outcome codes of <see cref="WriteResp.Status"/>: <see cref="RocksDb.Managed.Replication.Hosting.ClusterWriteStatus"/> plus errors.</summary>
public enum WriteOutcome
{
    Committed = 0,
    NotLeader = 1,
    LeadershipLost = 2,
    TimedOut = 3,
    /// <summary>The database write threw (e.g. the instance was replaced by a resync); indeterminate.</summary>
    Error = 4,
}

[MessagePackObject]
public sealed class WriteReq
{
    [Key(0)] public WriteKind Kind { get; set; }
    [Key(1)] public List<string> Keys { get; set; } = [];
    [Key(2)] public List<byte[]> Values { get; set; } = [];
    [Key(3)] public int TimeoutMs { get; set; }
}

[MessagePackObject]
public sealed class WriteResp
{
    [Key(0)] public WriteOutcome Status { get; set; }
    [Key(1)] public string? LeaderId { get; set; }
    [Key(2)] public long Term { get; set; }
    [Key(3)] public string? Error { get; set; }
}

[MessagePackObject]
public sealed class ReadResp
{
    [Key(0)] public bool Available { get; set; }
    /// <summary>One entry per key; null = absent.</summary>
    [Key(1)] public List<byte[]?> Values { get; set; } = [];
    [Key(2)] public bool IsLeader { get; set; }
    [Key(3)] public long Term { get; set; }
    [Key(4)] public string? LeaderId { get; set; }
    [Key(5)] public ulong LatestSeq { get; set; }
    [Key(6)] public ulong CommitSeq { get; set; }
}

[MessagePackObject]
public sealed class NodeStatusDto
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public int Role { get; set; }
    [Key(2)] public long Term { get; set; }
    [Key(3)] public string? LeaderId { get; set; }
    [Key(4)] public ulong LatestSeq { get; set; }
    [Key(5)] public ulong CommitSeq { get; set; }
    [Key(6)] public int Mode { get; set; }
    [Key(7)] public bool Resyncing { get; set; }
    [Key(8)] public int Resyncs { get; set; }
    [Key(9)] public int Pid { get; set; }
}

[MessagePackObject]
public sealed class DumpDto
{
    [Key(0)] public bool Available { get; set; }
    [Key(1)] public List<string> Keys { get; set; } = [];
    [Key(2)] public List<byte[]> Values { get; set; } = [];
    [Key(3)] public ulong LatestSeq { get; set; }
    [Key(4)] public ulong CommitSeq { get; set; }
    /// <summary>The node's Raft term map (serialized <c>RaftLogTerms</c>).</summary>
    [Key(5)] public string Terms { get; set; } = "";
    [Key(6)] public long Term { get; set; }
    /// <summary><c>DatabaseDigest</c> of the same snapshot.</summary>
    [Key(7)] public ulong DigestHash { get; set; }
    [Key(8)] public long DigestKeys { get; set; }
}

/// <summary>Key names and value encodings of the workload.</summary>
internal static class Keys
{
    public const string NoOp = "__raft_noop";

    public static string Reg(int index) => "r" + index.ToString("D5", CultureInfo.InvariantCulture);
    public static string Account(int i) => "b" + i.ToString("D3", CultureInfo.InvariantCulture);
    public static string Counter(int i) => "c" + i.ToString("D3", CultureInfo.InvariantCulture);

    public static bool TryParse(string key, out char space, out int index)
    {
        space = key.Length > 0 ? key[0] : '\0';
        index = -1;
        if (key.Length < 2 || (space != 'r' && space != 'b' && space != 'c')) return false;
        return int.TryParse(key.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out index);
    }

    public static byte[] U64(ulong v)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, v);
        return b;
    }

    public static bool TryU64(byte[]? v, out ulong x)
    {
        x = 0;
        if (v is null || v.Length != 8) return false;
        x = BinaryPrimitives.ReadUInt64LittleEndian(v);
        return true;
    }

    // Register value: 'R' | u32 key index | i32 version | u16 payload length | u32 crc32(payload) | payload.
    // The payload is derived from (key, version), so a value proves which write produced it.
    private const int Header = 15;

    public static byte[] RegValue(int key, int version)
    {
        int len = 8 + (int)(Mix((uint)key, (uint)version) % 120);
        byte[] b = new byte[Header + len];
        b[0] = (byte)'R';
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(1), key);
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(5), version);
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(9), (ushort)len);
        Span<byte> payload = b.AsSpan(Header);
        uint x = Mix((uint)key, (uint)version);
        for (int i = 0; i < payload.Length; i++)
        {
            x = x * 1664525 + 1013904223;
            payload[i] = (byte)(x >> 24);
        }
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(11), System.IO.Hashing.Crc32.HashToUInt32(payload));
        return b;
    }

    /// <summary>Decodes a register value: its version, or an error when it is not a value <see cref="RegValue"/> produced for <paramref name="key"/>.</summary>
    public static bool TryDecodeReg(int key, byte[] v, out int version, out string? error)
    {
        version = -1;
        error = null;
        if (v.Length < Header || v[0] != (byte)'R')
        {
            error = "not a register value (" + v.Length + " bytes)";
            return false;
        }
        int k = BinaryPrimitives.ReadInt32LittleEndian(v.AsSpan(1));
        version = BinaryPrimitives.ReadInt32LittleEndian(v.AsSpan(5));
        if (k != key)
        {
            error = "value of key " + k + " stored under key " + key;
            return false;
        }
        byte[] expected = RegValue(key, version);
        if (!expected.AsSpan().SequenceEqual(v))
        {
            error = "value of version " + version + " does not match its deterministic payload/checksum";
            return false;
        }
        return true;
    }

    private static uint Mix(uint a, uint b)
    {
        ulong h = (a * 0x9E3779B97F4A7C15UL) ^ (b * 0xC2B2AE3D27D4EB4FUL);
        h ^= h >> 29;
        h *= 0xBF58476D1CE4E5B9UL;
        h ^= h >> 32;
        return (uint)h;
    }

    public static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);
}
