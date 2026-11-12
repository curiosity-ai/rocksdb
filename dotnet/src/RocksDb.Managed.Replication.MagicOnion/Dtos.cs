using MessagePack;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.MagicOnion;

// Wire DTOs (MessagePack). Byte payloads are ReadOnlyMemory<byte>: on the sending side they refer to the tailer's /
// snapshot reader's buffer, which stays valid until the awaited stream write returns, so nothing is copied before
// serialization.

[MessagePackObject]
public sealed class WalChunkDto
{
    [Key(0)] public ulong FirstSequence { get; set; }
    [Key(1)] public int EntryCount { get; set; }
    [Key(2)] public ReadOnlyMemory<byte> Data { get; set; }
    [Key(3)] public string SourceId { get; set; } = "";
    [Key(4)] public long SourceTerm { get; set; }
    [Key(5)] public long EntryTerm { get; set; }
    /// <summary>
    /// For a fragment of a large write batch (<see cref="WalStreamMessage.IsFragment"/>): where <see cref="Data"/>
    /// starts in the serialized batch.
    /// </summary>
    [Key(6)] public int FragmentOffset { get; set; }
    /// <summary>For a fragment: the length of the whole serialized batch. 0: an ordinary message.</summary>
    [Key(7)] public int TotalLength { get; set; }
    [Key(8)] public bool ResyncRequired { get; set; }
    /// <summary>With <see cref="ResyncRequired"/>: <see cref="WalStreamMessage.SourceFault"/>.</summary>
    [Key(9)] public bool SourceFault { get; set; }
    [Key(10)] public string? Reason { get; set; }

    public static WalChunkDto From(in WalStreamMessage m) => new()
    {
        FirstSequence = m.FirstSequence,
        EntryCount = m.EntryCount,
        Data = m.Data,
        SourceId = m.SourceId,
        SourceTerm = m.SourceTerm,
        EntryTerm = m.EntryTerm,
        ResyncRequired = m.ResyncRequired,
        Reason = m.Reason,
        SourceFault = m.SourceFault,
        FragmentOffset = m.FragmentOffset,
        TotalLength = m.TotalLength,
    };

    public WalStreamMessage ToMessage() => ResyncRequired
        ? SourceFault
            ? WalStreamMessage.ResyncAfterSourceFault(SourceId, SourceTerm, Reason ?? "resync required")
            : WalStreamMessage.Resync(SourceId, SourceTerm, Reason ?? "resync required")
        : TotalLength != 0
            ? WalStreamMessage.Fragment(FirstSequence, FragmentOffset, TotalLength, EntryCount, Data, SourceId, SourceTerm, EntryTerm)
            : new WalStreamMessage(FirstSequence, EntryCount, Data, SourceId, SourceTerm, EntryTerm);
}

[MessagePackObject]
public sealed class SnapshotChunkDto
{
    [Key(0)] public int Kind { get; set; }
    [Key(1)] public ulong SequenceNumber { get; set; }
    [Key(2)] public List<string> KeepFiles { get; set; } = [];
    [Key(3)] public string? FileName { get; set; }
    [Key(4)] public long FileSize { get; set; }
    [Key(5)] public long Offset { get; set; }
    [Key(6)] public ReadOnlyMemory<byte> Content { get; set; }

    public static SnapshotChunkDto From(in SnapshotChunk c) => new()
    {
        Kind = (int)c.Kind,
        SequenceNumber = c.SequenceNumber,
        KeepFiles = [.. c.KeepFiles],
        FileName = c.FileName,
        FileSize = c.FileSize,
        Offset = c.Offset,
        Content = c.Content,
    };

    /// <summary>
    /// Throws <see cref="InvalidDataException"/> for an unknown kind: taking it for
    /// file data would write the message's content into the database directory - or, with an empty file name, fail
    /// with a message about the name - where the restore must simply fail.
    /// </summary>
    public SnapshotChunk ToChunk() => (SnapshotChunkKind)Kind switch
    {
        SnapshotChunkKind.Plan => SnapshotChunk.Plan(SequenceNumber, KeepFiles ?? []),
        SnapshotChunkKind.Complete => FileSize is >= 0 and <= int.MaxValue
            ? SnapshotChunk.Complete((int)FileSize)
            : throw new InvalidDataException("snapshot end marker with file count " + FileSize),
        SnapshotChunkKind.FileData => SnapshotChunk.FileData(FileName ?? "", FileSize, Offset, Content),
        _ => throw new InvalidDataException("snapshot chunk of unknown kind " + Kind),
    };
}

[MessagePackObject]
public sealed class FileInfoDto
{
    [Key(0)] public string Name { get; set; } = "";
    [Key(1)] public long Size { get; set; }
    [Key(2)] public string Hash { get; set; } = "";

    public static List<FileInfoDto> From(IReadOnlyList<ReplicationFileInfo> files)
    {
        var list = new List<FileInfoDto>(files.Count);
        foreach (ReplicationFileInfo f in files) list.Add(new FileInfoDto { Name = f.FileName, Size = f.Size, Hash = f.Hash });
        return list;
    }

    public static List<ReplicationFileInfo> To(List<FileInfoDto>? files)
    {
        var list = new List<ReplicationFileInfo>();
        if (files == null) return list;
        foreach (FileInfoDto f in files) list.Add(new ReplicationFileInfo { FileName = f.Name, Size = f.Size, Hash = f.Hash ?? "" });
        return list;
    }
}

[MessagePackObject]
public sealed class DigestDto
{
    [Key(0)] public bool Available { get; set; }
    [Key(1)] public long KeyCount { get; set; }
    [Key(2)] public ulong Hash { get; set; }
    [Key(3)] public ulong LatestSeq { get; set; }

    public static DigestDto From(DatabaseDigest? d) =>
        d is { } x ? new DigestDto { Available = true, KeyCount = x.KeyCount, Hash = x.Hash, LatestSeq = x.LatestSequence } : new DigestDto();

    public DatabaseDigest? ToDigest() => Available ? new DatabaseDigest(KeyCount, Hash, LatestSeq) : null;
}

// ---- cluster ----------------------------------------------------------------------------------------------------

[MessagePackObject]
public sealed class RequestVoteDto
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public string CandidateId { get; set; } = "";
    [Key(2)] public ulong LastLogSeq { get; set; }
    [Key(3)] public long LastLogTerm { get; set; }
}

[MessagePackObject]
public sealed class RequestVoteResultDto
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public bool VoteGranted { get; set; }
}

[MessagePackObject]
public sealed class HeartbeatDto
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public string LeaderId { get; set; } = "";
    [Key(2)] public ulong PrevLogSeq { get; set; }
    [Key(3)] public long PrevLogTerm { get; set; }
    [Key(4)] public ulong LeaderCommit { get; set; }
    [Key(5)] public int LeaderMode { get; set; }
}

[MessagePackObject]
public sealed class AppendEntriesDto
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public bool Success { get; set; }
    [Key(2)] public ulong LastSeq { get; set; }
    [Key(3)] public AppendEntriesRejectReason Reason { get; set; }
}

[MessagePackObject]
public sealed class ClusterStatusDto
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public int Role { get; set; }
    [Key(2)] public long Term { get; set; }
    [Key(3)] public string? LeaderId { get; set; }
    [Key(4)] public ulong LatestSeq { get; set; }
    [Key(5)] public ulong CommitSeq { get; set; }
    [Key(6)] public int Mode { get; set; }
    [Key(7)] public int BootstrapInSyncCount { get; set; }
    [Key(8)] public bool Resyncing { get; set; }
    [Key(9)] public int Resyncs { get; set; }

    public static ClusterStatusDto From(ClusterNodeStatus s) => new()
    {
        NodeId = s.NodeId,
        Role = (int)s.Role,
        Term = s.Term,
        LeaderId = s.LeaderId,
        LatestSeq = s.LatestSeq,
        CommitSeq = s.CommitSeq,
        Mode = (int)s.Mode,
        BootstrapInSyncCount = s.BootstrapInSyncCount,
        Resyncing = s.Resyncing,
        Resyncs = s.Resyncs,
    };

    public ClusterNodeStatus ToStatus() =>
        new(NodeId, (RaftRole)Role, Term, LeaderId, LatestSeq, CommitSeq, (ClusterMode)Mode, BootstrapInSyncCount, Resyncing, Resyncs);
}

[MessagePackObject]
public sealed class AttachDto
{
    [Key(0)] public bool IsLeader { get; set; }
    [Key(1)] public bool Consistent { get; set; }
    [Key(2)] public long Term { get; set; }
    [Key(3)] public string Terms { get; set; } = "";
}

// ---- pair -------------------------------------------------------------------------------------------------------

[MessagePackObject]
public sealed class PairStatusDto
{
    [Key(0)] public bool Available { get; set; }
    [Key(1)] public string NodeId { get; set; } = "";
    [Key(2)] public int Role { get; set; }
    [Key(3)] public long Epoch { get; set; }
    [Key(4)] public long PromisedEpoch { get; set; }
    [Key(5)] public string? PrimaryId { get; set; }
    [Key(6)] public ulong LatestSeq { get; set; }
    [Key(7)] public bool Promoting { get; set; }

    public static PairStatusDto From(ReplicationNodeStatus? s) => s == null ? new PairStatusDto() : new PairStatusDto
    {
        Available = true,
        NodeId = s.NodeId,
        Role = (int)s.Role,
        Epoch = s.Epoch,
        PromisedEpoch = s.PromisedEpoch,
        PrimaryId = s.PrimaryId,
        LatestSeq = s.LatestSeq,
        Promoting = s.Promoting,
    };

    public ReplicationNodeStatus? ToStatus() =>
        Available ? new ReplicationNodeStatus(NodeId, (ReplicationRole)Role, Epoch, PromisedEpoch, PrimaryId, LatestSeq, Promoting) : null;
}

[MessagePackObject]
public sealed class PromoteDto
{
    [Key(0)] public long Epoch { get; set; }
    [Key(1)] public string FromPrimaryId { get; set; } = "";
    [Key(2)] public ulong HandoffSeq { get; set; }
    [Key(3)] public int CatchUpTimeoutMs { get; set; }
}

[MessagePackObject]
public sealed class PromoteResultDto
{
    [Key(0)] public bool Promoted { get; set; }
    [Key(1)] public long Epoch { get; set; }
    [Key(2)] public int Role { get; set; }
    [Key(3)] public string? Reason { get; set; }
}

[MessagePackObject]
public sealed class ReplicaLogCheckDto
{
    [Key(0)] public bool IsPrimary { get; set; }
    [Key(1)] public bool Consistent { get; set; }
    [Key(2)] public long Epoch { get; set; }
    [Key(3)] public string Epochs { get; set; } = "";
}

[MessagePackObject]
public sealed class PairWriteResultDto
{
    /// <summary>A message without the field is not an acknowledgement.</summary>
    [Key(0)] public PairWriteStatus Status { get; set; } = PairWriteStatus.NotPrimary;
    [Key(1)] public string? PrimaryHint { get; set; }
    [Key(2)] public ulong Sequence { get; set; }

    /// <summary>Whether the write was acknowledged (<see cref="PairWriteStatus.Applied"/>). Not on the wire.</summary>
    [IgnoreMember] public bool Applied => Status == PairWriteStatus.Applied;

    public static PairWriteResultDto From(in PairWriteResult r) => new() { Status = r.Status, PrimaryHint = r.PrimaryHint, Sequence = r.Sequence };
}

[MessagePackObject]
public sealed class SwitchoverResultDto
{
    [Key(0)] public int Outcome { get; set; }
    [Key(1)] public long Epoch { get; set; }
    [Key(2)] public ulong HandoffSeq { get; set; }
    [Key(3)] public double WritesHeldMs { get; set; }
    [Key(4)] public string? Reason { get; set; }

    public static SwitchoverResultDto From(SwitchoverResult r) => new()
    {
        Outcome = (int)r.Outcome,
        Epoch = r.Epoch,
        HandoffSeq = r.HandoffSeq,
        WritesHeldMs = r.WritesHeld.TotalMilliseconds,
        Reason = r.Reason,
    };

    public SwitchoverResult ToResult() => new((SwitchoverOutcome)Outcome, Epoch, HandoffSeq, TimeSpan.FromMilliseconds(WritesHeldMs), Reason);
}
