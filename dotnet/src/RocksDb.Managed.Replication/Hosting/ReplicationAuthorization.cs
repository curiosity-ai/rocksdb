namespace RocksDb.Managed.Replication.Hosting;

/// <summary>What an authenticated caller of a replication endpoint is.</summary>
public enum ReplicationCallerKind : byte
{
    /// <summary>A node of the deployment (a pair node, a cluster member, a replica of a plain primary), named by its node id.</summary>
    Node = 1,

    /// <summary>An application client: may read the status and write, never call a node-to-node or an operator endpoint.</summary>
    Client = 2,

    /// <summary>An operator or coordinator: a client that may also ask for role changes (a planned handoff).</summary>
    Operator = 3,
}

/// <summary>
/// Who calls a replication endpoint, as the transport established it: the gRPC services take it from the caller's
/// certificate (<c>SharedSecretTls</c> binds the kind and the name into the handshake), an in-process transport states
/// it. The hosts authorize a call from it (<see cref="ReplicationAuthorizer"/>).
/// </summary>
public readonly record struct ReplicationCaller
{
    private readonly byte _trust; // 0: authenticated identity, 1: not authenticated (allowed by an explicit opt-in)

    private ReplicationCaller(ReplicationCallerKind kind, string name, byte trust)
    {
        Kind = kind;
        Name = name;
        _trust = trust;
    }

    /// <summary>The caller's class. Meaningless when <see cref="IsUnauthenticated"/>.</summary>
    public ReplicationCallerKind Kind { get; }

    /// <summary>The node id of a <see cref="ReplicationCallerKind.Node"/>; the (informational) name of a client or operator.</summary>
    public string Name { get; }

    /// <summary>
    /// True for the caller of a transport that runs without authentication because its owner said so explicitly
    /// (<c>GrpcSecurity.InsecureNoAuthentication</c>): every call is allowed, as nothing about the caller is known.
    /// </summary>
    public bool IsUnauthenticated => _trust == 1;

    /// <summary>The caller of a transport that authenticates nobody (see <see cref="IsUnauthenticated"/>).</summary>
    public static ReplicationCaller Unauthenticated => new(default, "", 1);

    /// <summary>The node <paramref name="nodeId"/>.</summary>
    public static ReplicationCaller Node(string nodeId) => new(ReplicationCallerKind.Node, Checked(nodeId, nameof(nodeId)), 0);

    /// <summary>An application client; <paramref name="name"/> only appears in refusals and logs.</summary>
    public static ReplicationCaller Client(string name) => new(ReplicationCallerKind.Client, Checked(name, nameof(name)), 0);

    /// <summary>An operator; <paramref name="name"/> only appears in refusals and logs.</summary>
    public static ReplicationCaller Operator(string name) => new(ReplicationCallerKind.Operator, Checked(name, nameof(name)), 0);

    private static string Checked(string name, string parameter)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("A caller needs a name.", parameter);
        return name;
    }

    public override string ToString() => IsUnauthenticated ? "unauthenticated caller" : Kind + " '" + Name + "'";
}

/// <summary>A call was refused: the caller is not allowed to make it (gRPC: PermissionDenied).</summary>
public sealed class ReplicationUnauthorizedException : Exception
{
    public ReplicationUnauthorizedException(string operation, string message) : base(operation + ": " + message)
    {
        Operation = operation;
    }

    /// <summary>The endpoint that was called.</summary>
    public string Operation { get; }
}

/// <summary>
/// Decides which caller may use which endpoint of a host. Every transport asks it before it hands a call to the host
/// (the gRPC services do; an in-process transport must too): the hosts' <c>Handle...</c> methods themselves are the
/// in-process API of the application that owns the host and check nothing.
/// <list type="bullet">
/// <item><see cref="RequirePeer"/>: node-to-node endpoints (votes, heartbeats, attach checks, WAL and snapshot streams,
/// progress reports, promotion requests, term / epoch maps). Only a <see cref="ReplicationCallerKind.Node"/> that is a
/// configured peer of this node, and only under its own name: a request that names a node (the candidate, the leader,
/// the follower, the old primary) must name the caller.</item>
/// <item><see cref="RequireOperatorOrPeer"/>: role changes asked for from outside (a planned handoff): an
/// <see cref="ReplicationCallerKind.Operator"/> or a configured peer.</item>
/// <item><see cref="RequireAuthenticated"/>: client endpoints (status, writes, checksums): any authenticated caller.</item>
/// </list>
/// A caller of a transport without authentication (<see cref="ReplicationCaller.IsUnauthenticated"/>) passes every check.
/// </summary>
public sealed class ReplicationAuthorizer
{
    private readonly string _selfId;
    private readonly HashSet<string> _peers;
    private readonly IReadOnlyList<string>? _replicasByIndex;

    /// <summary>For a node <paramref name="selfId"/> whose peers are <paramref name="memberIds"/> (its own id, if listed, is ignored).</summary>
    public ReplicationAuthorizer(string selfId, IEnumerable<string> memberIds)
    {
        ArgumentNullException.ThrowIfNull(selfId);
        ArgumentNullException.ThrowIfNull(memberIds);
        _selfId = selfId;
        _peers = new HashSet<string>(StringComparer.Ordinal);
        foreach (string id in memberIds)
        {
            if (!string.Equals(id, selfId, StringComparison.Ordinal)) _peers.Add(id);
        }
    }

    private ReplicationAuthorizer(IReadOnlyList<string> replicasByIndex)
    {
        _selfId = "";
        _peers = new HashSet<string>(replicasByIndex, StringComparer.Ordinal);
        _replicasByIndex = replicasByIndex;
    }

    /// <summary>
    /// For a plain replication primary: the node ids of its replicas, in the order of their replica index (the index a
    /// replica reports its progress under must be its own position in this list).
    /// </summary>
    public static ReplicationAuthorizer ForReplicas(IReadOnlyList<string> replicaNodeIds)
    {
        ArgumentNullException.ThrowIfNull(replicaNodeIds);
        return new ReplicationAuthorizer(new List<string>(replicaNodeIds));
    }

    /// <summary>The node ids whose node-to-node calls are accepted.</summary>
    public IReadOnlyCollection<string> Peers => _peers;

    /// <summary>
    /// A node-to-node call: the caller must be a configured peer, and <paramref name="claimedNodeId"/> (the node the
    /// request says it comes from; null when the request names none) must be the caller.
    /// </summary>
    public void RequirePeer(in ReplicationCaller caller, string? claimedNodeId, string operation)
    {
        if (caller.IsUnauthenticated) return;
        if (caller.Kind != ReplicationCallerKind.Node) throw new ReplicationUnauthorizedException(operation, caller + " is not a node");
        if (!_peers.Contains(caller.Name)) throw new ReplicationUnauthorizedException(operation, caller + " is not a configured peer of this node");
        if (claimedNodeId is not null && !string.Equals(claimedNodeId, caller.Name, StringComparison.Ordinal))
        {
            throw new ReplicationUnauthorizedException(operation, caller + " cannot act as node '" + claimedNodeId + "'");
        }
    }

    /// <summary>
    /// A progress report of a plain primary's replica: <see cref="RequirePeer"/>, and <paramref name="replicaIndex"/>
    /// must be the caller's position in the list given to <see cref="ForReplicas"/>.
    /// </summary>
    public void RequireReplica(in ReplicationCaller caller, int replicaIndex, string operation)
    {
        if (caller.IsUnauthenticated) return;
        RequirePeer(caller, null, operation);
        if (_replicasByIndex is null || replicaIndex < 0 || replicaIndex >= _replicasByIndex.Count ||
            !string.Equals(_replicasByIndex[replicaIndex], caller.Name, StringComparison.Ordinal))
        {
            throw new ReplicationUnauthorizedException(operation, caller + " is not replica " + replicaIndex);
        }
    }

    /// <summary>A role change asked for from outside the node: an operator, or a configured peer.</summary>
    public void RequireOperatorOrPeer(in ReplicationCaller caller, string operation)
    {
        if (caller.IsUnauthenticated || caller.Kind == ReplicationCallerKind.Operator) return;
        if (caller.Kind == ReplicationCallerKind.Node && _peers.Contains(caller.Name)) return;
        throw new ReplicationUnauthorizedException(operation, caller + " is neither an operator nor a configured peer of this node");
    }

    /// <summary>A client endpoint: any caller the transport authenticated (clients, operators, nodes).</summary>
    public void RequireAuthenticated(in ReplicationCaller caller, string operation)
    {
        if (caller.IsUnauthenticated) return;
        if (caller.Kind is ReplicationCallerKind.Node or ReplicationCallerKind.Client or ReplicationCallerKind.Operator) return;
        throw new ReplicationUnauthorizedException(operation, "the caller has no identity");
    }
}
