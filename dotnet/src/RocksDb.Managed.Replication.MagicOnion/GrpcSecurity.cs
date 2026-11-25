using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// How a gRPC server or client of this library is secured. There is no default: a server or a client says either
/// which shared secret authenticates its peers (<see cref="SharedSecret"/>) or, explicitly, that nothing does
/// (<see cref="InsecureNoAuthentication"/>).
/// </summary>
public sealed class GrpcSecurity
{
    private GrpcSecurity(SharedSecretTls? tls)
    {
        Tls = tls;
    }

    /// <summary>
    /// Plaintext HTTP/2 without any authentication: every caller that reaches the port may make every call, as any
    /// node, and nothing is encrypted. For tests, benchmarks and networks where every reachable process is trusted.
    /// </summary>
    public static GrpcSecurity InsecureNoAuthentication { get; } = new(null);

    /// <summary>
    /// TLS with mutual authentication from the deployment's shared secret; <paramref name="tls"/> carries this
    /// process's identity. The caller keeps ownership of <paramref name="tls"/> and disposes it after the servers and
    /// clients that use it.
    /// </summary>
    public static GrpcSecurity SharedSecret(SharedSecretTls tls)
    {
        ArgumentNullException.ThrowIfNull(tls);
        return new GrpcSecurity(tls);
    }

    /// <summary>The shared-secret TLS settings, or null for <see cref="InsecureNoAuthentication"/>.</summary>
    public SharedSecretTls? Tls { get; }

    /// <summary>
    /// Completes the channel options of a client for <paramref name="endpoint"/>: with a shared secret the endpoint must
    /// be <c>https://</c> and the channel's handler presents this process's certificate and accepts only the node
    /// <paramref name="expectedServerNodeId"/> (any node when null); without authentication the endpoint must be
    /// <c>http://</c>. Fills in the keep-alive handler (<see cref="GrpcTransportDefaults.Apply"/>).
    /// </summary>
    internal GrpcChannelOptions ApplyToChannel(GrpcChannelOptions options, string endpoint, string? expectedServerNodeId)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(endpoint);
        bool https = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
        if (Tls is null)
        {
            if (https)
            {
                throw new ArgumentException("An https:// endpoint needs GrpcSecurity.SharedSecret: GrpcSecurity.InsecureNoAuthentication is plaintext.",
                    nameof(endpoint));
            }
            return GrpcTransportDefaults.Apply(options);
        }
        if (!https)
        {
            throw new ArgumentException("GrpcSecurity.SharedSecret needs an https:// endpoint, not '" + endpoint + "'.", nameof(endpoint));
        }
        if (options.HttpClient is not null)
        {
            throw new ArgumentException("GrpcSecurity.SharedSecret configures the channel's handler: set HttpHandler (a SocketsHttpHandler) or neither, not HttpClient.",
                nameof(options));
        }
        if (options.HttpHandler is null)
        {
            options.HttpHandler = GrpcTransportDefaults.CreateHttpHandler();
            options.DisposeHttpClient = true;
        }
        if (options.HttpHandler is not System.Net.Http.SocketsHttpHandler handler)
        {
            throw new ArgumentException("GrpcSecurity.SharedSecret needs a SocketsHttpHandler as the channel's HttpHandler.", nameof(options));
        }
        Tls.ConfigureClient(handler, expectedServerNodeId);
        return options;
    }
}

/// <summary>
/// Tells a service who calls it: the identity the caller's certificate proved in the TLS handshake, or
/// <see cref="ReplicationCaller.Unauthenticated"/> on a server that runs without authentication. Registered as a
/// singleton by <see cref="GrpcNodeServer"/>; a host that maps the services in its own ASP.NET Core application
/// registers one itself (with the <see cref="SharedSecretTls"/> its Kestrel endpoint uses).
/// </summary>
public sealed class GrpcCallerResolver
{
    private static readonly object s_itemKey = new();

    private readonly SharedSecretTls? _tls;

    public GrpcCallerResolver(GrpcSecurity security)
    {
        ArgumentNullException.ThrowIfNull(security);
        _tls = security.Tls;
    }

    /// <summary>The caller of <paramref name="context"/>. Throws Unauthenticated when the connection proved no identity.</summary>
    public ReplicationCaller Resolve(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (_tls is null) return ReplicationCaller.Unauthenticated;
        HttpContext http = context.GetHttpContext();
        // One connection, one certificate: verified once per connection, not per call (heartbeats are frequent).
        IDictionary<object, object?>? items = http.Features.Get<IConnectionItemsFeature>()?.Items;
        if (items is not null && items.TryGetValue(s_itemKey, out object? cached) && cached is ReplicationCaller known) return known;
        if (!_tls.TryAuthenticate(http.Connection.ClientCertificate, out ReplicationCaller caller))
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.Unauthenticated, "the connection proved no identity");
        }
        if (items is not null)
        {
            lock (items) items[s_itemKey] = caller;
        }
        return caller;
    }
}

/// <summary>Maps a refusal of the host's <see cref="ReplicationAuthorizer"/> to gRPC's PermissionDenied.</summary>
internal static class GrpcAuthorization
{
    public static ReplicationCaller Peer(GrpcCallerResolver callers, ServerCallContext context, ReplicationAuthorizer authorizer, string? claimedNodeId,
        string operation)
    {
        ReplicationCaller caller = callers.Resolve(context);
        try
        {
            authorizer.RequirePeer(caller, claimedNodeId, operation);
        }
        catch (ReplicationUnauthorizedException e)
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.PermissionDenied, e.Message);
        }
        return caller;
    }

    public static void Replica(GrpcCallerResolver callers, ServerCallContext context, ReplicationAuthorizer authorizer, int replicaIndex, string operation)
    {
        ReplicationCaller caller = callers.Resolve(context);
        try
        {
            authorizer.RequireReplica(caller, replicaIndex, operation);
        }
        catch (ReplicationUnauthorizedException e)
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.PermissionDenied, e.Message);
        }
    }

    public static void OperatorOrPeer(GrpcCallerResolver callers, ServerCallContext context, ReplicationAuthorizer authorizer, string operation)
    {
        ReplicationCaller caller = callers.Resolve(context);
        try
        {
            authorizer.RequireOperatorOrPeer(caller, operation);
        }
        catch (ReplicationUnauthorizedException e)
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.PermissionDenied, e.Message);
        }
    }

    public static void Authenticated(GrpcCallerResolver callers, ServerCallContext context, ReplicationAuthorizer authorizer, string operation)
    {
        ReplicationCaller caller = callers.Resolve(context);
        try
        {
            authorizer.RequireAuthenticated(caller, operation);
        }
        catch (ReplicationUnauthorizedException e)
        {
            throw new ReturnStatusException(Grpc.Core.StatusCode.PermissionDenied, e.Message);
        }
    }
}
