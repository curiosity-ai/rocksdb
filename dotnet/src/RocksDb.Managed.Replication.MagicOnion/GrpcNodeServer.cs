using System.Net;
using MagicOnion;
using MagicOnion.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// Settings every gRPC channel and server of this library needs to be safe against a peer that vanishes without
/// closing its connection (power loss, a dropped route) and against large log entries:
/// <list type="bullet">
/// <item>HTTP/2 keep-alive pings, also while no call is active. A replica reading a WAL stream sends nothing, so
/// without pings it never learns that the primary is gone and waits on the dead connection for good; a server writing
/// to a vanished replica keeps the stream's handler (and the database it uses) until the host stops.</item>
/// <item>No limit on the size of a message a channel receives. gRPC's default of 4 MB is below what one write batch
/// may hold. A source of this version sends a batch larger than its chunk size in fragments to a follower that asked
/// for them (<see cref="Hosting.WalStreamCapabilities.Fragments"/>), so no message exceeds the chunk size; but a source
/// built before that - and a follower configured not to ask - still gets a chunk as one message, at least one whole
/// batch, and a replica that cannot receive it retries the same chunk forever. The limit therefore stays lifted on
/// channels. Servers keep gRPC's default (<see cref="GrpcNodeServerOptions.MaxReceiveMessageSize"/>).</item>
/// </list>
/// </summary>
public static class GrpcTransportDefaults
{
    /// <summary>How long a connection may be silent before a keep-alive ping is sent.</summary>
    public static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(10);

    /// <summary>How long a ping may stay unanswered before the connection is closed (its calls then fail).</summary>
    public static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A handler with keep-alive pings (see the class remarks).</summary>
    public static System.Net.Http.SocketsHttpHandler CreateHttpHandler() => new()
    {
        KeepAlivePingDelay = KeepAlivePingDelay,
        KeepAlivePingTimeout = KeepAlivePingTimeout,
        KeepAlivePingPolicy = System.Net.Http.HttpKeepAlivePingPolicy.Always,
        PooledConnectionIdleTimeout = System.Threading.Timeout.InfiniteTimeSpan,
        EnableMultipleHttp2Connections = true,
    };

    /// <summary>Channel options with <see cref="CreateHttpHandler"/> (owned by the channel) and no receive size limit.</summary>
    public static Grpc.Net.Client.GrpcChannelOptions CreateChannelOptions() => Apply(new Grpc.Net.Client.GrpcChannelOptions());

    /// <summary>
    /// Fills in what <paramref name="options"/> leaves open: the keep-alive handler when neither a handler nor a client
    /// is set, and no receive size limit when the limit is still gRPC's default (4 MB). Returns <paramref name="options"/>.
    /// </summary>
    public static Grpc.Net.Client.GrpcChannelOptions Apply(Grpc.Net.Client.GrpcChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.HttpHandler is null && options.HttpClient is null)
        {
            options.HttpHandler = CreateHttpHandler();
            options.DisposeHttpClient = true;
        }
        if (options.MaxReceiveMessageSize == DefaultMaxReceiveMessageSize) options.MaxReceiveMessageSize = null;
        return options;
    }

    private const int DefaultMaxReceiveMessageSize = 4 * 1024 * 1024;
}

/// <summary>Settings of a <see cref="GrpcNodeServer"/>.</summary>
public sealed class GrpcNodeServerOptions
{
    /// <summary>gRPC's own default for the largest message a server receives: 4 MiB.</summary>
    public const int DefaultMaxReceiveMessageSize = 4 * 1024 * 1024;

    /// <summary>
    /// The largest request message the server accepts (a larger one is refused with ResourceExhausted before the
    /// service sees it); null for no limit. The default is gRPC's, 4 MiB, unchanged: nothing this library sends to a
    /// server needs more - a client write above a chunk arrives in pieces (<c>WriteFragmentsAsync</c>,
    /// <see cref="FragmentedWrites"/>) - and the limit is what bounds the memory one request of any client can take.
    /// Raise it only for clients that cannot fragment (ones built before <c>WriteFragmentsAsync</c> sending large
    /// unary writes).
    /// </summary>
    public int? MaxReceiveMessageSize { get; init; } = DefaultMaxReceiveMessageSize;

    /// <summary>The limits of large client writes received in pieces.</summary>
    public FragmentedWriteOptions FragmentedWrites { get; init; } = new();
}

/// <summary>
/// A Kestrel (HTTP/2, plaintext, loopback by default) server exposing one MagicOnion service backed by a singleton -
/// the StartServerAsync of rocksdb-sharp's sample hosts. Port 0 picks a free port; <see cref="Endpoint"/> is the bound
/// address.
/// </summary>
public sealed class GrpcNodeServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private GrpcNodeServer(WebApplication app, string endpoint, FragmentedWriteBudget fragmentedWrites)
    {
        _app = app;
        Endpoint = endpoint;
        FragmentedWrites = fragmentedWrites;
    }

    /// <summary>The large client writes this server is receiving (<see cref="GrpcNodeServerOptions.FragmentedWrites"/>).</summary>
    public FragmentedWriteBudget FragmentedWrites { get; }

    /// <summary>The bound address, e.g. "http://127.0.0.1:51234".</summary>
    public string Endpoint { get; }

    /// <summary>Starts a server for <typeparamref name="TService"/> whose constructor receives <paramref name="backend"/>.</summary>
    public static Task<GrpcNodeServer> StartAsync<TService, TBackend>(TBackend backend, IPAddress address, int port, CancellationToken cancellationToken)
        where TService : class, IServiceMarker
        where TBackend : class =>
        StartAsync<TService, TBackend>(backend, address, port, new GrpcNodeServerOptions(), cancellationToken);

    /// <summary>Starts a server for <typeparamref name="TService"/> with <paramref name="options"/>.</summary>
    public static async Task<GrpcNodeServer> StartAsync<TService, TBackend>(TBackend backend, IPAddress address, int port, GrpcNodeServerOptions options,
        CancellationToken cancellationToken)
        where TService : class, IServiceMarker
        where TBackend : class
    {
        ArgumentNullException.ThrowIfNull(backend);
        ArgumentNullException.ThrowIfNull(address);
        ArgumentNullException.ThrowIfNull(options);
        var fragmentedWrites = new FragmentedWriteBudget(options.FragmentedWrites);
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddGrpc(o => o.MaxReceiveMessageSize = options.MaxReceiveMessageSize);
        builder.Services.AddMagicOnion();
        builder.Services.AddSingleton(backend);
        builder.Services.AddSingleton(fragmentedWrites);
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.Listen(address, port, lo => lo.Protocols = HttpProtocols.Http2);
            // A client that vanished without closing its connection is noticed (and its streams end) through pings.
            o.Limits.Http2.KeepAlivePingDelay = GrpcTransportDefaults.KeepAlivePingDelay;
            o.Limits.Http2.KeepAlivePingTimeout = GrpcTransportDefaults.KeepAlivePingTimeout;
        });
        WebApplication app = builder.Build();
        app.MapMagicOnionService<TService>();
        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        string bound = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new GrpcNodeServer(app, bound.Replace("[::]", "127.0.0.1", StringComparison.Ordinal), fragmentedWrites);
    }

    /// <summary>
    /// A currently free loopback TCP port, for configurations that must name every member's endpoint before the servers
    /// start (a Raft member list). Another process may take it in between; callers retry on a bind failure.
    /// </summary>
    public static int FindFreePort()
    {
        using var l = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    /// <summary>Starts a loopback server on a free port.</summary>
    public static Task<GrpcNodeServer> StartAsync<TService, TBackend>(TBackend backend, CancellationToken cancellationToken)
        where TService : class, IServiceMarker
        where TBackend : class =>
        StartAsync<TService, TBackend>(backend, IPAddress.Loopback, 0, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await _app.StopAsync(timeout.Token).ConfigureAwait(false);
        }
        finally
        {
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
