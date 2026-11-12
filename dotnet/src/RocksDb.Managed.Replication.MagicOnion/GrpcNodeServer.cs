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
/// closing its connection (power loss, a dropped route): HTTP/2 keep-alive pings, also while no call is active. A
/// replica reading a WAL stream sends nothing, so without pings it never learns that the primary is gone and waits on
/// the dead connection for good; a server writing to a vanished replica keeps the stream's handler (and the database it
/// uses) until the host stops.
/// <para>
/// Message sizes: channels and servers keep gRPC's default receive limit of 4 MiB
/// (<see cref="MaxReceiveMessageSize"/>), which bounds the memory one message of a peer can take. Everything this
/// library sends stays below it, whatever the size of the data: a WAL chunk and a fragment of a large write batch carry
/// at most <c>MaxChunkBytes</c> (256 KiB by default), a snapshot message at most <c>SnapshotChunkBytes</c> (1 MiB by
/// default), a piece of a large client write at most <see cref="GrpcWriteOptions.MaxChunkBytes"/>, and a list of files
/// at most <see cref="MaxFileListBytes"/>. Those chunk sizes are configurable and must stay below the limit - leave
/// room for the message's other fields (<see cref="MaxPayloadBytes"/>). The one message that grows without a bound of
/// its own is the term (epoch) map of a node: one short line per leadership (primary) change that wrote entries, so
/// it reaches the limit only after more than 100 000 such changes.
/// </para>
/// </summary>
public static class GrpcTransportDefaults
{
    /// <summary>How long a connection may be silent before a keep-alive ping is sent.</summary>
    public static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(10);

    /// <summary>How long a ping may stay unanswered before the connection is closed (its calls then fail).</summary>
    public static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(10);

    /// <summary>gRPC's default for the largest message a channel or a server receives: 4 MiB. Not changed by this library.</summary>
    public const int MaxReceiveMessageSize = 4 * 1024 * 1024;

    /// <summary>
    /// The largest <c>MaxChunkBytes</c> / <c>SnapshotChunkBytes</c> / <see cref="GrpcWriteOptions.MaxChunkBytes"/> that
    /// still fits into a message of <see cref="MaxReceiveMessageSize"/> together with the message's other fields
    /// (64 KiB are left for ids, a file name and the framing).
    /// </summary>
    public const int MaxPayloadBytes = MaxReceiveMessageSize - 64 * 1024;

    /// <summary>
    /// The most a list of files (the candidates and the answer of a hash request, the inventory of a snapshot request
    /// and with it the plan of the snapshot) may take in one message: 1 MiB, about 10 000 files. A longer list of
    /// candidates is asked in several calls; a longer inventory is cut to its largest files
    /// (<see cref="GrpcClusterClient.StreamSnapshotAsync"/>).
    /// </summary>
    public const int MaxFileListBytes = 1024 * 1024;

    /// <summary>A handler with keep-alive pings (see the class remarks).</summary>
    public static System.Net.Http.SocketsHttpHandler CreateHttpHandler() => new()
    {
        KeepAlivePingDelay = KeepAlivePingDelay,
        KeepAlivePingTimeout = KeepAlivePingTimeout,
        KeepAlivePingPolicy = System.Net.Http.HttpKeepAlivePingPolicy.Always,
        PooledConnectionIdleTimeout = System.Threading.Timeout.InfiniteTimeSpan,
        EnableMultipleHttp2Connections = true,
    };

    /// <summary>Channel options with <see cref="CreateHttpHandler"/> (owned by the channel).</summary>
    public static Grpc.Net.Client.GrpcChannelOptions CreateChannelOptions() => Apply(new Grpc.Net.Client.GrpcChannelOptions());

    /// <summary>
    /// Fills in what <paramref name="options"/> leaves open: the keep-alive handler when neither a handler nor a client
    /// is set. The receive size limit is left as it is (gRPC's default of 4 MiB unless the caller changed it). Returns
    /// <paramref name="options"/>.
    /// </summary>
    public static Grpc.Net.Client.GrpcChannelOptions Apply(Grpc.Net.Client.GrpcChannelOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.HttpHandler is null && options.HttpClient is null)
        {
            options.HttpHandler = CreateHttpHandler();
            options.DisposeHttpClient = true;
        }
        return options;
    }
}

/// <summary>Settings of a <see cref="GrpcNodeServer"/>.</summary>
public sealed class GrpcNodeServerOptions
{
    /// <summary>gRPC's own default for the largest message a server receives: 4 MiB.</summary>
    public const int DefaultMaxReceiveMessageSize = GrpcTransportDefaults.MaxReceiveMessageSize;

    /// <summary>
    /// The largest request message the server accepts (a larger one is refused with ResourceExhausted before the
    /// service sees it); null for no limit. The default is gRPC's, 4 MiB, unchanged: nothing this library sends to a
    /// server needs more - a client write above a chunk arrives in pieces (<c>WriteFragmentsAsync</c>,
    /// <see cref="FragmentedWrites"/>) - and the limit is what bounds the memory one request of any client can take.
    /// Raise it only for clients that send large writes through the unary RPC directly.
    /// </summary>
    public int? MaxReceiveMessageSize { get; init; } = DefaultMaxReceiveMessageSize;

    /// <summary>The limits of large client writes received in pieces.</summary>
    public FragmentedWriteOptions FragmentedWrites { get; init; } = new();

    /// <summary>
    /// Null (the default): plaintext HTTP/2 that accepts every caller - for a trusted network only. Otherwise the
    /// server speaks TLS and accepts only clients that hold the same shared secret (<see cref="SharedSecretTls"/>);
    /// <see cref="GrpcNodeServer.Endpoint"/> is then an <c>https://</c> address. The caller keeps ownership and
    /// disposes it after the server.
    /// </summary>
    public SharedSecretTls? Tls { get; init; }
}

/// <summary>
/// A Kestrel (HTTP/2; plaintext unless <see cref="GrpcNodeServerOptions.Tls"/> is set; loopback by default) server exposing one MagicOnion service backed by a singleton -
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

    /// <summary>The bound address, e.g. "http://127.0.0.1:51234" ("https://..." with <see cref="GrpcNodeServerOptions.Tls"/>).</summary>
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
            o.Listen(address, port, lo =>
            {
                lo.Protocols = HttpProtocols.Http2;
                if (options.Tls is { } tls)
                {
                    lo.UseHttps(tls.ConfigureServer);
                }
            });
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
