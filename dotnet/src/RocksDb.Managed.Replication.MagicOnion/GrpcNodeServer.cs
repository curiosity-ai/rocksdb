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
/// <item>No limit on the size of a received message. gRPC's default of 4 MB is below what one write batch may hold:
/// a WAL chunk is at least one whole batch, and a replica that cannot receive it retries the same chunk forever.</item>
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

/// <summary>
/// A Kestrel (HTTP/2, plaintext, loopback by default) server exposing one MagicOnion service backed by a singleton -
/// the StartServerAsync of rocksdb-sharp's sample hosts. Port 0 picks a free port; <see cref="Endpoint"/> is the bound
/// address.
/// </summary>
public sealed class GrpcNodeServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private GrpcNodeServer(WebApplication app, string endpoint)
    {
        _app = app;
        Endpoint = endpoint;
    }

    /// <summary>The bound address, e.g. "http://127.0.0.1:51234".</summary>
    public string Endpoint { get; }

    /// <summary>Starts a server for <typeparamref name="TService"/> whose constructor receives <paramref name="backend"/>.</summary>
    public static async Task<GrpcNodeServer> StartAsync<TService, TBackend>(TBackend backend, IPAddress address, int port, CancellationToken cancellationToken)
        where TService : class, IServiceMarker
        where TBackend : class
    {
        ArgumentNullException.ThrowIfNull(backend);
        ArgumentNullException.ThrowIfNull(address);
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddGrpc();
        builder.Services.AddMagicOnion();
        builder.Services.AddSingleton(backend);
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
        return new GrpcNodeServer(app, bound.Replace("[::]", "127.0.0.1", StringComparison.Ordinal));
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
