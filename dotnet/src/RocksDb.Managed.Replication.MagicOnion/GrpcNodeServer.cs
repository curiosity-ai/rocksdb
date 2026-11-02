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
        builder.WebHost.ConfigureKestrel(o => o.Listen(address, port, lo => lo.Protocols = HttpProtocols.Http2));
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
