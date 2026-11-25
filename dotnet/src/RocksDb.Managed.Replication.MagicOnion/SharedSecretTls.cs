using System.Formats.Asn1;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using RocksDb.Managed.Replication.Hosting;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// Encryption and mutual authentication of the gRPC transport from a secret the nodes of a deployment share, with the
/// identity of each holder bound into the handshake.
/// <para>
/// Each instance stands for one participant: a node (named by its node id), an operator or an application client
/// (<see cref="ReplicationCaller"/>). It creates a random key pair and a self-signed certificate that carries, in private
/// extensions, that identity and an HMAC over its public key and its identity, keyed with (a key derived from) the
/// secret. Servers and clients both present their certificate (mutual TLS) and accept a peer only when the HMAC in the
/// peer's certificate matches (<see cref="TryAuthenticate"/>). Only a holder of the secret can make such a certificate, a
/// copied certificate is useless without its private key (the TLS handshake proves possession), the identity cannot be
/// changed without the secret, and the secret itself never travels. A server learns who calls it
/// (<see cref="GrpcCallerResolver"/>) and the hosts decide what that caller may do
/// (<see cref="ReplicationAuthorizer"/>); a client checks that the server is the node it meant to reach
/// (<see cref="CreateChannelOptions(string)"/>).
/// </para>
/// <para>
/// <b>What the identity is worth.</b> The secret is the trust root: whoever holds the nodes' secret can make a
/// certificate for any identity. The identity therefore protects against a node (or tool) that is misconfigured or
/// talks to the wrong member of the deployment, and keeps the classes apart only as far as their secrets are apart. To
/// keep application clients from acting as nodes or operators, give them a secret of their own: nodes are created with
/// both secrets (<see cref="SharedSecretTls(ReadOnlySpan{byte}, ReadOnlySpan{byte}, ReplicationCaller)"/>), clients
/// with the client secret only (<see cref="ForClient"/>). A holder of only the client secret is accepted as a
/// <see cref="ReplicationCallerKind.Client"/> and as nothing else. (It can verify that a server holds the client
/// secret, not that it holds the nodes' secret: clients trust each other not to pose as a node towards other clients.)
/// </para>
/// <para>
/// What is not checked: host names, validity dates, certificate chains and revocation. To change a secret, restart
/// the nodes with the new one; a node accepts one secret of each kind at a time.
/// </para>
/// <para>
/// Use: one instance per process, given to the server (<see cref="GrpcSecurity.SharedSecret"/> in
/// <see cref="GrpcNodeServerOptions.Security"/>) and to every client of the process (with an <c>https://</c>
/// endpoint). A host that runs the MagicOnion services in its own ASP.NET Core application calls
/// <see cref="ConfigureServer"/> in its Kestrel <c>UseHttps</c> callback and registers a
/// <see cref="GrpcCallerResolver"/> for the services. Dispose the instance after the servers and channels that use it.
/// </para>
/// </summary>
public sealed class SharedSecretTls : IDisposable
{
    /// <summary>The shortest secret accepted, in bytes. Use random bytes: the secret is not stretched like a password.</summary>
    public const int MinSecretBytes = 16;

    /// <summary>The longest caller name (node id) accepted, in UTF-8 bytes.</summary>
    public const int MaxNameBytes = 256;

    // Private extension ids (ITU-T X.667: 2.25.<a UUID as a decimal number>), used for nothing else.
    internal const string NodeProofExtensionOid = "2.25.204763142377514856327684105921739526041";
    internal const string IdentityExtensionOid = "2.25.204763142377514856327684105921739526042";
    internal const string ClientProofExtensionOid = "2.25.204763142377514856327684105921739526043";

    private static readonly byte[] s_nodeKeyLabel = "RocksDb.Managed.Replication shared secret TLS v2 node"u8.ToArray();
    private static readonly byte[] s_clientKeyLabel = "RocksDb.Managed.Replication shared secret TLS v2 client"u8.ToArray();

    private readonly byte[]? _nodeKey;
    private readonly byte[]? _clientKey;
    private readonly X509Certificate2 _certificate;
    private readonly SslStreamCertificateContext _clientContext;

    /// <summary>
    /// A participant that holds the nodes' secret: a node, an operator, or a client of a deployment without a separate
    /// client secret. Creates its key pair and certificate (<paramref name="secret"/> is copied; at least
    /// <see cref="MinSecretBytes"/> bytes).
    /// </summary>
    public SharedSecretTls(ReadOnlySpan<byte> secret, ReplicationCaller identity) : this(DeriveKey(secret, s_nodeKeyLabel, nameof(secret)), null, identity)
    {
    }

    /// <summary>
    /// A node (or operator) of a deployment whose application clients hold <paramref name="clientSecret"/> instead of
    /// the nodes' <paramref name="secret"/>: it also accepts holders of the client secret, as clients only.
    /// </summary>
    public SharedSecretTls(ReadOnlySpan<byte> secret, ReadOnlySpan<byte> clientSecret, ReplicationCaller identity)
        : this(DeriveKey(secret, s_nodeKeyLabel, nameof(secret)), DeriveKey(clientSecret, s_clientKeyLabel, nameof(clientSecret)), identity)
    {
    }

    /// <summary>
    /// An application client that holds only the client secret of its deployment: accepted by the nodes as a
    /// <see cref="ReplicationCallerKind.Client"/> named <paramref name="clientName"/>, and by nothing as a node or operator.
    /// </summary>
    public static SharedSecretTls ForClient(ReadOnlySpan<byte> clientSecret, string clientName) =>
        new(null, DeriveKey(clientSecret, s_clientKeyLabel, nameof(clientSecret)), ReplicationCaller.Client(clientName));

    private SharedSecretTls(byte[]? nodeKey, byte[]? clientKey, ReplicationCaller identity)
    {
        if (identity.IsUnauthenticated || identity.Name is null)
        {
            throw new ArgumentException("A certificate needs an identity (a node, a client or an operator).", nameof(identity));
        }
        if (System.Text.Encoding.UTF8.GetByteCount(identity.Name) > MaxNameBytes)
        {
            throw new ArgumentException("The caller name must have at most " + MaxNameBytes + " UTF-8 bytes.", nameof(identity));
        }
        _nodeKey = nodeKey;
        _clientKey = clientKey;
        Identity = identity;
        _certificate = CreateCertificate(nodeKey, clientKey, identity);
        _clientContext = SslStreamCertificateContext.Create(_certificate, additionalCertificates: null, offline: true);
    }

    private static byte[] DeriveKey(ReadOnlySpan<byte> secret, byte[] label, string parameter)
    {
        if (secret.Length < MinSecretBytes)
        {
            throw new ArgumentException("The shared secret must have at least " + MinSecretBytes + " bytes.", parameter);
        }
        var key = new byte[32];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, secret, key, salt: default, info: label);
        return key;
    }

    /// <summary>Who this process is to its peers.</summary>
    public ReplicationCaller Identity { get; }

    /// <summary>This process's certificate (with its private key).</summary>
    public X509Certificate2 Certificate => _certificate;

    /// <summary>True when <paramref name="certificate"/> passes <see cref="TryAuthenticate"/>.</summary>
    public bool IsTrustedPeer(X509Certificate? certificate) => TryAuthenticate(certificate, out _);

    /// <summary>
    /// The identity <paramref name="certificate"/> proves, if it was made by a holder of a secret this instance knows:
    /// with the nodes' secret, whatever identity it states; with only the client secret, a client towards a node (and,
    /// towards an instance that itself holds only the client secret, a node). The caller's TLS handshake proves that
    /// the peer holds the certificate's private key.
    /// </summary>
    public bool TryAuthenticate(X509Certificate? certificate, out ReplicationCaller caller)
    {
        caller = default;
        if (certificate is null)
        {
            return false;
        }
        X509Certificate2? created = null;
        try
        {
            X509Certificate2 cert = certificate as X509Certificate2 ?? (created = new X509Certificate2(certificate));
            X509Extension? identity = cert.Extensions[IdentityExtensionOid];
            if (identity is null || !TryReadOctets(identity, out byte[] identityBytes) || !TryDecodeIdentity(identityBytes, out ReplicationCaller stated))
            {
                return false;
            }
            byte[] spki = cert.PublicKey.ExportSubjectPublicKeyInfo();
            if (_nodeKey is not null && Matches(cert.Extensions[NodeProofExtensionOid], _nodeKey, spki, identityBytes))
            {
                caller = stated;
                return true;
            }
            if (_clientKey is not null && Matches(cert.Extensions[ClientProofExtensionOid], _clientKey, spki, identityBytes))
            {
                // The client secret alone proves less: to a node, that the peer is a client; to a client, that the server
                // is one of the holders of the client secret, which the nodes are.
                ReplicationCallerKind provable = _nodeKey is not null ? ReplicationCallerKind.Client : ReplicationCallerKind.Node;
                if (stated.Kind != provable) return false;
                caller = stated;
                return true;
            }
            return false;
        }
        catch (Exception e) when (e is CryptographicException or AsnContentException)
        {
            return false;
        }
        finally
        {
            created?.Dispose();
        }
    }

    private static bool TryReadOctets(X509Extension extension, out byte[] value)
    {
        value = AsnDecoder.ReadOctetString(extension.RawData, AsnEncodingRules.DER, out int consumed);
        return consumed == extension.RawData.Length;
    }

    private static bool Matches(X509Extension? proof, byte[] key, byte[] spki, byte[] identity)
    {
        if (proof is null || !TryReadOctets(proof, out byte[] claimed)) return false;
        Span<byte> expected = stackalloc byte[HMACSHA256.HashSizeInBytes];
        ComputeProof(key, spki, identity, expected);
        return CryptographicOperations.FixedTimeEquals(claimed, expected);
    }

    // HMAC(key, length-prefixed public key || identity): the prefix keeps the two fields apart.
    private static void ComputeProof(byte[] key, byte[] spki, byte[] identity, Span<byte> destination)
    {
        using var mac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
        Span<byte> length = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(length, spki.Length);
        mac.AppendData(length);
        mac.AppendData(spki);
        mac.AppendData(identity);
        mac.GetHashAndReset(destination);
    }

    private static byte[] EncodeIdentity(ReplicationCaller identity)
    {
        byte[] name = System.Text.Encoding.UTF8.GetBytes(identity.Name);
        var bytes = new byte[1 + name.Length];
        bytes[0] = (byte)identity.Kind;
        name.CopyTo(bytes, 1);
        return bytes;
    }

    private static bool TryDecodeIdentity(byte[] bytes, out ReplicationCaller identity)
    {
        identity = default;
        if (bytes.Length < 2 || bytes.Length > 1 + MaxNameBytes) return false;
        string name;
        try
        {
            name = new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetString(bytes, 1, bytes.Length - 1);
        }
        catch (ArgumentException)
        {
            return false;
        }
        if (string.IsNullOrWhiteSpace(name)) return false;
        switch ((ReplicationCallerKind)bytes[0])
        {
            case ReplicationCallerKind.Node: identity = ReplicationCaller.Node(name); return true;
            case ReplicationCallerKind.Client: identity = ReplicationCaller.Client(name); return true;
            case ReplicationCallerKind.Operator: identity = ReplicationCaller.Operator(name); return true;
            default: return false;
        }
    }

    /// <summary>
    /// Makes a Kestrel HTTPS endpoint present this certificate and accept only clients whose certificate passes
    /// <see cref="TryAuthenticate"/>. A client without a certificate is refused during the handshake.
    /// </summary>
    public void ConfigureServer(HttpsConnectionAdapterOptions https)
    {
        ArgumentNullException.ThrowIfNull(https);
        https.ServerCertificate = _certificate;
        https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
        https.CheckCertificateRevocation = false;
        https.ClientCertificateValidation = (certificate, _, _) => IsTrustedPeer(certificate);
    }

    /// <summary>
    /// Makes <paramref name="handler"/> present this certificate and accept only a server that proves to be a node:
    /// the node <paramref name="expectedServerNodeId"/>, or any node when that is null. Replaces the handler's
    /// <see cref="System.Net.Http.SocketsHttpHandler.SslOptions"/>.
    /// </summary>
    public void ConfigureClient(System.Net.Http.SocketsHttpHandler handler, string? expectedServerNodeId)
    {
        ArgumentNullException.ThrowIfNull(handler);
        handler.SslOptions = new SslClientAuthenticationOptions
        {
            ClientCertificateContext = _clientContext,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            RemoteCertificateValidationCallback = (_, certificate, _, _) => IsExpectedServer(certificate, expectedServerNodeId),
        };
    }

    /// <summary>True when <paramref name="certificate"/> proves a node: <paramref name="expectedNodeId"/>, or any node when that is null.</summary>
    public bool IsExpectedServer(X509Certificate? certificate, string? expectedNodeId) =>
        TryAuthenticate(certificate, out ReplicationCaller server) && server.Kind == ReplicationCallerKind.Node &&
        (expectedNodeId is null || string.Equals(server.Name, expectedNodeId, StringComparison.Ordinal));

    /// <summary>
    /// Channel options for an <c>https://</c> endpoint of any node of the deployment: the handler of
    /// <see cref="GrpcTransportDefaults.CreateHttpHandler"/> (keep-alive pings) with <see cref="ConfigureClient"/>.
    /// The channel owns the handler.
    /// </summary>
    public GrpcChannelOptions CreateChannelOptions() => CreateOptions(null);

    /// <summary><see cref="CreateChannelOptions()"/> for the endpoint of the node <paramref name="expectedServerNodeId"/>: any other server is refused.</summary>
    public GrpcChannelOptions CreateChannelOptions(string expectedServerNodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedServerNodeId);
        return CreateOptions(expectedServerNodeId);
    }

    private GrpcChannelOptions CreateOptions(string? expectedServerNodeId)
    {
        System.Net.Http.SocketsHttpHandler handler = GrpcTransportDefaults.CreateHttpHandler();
        ConfigureClient(handler, expectedServerNodeId);
        return new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true };
    }

    public void Dispose()
    {
        _certificate.Dispose();
        if (_nodeKey is not null) CryptographicOperations.ZeroMemory(_nodeKey);
        if (_clientKey is not null) CryptographicOperations.ZeroMemory(_clientKey);
    }

    private static X509Certificate2 CreateCertificate(byte[]? nodeKey, byte[]? clientKey, ReplicationCaller identity)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=RocksDb.Managed.Replication participant", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, critical: true));
        // Server and client authentication: one certificate serves both directions.
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
        byte[] identityBytes = EncodeIdentity(identity);
        byte[] spki = request.PublicKey.ExportSubjectPublicKeyInfo();
        request.CertificateExtensions.Add(OctetExtension(IdentityExtensionOid, identityBytes));
        if (nodeKey is not null) request.CertificateExtensions.Add(CreateProof(NodeProofExtensionOid, nodeKey, spki, identityBytes));
        if (clientKey is not null) request.CertificateExtensions.Add(CreateProof(ClientProofExtensionOid, clientKey, spki, identityBytes));
        // The dates are not checked by TryAuthenticate; they only have to be well-formed.
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 created = request.CreateSelfSigned(now.AddDays(-1), now.AddYears(50));
        if (!OperatingSystem.IsWindows())
        {
            return new X509Certificate2(created);
        }
        // Windows' TLS provider cannot use a private key that exists only in memory: import it into a key container.
        return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pkcs12), password: null);
    }

    private static X509Extension OctetExtension(string oid, ReadOnlySpan<byte> value)
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        writer.WriteOctetString(value);
        return new X509Extension(oid, writer.Encode(), critical: false);
    }

    internal static X509Extension CreateProof(string oid, byte[] key, byte[] spki, byte[] identity)
    {
        Span<byte> proof = stackalloc byte[HMACSHA256.HashSizeInBytes];
        ComputeProof(key, spki, identity, proof);
        return OctetExtension(oid, proof);
    }
}
