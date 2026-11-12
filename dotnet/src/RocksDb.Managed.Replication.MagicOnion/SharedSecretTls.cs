using System.Formats.Asn1;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace RocksDb.Managed.Replication.MagicOnion;

/// <summary>
/// Encryption and mutual authentication of the gRPC transport from one secret that every node and client of a
/// deployment shares.
/// <para>
/// Each instance creates a random key pair and a self-signed certificate that carries, in a private extension, an
/// HMAC of its public key keyed with (a key derived from) the secret. Servers and clients both present their
/// certificate (mutual TLS) and accept a peer only when the HMAC in the peer's certificate matches its public key
/// (<see cref="IsTrustedPeer"/>). Only a holder of the secret can make such a certificate, a copied certificate is
/// useless without its private key (the TLS handshake proves possession), and the secret itself never travels.
/// </para>
/// <para>
/// What is not checked: host names, validity dates, certificate chains and revocation - the secret is the only
/// identity. Every holder of the secret is trusted as any node and any client. To change the secret, restart the
/// nodes with the new one; a node accepts one secret at a time.
/// </para>
/// <para>
/// Use: one instance per process, given to the server (<see cref="GrpcNodeServerOptions.Tls"/>) and to every client
/// (<see cref="CreateChannelOptions"/>, with an <c>https://</c> endpoint). A host that runs the MagicOnion services
/// in its own ASP.NET Core application calls <see cref="ConfigureServer"/> in its Kestrel <c>UseHttps</c> callback.
/// Dispose the instance after the servers and channels that use it.
/// </para>
/// </summary>
public sealed class SharedSecretTls : IDisposable
{
    /// <summary>The shortest secret accepted, in bytes. Use random bytes: the secret is not stretched like a password.</summary>
    public const int MinSecretBytes = 16;

    // A private extension id (ITU-T X.667: 2.25.<a UUID as a decimal number>), used for nothing else.
    internal const string ProofExtensionOid = "2.25.204763142377514856327684105921739526041";

    private static readonly byte[] s_keyLabel = "RocksDb.Managed.Replication shared secret TLS v1"u8.ToArray();

    private readonly byte[] _macKey;
    private readonly X509Certificate2 _certificate;
    private readonly SslStreamCertificateContext _clientContext;

    /// <summary>Creates this process's key pair and certificate for <paramref name="secret"/> (copied; at least <see cref="MinSecretBytes"/> bytes).</summary>
    public SharedSecretTls(ReadOnlySpan<byte> secret)
    {
        if (secret.Length < MinSecretBytes)
        {
            throw new ArgumentException("The shared secret must have at least " + MinSecretBytes + " bytes.", nameof(secret));
        }
        _macKey = new byte[32];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, secret, _macKey, salt: default, info: s_keyLabel);
        _certificate = CreateCertificate(_macKey);
        _clientContext = SslStreamCertificateContext.Create(_certificate, additionalCertificates: null, offline: true);
    }

    /// <summary>This process's certificate (with its private key).</summary>
    public X509Certificate2 Certificate => _certificate;

    /// <summary>
    /// True when <paramref name="certificate"/> was made by a holder of the same secret: its extension holds the HMAC
    /// of its own public key. The caller's TLS handshake proves that the peer holds the private key.
    /// </summary>
    public bool IsTrustedPeer(X509Certificate? certificate)
    {
        if (certificate is null)
        {
            return false;
        }
        X509Certificate2? created = null;
        try
        {
            X509Certificate2 cert = certificate as X509Certificate2 ?? (created = new X509Certificate2(certificate));
            X509Extension? proof = cert.Extensions[ProofExtensionOid];
            if (proof is null)
            {
                return false;
            }
            byte[] claimed = AsnDecoder.ReadOctetString(proof.RawData, AsnEncodingRules.DER, out int consumed);
            if (consumed != proof.RawData.Length)
            {
                return false;
            }
            Span<byte> expected = stackalloc byte[HMACSHA256.HashSizeInBytes];
            HMACSHA256.HashData(_macKey, cert.PublicKey.ExportSubjectPublicKeyInfo(), expected);
            return CryptographicOperations.FixedTimeEquals(claimed, expected);
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

    /// <summary>
    /// Makes a Kestrel HTTPS endpoint present this certificate and accept only clients whose certificate passes
    /// <see cref="IsTrustedPeer"/>. A client without a certificate is refused during the handshake.
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
    /// Makes <paramref name="handler"/> present this certificate and accept only servers whose certificate passes
    /// <see cref="IsTrustedPeer"/>. Replaces the handler's <see cref="System.Net.Http.SocketsHttpHandler.SslOptions"/>.
    /// </summary>
    public void ConfigureClient(System.Net.Http.SocketsHttpHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        handler.SslOptions = new SslClientAuthenticationOptions
        {
            ClientCertificateContext = _clientContext,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            RemoteCertificateValidationCallback = (_, certificate, _, _) => IsTrustedPeer(certificate),
        };
    }

    /// <summary>
    /// Channel options for an <c>https://</c> endpoint of a server using the same secret: the handler of
    /// <see cref="GrpcTransportDefaults.CreateHttpHandler"/> (keep-alive pings) with <see cref="ConfigureClient"/>.
    /// The channel owns the handler.
    /// </summary>
    public GrpcChannelOptions CreateChannelOptions()
    {
        System.Net.Http.SocketsHttpHandler handler = GrpcTransportDefaults.CreateHttpHandler();
        ConfigureClient(handler);
        return new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true };
    }

    public void Dispose()
    {
        _certificate.Dispose();
        CryptographicOperations.ZeroMemory(_macKey);
    }

    private static X509Certificate2 CreateCertificate(byte[] macKey)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=RocksDb.Managed.Replication node", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, critical: true));
        // Server and client authentication: one certificate serves both directions.
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
        request.CertificateExtensions.Add(CreateProof(macKey, request.PublicKey));
        // The dates are not checked by IsTrustedPeer; they only have to be well-formed.
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 created = request.CreateSelfSigned(now.AddDays(-1), now.AddYears(50));
        if (!OperatingSystem.IsWindows())
        {
            return new X509Certificate2(created);
        }
        // Windows' TLS provider cannot use a private key that exists only in memory: import it into a key container.
        return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pkcs12), password: null);
    }

    internal static X509Extension CreateProof(byte[] macKey, PublicKey publicKey)
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        writer.WriteOctetString(HMACSHA256.HashData(macKey, publicKey.ExportSubjectPublicKeyInfo()));
        return new X509Extension(ProofExtensionOid, writer.Encode(), critical: false);
    }
}
