using MagicOnion;
using MagicOnion.Server;
using RocksDb.Managed.Replication.MagicOnion;

namespace RocksDb.Managed.Replication.Tests.LegacyWire;

// A pair node's gRPC service as a node built before WriteFragmentsAsync exposes it, reduced to the client write: the
// interface has the name of the real one (MagicOnion derives the gRPC service name from it), so a client of this
// version reaches it, and every method it lacks answers Unimplemented - like an old node.

/// <summary>The contract of <c>IPairService</c> before large writes (the write only).</summary>
public interface IPairService : IService<IPairService>
{
    UnaryResult<PairWriteResultDto> WriteAsync(string key, string value);
}

/// <summary>Server side: records what it was sent.</summary>
public sealed class OldPairService(Audit4ReplicationFragmentTests.OldPairBackend backend) : ServiceBase<IPairService>, IPairService
{
    public UnaryResult<PairWriteResultDto> WriteAsync(string key, string value)
    {
        backend.ValueLengths[key] = value.Length;
        int n = Interlocked.Increment(ref backend.Writes);
        return UnaryResult.FromResult(new PairWriteResultDto { Applied = true, Sequence = (ulong)n });
    }
}
