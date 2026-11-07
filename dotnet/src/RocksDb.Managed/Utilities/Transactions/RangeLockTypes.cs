namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// An endpoint of a range lock (rocksdb::Endpoint, include/rocksdb/utilities/transaction.h). Keys are ordered by the
/// column family's comparator; <see cref="InfSuffix"/> marks the endpoint as the key followed by an infinite suffix,
/// i.e. greater than every key that starts with <see cref="Key"/>. This defines prefix ranges: with a lexicographic
/// comparator, [(<c>p</c>, false), (<c>p</c>, true)] covers every key starting with <c>p</c>. Endpoints are compared
/// on the common prefix of their keys first; when one key is a prefix of the other, a non-suffixed shorter key sorts
/// before the longer one and a suffixed one after it (RangeTreeLockManager::CompareDbtEndpoints).
/// <para>
/// The comparison does not compensate for reverse-ordered comparators: on such a column family the caller passes the
/// range flipped (swapped endpoints with negated suffix flags), like upstream.
/// </para>
/// </summary>
public readonly struct RangeLockEndpoint
{
    /// <summary>An endpoint at <paramref name="key"/> (no infinite suffix).</summary>
    public RangeLockEndpoint(ReadOnlyMemory<byte> key)
    {
        Key = key;
        InfSuffix = false;
    }

    /// <summary>An endpoint at <paramref name="key"/>, with an infinite suffix if <paramref name="infSuffix"/>.</summary>
    public RangeLockEndpoint(ReadOnlyMemory<byte> key, bool infSuffix)
    {
        Key = key;
        InfSuffix = infSuffix;
    }

    /// <summary>The key (or key prefix).</summary>
    public ReadOnlyMemory<byte> Key { get; }

    /// <summary>True if the key has a "+infinity" suffix that compares greater than any other suffix.</summary>
    public bool InfSuffix { get; }

    public override string ToString() => Convert.ToHexString(Key.Span) + (InfSuffix ? "+inf" : "");
}

/// <summary>A held range lock (RangeLockInfo): its endpoints, owners and mode.</summary>
public sealed record RangeLockInfo(uint ColumnFamilyId, RangeLockEndpoint Start, RangeLockEndpoint End, IReadOnlyList<ulong> TransactionIds,
    bool Exclusive);

/// <summary>One transaction waiting in a range-lock deadlock cycle (RangeDeadlockInfo): the range it waits for.</summary>
public sealed record RangeDeadlockInfo(ulong TransactionId, uint ColumnFamilyId, bool Exclusive, RangeLockEndpoint Start, RangeLockEndpoint End);

/// <summary>A detected range-lock deadlock (RangeDeadlockPath): the cycle, or <see cref="LimitExceeded"/>.</summary>
public sealed record RangeDeadlockPath(IReadOnlyList<RangeDeadlockInfo> Path, bool LimitExceeded, DateTimeOffset DeadlockTime);

/// <summary>Counters of a <see cref="RangeLockManager"/> (RangeLockManagerHandle::Counters).</summary>
/// <param name="EscalationCount">Number of lock escalations (for all column families).</param>
/// <param name="LockWaitCount">
/// Number of lock requests that went through the wait step. Like upstream's LTM_WAIT_COUNT this counts every range
/// lock request (granted at once, after a wait, timed out or failed), not only the ones that blocked.
/// </param>
/// <param name="CurrentLockMemory">Estimated memory used by the held locks, in bytes (all column families).</param>
public readonly record struct RangeLockManagerStatus(ulong EscalationCount, ulong LockWaitCount, ulong CurrentLockMemory);
