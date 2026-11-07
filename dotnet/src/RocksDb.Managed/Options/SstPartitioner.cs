using System.Globalization;
using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>Port of rocksdb::PartitionerResult (include/rocksdb/sst_partitioner.h).</summary>
public enum PartitionerResult : byte
{
    /// <summary>The partitioner does not require a new file.</summary>
    NotRequired = 0,
    /// <summary>The partitioner requests a new file before the current key.</summary>
    Required = 1,
}

/// <summary>Port of SstPartitioner::Context: information about the compaction a partitioner is created for.</summary>
/// <param name="IsFullCompaction">Whether the compaction includes all data files.</param>
/// <param name="IsManualCompaction">Whether the compaction was requested by the client (CompactRange).</param>
/// <param name="OutputLevel">Output level of the compaction.</param>
/// <param name="SmallestUserKey">Smallest user key of the compaction's inputs.</param>
/// <param name="LargestUserKey">Largest user key of the compaction's inputs.</param>
public readonly record struct SstPartitionerContext(bool IsFullCompaction, bool IsManualCompaction, int OutputLevel,
    ReadOnlyMemory<byte> SmallestUserKey, ReadOnlyMemory<byte> LargestUserKey);

/// <summary>
/// Port of rocksdb::SstPartitioner (include/rocksdb/sst_partitioner.h): decides where compaction cuts its output files,
/// so files end on partition boundaries (for example key prefixes) and later move to lower levels without rewrites.
/// One instance is created per compaction (by <see cref="SstPartitionerFactory.CreatePartitioner"/>) and used from the
/// compaction's thread only. Not used by flushes (outputs to level 0 are never partitioned).
/// </summary>
public abstract class SstPartitioner
{
    /// <summary>Name of this partitioner.</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Called with the first key of an output file. When it returns a length <c>n &gt;= 0</c>, the first
    /// <c>n</c> bytes of <paramref name="userKey"/> are a prefix and a partition is requested EXACTLY at the next key
    /// that does not start with that prefix (a key shorter than the prefix included); <c>n == 0</c> requests no further
    /// partitions. A negative result means the compaction must call <see cref="ShouldPartition"/> on every key. Should be
    /// consistent with ShouldPartition. Default: -1. Implementing it makes compaction cheaper (no per-key callback or
    /// key copy).
    /// </summary>
    public virtual int ShouldPartitionByPrefix(ReadOnlySpan<byte> userKey) => -1;

    /// <summary>
    /// Called for every key of the compaction output (unless <see cref="ShouldPartitionByPrefix"/> gave a prefix):
    /// <see cref="PartitionerResult.Required"/> finishes the current file after <paramref name="prevUserKey"/> and
    /// starts a new one with <paramref name="currentUserKey"/>.
    /// </summary>
    public abstract PartitionerResult ShouldPartition(ReadOnlySpan<byte> prevUserKey, ReadOnlySpan<byte> currentUserKey, ulong currentOutputFileSize);

    /// <summary>Called with a file's smallest and largest user keys when compaction tries a trivial move; false prevents it.</summary>
    public abstract bool CanDoTrivialMove(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey);
}

/// <summary>
/// Port of rocksdb::SstPartitionerFactory: creates an <see cref="SstPartitioner"/> per compaction. Set it with
/// <see cref="ColumnFamilyOptions.SstPartitionerFactory"/>; it is persisted in the OPTIONS file
/// (<c>sst_partitioner_factory</c>) as its id, or "{id=...;option=value;}" when it has options. Implementations must be
/// thread-safe (compactions run concurrently) and must not throw.
/// </summary>
public abstract class SstPartitionerFactory
{
    /// <summary>The id of the factory (persisted).</summary>
    public abstract string Name { get; }

    /// <summary>Creates the partitioner of one compaction, or null for none.</summary>
    public abstract SstPartitioner? CreatePartitioner(in SstPartitionerContext context);

    /// <summary>
    /// The OPTIONS file value (Customizable::ToString): the id alone, or "{id=Name;k=v;}" with the factory's options.
    /// Override together with a registration in <see cref="CreateFromString"/> only for built-in factories; custom
    /// factories are written by id.
    /// </summary>
    public virtual string ToOptionString() => Name;

    /// <summary>
    /// Port of SstPartitionerFactory::CreateFromString for the built-in factory: "SstPartitionerFixedPrefixFactory"
    /// (length 0) or "{id=SstPartitionerFixedPrefixFactory;length=N;}". Returns null for "", "nullptr" and unknown ids
    /// (the caller keeps their text so it is written back verbatim).
    /// </summary>
    public static SstPartitionerFactory? CreateFromString(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        string v = value.Trim();
        if (v == "nullptr") return null;
        string id;
        string? length = null;
        if (v.Contains('=', StringComparison.Ordinal))
        {
            id = "";
            foreach (KeyValuePair<string, string> kv in OptionStrings.ParseStruct("sst_partitioner_factory", v))
            {
                if (kv.Key == "id") id = kv.Value;
                else if (kv.Key == "length") length = kv.Value;
                else return null; // option of an unknown factory
            }
        }
        else
        {
            id = v.Length > 1 && v[0] == '{' && v[^1] == '}' ? v[1..^1].Trim() : v;
        }
        if (id != SstPartitionerFixedPrefixFactory.ClassName) return null;
        ulong len = length is null ? 0 : OptionStrings.ParseUInt64("sst_partitioner_factory.length", length);
        return new SstPartitionerFixedPrefixFactory(checked((int)len));
    }
}

/// <summary>
/// Port of rocksdb::SstPartitionerFixedPrefix (db/compaction/sst_partitioner.cc): cuts compaction outputs where the
/// first <c>length</c> bytes of the user key change.
/// </summary>
public sealed class SstPartitionerFixedPrefix : SstPartitioner
{
    private readonly int _len;

    public SstPartitionerFixedPrefix(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _len = length;
    }

    public override string Name => "SstPartitionerFixedPrefix";

    /// <summary>
    /// The first <c>length</c> bytes; for a shorter key, -1: ShouldPartition cuts a partition for every unique key
    /// outside the prefix length domain, which a prefix cannot represent.
    /// </summary>
    public override int ShouldPartitionByPrefix(ReadOnlySpan<byte> userKey) => userKey.Length >= _len ? _len : -1;

    public override PartitionerResult ShouldPartition(ReadOnlySpan<byte> prevUserKey, ReadOnlySpan<byte> currentUserKey, ulong currentOutputFileSize)
    {
        ReadOnlySpan<byte> last = prevUserKey.Length > _len ? prevUserKey[.._len] : prevUserKey;
        ReadOnlySpan<byte> current = currentUserKey.Length > _len ? currentUserKey[.._len] : currentUserKey;
        return last.SequenceEqual(current) ? PartitionerResult.NotRequired : PartitionerResult.Required;
    }

    public override bool CanDoTrivialMove(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey) =>
        ShouldPartition(smallestUserKey, largestUserKey, 0) == PartitionerResult.NotRequired;
}

/// <summary>
/// Port of rocksdb::SstPartitionerFixedPrefixFactory (NewSstPartitionerFixedPrefixFactory): a
/// <see cref="SstPartitionerFixedPrefix"/> of the given prefix length for every compaction. Persisted as
/// "{id=SstPartitionerFixedPrefixFactory;length=N;}", which native RocksDB loads.
/// </summary>
public sealed class SstPartitionerFixedPrefixFactory : SstPartitionerFactory
{
    /// <summary>kClassName.</summary>
    public const string ClassName = "SstPartitionerFixedPrefixFactory";

    public SstPartitionerFixedPrefixFactory(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        Length = length;
    }

    /// <summary>Prefix length in bytes.</summary>
    public int Length { get; }

    public override string Name => ClassName;

    public override SstPartitioner CreatePartitioner(in SstPartitionerContext context) => new SstPartitionerFixedPrefix(Length);

    public override string ToOptionString() => "{id=" + ClassName + ";length=" + Length.ToString(CultureInfo.InvariantCulture) + ";}";
}
