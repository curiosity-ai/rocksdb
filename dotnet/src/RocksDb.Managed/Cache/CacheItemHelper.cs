using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Cache;

/// <summary>
/// Per-entry callbacks of a cache entry: its role and, for entries that can live in a secondary cache, how to save the
/// value to a persistable byte form and how to re-create it. Port of Cache::CacheItemHelper
/// (include/rocksdb/advanced_cache.h): <c>size_cb</c> = <see cref="GetSaveSize"/>, <c>saveto_cb</c> = <see cref="SaveTo"/>,
/// <c>create_cb</c> = <see cref="TryCreate"/>, <c>without_secondary_compat</c> = <see cref="WithoutSecondaryCompat"/>.
/// (<c>del_cb</c> is the value's <see cref="IDisposable"/> / pooled buffer release, see <see cref="CacheValue"/>.)
/// <para>
/// Helpers are long-lived singletons (typically one per role per owner); a helper is attached to an entry with
/// <see cref="CacheValue.WithHelper"/>. An entry without a helper is not secondary-cache compatible.
/// </para>
/// </summary>
public abstract class CacheItemHelper
{
    private CacheItemHelper? _withoutSecondaryCompat;

    protected CacheItemHelper(CacheEntryRole role) => Role = role;

    /// <summary>The entry role (role).</summary>
    public CacheEntryRole Role { get; }

    /// <summary>Whether entries can be saved to / created from a secondary cache (IsSecondaryCacheCompatible).</summary>
    public virtual bool IsSecondaryCacheCompatible => true;

    /// <summary>
    /// A helper with the same role that is not secondary-cache compatible (without_secondary_compat): used for entries
    /// that are already kept in the secondary cache so that they don't spill into it again.
    /// </summary>
    public CacheItemHelper WithoutSecondaryCompat
    {
        get
        {
            if (!IsSecondaryCacheCompatible) return this;
            return Volatile.Read(ref _withoutSecondaryCompat) ?? CreateBasic();
        }
    }

    private CacheItemHelper CreateBasic()
    {
        var basic = new BasicCacheItemHelper(Role);
        return Interlocked.CompareExchange(ref _withoutSecondaryCompat, basic, null) ?? basic;
    }

    /// <summary>Size in bytes of the persistable form of <paramref name="value"/> (size_cb).</summary>
    public abstract int GetSaveSize(in CacheValue value);

    /// <summary>Writes the persistable form of <paramref name="value"/> (exactly <see cref="GetSaveSize"/> bytes) (saveto_cb).</summary>
    public abstract void SaveTo(in CacheValue value, Span<byte> destination);

    /// <summary>
    /// Re-creates a cache value from its saved form (create_cb). <paramref name="data"/> is compressed with
    /// <paramref name="type"/> when the tier that held it kept the compressed block (<paramref name="source"/> names the
    /// tier responsible for the data). <paramref name="createContext"/> is the context the lookup was started with
    /// (for block-based tables: the table reader). On success the cache takes ownership of <paramref name="value"/>
    /// with charge <paramref name="charge"/>.
    /// </summary>
    public abstract bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
        out CacheValue value, out long charge);

    /// <summary>A helper with no secondary cache support (the basic/no-op CacheItemHelper of a role).</summary>
    public static CacheItemHelper Basic(CacheEntryRole role) => BasicCacheItemHelper.ForRole(role);
}

/// <summary>A CacheItemHelper with only a role (size_cb == nullptr): never saved to a secondary cache.</summary>
internal sealed class BasicCacheItemHelper : CacheItemHelper
{
    private static readonly BasicCacheItemHelper[] s_byRole = CreateAll();

    internal BasicCacheItemHelper(CacheEntryRole role) : base(role) { }

    private static BasicCacheItemHelper[] CreateAll()
    {
        var a = new BasicCacheItemHelper[CacheEntryRoles.Count];
        for (int i = 0; i < a.Length; i++) a[i] = new BasicCacheItemHelper((CacheEntryRole)i);
        return a;
    }

    internal static BasicCacheItemHelper ForRole(CacheEntryRole role) => s_byRole[(int)role];

    public override bool IsSecondaryCacheCompatible => false;

    public override int GetSaveSize(in CacheValue value) => throw new NotSupportedException();

    public override void SaveTo(in CacheValue value, Span<byte> destination) => throw new NotSupportedException();

    public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
        out CacheValue value, out long charge) => throw new NotSupportedException();
}

/// <summary>
/// Helper for entries whose value is just bytes (<see cref="CacheValue.Buffer"/>): saves them as-is and re-creates a
/// pooled copy (the kSliceCacheItemHelper / BasicTypedCacheHelper for raw blocks). Data held compressed by a tier is
/// decompressed with RocksDB block framing (format_version 2).
/// </summary>
public sealed class BytesCacheItemHelper : CacheItemHelper
{
    public BytesCacheItemHelper(CacheEntryRole role) : base(role) { }

    public override int GetSaveSize(in CacheValue value) => value.Length;

    public override void SaveTo(in CacheValue value, Span<byte> destination) =>
        new ReadOnlySpan<byte>(value.Buffer, 0, value.Length).CopyTo(destination);

    public override bool TryCreate(ReadOnlySpan<byte> data, CompressionType type, CacheTier source, object? createContext,
        out CacheValue value, out long charge)
    {
        if (type != CompressionType.None)
        {
            var w = new Util.PooledBufferWriter(data.Length * 2);
            try
            {
                BlockCompression.Decompress(type, data, 2, w);
                byte[] copy = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(1, w.WrittenCount));
                w.WrittenSpan.CopyTo(copy);
                value = CacheValue.FromPooledBuffer(copy, w.WrittenCount).WithHelper(this);
                charge = w.WrittenCount;
                return true;
            }
            finally
            {
                w.Dispose();
            }
        }
        byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(1, data.Length));
        data.CopyTo(buf);
        value = CacheValue.FromPooledBuffer(buf, data.Length).WithHelper(this);
        charge = data.Length;
        return true;
    }
}
