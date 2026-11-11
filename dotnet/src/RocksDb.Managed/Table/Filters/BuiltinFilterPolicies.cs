namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// Port of BuiltinFilterPolicy / BloomLikeFilterPolicy (table/block_based/filter_policy.cc). All built-in
/// policies share <see cref="CompatibilityName"/> "rocksdb.BuiltinBloomFilter" (the filter block's metaindex
/// key is "fullfilter." + this name), and any built-in reader can read any built-in filter format.
/// </summary>
public abstract class BloomLikeFilterPolicy : FilterPolicy
{
    /// <summary>kBuiltinFilterMetadataName.</summary>
    public const string BuiltinFilterMetadataName = "rocksdb.BuiltinBloomFilter";

    private readonly int _millibitsPerKey;
    private readonly double _desiredOneInFpRate;
    private readonly int _wholeBitsPerKey;
    /// <summary>
    /// aggregate_rounding_balance_: sum over all generated filters f of (estimated FP rate of f - target FP rate of f)
    /// * 2^32, used by optimize_filters_for_memory to keep the aggregate FP rate on target while rounding sizes.
    /// </summary>
    private readonly System.Runtime.CompilerServices.StrongBox<long> _aggregateRoundingBalance = new();

    protected BloomLikeFilterPolicy(double bitsPerKey)
    {
        // Sanitize bits_per_key
        if (bitsPerKey < 0.5)
        {
            // Round down to no filter
            bitsPerKey = 0;
        }
        else if (bitsPerKey < 1.0)
        {
            // Minimum 1 bit per key (equiv) when creating filter
            bitsPerKey = 1.0;
        }
        else if (!(bitsPerKey < 100.0))
        {
            // including NaN
            bitsPerKey = 100.0;
        }

        // Includes a nudge toward rounding up, to ensure on all platforms
        // that doubles specified with three decimal digits after the decimal
        // point are interpreted accurately.
        _millibitsPerKey = (int)((bitsPerKey * 1000.0) + 0.500001);

        // For now configure Ribbon filter to match Bloom FP rate and save
        // memory. (Ribbon bits per key will be ~30% less than Bloom bits per key
        // for same FP rate.)
        _desiredOneInFpRate = 1.0 / BloomMath.CacheLocalFpRate(
            bitsPerKey, FastLocalBloomImpl.ChooseNumProbes(_millibitsPerKey), /*cache_line_bits*/ 512);

        // For better or worse, this is a rounding up of a nudged rounding up,
        // e.g. 7.4999999999999 will round up to 8, but that provides more
        // predictability against small arithmetic errors in floating point.
        _wholeBitsPerKey = (_millibitsPerKey + 500) / 1000;
    }

    public sealed override string CompatibilityName => BuiltinFilterMetadataName;

    /// <summary>Configured bits/key in thousandths. 0 means "no filter" (<see cref="CreateBuilder(int)"/> throws).</summary>
    public int MillibitsPerKey => _millibitsPerKey;

    /// <summary>Whole bits/key used by the legacy Bloom filter.</summary>
    public int WholeBitsPerKey => _wholeBitsPerKey;

    /// <summary>Target 1/FP-rate for Ribbon (matches the Bloom FP rate of the same bits/key).</summary>
    public double DesiredOneInFpRate => _desiredOneInFpRate;

    /// <summary>False when bits/key sanitized to 0: the table builder must not write a filter at all.</summary>
    public bool CreatesFilters => _millibitsPerKey != 0;

    /// <summary>Policy id as in GetId(), e.g. "bloomfilter:10:false" / "ribbonfilter:10:-1" (OPTIONS file form).</summary>
    public abstract string Id { get; }

    /// <summary>GetBitsPerKeySuffix: ":10" or ":9.5" etc.</summary>
    protected string GetBitsPerKeySuffix()
    {
        string rv = ":" + (_millibitsPerKey / 1000).ToString(System.Globalization.CultureInfo.InvariantCulture);
        int frac = _millibitsPerKey % 1000;
        if (frac > 0)
        {
            rv += "." + (char)('0' + (frac / 100));
            frac %= 100;
            if (frac > 0)
            {
                rv += (char)('0' + (frac / 10));
                frac %= 10;
                if (frac > 0)
                {
                    rv += (char)('0' + frac);
                }
            }
        }
        return rv;
    }

    protected void ThrowIfNoFilter()
    {
        if (_millibitsPerKey == 0)
        {
            throw new InvalidOperationException("Filter policy with < 0.5 bits/key does not create filters; check CreatesFilters.");
        }
    }

    protected IFilterBitsBuilder CreateFastLocalBloomBuilder() => new FastLocalBloomBitsBuilder(_millibitsPerKey);

    /// <summary>FastLocalBloom builder; rounds sizes against this policy's balance when <paramref name="optimizeForMemory"/>.</summary>
    protected IFilterBitsBuilder CreateFastLocalBloomBuilder(bool optimizeForMemory) =>
        new FastLocalBloomBitsBuilder(_millibitsPerKey, optimizeForMemory ? _aggregateRoundingBalance : null);

    protected IFilterBitsBuilder CreateLegacyBloomBuilder() => new LegacyBloomBitsBuilder(_wholeBitsPerKey);

    protected IFilterBitsBuilder CreateStandard128RibbonBuilder() => new Standard128RibbonBitsBuilder(_desiredOneInFpRate, _millibitsPerKey);

    /// <summary>Standard128 Ribbon builder; rounds sizes against this policy's balance when <paramref name="optimizeForMemory"/>.</summary>
    protected IFilterBitsBuilder CreateStandard128RibbonBuilder(bool optimizeForMemory) =>
        new Standard128RibbonBitsBuilder(_desiredOneInFpRate, _millibitsPerKey, optimizeForMemory ? _aggregateRoundingBalance : null);

    /// <summary>Current optimize_filters_for_memory FP-rate balance (for tests and diagnostics).</summary>
    public long AggregateRoundingBalance => Interlocked.Read(ref _aggregateRoundingBalance.Value);

    /// <summary>Any built-in filter format can be read regardless of the policy configuration.</summary>
    public sealed override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents) => BuiltinFilterBitsReader.Create(contents);
}

/// <summary>
/// Port of ReadOnlyBuiltinFilterPolicy (table/block_based/filter_policy_internal.h): reads every built-in filter format
/// but writes no filters (GetBuilderWithContext returns nullptr). Its name and id are "rocksdb.BuiltinBloomFilter", the
/// value FilterPolicy::CreateFromString maps to it.
/// </summary>
public sealed class ReadOnlyBuiltinFilterPolicy : BloomLikeFilterPolicy
{
    /// <summary>bits/key 0: <see cref="BloomLikeFilterPolicy.CreatesFilters"/> is false, so table builders write no filter.</summary>
    public ReadOnlyBuiltinFilterPolicy() : base(0) { }

    public override string Name => BuiltinFilterMetadataName;

    public override string Id => BuiltinFilterMetadataName;

    public override IFilterBitsBuilder CreateBuilder(int formatVersion)
    {
        ThrowIfNoFilter();
        throw new InvalidOperationException();
    }
}

/// <summary>
/// Port of BloomFilterPolicy (<c>NewBloomFilterPolicy(bits_per_key)</c>): legacy Bloom for
/// format_version &lt; 5, FastLocalBloom for format_version &gt;= 5.
/// </summary>
public sealed class BloomFilterPolicy : BloomLikeFilterPolicy
{
    /// <summary>kClassName.</summary>
    public const string ClassName = "bloomfilter";

    /// <summary>kNickName.</summary>
    public const string NickName = "rocksdb.BloomFilter";

    public BloomFilterPolicy(double bitsPerKey) : base(bitsPerKey) { }

    public override string Name => ClassName;

    public override string Id => ClassName + GetBitsPerKeySuffix() + ":false";

    public override IFilterBitsBuilder CreateBuilder(int formatVersion)
    {
        ThrowIfNoFilter();
        return formatVersion < 5 ? CreateLegacyBloomBuilder() : CreateFastLocalBloomBuilder();
    }

    /// <summary>Port of BloomFilterPolicy::GetBuilderWithContext (optimize_filters_for_memory from the context).</summary>
    public override IFilterBitsBuilder CreateBuilder(in FilterBuildingContext context)
    {
        ThrowIfNoFilter();
        return context.FormatVersion < 5 ? CreateLegacyBloomBuilder() : CreateFastLocalBloomBuilder(context.OptimizeFiltersForMemory);
    }
}

/// <summary>
/// Port of RibbonFilterPolicy (<c>NewRibbonFilterPolicy(bloom_equivalent_bits_per_key, bloom_before_level)</c>):
/// Standard128 Ribbon, except FastLocalBloom for levels below <see cref="BloomBeforeLevel"/>
/// (flush counts as level -1; <see cref="int.MaxValue"/> means always Bloom; -1 means always Ribbon).
/// </summary>
public sealed class RibbonFilterPolicy : BloomLikeFilterPolicy
{
    /// <summary>kClassName.</summary>
    public const string ClassName = "ribbonfilter";

    /// <summary>kNickName.</summary>
    public const string NickName = "rocksdb.RibbonFilter";

    private volatile int _bloomBeforeLevel;

    public RibbonFilterPolicy(double bloomEquivalentBitsPerKey, int bloomBeforeLevel) : base(bloomEquivalentBitsPerKey)
    {
        _bloomBeforeLevel = bloomBeforeLevel;
    }

    /// <summary>Mutable like upstream's atomic bloom_before_level.</summary>
    public int BloomBeforeLevel
    {
        get => _bloomBeforeLevel;
        set => _bloomBeforeLevel = value;
    }

    public override string Name => ClassName;

    public override string Id => ClassName + GetBitsPerKeySuffix() + ":" + _bloomBeforeLevel.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Without context the level is unknown, which upstream treats as bottommost.</summary>
    public override IFilterBitsBuilder CreateBuilder(int formatVersion)
        => CreateBuilder(new FilterBuildingContext(formatVersion, -1, false));

    public override IFilterBitsBuilder CreateBuilder(in FilterBuildingContext context)
    {
        ThrowIfNoFilter();
        // Treat unknown same as bottommost, INT_MAX - 1.
        // INT_MAX is reserved for "always use Bloom".
        int levelish = int.MaxValue - 1;
        int bloomBeforeLevel = _bloomBeforeLevel;
        // FIFO and "none" compaction styles are treated as bottommost (RibbonFilterPolicy::GetBuilderWithContext).
        if (bloomBeforeLevel < int.MaxValue && context.CompactionStyle is CompactionStyle.Level or CompactionStyle.Universal)
        {
            if (context.IsFlush)
            {
                // Treat flush as level -1
                levelish = -1;
            }
            else if (context.LevelAtCreation != -1)
            {
                levelish = context.LevelAtCreation;
            }
        }
        return levelish < bloomBeforeLevel
            ? CreateFastLocalBloomBuilder(context.OptimizeFiltersForMemory)
            : CreateStandard128RibbonBuilder(context.OptimizeFiltersForMemory);
    }
}
