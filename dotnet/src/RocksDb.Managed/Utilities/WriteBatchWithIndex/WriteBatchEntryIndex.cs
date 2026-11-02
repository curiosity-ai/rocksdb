using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Utilities.WriteBatchWithIndex;

/// <summary>
/// The index of a <see cref="Managed.WriteBatchWithIndex"/>: a skip list of entries pointing into the batch rep,
/// ordered by (column family id, user key by the column family's comparator, record offset descending) so that the
/// most recent update of a key comes first. Port of WriteBatchEntrySkipList / WriteBatchIndexEntry /
/// WriteBatchEntryComparator (utilities/write_batch_with_index/write_batch_with_index_internal.{h,cc}).
/// <para>
/// Nodes live in parallel arrays (struct-of-arrays, int links) that are reused across <see cref="Clear"/>, so a
/// warmed-up index does not allocate per entry (upstream allocates entries from an arena). Not thread-safe.
/// </para>
/// </summary>
internal sealed class WriteBatchEntryIndex
{
    private const int MaxHeight = 12;
    private const int Branching = 4;
    private const int Head = 0;

    private readonly Db.WriteBatch _batch;
    private readonly Comparator _defaultComparator;
    private Comparator?[] _cfComparators = [];

    // Node arrays; node 0 is the head.
    private int[] _offset = new int[16];     // record offset in the rep (upstream: offset)
    private uint[] _cf = new uint[16];
    private int[] _keyOffset = new int[16];
    private int[] _keySize = new int[16];
    private int[] _nextStart = new int[16];  // index of the node's level-0 link in _next
    private byte[] _height = new byte[16];
    private int[] _next = new int[64];
    private int _count;                      // nodes including the head
    private int _nextUsed;
    private int _maxHeight = 1;
    private uint _rnd = 0xdeadbeef;

    public WriteBatchEntryIndex(Db.WriteBatch batch, Comparator defaultComparator)
    {
        _batch = batch;
        _defaultComparator = defaultComparator;
        Clear();
    }

    /// <summary>WriteBatchEntryComparator::SetComparatorForCF: the first comparator registered for a CF is kept.</summary>
    public void SetComparator(uint cf, Comparator comparator)
    {
        if (cf >= (uint)_cfComparators.Length) Array.Resize(ref _cfComparators, (int)cf + 1);
        _cfComparators[cf] ??= comparator;
    }

    /// <summary>WriteBatchEntryComparator::GetComparator(cf): the CF's comparator, or the default one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Comparator GetComparator(uint cf) => cf < (uint)_cfComparators.Length && _cfComparators[cf] is { } c ? c : _defaultComparator;

    public int Count => _count - 1;

    public void Clear()
    {
        _count = 1;
        _nextUsed = 0;
        _maxHeight = 1;
        _height[Head] = MaxHeight;
        _nextStart[Head] = AllocLinks(MaxHeight);
        _cf[Head] = 0;
    }

    private int AllocLinks(int height)
    {
        if (_nextUsed + height > _next.Length) Array.Resize(ref _next, Math.Max(_next.Length * 2, _nextUsed + height));
        int start = _nextUsed;
        _next.AsSpan(start, height).Fill(-1);
        _nextUsed += height;
        return start;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Next(int node, int level) => _next[_nextStart[node] + level];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetNext(int node, int level, int value) => _next[_nextStart[node] + level] = value;

    private int RandomHeight()
    {
        int height = 1;
        while (height < MaxHeight)
        {
            // xorshift32
            _rnd ^= _rnd << 13;
            _rnd ^= _rnd >> 17;
            _rnd ^= _rnd << 5;
            if (_rnd % Branching != 0) break;
            height++;
        }
        return height;
    }

    // ---- node accessors ---------------------------------------------------------------------------------

    public int RecordOffset(int node) => _offset[node];
    public uint ColumnFamily(int node) => _cf[node];
    public ReadOnlySpan<byte> Key(int node) => _batch.Data.Slice(_keyOffset[node], _keySize[node]);

    /// <summary>Points an existing node at a newer record of the same key (overwrite_key mode).</summary>
    public void UpdateOffset(int node, int recordOffset) => _offset[node] = recordOffset;

    // ---- comparison -------------------------------------------------------------------------------------

    /// <summary>
    /// WriteBatchEntryComparator: compares node <paramref name="node"/> with the search entry (cf, key, offset); a
    /// search with <paramref name="minInCf"/> sorts before every entry of its column family.
    /// </summary>
    private int CompareNode(int node, uint cf, ReadOnlySpan<byte> key, long offset, bool minInCf)
    {
        uint ncf = _cf[node];
        if (ncf != cf) return ncf > cf ? 1 : -1;
        if (minInCf) return 1;
        int c = GetComparator(cf).CompareWithoutTimestamp(Key(node), false, key, false);
        if (c != 0) return c;
        long no = _offset[node];
        // The larger offset is smaller: the most recent update of a key comes first.
        return no < offset ? 1 : no > offset ? -1 : 0;
    }

    /// <summary>First node &gt;= the search entry (or -1), filling <paramref name="prev"/> when given.</summary>
    private int FindGreaterOrEqual(uint cf, ReadOnlySpan<byte> key, long offset, bool minInCf, Span<int> prev)
    {
        int x = Head;
        int level = _maxHeight - 1;
        while (true)
        {
            int next = Next(x, level);
            if (next >= 0 && CompareNode(next, cf, key, offset, minInCf) < 0)
            {
                x = next;
            }
            else
            {
                if (!prev.IsEmpty) prev[level] = x;
                if (level == 0) return next;
                level--;
            }
        }
    }

    /// <summary>Last node &lt; the search entry, or the head.</summary>
    private int FindLessThan(uint cf, ReadOnlySpan<byte> key, long offset, bool minInCf)
    {
        int x = Head;
        int level = _maxHeight - 1;
        while (true)
        {
            int next = Next(x, level);
            if (next >= 0 && CompareNode(next, cf, key, offset, minInCf) < 0)
            {
                x = next;
            }
            else
            {
                if (level == 0) return x;
                level--;
            }
        }
    }

    private int FindLast()
    {
        int x = Head;
        int level = _maxHeight - 1;
        while (true)
        {
            int next = Next(x, level);
            if (next >= 0) x = next;
            else
            {
                if (level == 0) return x;
                level--;
            }
        }
    }

    // ---- insert -----------------------------------------------------------------------------------------

    /// <summary>Adds an entry for the record at <paramref name="recordOffset"/> whose key is at <paramref name="keyOffset"/>.</summary>
    [SkipLocalsInit]
    public void Insert(uint cf, int recordOffset, int keyOffset, int keySize)
    {
        int node = _count;
        if (node == _offset.Length)
        {
            int n = node * 2;
            Array.Resize(ref _offset, n);
            Array.Resize(ref _cf, n);
            Array.Resize(ref _keyOffset, n);
            Array.Resize(ref _keySize, n);
            Array.Resize(ref _nextStart, n);
            Array.Resize(ref _height, n);
        }
        _offset[node] = recordOffset;
        _cf[node] = cf;
        _keyOffset[node] = keyOffset;
        _keySize[node] = keySize;
        Span<int> prev = stackalloc int[MaxHeight];
        FindGreaterOrEqual(cf, _batch.Data.Slice(keyOffset, keySize), recordOffset, false, prev);
        int height = RandomHeight();
        if (height > _maxHeight)
        {
            for (int i = _maxHeight; i < height; i++) prev[i] = Head;
            _maxHeight = height;
        }
        _height[node] = (byte)height;
        _nextStart[node] = AllocLinks(height);
        _count++;
        for (int i = 0; i < height; i++)
        {
            SetNext(node, i, Next(prev[i], i));
            SetNext(prev[i], i, node);
        }
    }

    // ---- iteration --------------------------------------------------------------------------------------

    /// <summary>First node &gt;= (cf, key, offset) (Seek with offset = max finds the most recent update of key).</summary>
    public int Seek(uint cf, ReadOnlySpan<byte> key, long offset) => FindGreaterOrEqual(cf, key, offset, false, default);

    /// <summary>First node of column family <paramref name="cf"/> or later (kFlagMinInCf).</summary>
    public int SeekToFirstOfCf(uint cf) => FindGreaterOrEqual(cf, default, 0, true, default);

    /// <summary>Last node &lt; (cf, key, offset), or -1.</summary>
    public int SeekLessThan(uint cf, ReadOnlySpan<byte> key, long offset)
    {
        int x = FindLessThan(cf, key, offset, false);
        return x == Head ? -1 : x;
    }

    /// <summary>Last node before the first node of <paramref name="cf"/>, or -1.</summary>
    public int SeekLessThanCf(uint cf)
    {
        int x = FindLessThan(cf, default, 0, true);
        return x == Head ? -1 : x;
    }

    public int Last()
    {
        int x = FindLast();
        return x == Head ? -1 : x;
    }

    public int NextNode(int node) => Next(node, 0);

    /// <summary>The node before <paramref name="node"/>, or -1.</summary>
    public int PrevNode(int node)
    {
        int x = FindLessThan(_cf[node], Key(node), _offset[node], false);
        return x == Head ? -1 : x;
    }
}
