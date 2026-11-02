using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Memtable;

/// <summary>A decoded search key: pointer + length of the comparable bytes (e.g. the internal key of a memtable entry).</summary>
public readonly unsafe struct SkipListKey
{
    public readonly byte* Ptr;
    public readonly int Length;

    public SkipListKey(byte* ptr, int length)
    {
        Ptr = ptr;
        Length = length;
    }

    public ReadOnlySpan<byte> Span => new(Ptr, Length);
}

/// <summary>
/// Key comparator for <see cref="InlineSkipList{TComparer}"/>. Implement as a <c>readonly struct</c> so the JIT
/// specializes and inlines it (the analogue of upstream's template parameter). Port of the comparator contract of
/// memtable/inlineskiplist.h (<c>operator()(const char*, const DecodedType&amp;)</c> and <c>decode_key</c>).
/// </summary>
public unsafe interface ISkipListComparer
{
    /// <summary>Decodes a stored (encoded) key into its comparable form.</summary>
    SkipListKey Decode(byte* encodedKey);

    /// <summary>Compares a stored key with a decoded key; negative if the stored key sorts first.</summary>
    int Compare(byte* encodedKey, SkipListKey decoded);
}

/// <summary>
/// Concurrent skiplist with keys stored inline in the node. Port of rocksdb::InlineSkipList
/// (memtable/inlineskiplist.h).
/// </summary>
/// <remarks>
/// <para><b>Layout</b> (identical to upstream): a node of height h is allocated from the arena as
/// <c>[next[h-1] ... next[1]][next[0]][key bytes]</c>; the node pointer points at <c>next[0]</c>, link i lives at
/// <c>node - 8*i</c> and the key starts at <c>node + 8</c>. Before insertion the height is stashed in <c>next[0]</c>.</para>
/// <para><b>Thread safety.</b> Writes require external synchronization except <see cref="InsertConcurrently"/>,
/// which may run concurrently with other <see cref="InsertConcurrently"/> calls. Reads (iterators,
/// <see cref="Contains"/>) are lock-free and may run concurrently with any insert; they only need the list (and its
/// arena) to stay alive.</para>
/// <para><b>Memory ordering.</b> A node's key bytes and its own links are written with plain stores, then the node is
/// published by a release store (<see cref="Volatile.Write(ref nint, nint)"/>) or a full-fence
/// <see cref="Interlocked.CompareExchange(ref nint, nint, nint)"/> of the predecessor's link. Readers load links with
/// acquire semantics (<see cref="Volatile.Read(ref nint)"/>), so a reader that observes a node also observes its key
/// and links (upstream: Store = release, Load = acquire, NoBarrier_* = relaxed). The current max height is read
/// relaxed: a stale smaller value only makes a search start lower; a larger value only exposes head links that are
/// null until a node is published at that level, which searches handle.</para>
/// </remarks>
public sealed unsafe class InlineSkipList<TComparer> where TComparer : struct, ISkipListComparer
{
    public const int MaxPossibleHeight = 32;

    private struct Splice
    {
        // The invariant of a Splice is that prev[i+1].key <= prev[i].key < next[i].key <= next[i+1].key for all
        // i. prev[height] is head and next[height] is null.
        public int Height;
        public byte** Prev;
        public byte** Next;
    }

    private readonly int _kMaxHeight;
    private readonly int _kBranching;
    private readonly uint _kScaledInverseBranching;
    private readonly Allocator _allocator;
    private readonly TComparer _compare;
    private readonly byte* _head;
    // Height of the entire list; modified only by Insert (CAS). Read racily by readers (stale values are OK).
    private int _maxHeight;
    // Used for sequential (non-concurrent) inserts in the common append-mostly case.
    private readonly Splice* _seqSplice;

    [ThreadStatic] private static uint t_rngState;

    public InlineSkipList(TComparer comparer, Allocator allocator, int maxHeight, int branchingFactor)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxHeight, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxHeight, MaxPossibleHeight);
        ArgumentOutOfRangeException.ThrowIfLessThan(branchingFactor, 2);
        _kMaxHeight = maxHeight;
        _kBranching = branchingFactor;
        _kScaledInverseBranching = (uint)(((ulong)uint.MaxValue + 1) / (ulong)branchingFactor);
        _allocator = allocator;
        _compare = comparer;
        _head = AllocateNode(0, maxHeight);
        _maxHeight = 1;
        for (int i = 0; i < maxHeight; i++) NoBarrierSetNext(_head, i, null);
        _seqSplice = AllocateSplice();
    }

    /// <summary>Defaults used by the skiplist memtable rep: max height 12, branching factor 4.</summary>
    public InlineSkipList(TComparer comparer, Allocator allocator) : this(comparer, allocator, 12, 4) { }

    public TComparer Comparer => _compare;

    /// <summary>Current height of the list (relaxed read).</summary>
    public int Height => Volatile.Read(ref _maxHeight);

    // ---------------------------------------------------------------- node helpers

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* KeyOf(byte* node) => node + sizeof(nint);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* NodeOfKey(byte* key) => key - sizeof(nint);

    /// <summary>Acquire load of link <paramref name="n"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* Next(byte* node, int n) => (byte*)Volatile.Read(ref *(nint*)(node - n * sizeof(nint)));

    /// <summary>Release store of link <paramref name="n"/>: publishes <paramref name="x"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetNext(byte* node, int n, byte* x) => Volatile.Write(ref *(nint*)(node - n * sizeof(nint)), (nint)x);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CasNext(byte* node, int n, byte* expected, byte* x) =>
        Interlocked.CompareExchange(ref *(nint*)(node - n * sizeof(nint)), (nint)x, (nint)expected) == (nint)expected;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* NoBarrierNext(byte* node, int n) => *(byte**)(node - n * sizeof(nint));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void NoBarrierSetNext(byte* node, int n, byte* x) => *(byte**)(node - n * sizeof(nint)) = x;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Prefetch(byte* p)
    {
        if (Sse.IsSupported && p != null) Sse.Prefetch0(p);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool KeyIsAfterNode(SkipListKey key, byte* n) => n != null && _compare.Compare(KeyOf(n), key) < 0;

    // ---------------------------------------------------------------- allocation

    private int RandomHeight()
    {
        // xorshift32 per thread; distribution only affects performance, never the persisted format.
        uint x = t_rngState;
        if (x == 0) x = (uint)Environment.CurrentManagedThreadId * 2654435761u | 1u;
        int height = 1;
        while (height < _kMaxHeight)
        {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            if (x >= _kScaledInverseBranching) break;
            height++;
        }
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        t_rngState = x;
        return height;
    }

    private byte* AllocateNode(int keySize, int height)
    {
        int prefix = sizeof(nint) * (height - 1);
        // prefix is space for the height - 1 pointers that we store before the Node instance (next[-(height - 1)]
        // .. next[-1]). Node starts at raw + prefix, and holds the bottom-most (level 0) skip list pointer next[0].
        byte* raw = _allocator.AllocateAligned(prefix + sizeof(nint) + keySize);
        byte* x = raw + prefix;
        // Once we've linked the node into the skip list we don't actually need to know its height, because we can
        // implicitly use the fact that we traversed into a node at level h to know that h is a valid level for that
        // node. We need to convey the height to the Insert step, however, so that it can perform the proper links.
        *(int*)x = height;
        return x;
    }

    private Splice* AllocateSplice()
    {
        // size of prev and next arrays
        int arraySize = sizeof(nint) * (_kMaxHeight + 1);
        byte* raw = _allocator.AllocateAligned(sizeof(Splice) + arraySize * 2);
        Splice* splice = (Splice*)raw;
        splice->Height = 0;
        splice->Prev = (byte**)(raw + sizeof(Splice));
        splice->Next = (byte**)(raw + sizeof(Splice) + arraySize);
        return splice;
    }

    /// <summary>
    /// Allocates a node with a random height and room for <paramref name="keySize"/> key bytes; returns the key
    /// pointer, which the caller fills and then passes to <see cref="Insert"/> / <see cref="InsertConcurrently"/>.
    /// Thread-safe if the allocator is.
    /// </summary>
    public byte* AllocateKey(int keySize) => KeyOf(AllocateNode(keySize, RandomHeight()));

    // ---------------------------------------------------------------- search

    /// <summary>Returns the earliest node with a key &gt;= key, or null.</summary>
    private byte* FindGreaterOrEqual(SkipListKey key)
    {
        // Note: It looks like we could reduce duplication by implementing this function as FindLessThan(key)->Next(0),
        // but we wouldn't be able to exit early on equality and the result wouldn't even be correct. A concurrent
        // insert might occur after FindLessThan(key) but before we get a chance to call Next(0).
        byte* x = _head;
        int level = Height - 1;
        byte* lastBigger = null;
        while (true)
        {
            byte* next = Next(x, level);
            if (next != null) Prefetch(NoBarrierNext(next, level));
            // Make sure the lists are sorted
            Debug.Assert(x == _head || next == null || KeyIsAfterNode(_compare.Decode(KeyOf(next)), x));
            // Make sure we haven't overshot during our search
            Debug.Assert(x == _head || KeyIsAfterNode(key, x));
            int cmp = (next == null || next == lastBigger) ? 1 : _compare.Compare(KeyOf(next), key);
            if (cmp == 0 || (cmp > 0 && level == 0))
            {
                return next;
            }
            if (cmp < 0)
            {
                // Keep searching in this list
                x = next;
            }
            else
            {
                // Switch to next list, reuse compare() result
                lastBigger = next;
                level--;
            }
        }
    }

    /// <summary>Returns the latest node with a key &lt; key, or head if there is no such node.</summary>
    private byte* FindLessThan(SkipListKey key)
    {
        int level = Height - 1;
        byte* x = _head;
        // KeyIsAfter(key, last_not_after) is definitely false
        byte* lastNotAfter = null;
        while (true)
        {
            byte* next = Next(x, level);
            if (next != null) Prefetch(NoBarrierNext(next, level));
            if (next != lastNotAfter && KeyIsAfterNode(key, next))
            {
                // Keep searching in this list
                x = next;
            }
            else
            {
                if (level == 0) return x;
                // Switch to next list, reuse KeyIsAfterNode() result
                lastNotAfter = next;
                level--;
            }
        }
    }

    /// <summary>Returns the last node in the list, or head if empty.</summary>
    private byte* FindLast()
    {
        byte* x = _head;
        int level = Height - 1;
        while (true)
        {
            byte* next = Next(x, level);
            if (next == null)
            {
                if (level == 0) return x;
                level--;
            }
            else
            {
                x = next;
            }
        }
    }

    /// <summary>True iff an entry that compares equal to <paramref name="key"/> is in the list.</summary>
    public bool Contains(SkipListKey key)
    {
        byte* x = FindGreaterOrEqual(key);
        return x != null && _compare.Compare(KeyOf(x), key) == 0;
    }

    /// <summary>
    /// Estimates the number of entries in [start, end) by sampling the upper levels (port of ApproximateNumEntries).
    /// </summary>
    public ulong ApproximateNumEntries(SkipListKey start, SkipListKey end)
    {
        // See upstream for the rationale: count the entries at the highest level that has "enough" samples and
        // scale by the branching factor for each level below.
        byte* lb = _head;
        byte* ub = null;
        ulong count = 0;
        for (int level = Height - 1; level >= 0; level--)
        {
            ulong sufficientSamples = (ulong)level * (ulong)_kBranching + 10UL;
            if (count >= sufficientSamples)
            {
                // No more counting; apply powers of kBranching and avoid floating point
                count *= (ulong)_kBranching;
                continue;
            }
            count = 0;
            byte* next;
            // Get a more precise lower bound (for start key)
            for (; ; )
            {
                next = Next(lb, level);
                if (next == ub) break;
                if (_compare.Compare(KeyOf(next), start) >= 0) break;
                lb = next;
            }
            // Count entries on this level until upper bound (for end key)
            for (; ; )
            {
                if (next == ub) break;
                if (_compare.Compare(KeyOf(next), end) >= 0)
                {
                    // Save refined upper bound to potentially save key comparison
                    ub = next;
                    break;
                }
                count++;
                next = Next(next, level);
            }
        }
        return count;
    }

    // ---------------------------------------------------------------- insert

    /// <summary>
    /// Inserts a key allocated by <see cref="AllocateKey"/>. Requires external synchronization with other writers
    /// (concurrent readers are fine). Returns false (and does not insert) if an equal key already exists.
    /// </summary>
    public bool Insert(byte* key) => Insert(key, _seqSplice, useCas: false, allowPartialSpliceFix: false);

    /// <summary>Like <see cref="Insert"/>, but may run concurrently with other <see cref="InsertConcurrently"/> calls.</summary>
    [SkipLocalsInit]
    public bool InsertConcurrently(byte* key)
    {
        byte** prev = stackalloc byte*[MaxPossibleHeight + 1];
        byte** next = stackalloc byte*[MaxPossibleHeight + 1];
        Splice splice;
        splice.Height = 0;
        splice.Prev = prev;
        splice.Next = next;
        return Insert(key, &splice, useCas: true, allowPartialSpliceFix: false);
    }

    private void FindSpliceForLevel(SkipListKey key, byte* before, byte* after, int level, byte** outPrev, byte** outNext, bool prefetchBefore)
    {
        while (true)
        {
            byte* next = Next(before, level);
            if (next != null)
            {
                Prefetch(NoBarrierNext(next, level));
                if (prefetchBefore && level > 0) Prefetch(NoBarrierNext(next, level - 1));
            }
            Debug.Assert(before == _head || KeyIsAfterNode(key, before));
            if (next == after || !KeyIsAfterNode(key, next))
            {
                // found it
                *outPrev = before;
                *outNext = next;
                return;
            }
            before = next;
        }
    }

    private void RecomputeSpliceLevels(SkipListKey key, Splice* splice, int recomputeLevel)
    {
        Debug.Assert(recomputeLevel > 0 && recomputeLevel <= splice->Height);
        for (int i = recomputeLevel - 1; i >= 0; --i)
        {
            FindSpliceForLevel(key, splice->Prev[i + 1], splice->Next[i + 1], i, &splice->Prev[i], &splice->Next[i], prefetchBefore: true);
        }
    }

    private bool Insert(byte* key, Splice* splice, bool useCas, bool allowPartialSpliceFix)
    {
        byte* x = NodeOfKey(key);
        SkipListKey keyDecoded = _compare.Decode(key);
        int height = *(int*)x;
        Debug.Assert(height >= 1 && height <= _kMaxHeight);

        int maxHeight = Volatile.Read(ref _maxHeight);
        while (height > maxHeight)
        {
            int observed = Interlocked.CompareExchange(ref _maxHeight, height, maxHeight);
            if (observed == maxHeight)
            {
                // successfully updated it
                maxHeight = height;
                break;
            }
            // else retry, possibly exiting the loop because somebody else increased it
            maxHeight = observed;
        }

        int recomputeHeight = 0;
        if (splice->Height < maxHeight)
        {
            // Either splice has never been used or max_height has grown since last use. We could potentially fix it
            // in the latter case, but that is tricky.
            splice->Prev[maxHeight] = _head;
            splice->Next[maxHeight] = null;
            splice->Height = maxHeight;
            recomputeHeight = maxHeight;
        }
        else
        {
            // Splice is a valid proper-height splice that brackets some key, but does it bracket this one? We need
            // to validate it and recompute a portion of the splice (levels 0..recompute_height-1) that is a superset
            // of all levels that don't bracket the new key. Several choices are reasonable, because we have to
            // balance the work saved against the extra comparisons required to validate the Splice.
            while (recomputeHeight < maxHeight)
            {
                if (Next(splice->Prev[recomputeHeight], recomputeHeight) != splice->Next[recomputeHeight])
                {
                    // splice isn't tight at this level, there must have been some inserts to this location that
                    // didn't update the splice. We might only be a little stale, but if the splice is very stale it
                    // would be O(N) to fix it. We haven't used up any of our budget of comparisons, so always move up
                    // even if we are pessimistic about our chances of success.
                    ++recomputeHeight;
                }
                else if (splice->Prev[recomputeHeight] != _head && !KeyIsAfterNode(keyDecoded, splice->Prev[recomputeHeight]))
                {
                    // key is from before splice
                    if (allowPartialSpliceFix)
                    {
                        // skip all levels with the same node without more comparisons
                        byte* bad = splice->Prev[recomputeHeight];
                        while (splice->Prev[recomputeHeight] == bad) ++recomputeHeight;
                    }
                    else
                    {
                        // we're pessimistic, recompute everything
                        recomputeHeight = maxHeight;
                    }
                }
                else if (KeyIsAfterNode(keyDecoded, splice->Next[recomputeHeight]))
                {
                    // key is from after splice
                    if (allowPartialSpliceFix)
                    {
                        byte* bad = splice->Next[recomputeHeight];
                        while (splice->Next[recomputeHeight] == bad) ++recomputeHeight;
                    }
                    else
                    {
                        recomputeHeight = maxHeight;
                    }
                }
                else
                {
                    // this level brackets the key, we won!
                    break;
                }
            }
        }
        Debug.Assert(recomputeHeight <= maxHeight);
        if (recomputeHeight > 0) RecomputeSpliceLevels(keyDecoded, splice, recomputeHeight);

        bool spliceIsValid = true;
        if (useCas)
        {
            for (int i = 0; i < height; ++i)
            {
                while (true)
                {
                    // Checking for duplicate keys on the level 0 is sufficient
                    byte* sn = splice->Next[i];
                    if (i == 0 && sn != null && _compare.Compare(KeyOf(sn), keyDecoded) <= 0)
                    {
                        // duplicate key
                        return false;
                    }
                    byte* sp = splice->Prev[i];
                    if (i == 0 && sp != _head && _compare.Compare(KeyOf(sp), keyDecoded) >= 0)
                    {
                        // duplicate key
                        return false;
                    }
                    NoBarrierSetNext(x, i, sn);
                    if (CasNext(sp, i, sn, x))
                    {
                        // success
                        break;
                    }
                    // CAS failed, we need to recompute prev and next. It is unlikely to be helpful to try to use a
                    // different level as we redo the search, because it should be unlikely that lots of nodes have
                    // been inserted between prev[i] and next[i]. No point in using next[i] as the after hint,
                    // because we know it is stale.
                    FindSpliceForLevel(keyDecoded, splice->Prev[i], null, i, &splice->Prev[i], &splice->Next[i], prefetchBefore: false);

                    // Since we've narrowed the bracket for level i, we might have violated the Splice constraint
                    // between i and i-1. Make sure we recompute the whole thing next time.
                    if (i > 0) spliceIsValid = false;
                }
            }
        }
        else
        {
            for (int i = 0; i < height; ++i)
            {
                if (i >= recomputeHeight && Next(splice->Prev[i], i) != splice->Next[i])
                {
                    FindSpliceForLevel(keyDecoded, splice->Prev[i], null, i, &splice->Prev[i], &splice->Next[i], prefetchBefore: false);
                }
                // Checking for duplicate keys on the level 0 is sufficient
                byte* sn = splice->Next[i];
                if (i == 0 && sn != null && _compare.Compare(KeyOf(sn), keyDecoded) <= 0)
                {
                    return false;
                }
                byte* sp = splice->Prev[i];
                if (i == 0 && sp != _head && _compare.Compare(KeyOf(sp), keyDecoded) >= 0)
                {
                    return false;
                }
                Debug.Assert(Next(sp, i) == sn);
                NoBarrierSetNext(x, i, sn);
                SetNext(sp, i, x);
            }
        }
        if (spliceIsValid)
        {
            for (int i = 0; i < height; ++i) splice->Prev[i] = x;
        }
        else
        {
            splice->Height = 0;
        }
        return true;
    }

    /// <summary>Validates ordering and level structure (tests only; requires no concurrent writers).</summary>
    internal void Validate()
    {
        // Interate over all levels at the same time, and verify nodes appear in the right order, and nodes appear in
        // upper level also appear in lower levels.
        int maxHeight = Height;
        byte** nodes = stackalloc byte*[MaxPossibleHeight];
        for (int i = 0; i < maxHeight; i++) nodes[i] = _head;
        while (nodes[0] != null)
        {
            byte* l0Next = Next(nodes[0], 0);
            if (l0Next == null) break;
            if (nodes[0] != _head && _compare.Compare(KeyOf(nodes[0]), _compare.Decode(KeyOf(l0Next))) >= 0)
                throw new InvalidOperationException("skiplist level 0 out of order");
            nodes[0] = l0Next;
            int i = 1;
            while (i < maxHeight)
            {
                byte* next = Next(nodes[i], i);
                if (next == null) break;
                int cmp = _compare.Compare(KeyOf(nodes[0]), _compare.Decode(KeyOf(next)));
                if (cmp > 0) throw new InvalidOperationException("skiplist upper level skipped a level-0 node");
                if (cmp == 0)
                {
                    if (next != nodes[0]) throw new InvalidOperationException("skiplist duplicate key at upper level");
                    nodes[i] = next;
                }
                else break;
                i++;
            }
        }
        for (int i = 1; i < maxHeight; i++)
        {
            if (nodes[i] != null && Next(nodes[i], i) != null) throw new InvalidOperationException("skiplist upper level has extra nodes");
        }
    }

    // ---------------------------------------------------------------- iterator

    /// <summary>Allocation-free iterator over the list. Valid while the list (and its arena) is alive.</summary>
    public struct Iterator
    {
        private readonly InlineSkipList<TComparer> _list;
        private byte* _node;

        public Iterator(InlineSkipList<TComparer> list)
        {
            _list = list;
            _node = null;
        }

        public readonly bool Valid => _node != null;

        /// <summary>The stored (encoded) key at the current position. REQUIRES: Valid.</summary>
        public readonly byte* Key
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Valid);
                return KeyOf(_node);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Next()
        {
            Debug.Assert(Valid);
            byte* n = InlineSkipList<TComparer>.Next(_node, 0);
            _node = n;
            // Nodes of a randomly filled list are scattered over the arena: start loading the following node while the
            // caller processes this one (a flush iterates the whole list).
            if (n != null) Prefetch(NoBarrierNext(n, 0));
        }

        public void Prev()
        {
            // Instead of using explicit "prev" links, we just search for the last node that falls before key.
            Debug.Assert(Valid);
            _node = _list.FindLessThan(_list._compare.Decode(KeyOf(_node)));
            if (_node == _list._head) _node = null;
        }

        /// <summary>Advance to the first entry with a key &gt;= target.</summary>
        public void Seek(SkipListKey target) => _node = _list.FindGreaterOrEqual(target);

        /// <summary>Retreat to the last entry with a key &lt;= target.</summary>
        public void SeekForPrev(SkipListKey target)
        {
            Seek(target);
            if (!Valid) SeekToLast();
            while (Valid && _list._compare.Compare(KeyOf(_node), target) > 0) Prev();
        }

        public void SeekToFirst() => _node = InlineSkipList<TComparer>.Next(_list._head, 0);

        public void SeekToLast()
        {
            _node = _list.FindLast();
            if (_node == _list._head) _node = null;
        }
    }

    public Iterator GetIterator() => new(this);
}
