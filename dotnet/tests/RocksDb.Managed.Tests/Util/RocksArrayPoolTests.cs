using System.Buffers;
using System.Text.RegularExpressions;

namespace RocksDb.Managed.Tests.Util;

/// <summary>
/// <see cref="RocksArrayPool{T}"/>: the engine's private copy of the shared array pool. It must behave like
/// <see cref="ArrayPool{T}.Shared"/> and never exchange arrays with it.
/// </summary>
[TestClass]
public sealed class RocksArrayPoolTests
{
    [TestMethod]
    public void RentRoundsUpToPowerOfTwoBucketsAndReuses()
    {
        ArrayPool<byte> pool = RocksArrayPool<byte>.Shared;
        (int Request, int Length)[] cases = [(1, 16), (16, 16), (17, 32), (4096, 4096), (4101, 8192), ((1 << 20) + 1, 1 << 21)];
        foreach ((int request, int length) in cases)
        {
            byte[] a = pool.Rent(request);
            Assert.AreEqual(length, a.Length, "request " + request);
            pool.Return(a);
            Assert.AreSame(a, pool.Rent(request), "the returned array is reused on the same thread, request " + request);
            pool.Return(a);
        }
    }

    [TestMethod]
    public void EdgeCasesMatchTheSharedPool()
    {
        ArrayPool<int> pool = RocksArrayPool<int>.Shared;
        Assert.AreEqual(0, pool.Rent(0).Length);
        Assert.AreSame(Array.Empty<int>(), pool.Rent(0));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => pool.Rent(-1));
        Assert.ThrowsExactly<ArgumentNullException>(() => pool.Return(null!));
        Assert.ThrowsExactly<ArgumentException>(() => pool.Return(new int[100]), "a length that is not a bucket size is rejected");
        pool.Return(new int[1 << 6]); // a bucket-sized array from anywhere is accepted
        pool.Return([]);              // empty arrays are accepted and dropped

        // Above the largest bucket (2^30 elements) arrays are allocated and, when returned, dropped; checked with the
        // bucket arithmetic rather than by allocating them.
        Assert.AreEqual(26, PrivateSharedArrayPool<byte>.SelectBucketIndex(1 << 30));
        Assert.AreEqual(27, PrivateSharedArrayPool<byte>.SelectBucketIndex((1 << 30) + 1));
        Assert.AreEqual(1 << 30, PrivateSharedArrayPool<byte>.GetMaxSizeForBucket(26));
    }

    private struct FlagAndValue
    {
        public bool Flag;
        public long Value;
    }

    [TestMethod]
    public void NewArraysOfNonPrimitiveTypesAreZeroed()
    {
        // Only primitive element types (except bool) may come back uninitialized; a struct with a bool field must
        // never see an arbitrary bit pattern. The type is private to this test, so these rents are fresh allocations.
        FlagAndValue[] a = RocksArrayPool<FlagAndValue>.Shared.Rent(1 << 16);
        foreach (FlagAndValue v in a)
        {
            Assert.IsFalse(v.Flag);
            Assert.AreEqual(0L, v.Value);
        }
        RocksArrayPool<FlagAndValue>.Shared.Return(a);
    }

    [TestMethod]
    public void ClearArrayZeroesOnReturn()
    {
        ArrayPool<byte> pool = RocksArrayPool<byte>.Shared;
        byte[] a = pool.Rent(64);
        a.AsSpan().Fill(0xAB);
        pool.Return(a, clearArray: true);
        byte[] b = pool.Rent(64);
        Assert.AreSame(a, b);
        Assert.IsFalse(b.AsSpan().ContainsAnyExcept((byte)0));
        pool.Return(b);
    }

    [TestMethod]
    public void IsIsolatedFromArrayPoolShared()
    {
        Assert.AreNotSame(ArrayPool<byte>.Shared, RocksArrayPool<byte>.Shared);
        Assert.IsInstanceOfType<PrivateSharedArrayPool<byte>>(RocksArrayPool<byte>.Shared);

        // Arrays given to the process-wide shared pool (e.g. by code that keeps using them afterwards) are never handed
        // out by the engine's pool, and the engine's arrays never reach the shared pool.
        const int Size = 1 << 13;
        var foreign = new HashSet<byte[]>(ReferenceEqualityComparer.Instance);
        for (int i = 0; i < 200; i++)
        {
            byte[] f = ArrayPool<byte>.Shared.Rent(Size);
            foreign.Add(f);
            ArrayPool<byte>.Shared.Return(f);
        }
        var ours = new List<byte[]>();
        for (int i = 0; i < 200; i++) ours.Add(RocksArrayPool<byte>.Shared.Rent(Size));
        foreach (byte[] o in ours) Assert.IsFalse(foreign.Contains(o), "an array of ArrayPool.Shared came out of RocksArrayPool");
        foreach (byte[] o in ours) RocksArrayPool<byte>.Shared.Return(o);
        var mine = new HashSet<byte[]>(ours, ReferenceEqualityComparer.Instance);
        for (int i = 0; i < 200; i++)
        {
            byte[] f = ArrayPool<byte>.Shared.Rent(Size);
            Assert.IsFalse(mine.Contains(f), "an array of RocksArrayPool came out of ArrayPool.Shared");
            ArrayPool<byte>.Shared.Return(f);
        }
    }

    [TestMethod]
    public async Task ConcurrentRentReturnAcrossThreadsNeverHandsOutAnArrayTwice()
    {
        // Arrays rented on one thread and returned on another (as files and cache entries are) go through the per-core
        // partitions; under contention no array may be held by two renters at once.
        ArrayPool<byte> pool = RocksArrayPool<byte>.Shared;
        var inUse = new System.Collections.Concurrent.ConcurrentDictionary<byte[], byte>(ReferenceEqualityComparer.Instance);
        var handoff = new System.Collections.Concurrent.BlockingCollection<byte[]>(1024);
        int violations = 0;
        const int PerThread = 20_000;
        Task[] producers = new Task[4];
        for (int t = 0; t < producers.Length; t++)
        {
            int seed = t;
            producers[t] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                for (int i = 0; i < PerThread; i++)
                {
                    byte[] a = pool.Rent(rnd.Next(1, 9000));
                    if (!inUse.TryAdd(a, 0)) Interlocked.Increment(ref violations);
                    if ((i & 1) == 0) handoff.Add(a); // returned by the consumer thread
                    else
                    {
                        inUse.TryRemove(a, out _);
                        pool.Return(a);
                    }
                }
            });
        }
        Task consumer = Task.Run(() =>
        {
            foreach (byte[] a in handoff.GetConsumingEnumerable())
            {
                inUse.TryRemove(a, out _);
                pool.Return(a);
            }
        });
        await Task.WhenAll(producers);
        handoff.CompleteAdding();
        await consumer;
        Assert.AreEqual(0, violations);
    }

    [TestMethod]
    public void TrimKeepsThePoolUsable()
    {
        var pool = (PrivateSharedArrayPool<byte>)RocksArrayPool<byte>.Shared;
        byte[] a = pool.Rent(256);
        pool.Return(a);
        for (int i = 0; i < 3; i++) Assert.IsTrue(pool.Trim());
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        byte[] b = pool.Rent(256);
        Assert.AreEqual(256, b.Length);
        pool.Return(b);
    }

    /// <summary>
    /// Guard: the engine and its companion projects rent pooled arrays only from <see cref="RocksArrayPool{T}"/>, so a
    /// misuse of <see cref="ArrayPool{T}.Shared"/> elsewhere in the process cannot corrupt their buffers.
    /// </summary>
    [TestMethod]
    public void SourcesDoNotUseArrayPoolShared()
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "RocksDb.Managed.slnx"))) dir = Path.GetDirectoryName(dir);
        if (dir is null)
        {
            Assert.Inconclusive("source tree not found next to the test binaries");
            return;
        }
        var shared = new Regex(@"(?<![A-Za-z])ArrayPool\s*<[^>]+>\s*\.\s*Shared", RegexOptions.CultureInvariant);
        var offenders = new List<string>();
        foreach (string project in new[] { "src", "tools", "bench" })
        {
            foreach (string file in Directory.EnumerateFiles(Path.Combine(dir, project), "*.cs", SearchOption.AllDirectories))
            {
                if (file.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
                    file.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                    continue;
                string[] lines = File.ReadAllLines(file);
                for (int i = 0; i < lines.Length; i++)
                {
                    string code = lines[i];
                    int comment = code.IndexOf("//", StringComparison.Ordinal);
                    if (comment >= 0) code = code[..comment];
                    if (shared.IsMatch(code)) offenders.Add($"{Path.GetRelativePath(dir, file)}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }
        Assert.AreEqual(0, offenders.Count, "use RocksArrayPool<T>.Shared instead of ArrayPool<T>.Shared:\n" + string.Join("\n", offenders));
    }
}
