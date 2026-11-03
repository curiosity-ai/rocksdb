using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Env;

/// <summary>
/// Semantics shared by every <see cref="IFileSystem"/> (env/env_test.cc, env/mock_env_test.cc subsets), run against
/// both <see cref="LocalFileSystem"/> and <see cref="MemoryFileSystem"/>.
/// </summary>
[TestClass]
public sealed partial class FileSystemTests
{
    public static IEnumerable<object[]> FileSystems => [["local"], ["memory"]];

    private sealed class Fixture : IDisposable
    {
        public readonly IFileSystem Fs;
        public readonly string Dir;

        public Fixture(string kind)
        {
            Dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-env-" + Guid.NewGuid().ToString("N"));
            Fs = kind == "local" ? LocalFileSystem.Instance : new MemoryFileSystem();
            Fs.CreateDirectoryIfMissing(Dir);
        }

        public string P(string name) => Path.Combine(Dir, name);

        public void Dispose()
        {
            if (Fs is LocalFileSystem)
            {
                try { Directory.Delete(Dir, recursive: true); } catch (IOException) { }
            }
        }
    }

    private static byte[] Pattern(int length, int seed)
    {
        var b = new byte[length];
        new Random(seed).NextBytes(b);
        return b;
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        var buf = new byte[f.Length];
        Assert.AreEqual(buf.Length, f.Read(0, buf));
        return buf;
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public void AppendBufferingAndSize(string kind)
    {
        using var fx = new Fixture(kind);
        byte[] data = Pattern(300_000, 1);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("a"), true, new WritableFileOptions { InitialBufferSize = 4096, MaxBufferSize = 16384 }))
        {
            int pos = 0;
            var rnd = new Random(2);
            while (pos < data.Length)
            {
                int n = Math.Min(data.Length - pos, rnd.Next(0, 40_000));
                w.Append(data.AsSpan(pos, n));
                pos += n;
                Assert.AreEqual(pos, w.Size);
            }
            w.Flush();
            Assert.AreEqual(data.Length, fx.Fs.GetFileSize(fx.P("a")));
            w.Sync();
        }
        CollectionAssert.AreEqual(data, ReadAll(fx.Fs, fx.P("a")));
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public async Task AsyncAppendFlushSync(string kind)
    {
        using var fx = new Fixture(kind);
        byte[] data = Pattern(200_000, 3);
        await using (IWritableFile w = fx.Fs.CreateWritable(fx.P("b"), true, WritableFileOptions.Default))
        {
            for (int pos = 0; pos < data.Length; pos += 7_001)
                await w.AppendAsync(data.AsMemory(pos, Math.Min(7_001, data.Length - pos)));
            await w.FlushAsync();
            await w.SyncAsync();
            Assert.AreEqual(data.Length, w.Size);
            await w.AppendAsync(data.AsMemory(0, 10)); // buffered, flushed by DisposeAsync
        }
        byte[] all = ReadAll(fx.Fs, fx.P("b"));
        Assert.AreEqual(data.Length + 10, all.Length);
        CollectionAssert.AreEqual(data, all[..data.Length]);

        using IRandomAccessFile r = fx.Fs.OpenRandomAccess(fx.P("b"));
        var buf = new byte[1000];
        Assert.AreEqual(1000, await r.ReadAsync(5000, buf));
        CollectionAssert.AreEqual(data[5000..6000], buf);
        Assert.AreEqual(10, await r.ReadAsync(data.Length, buf)); // short read at EOF
        Assert.AreEqual(0, await r.ReadAsync(data.Length + 10, buf));
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public async Task SequentialReadAndSkip(string kind)
    {
        using var fx = new Fixture(kind);
        byte[] data = Pattern(100_000, 4);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("s")))
            w.Append(data);
        foreach (int readahead in new[] { 0, 4096 })
        {
            using ISequentialFile s = fx.Fs.OpenSequential(fx.P("s"), new SequentialFileOptions { ReadaheadSize = readahead });
            var buf = new byte[100];
            Assert.AreEqual(100, s.Read(buf));
            CollectionAssert.AreEqual(data[..100], buf);
            s.Skip(900);
            Assert.AreEqual(100, await s.ReadAsync(buf));
            CollectionAssert.AreEqual(data[1000..1100], buf);
            s.Skip(50_000);
            var big = new byte[60_000];
            int total = 0, n;
            while ((n = s.Read(big.AsSpan(total))) > 0) total += n;
            Assert.AreEqual(data.Length - 51_100, total);
            CollectionAssert.AreEqual(data[51_100..], big[..total]);
        }
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public void RenameDeleteChildrenExists(string kind)
    {
        using var fx = new Fixture(kind);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("CURRENT.tmp"))) w.Append("MANIFEST-000001\n"u8);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("CURRENT"))) w.Append("old\n"u8);
        fx.Fs.RenameFile(fx.P("CURRENT.tmp"), fx.P("CURRENT"));
        Assert.IsFalse(fx.Fs.FileExists(fx.P("CURRENT.tmp")));
        CollectionAssert.AreEqual("MANIFEST-000001\n"u8.ToArray(), ReadAll(fx.Fs, fx.P("CURRENT")));

        fx.Fs.CreateDirectoryIfMissing(fx.P("sub"));
        fx.Fs.CreateDirectoryIfMissing(fx.P("sub")); // idempotent
        Assert.IsTrue(fx.Fs.DirectoryExists(fx.P("sub")));
        var children = new List<string>(fx.Fs.GetChildren(fx.Dir));
        children.Sort(StringComparer.Ordinal);
        CollectionAssert.AreEqual(new[] { "CURRENT", "sub" }, children);

        fx.Fs.DeleteFile(fx.P("CURRENT"));
        Assert.IsFalse(fx.Fs.FileExists(fx.P("CURRENT")));
        fx.Fs.DeleteDirectory(fx.P("sub"));
        Assert.IsFalse(fx.Fs.DirectoryExists(fx.P("sub")));
        fx.Fs.SyncDirectory(fx.Dir);

        var ex = Assert.ThrowsExactly<RocksDbException>(() => fx.Fs.OpenRandomAccess(fx.P("missing")));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual(StatusSubCode.PathNotFound, ex.SubCode);
        ex = Assert.ThrowsExactly<RocksDbException>(() => fx.Fs.GetFileSize(fx.P("missing")));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public void ReopenWritableAppendsAtEnd(string kind)
    {
        using var fx = new Fixture(kind);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("r"))) w.Append("hello "u8);
        using (IWritableFile w = fx.Fs.ReopenWritable(fx.P("r"), WritableFileOptions.Default))
        {
            Assert.AreEqual(6, w.Size);
            w.Append("world"u8);
        }
        CollectionAssert.AreEqual("hello world"u8.ToArray(), ReadAll(fx.Fs, fx.P("r")));
        // truncate=false keeps existing content and overwrites from offset 0 (ReuseWritableFile)
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("r"), false)) w.Append("J"u8);
        CollectionAssert.AreEqual("Jello world"u8.ToArray(), ReadAll(fx.Fs, fx.P("r")));
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public void LockFileExcludesSecondLock(string kind)
    {
        using var fx = new Fixture(kind);
        string lockPath = fx.P("LOCK");
        IDisposable l = fx.Fs.LockFile(lockPath);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => fx.Fs.LockFile(lockPath));
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        l.Dispose();
        l.Dispose(); // idempotent
        using IDisposable again = fx.Fs.LockFile(lockPath);
    }

    /// <summary>
    /// Regression: on Linux the LOCK is an open-file-description (OFD) lock, which belongs to the open file
    /// description rather than to the descriptor. A child process started by any thread of this process (vfork/fork
    /// before exec) briefly shares that description, so merely closing our descriptor did not release the lock until
    /// the child exec'd, and an immediate reopen failed with "Resource temporarily unavailable". The extra reference
    /// is modelled here with dup(2): releasing the lock must not depend on other references closing.
    /// </summary>
    [TestMethod]
    public void LockFileReleasedWhileDescriptionStillReferenced()
    {
        if (!OperatingSystem.IsLinux()) Assert.Inconclusive("OFD locks are Linux-only");
        using var fx = new Fixture("local");
        string lockPath = fx.P("LOCK");
        IDisposable l = fx.Fs.LockFile(lockPath);
        int dupFd = DupDescriptorOf(lockPath);
        try
        {
            l.Dispose();
            using IDisposable again = fx.Fs.LockFile(lockPath);
        }
        finally
        {
            Assert.AreEqual(0, LibC.close(dupFd));
        }
    }

    /// <summary>dup(2) of this process's descriptor open on <paramref name="path"/> (found through /proc/self/fd).</summary>
    private static int DupDescriptorOf(string path)
    {
        string full = Path.GetFullPath(path);
        foreach (string link in Directory.GetFiles("/proc/self/fd"))
        {
            string? target;
            try { target = new FileInfo(link).LinkTarget; }
            catch (IOException) { continue; }
            if (target != full) continue;
            int fd = LibC.dup(int.Parse(Path.GetFileName(link), System.Globalization.CultureInfo.InvariantCulture));
            Assert.IsGreaterThanOrEqualTo(0, fd, "dup failed");
            return fd;
        }
        Assert.Fail("no descriptor open on " + full);
        return -1;
    }

    private static partial class LibC
    {
        [System.Runtime.InteropServices.LibraryImport("libc", SetLastError = true)]
        public static partial int dup(int fd);

        [System.Runtime.InteropServices.LibraryImport("libc", SetLastError = true)]
        public static partial int close(int fd);
    }

    [TestMethod]
    [DynamicData(nameof(FileSystems))]
    public async Task ConcurrentRandomReads(string kind)
    {
        using var fx = new Fixture(kind);
        byte[] data = Pattern(1 << 20, 5);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("c"))) w.Append(data);
        using IRandomAccessFile r = fx.Fs.OpenRandomAccess(fx.P("c"));
        var tasks = new Task[4];
        for (int t = 0; t < tasks.Length; t++)
        {
            int seed = t;
            tasks[t] = Task.Run(async () =>
            {
                var rnd = new Random(seed);
                var buf = new byte[4096];
                for (int i = 0; i < 500; i++)
                {
                    int off = rnd.Next(0, data.Length - buf.Length);
                    int n = (i & 1) == 0 ? r.Read(off, buf) : await r.ReadAsync(off, buf);
                    Assert.AreEqual(buf.Length, n);
                    Assert.IsTrue(buf.AsSpan().SequenceEqual(data.AsSpan(off, buf.Length)));
                }
            });
        }
        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public void LocalPreallocationAndRangeSyncKeepLogicalSize()
    {
        using var fx = new Fixture("local");
        byte[] data = Pattern(200_000, 6);
        var opts = new WritableFileOptions { PreallocationBlockSize = 1 << 20, BytesPerSync = 64 * 1024, InitialBufferSize = 8192, MaxBufferSize = 8192 };
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("p"), true, opts))
        {
            w.Append(data);
            w.Sync();
            Assert.AreEqual(data.Length, fx.Fs.GetFileSize(fx.P("p")));
        }
        Assert.AreEqual(data.Length, new FileInfo(fx.P("p")).Length);
        CollectionAssert.AreEqual(data, ReadAll(fx.Fs, fx.P("p")));
    }

    [TestMethod]
    public void LocalRateLimiterIsConsulted()
    {
        using var fx = new Fixture("local");
        var limiter = new CountingRateLimiter(1000);
        byte[] data = Pattern(10_000, 7);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("rl"), true, new WritableFileOptions { RateLimiter = limiter, IoPriority = IoPriority.High }))
        {
            w.Append(data);
            w.Flush();
        }
        Assert.AreEqual(10_000, limiter.Bytes);
        Assert.AreEqual(10, limiter.Requests);
        Assert.AreEqual(IoPriority.High, limiter.LastPriority);
        CollectionAssert.AreEqual(data, ReadAll(fx.Fs, fx.P("rl")));
    }

    [TestMethod]
    public void LocalDirectIORoundTrip()
    {
        using var fx = new Fixture("local");
        if (!OperatingSystem.IsLinux()) Assert.Inconclusive("direct I/O is Linux only");
        byte[] data = Pattern(50_001, 8);
        var wopts = new WritableFileOptions { UseDirectIO = true, InitialBufferSize = 16384 };
        IWritableFile w;
        try
        {
            w = fx.Fs.CreateWritable(fx.P("d"), true, wopts);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
        {
            Assert.Inconclusive("file system does not support O_DIRECT: " + e.Message);
            return;
        }
        using (w)
        {
            w.Append(data.AsSpan(0, 10_000));
            w.Sync(); // writes a padded partial page
            w.Append(data.AsSpan(10_000));
        }
        Assert.AreEqual(data.Length, new FileInfo(fx.P("d")).Length);
        using IRandomAccessFile r = fx.Fs.OpenRandomAccess(fx.P("d"), new RandomAccessFileOptions { UseDirectIO = true });
        var buf = new byte[12_345];
        Assert.AreEqual(buf.Length, r.Read(777, buf));
        CollectionAssert.AreEqual(data[777..(777 + buf.Length)], buf);
        Assert.AreEqual(1, r.Read(data.Length - 1, buf));
        Assert.AreEqual(data[^1], buf[0]);
        using (IWritableFile w2 = fx.Fs.ReopenWritable(fx.P("d"), wopts)) w2.Append("xyz"u8);
        byte[] all = File.ReadAllBytes(fx.P("d"));
        Assert.AreEqual(data.Length + 3, all.Length);
        CollectionAssert.AreEqual(data, all[..data.Length]);
    }

    [TestMethod]
    public void PrefetchBufferReadsAheadAndDoubles()
    {
        using var fx = new Fixture("memory");
        byte[] data = Pattern(1 << 20, 9);
        using (IWritableFile w = fx.Fs.CreateWritable(fx.P("f"))) w.Append(data);
        using IRandomAccessFile r = fx.Fs.OpenRandomAccess(fx.P("f"));
        using var pb = new FilePrefetchBuffer(r, 8 * 1024, 256 * 1024);
        var buf = new byte[4096];
        long off = 0;
        for (int i = 0; i < 64; i++)
        {
            Assert.AreEqual(buf.Length, pb.Read(off, buf));
            Assert.IsTrue(buf.AsSpan().SequenceEqual(data.AsSpan((int)off, buf.Length)));
            off += buf.Length;
        }
        // 256 KiB read sequentially with 8K..256K doubling readahead needs only a handful of physical reads.
        Assert.IsTrue(pb.PhysicalReads <= 7, "physical reads: " + pb.PhysicalReads);
        Assert.IsTrue(pb.ReadaheadSize >= 64 * 1024, "readahead: " + pb.ReadaheadSize);
        Assert.IsTrue(pb.TryGetSpan(off, 100, out ReadOnlySpan<byte> view));
        Assert.IsTrue(view.SequenceEqual(data.AsSpan((int)off, 100)));
        // Random jump resets readahead.
        Assert.AreEqual(buf.Length, pb.Read(900_000, buf));
        Assert.AreEqual(8 * 1024, pb.ReadaheadSize);
        Assert.IsTrue(buf.AsSpan().SequenceEqual(data.AsSpan(900_000, buf.Length)));
        // Reads past EOF are short.
        Assert.AreEqual(10, pb.Read(data.Length - 10, buf));
    }

    private sealed class CountingRateLimiter(long burst) : IRateLimiter
    {
        public long Bytes;
        public int Requests;
        public IoPriority LastPriority;
        public long SingleBurstBytes => burst;

        public void Request(long bytes, IoPriority priority)
        {
            Assert.IsTrue(bytes <= burst);
            Bytes += bytes;
            Requests++;
            LastPriority = priority;
        }

        public ValueTask RequestAsync(long bytes, IoPriority priority, CancellationToken cancellationToken)
        {
            Request(bytes, priority);
            return ValueTask.CompletedTask;
        }
    }
    /// <summary>
    /// <see cref="AsyncPrefetchBuffer"/> (FilePrefetchBuffer async_io): sequential reads of random sizes (blocks
    /// straddling the prefetched chunks, blocks larger than the straddle reserve and the readahead), forward jumps and
    /// backward jumps return exactly the file's bytes, and sequential reads are served by prefetches, not by
    /// synchronous reads. The test file's ReadAsync completes asynchronously.
    /// </summary>
    [TestMethod]
    public void AsyncPrefetchBufferReturnsFileBytes()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        byte[] data = new byte[3_000_000];
        rnd.NextBytes(data);
        var file = new RocksDb.Managed.Tests.Table.MemRandomAccessFile(data);
        using var buf = new AsyncPrefetchBuffer(64 * 1024);
        buf.Reset(file);
        long offset = 0;
        int sequential = 0;
        while (offset < data.Length)
        {
            int len = rnd.Next(10) == 0 ? rnd.Next(70_000, 150_000) : rnd.Next(1, 9000);
            len = (int)Math.Min(len, data.Length - offset);
            Assert.IsTrue(buf.TryRead(offset, len, out ReadOnlySpan<byte> got), "seed=" + seed + " offset " + offset);
            Assert.IsTrue(got.SequenceEqual(data.AsSpan((int)offset, len)), "seed=" + seed + " offset " + offset + " len " + len);
            offset += len;
            sequential++;
            int jump = rnd.Next(50);
            if (jump == 0) offset = Math.Max(0, offset - rnd.Next(200_000));   // backward
            else if (jump == 1) offset += rnd.Next(300_000);                 // forward gap
        }
        Assert.IsTrue(buf.Prefetches > 0);
        Assert.IsTrue(buf.SyncReads < sequential / 4, "seed=" + seed + ": " + buf.SyncReads + " sync reads for " + sequential + " reads");
        Assert.IsFalse(buf.TryRead(data.Length - 10, 20, out _), "beyond the end");
        buf.Reset(null);
    }
}

