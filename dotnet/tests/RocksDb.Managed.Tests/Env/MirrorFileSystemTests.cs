using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Env;

/// <summary>
/// <see cref="MirrorFileSystem"/> (port of EnvMirror). Ports of utilities/env_mirror_test.cc (Basics, ReadWrite,
/// Locks, Misc, LargeWrite: two in-memory file systems, as upstream's two MockEnvs), mismatch detection, and a database
/// workload over <see cref="LocalFileSystem"/> mirrored with <see cref="MemoryFileSystem"/>.
/// </summary>
[TestClass]
public sealed class MirrorFileSystemTests
{
    private readonly MemoryFileSystem _a = new();
    private readonly MemoryFileSystem _b = new();
    private MirrorFileSystem Env => new(_a, _b);

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static void Write(IFileSystem fs, string path, params string[] parts)
    {
        using IWritableFile f = fs.CreateWritable(path);
        foreach (string p in parts) f.Append(U(p));
    }

    [TestMethod]
    public void Basics()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");

        // Check that the directory is empty.
        Assert.IsFalse(env.FileExists("/dir/non_existent"));
        Assert.ThrowsExactly<RocksDbException>(() => env.GetFileSize("/dir/non_existent"));
        Assert.AreEqual(0, env.GetChildren("/dir").Count());

        // Create a file.
        env.CreateWritable("/dir/f").Dispose();
        Assert.IsTrue(env.FileExists("/dir/f"));
        Assert.IsTrue(_a.FileExists("/dir/f"));
        Assert.IsTrue(_b.FileExists("/dir/f"));
        Assert.AreEqual(0L, env.GetFileSize("/dir/f"));
        CollectionAssert.AreEqual(new[] { "f" }, env.GetChildren("/dir").ToArray());
        CollectionAssert.AreEqual(new[] { "f" }, _a.GetChildren("/dir").ToArray());
        CollectionAssert.AreEqual(new[] { "f" }, _b.GetChildren("/dir").ToArray());

        // Write to the file.
        Write(env, "/dir/f", "abc");
        Assert.AreEqual(3L, env.GetFileSize("/dir/f"));
        Assert.AreEqual(3L, _a.GetFileSize("/dir/f"));
        Assert.AreEqual(3L, _b.GetFileSize("/dir/f"));

        // Check that renaming works.
        Assert.ThrowsExactly<RocksDbException>(() => env.RenameFile("/dir/non_existent", "/dir/g"));
        env.RenameFile("/dir/f", "/dir/g");
        Assert.IsFalse(env.FileExists("/dir/f"));
        Assert.IsTrue(env.FileExists("/dir/g"));
        Assert.AreEqual(3L, env.GetFileSize("/dir/g"));
        Assert.AreEqual(3L, _a.GetFileSize("/dir/g"));
        Assert.AreEqual(3L, _b.GetFileSize("/dir/g"));

        // Check that opening non-existent file fails.
        Assert.ThrowsExactly<RocksDbException>(() => env.OpenSequential("/dir/non_existent"));
        Assert.ThrowsExactly<RocksDbException>(() => env.OpenRandomAccess("/dir/non_existent"));

        // Check that deleting works.
        env.DeleteFile("/dir/g");
        Assert.IsFalse(env.FileExists("/dir/g"));
        Assert.AreEqual(0, env.GetChildren("/dir").Count());
        env.DeleteDirectory("/dir");
        Assert.IsFalse(env.DirectoryExists("/dir"));
    }

    [TestMethod]
    public void ReadWrite()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");
        Write(env, "/dir/f", "hello ", "world");
        byte[] scratch = new byte[100];

        // Read sequentially.
        using (ISequentialFile seq = env.OpenSequential("/dir/f"))
        {
            Assert.AreEqual("hello", Encoding.UTF8.GetString(scratch, 0, seq.Read(scratch.AsSpan(0, 5))));
            seq.Skip(1);
            Assert.AreEqual("world", Encoding.UTF8.GetString(scratch, 0, seq.Read(scratch)));
            Assert.AreEqual(0, seq.Read(scratch)); // Try reading past EOF.
            seq.Skip(100); // Try to skip past end of file.
            Assert.AreEqual(0, seq.Read(scratch));
        }

        // Random reads.
        using IRandomAccessFile rand = env.OpenRandomAccess("/dir/f");
        Assert.AreEqual("world", Encoding.UTF8.GetString(scratch, 0, rand.Read(6, scratch.AsSpan(0, 5))));
        Assert.AreEqual("hello", Encoding.UTF8.GetString(scratch, 0, rand.Read(0, scratch.AsSpan(0, 5))));
        Assert.AreEqual("d", Encoding.UTF8.GetString(scratch, 0, rand.Read(10, scratch)));
        Assert.AreEqual(11L, rand.Length);
        // Too high offset: nothing to read (both sides agree).
        Assert.AreEqual(0, rand.Read(1000, scratch.AsSpan(0, 5)));
    }

    [TestMethod]
    public async Task ReadWriteAsync()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");
        await using (IWritableFile w = env.CreateWritable("/dir/f"))
        {
            await w.AppendAsync(U("hello world"));
            await w.SyncAsync();
            Assert.AreEqual(11L, w.Size);
        }
        byte[] scratch = new byte[100];
        using (IRandomAccessFile rand = env.OpenRandomAccess("/dir/f"))
        {
            Assert.AreEqual(5, await rand.ReadAsync(6, scratch.AsMemory(0, 5)));
        }
        using ISequentialFile seq = env.OpenSequential("/dir/f");
        Assert.AreEqual(11, await seq.ReadAsync(scratch));
    }

    [TestMethod]
    public void Locks()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");
        IDisposable l = env.LockFile("/dir/some file");
        // Both sides hold the lock: a second lock fails on both.
        Assert.ThrowsExactly<RocksDbException>(() => env.LockFile("/dir/some file"));
        l.Dispose();
        env.LockFile("/dir/some file").Dispose();
    }

    [TestMethod]
    public void Misc()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/a");
        using IWritableFile w = env.CreateWritable("/a/b");
        // These are no-ops, but we test they return success.
        w.Sync();
        w.Flush();
        env.SyncDirectory("/a");
    }

    [TestMethod]
    public void LargeWrite()
    {
        const int kWriteSize = 300 * 1024;
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");
        byte[] data = new byte[kWriteSize];
        for (int i = 0; i < kWriteSize; ++i) data[i] = (byte)i;
        using (IWritableFile w = env.CreateWritable("/dir/f"))
        {
            w.Append(U("foo"));
            w.Append(data);
        }

        using ISequentialFile seq = env.OpenSequential("/dir/f");
        byte[] scratch = new byte[kWriteSize * 2];
        Assert.AreEqual("foo", Encoding.UTF8.GetString(scratch, 0, seq.Read(scratch.AsSpan(0, 3))));
        int read = 0;
        var readData = new MemoryStream();
        while (read < kWriteSize)
        {
            int n = seq.Read(scratch.AsSpan(0, kWriteSize - read));
            Assert.IsTrue(n > 0);
            readData.Write(scratch, 0, n);
            read += n;
        }
        CollectionAssert.AreEqual(data, readData.ToArray());
    }

    [TestMethod]
    public void DetectsDivergingFileSystems()
    {
        MirrorFileSystem env = Env;
        env.CreateDirectoryIfMissing("/dir");
        Write(env, "/dir/f", "same");

        // A file only one side has.
        Write(_b, "/dir/extra", "x");
        Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => env.GetChildren("/dir"));
        Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => env.FileExists("/dir/extra"));
        Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => env.OpenRandomAccess("/dir/extra"));
        _b.DeleteFile("/dir/extra");

        // Same length, different contents.
        Write(_b, "/dir/f", "SAME");
        using (IRandomAccessFile r = env.OpenRandomAccess("/dir/f"))
        {
            Assert.AreEqual(4L, r.Length);
            Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => r.Read(0, new byte[4]));
        }
        using (ISequentialFile s = env.OpenSequential("/dir/f"))
        {
            Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => s.Read(new byte[4]));
        }

        // Different sizes.
        Write(_b, "/dir/f", "longer");
        Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => env.GetFileSize("/dir/f"));

        // One side failing: the error is reported as a mismatch, with the failure as inner exception.
        _b.SetFilesystemActive(false);
        var e = Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => env.CreateWritable("/dir/g"));
        Assert.IsInstanceOfType<RocksDbException>(e.InnerException);
        _b.SetFilesystemActive(true);
        Assert.IsTrue(_a.FileExists("/dir/g"));
    }

    /// <summary>A DB workload (writes, flushes, compactions, reopen, iteration) on disk, mirrored in memory.</summary>
    [TestMethod]
    public async Task DatabaseOverLocalMirroredWithMemory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-mirror-" + Guid.NewGuid().ToString("N"));
        var memory = new MemoryFileSystem();
        var env = new MirrorFileSystem(LocalFileSystem.Instance, memory);
        env.CreateDirectoryIfMissing(dir);
        try
        {
            var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, Level0FileNumCompactionTrigger = 2, TargetFileSizeBase = 64 * 1024 };
            var options = new DbOptions { CreateIfMissing = true, FileSystem = env };
            await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir, options, cfo))
            {
                for (int i = 0; i < 5000; i++) await db.PutAsync(U("key" + i.ToString("D5")), U("value-" + i + new string('x', 100)));
                await db.FlushAsync();
                for (int i = 0; i < 5000; i += 3) db.Delete(U("key" + i.ToString("D5")));
                await db.CompactRangeAsync();
                db.WaitForCompact();
            }
            await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir, new DbOptions { FileSystem = env }, cfo))
            {
                int n = 0;
                using Iterator it = db.NewIterator();
                for (it.SeekToFirst(); it.Valid; it.Next()) n++;
                Assert.AreEqual(5000 - 1667, n);
                Assert.AreEqual("value-1" + new string('x', 100), Encoding.UTF8.GetString(db.Get(U("key00001"))!));
            }
            // The two copies are identical file by file.
            List<string> names = env.GetChildren(dir).ToList();
            Assert.IsTrue(names.Exists(static n => n.EndsWith(".sst", StringComparison.Ordinal)));
            foreach (string name in names)
            {
                if (name == "LOG" || name.StartsWith("LOG.old", StringComparison.Ordinal)) continue; // timestamps differ per write
                string p = Path.Combine(dir, name);
                if (env.DirectoryExists(p)) continue;
                CollectionAssert.AreEqual(File.ReadAllBytes(p), ReadAll(memory, p), name);
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    private static byte[] ReadAll(IFileSystem fs, string path)
    {
        using IRandomAccessFile f = fs.OpenRandomAccess(path);
        byte[] b = new byte[f.Length];
        int n = f.Read(0, b);
        Assert.AreEqual(b.Length, n);
        return b;
    }
}
