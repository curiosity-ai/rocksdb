using System.Buffers.Binary;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.CompatTests.Audit15;

/// <summary>
/// Audit #15: the two Mosaik profiles (<see cref="MosaikProfile"/>, <see cref="NativeMosaikProfile"/>) describe the
/// same database. Each engine creates a database with its profile in both of Mosaik's resource branches, writes a key
/// to every family, and the other engine opens it with its own profile and reads every key back; the OPTIONS file each
/// one writes is accepted by the other.
/// </summary>
[TestClass]
public sealed class Audit15MosaikProfileCompatTests
{
    private static byte[] Key(MosaikProfile.Family f)
    {
        if (f.Shape == MosaikProfile.Shape.Tracker)
        {
            var k = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(k, 42);
            return k;
        }
        var key = new byte[Math.Max(16, f.PrefixLength + 8)];
        for (int i = 0; i < key.Length; i++) key[i] = (byte)(f.Name.Length + i);
        return key;
    }

    private static byte[] Value(MosaikProfile.Family f) => System.Text.Encoding.UTF8.GetBytes("value of " + f.Name);

    [TestMethod]
    [DataRow(true, 2)]
    [DataRow(false, 8)]
    public void ManagedProfileDatabaseOpensOnNativeProfile(bool lowMemoryMode, int processorCount)
    {
        using var dir = new TempDir("a15-profile-m");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "wal");

        var managed = new MosaikProfile(wal, lowMemoryMode, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        using (RocksDatabase m = managed.Open(db))
        {
            Dictionary<string, ColumnFamily> handles = MosaikProfile.Handles(m);
            foreach (MosaikProfile.Family f in MosaikProfile.Families) handles[f.Name].Put(Key(f), Value(f));
            foreach (MosaikProfile.Family f in MosaikProfile.Families) handles[f.Name].Flush();
        }

        var native = new NativeMosaikProfile(wal, lowMemoryMode, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        using (RocksDbSharp.RocksDb n = native.Open(db))
        {
            foreach (MosaikProfile.Family f in MosaikProfile.Families)
            {
                CollectionAssert.AreEqual(Value(f), n.Get(Key(f), n.GetColumnFamily(f.Name)), f.Name);
            }
        }
    }

    [TestMethod]
    [DataRow(true, 2)]
    [DataRow(false, 8)]
    public void NativeProfileDatabaseOpensOnManagedProfile(bool lowMemoryMode, int processorCount)
    {
        using var dir = new TempDir("a15-profile-n");
        string db = Path.Combine(dir.Path, "db"), wal = Path.Combine(dir.Path, "wal");

        var native = new NativeMosaikProfile(wal, lowMemoryMode, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        using (RocksDbSharp.RocksDb n = native.Open(db))
        {
            foreach (MosaikProfile.Family f in MosaikProfile.Families) n.Put(Key(f), Value(f), n.GetColumnFamily(f.Name));
        }

        // Unflushed on the native side: the managed open recovers every family from the WAL in the separate journal dir.
        var managed = new MosaikProfile(wal, lowMemoryMode, processorCount, cacheBytes: 16L << 20, writeBufferBytes: 32L << 20);
        using RocksDatabase m = managed.Open(db);
        Dictionary<string, ColumnFamily> handles = MosaikProfile.Handles(m);
        foreach (MosaikProfile.Family f in MosaikProfile.Families)
        {
            CollectionAssert.AreEqual(Value(f), handles[f.Name].Get(Key(f)), f.Name);
        }
    }
}
