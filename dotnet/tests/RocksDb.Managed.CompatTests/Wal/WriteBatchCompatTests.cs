using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>WriteBatch rep compatibility between the managed port and native RocksDB.</summary>
[TestClass]
public sealed class WriteBatchCompatTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static unsafe void NativeSingleDelete(RocksDbSharp.WriteBatch b, ReadOnlySpan<byte> key, RocksDbSharp.ColumnFamilyHandle? cf)
    {
        fixed (byte* k = key)
        {
            if (cf is null) RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete(b.Handle, k, (UIntPtr)key.Length);
            else RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete_cf(b.Handle, cf.Handle, k, (UIntPtr)key.Length);
        }
    }

    private static void NativeDeleteRange(RocksDbSharp.WriteBatch b, byte[] begin, byte[] end, RocksDbSharp.ColumnFamilyHandle? cf) =>
        b.DeleteRange(begin, (ulong)begin.Length, end, (ulong)end.Length, cf);

    [TestMethod]
    public void ManagedRepEqualsNativeForEveryOp()
    {
        using var dir = new TempDir("wb-bytes");
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), merge);
        RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
        RocksDbSharp.ColumnFamilyHandle two = db.GetColumnFamily("two");
        uint oneId = CfId(one), twoId = CfId(two);
        Assert.AreNotEqual(0u, oneId);

        var rnd = new Random(42);
        byte[] bigKey = new byte[300];
        byte[] bigValue = new byte[100_000]; // multi-byte varints
        rnd.NextBytes(bigKey);
        rnd.NextBytes(bigValue);

        using var native = new RocksDbSharp.WriteBatch();
        using var managed = new WriteBatch();

        native.Put("k1"u8, "v1"u8); managed.Put("k1"u8, "v1"u8);
        native.Put(B(""), B("")); managed.Put([], []);
        native.Put(bigKey, bigValue); managed.Put(bigKey, bigValue);
        native.Delete("k2"u8); managed.Delete("k2"u8);
        NativeSingleDelete(native, "k3"u8, null); managed.SingleDelete("k3"u8);
        NativeDeleteRange(native, B("a"), B("b"), null); managed.DeleteRange("a"u8, "b"u8);
        native.Merge("m"u8, "x"u8); managed.Merge("m"u8, "x"u8);
        native.PutLogData(B("blob"), 4); managed.PutLogData("blob"u8);
        native.Put("c1"u8, "cv1"u8, one); managed.Put(oneId, "c1"u8, "cv1"u8);
        native.Put(bigKey, bigValue, two); managed.Put(twoId, bigKey, bigValue);
        native.Delete("c2"u8, one); managed.Delete(oneId, "c2"u8);
        NativeSingleDelete(native, "c3"u8, two); managed.SingleDelete(twoId, "c3"u8);
        NativeDeleteRange(native, B("x"), B("y"), one); managed.DeleteRange(oneId, "x"u8, "y"u8);
        native.Merge("cm"u8, "y"u8, two); managed.Merge(twoId, "cm"u8, "y"u8);

        Assert.AreEqual(native.Count(), managed.Count);
        CollectionAssert.AreEqual(native.ToBytes(), managed.Data.ToArray());

        // Save points behave identically.
        native.SetSavePoint(); managed.SetSavePoint();
        native.Put("sp"u8, "1"u8); managed.Put("sp"u8, "1"u8);
        native.RollbackToSavePoint(); Assert.IsTrue(managed.RollbackToSavePoint());
        CollectionAssert.AreEqual(native.ToBytes(), managed.Data.ToArray());

        // Native parses the managed rep (rocksdb_writebatch_create_from) and the managed reader parses the native rep.
        using var fromManaged = new RocksDbSharp.WriteBatch(managed.Data.ToArray());
        Assert.AreEqual(managed.Count, fromManaged.Count());
        CollectionAssert.AreEqual(managed.Data.ToArray(), fromManaged.ToBytes());
        Assert.AreEqual(Describe(managed.Data), Describe(native.ToBytes()));

        native.Clear(); managed.Clear();
        CollectionAssert.AreEqual(native.ToBytes(), managed.Data.ToArray());
    }

    [TestMethod]
    public void NativeAppliesManagedBatch()
    {
        using var dir = new TempDir("wb-apply");
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), merge);
        RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
        uint oneId = CfId(one);
        db.Put("gone"u8, "x"u8);
        db.Put("r1"u8, "x"u8);
        db.Put("r2"u8, "x"u8);
        db.Put("single"u8, "x"u8);

        using var managed = new WriteBatch();
        managed.Put("a"u8, "1"u8).Put(oneId, "a"u8, "one"u8).Delete("gone"u8).DeleteRange("r"u8, "s"u8).SingleDelete("single"u8)
            .Merge("m"u8, "p"u8).Merge("m"u8, "q"u8).Merge(oneId, "m"u8, "z"u8).PutLogData("ignored"u8);
        using (var nb = new RocksDbSharp.WriteBatch(managed.Data.ToArray()))
        {
            db.Write(nb);
        }

        Assert.AreEqual("1", db.Get("a"));
        Assert.AreEqual("one", db.Get("a", one));
        Assert.IsNull(db.Get("gone"));
        Assert.IsNull(db.Get("r1"));
        Assert.IsNull(db.Get("r2"));
        Assert.IsNull(db.Get("single"));
        Assert.AreEqual("p,q", db.Get("m"));
        Assert.AreEqual("z", db.Get("m", one));
    }

    [TestMethod]
    public void ParseNativeBatchesFromGetUpdatesSince()
    {
        using var dir = new TempDir("wb-updates");
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), merge);
        RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
        RocksDbSharp.ColumnFamilyHandle two = db.GetColumnFamily("two");
        uint oneId = CfId(one), twoId = CfId(two);

        db.Put("p1"u8, "v1"u8);                                     // seq 1
        db.Remove("p1");                                            // seq 2
        db.Merge("m"u8, "a"u8);                                     // seq 3
        using (var b = new RocksDbSharp.WriteBatch())               // seq 4..8
        {
            b.Put("k"u8, "v"u8, one);
            NativeDeleteRange(b, B("a"), B("c"), two);
            b.Merge("m"u8, "b"u8);
            b.PutLogData(B("meta"), 4);
            NativeSingleDelete(b, "s"u8, null);
            b.Delete("d"u8, two);
            db.Write(b);
        }

        var expected = new[]
        {
            "Put(0,p1,v1)@1",
            "Delete(0,p1)@2",
            "Merge(0,m,a)@3",
            $"Put({oneId},k,v)@4 DeleteRange({twoId},a,c)@5 Merge(0,m,b)@6 LogData(meta) SingleDelete(0,s)@7 Delete({twoId},d)@8",
        };
        var actual = new List<string>();
        using (RocksDbSharp.TransactionLogIterator it = db.GetUpdatesSince(0))
        {
            for (; it.Valid(); it.Next())
            {
                using RocksDbSharp.WriteBatch nb = it.GetBatch(out ulong seq);
                byte[] rep = nb.ToBytes();
                var reader = new WriteBatchReader(rep);
                Assert.AreEqual(seq, reader.Sequence);
                actual.Add(Describe(rep));

                // Handler-style iteration validates the count too.
                var counter = new CountingHandler();
                WriteBatch.Iterate(rep, ref counter);
                Assert.AreEqual(nb.Count(), counter.Count);
            }
        }
        CollectionAssert.AreEqual(expected, actual);
    }

    private struct CountingHandler : IWriteBatchHandler
    {
        public int Count;
        public bool Continue() => true;
        public void Put(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Count++;
        public void TimedPut(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v, ulong t) => Count++;
        public void PutEntity(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> e) => Count++;
        public void Delete(uint cf, ReadOnlySpan<byte> k) => Count++;
        public void SingleDelete(uint cf, ReadOnlySpan<byte> k) => Count++;
        public void DeleteRange(uint cf, ReadOnlySpan<byte> b, ReadOnlySpan<byte> e) => Count++;
        public void Merge(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Count++;
        public void PutBlobIndex(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Count++;
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> ts) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }
}
