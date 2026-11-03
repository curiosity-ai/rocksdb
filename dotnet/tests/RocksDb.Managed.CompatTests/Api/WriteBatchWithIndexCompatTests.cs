using System.Buffers;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// WriteBatchWithIndex against native rocksdb::WriteBatchWithIndex: the same operations give a byte-identical batch
/// rep (also after RollbackToSavePoint), and GetFromBatchAndDB / MultiGetFromBatchAndDB / NewIteratorWithBase return
/// the same results on the same DB contents (merges included).
/// </summary>
[TestClass]
public sealed class WriteBatchWithIndexCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";
        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }
    }

    /// <summary>One random operation applied to both batches.</summary>
    private readonly record struct Op(int Kind, bool OtherCf, string Key, string Value);

    private static List<Op> RandomOps(Random rnd, int n)
    {
        var ops = new List<Op>();
        for (int i = 0; i < n; i++)
        {
            ops.Add(new Op(rnd.Next(5), rnd.Next(3) == 0, "k" + rnd.Next(40).ToString("D2"), "v" + i));
        }
        return ops;
    }

    private static unsafe void ApplyNative(RocksDbSharp.WriteBatchWithIndex nb, Op op, RocksDbSharp.ColumnFamilyHandle? cf)
    {
        byte[] k = U(op.Key), v = U(op.Value);
        switch (op.Kind)
        {
            case 0: nb.Put(k, v, cf); break;
            case 1: nb.Delete(k, cf); break;
            case 2: nb.Merge(k, v, cf); break;
            case 3:
                if (cf is null) RocksDbSharp.Native.Instance.rocksdb_writebatch_wi_singledelete(nb.Handle, k, (UIntPtr)k.Length);
                else RocksDbSharp.Native.Instance.rocksdb_writebatch_wi_singledelete_cf(nb.Handle, cf.Handle, k, (UIntPtr)k.Length);
                break;
            default:
                nb.Put(k, v, cf);
                break;
        }
    }

    private static void ApplyManaged(WriteBatchWithIndex mb, Op op, ColumnFamily? cf)
    {
        byte[] k = U(op.Key), v = U(op.Value);
        switch (op.Kind)
        {
            case 0: mb.Put(cf, k, v); break;
            case 1: mb.Delete(cf, k); break;
            case 2: mb.Merge(cf, k, v); break;
            case 3: mb.SingleDelete(cf, k); break;
            default: mb.Put(cf, k, v); break;
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void BatchRepIsByteIdenticalAndReadsMatchNative(bool overwriteKey)
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        string ctx = "seed=" + seed + " overwrite=" + overwriteKey;
        using var dir = new TempDir("wbwi");
        List<Op> baseOps = RandomOps(rnd, 120);
        List<Op> batchOps = RandomOps(rnd, 150);
        var keys = new List<string>();
        for (int i = 0; i < 42; i++) keys.Add("k" + i.ToString("D2"));

        byte[] nativeRep, nativeRepAfterRollback;
        var nativeGets = new Dictionary<(bool, string), string?>();
        var nativeScan = new Dictionary<bool, List<string>>();
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), op))
        {
            RocksDbSharp.ColumnFamilyHandle one = db.GetColumnFamily("one");
            foreach (Op o in baseOps)
            {
                byte[] k = U(o.Key), v = U(o.Value);
                RocksDbSharp.ColumnFamilyHandle? cf = o.OtherCf ? one : null;
                if (o.Kind == 1) db.Remove(k, cf);
                else if (o.Kind == 2) db.Merge(k, v, cf);
                else db.Put(k, v, cf);
            }
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            using var nb = new RocksDbSharp.WriteBatchWithIndex(0, overwriteKey);
            foreach (Op o in batchOps.Take(100)) ApplyNative(nb, o, o.OtherCf ? one : db.GetDefaultColumnFamily());
            nb.SetSavePoint();
            foreach (Op o in batchOps.Skip(100)) ApplyNative(nb, o, o.OtherCf ? one : db.GetDefaultColumnFamily());
            nativeRep = nb.ToBytes();
            nb.RollbackToSavePoint();
            nativeRepAfterRollback = nb.ToBytes();
            // SingleDelete + Merge on the same key is undefined: compare reads only for keys without SingleDelete.
            foreach (bool other in new[] { false, true })
            {
                RocksDbSharp.ColumnFamilyHandle cf = other ? one : db.GetDefaultColumnFamily();
                foreach (string k in keys)
                {
                    byte[]? v = nb.Get(db, U(k), cf);
                    nativeGets[(other, k)] = v is null ? null : Encoding.UTF8.GetString(v);
                }
                var scan = new List<string>();
                using (RocksDbSharp.Iterator it = nb.NewIterator(db.NewIterator(cf), cf))
                {
                    for (it.SeekToFirst(); it.Valid(); it.Next()) scan.Add(Encoding.UTF8.GetString(it.Key()) + "=" + Encoding.UTF8.GetString(it.Value()));
                }
                nativeScan[other] = scan;
            }
        }

        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppend() };
        using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), [new("default", cfo), new("one", cfo), new("two", cfo)]);
        ColumnFamily mone = mdb.GetColumnFamily("one");
        using var mb = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwriteKey, 0);
        foreach (Op o in batchOps.Take(100)) ApplyManaged(mb, o, o.OtherCf ? mone : mdb.DefaultColumnFamily);
        mb.SetSavePoint();
        foreach (Op o in batchOps.Skip(100)) ApplyManaged(mb, o, o.OtherCf ? mone : mdb.DefaultColumnFamily);
        CollectionAssert.AreEqual(nativeRep, mb.Data.ToArray(), ctx);
        Assert.IsTrue(mb.RollbackToSavePoint());
        CollectionAssert.AreEqual(nativeRepAfterRollback, mb.Data.ToArray(), ctx);

        var singleDeleted = new HashSet<(bool, string)>();
        foreach (Op o in batchOps.Take(100)) if (o.Kind == 3) singleDeleted.Add((o.OtherCf, o.Key));
        foreach (Op o in baseOps) if (o.Kind == 3) singleDeleted.Add((o.OtherCf, o.Key));
        foreach (bool other in new[] { false, true })
        {
            ColumnFamily cf = other ? mone : mdb.DefaultColumnFamily;
            foreach (string k in keys)
            {
                if (singleDeleted.Contains((other, k))) continue;
                Assert.AreEqual(nativeGets[(other, k)], S(mb.GetFromBatchAndDB(mdb, null, cf, U(k))), ctx + " get " + other + " " + k);
            }
            // MultiGetFromBatchAndDB: one DB MultiGet for the keys the batch does not settle, same results.
            using (var multi = new MultiGetBatch())
            {
                foreach (string k in keys) multi.Add(U(k));
                mb.MultiGetFromBatchAndDB(mdb, null, cf, multi);
                for (int i = 0; i < keys.Count; i++)
                {
                    if (singleDeleted.Contains((other, keys[i]))) continue;
                    Assert.AreNotEqual(MultiGetStatus.Error, multi.GetStatus(i), ctx + " multiget error " + multi.GetError(i)?.Message);
                    string? got = multi.TryGetValue(i, out ReadOnlySpan<byte> mv) ? Encoding.UTF8.GetString(mv) : null;
                    Assert.AreEqual(nativeGets[(other, keys[i])], got, ctx + " multiget " + other + " " + keys[i]);
                }
            }
            var scan = new List<string>();
            using (BaseDeltaIterator it = mb.NewIteratorWithBase(cf, cf.NewIterator(), null))
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) scan.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            }
            List<string> expected = nativeScan[other].Where(e => !singleDeleted.Contains((other, e[..e.IndexOf('=')]))).ToList();
            CollectionAssert.AreEqual(expected, scan.Where(e => !singleDeleted.Contains((other, e[..e.IndexOf('=')]))).ToList(), ctx + " scan " + other);
        }
    }

    private static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);
}
