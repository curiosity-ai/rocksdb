using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, transactions slice: differential tests of WriteBatchWithIndex / BaseDeltaIterator against native
/// librocksdb (utilities/write_batch_with_index/write_batch_with_index_internal.cc).
/// </summary>
[TestClass]
public sealed class Audit4TransactionsCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private readonly record struct Op(int Kind, string Key, string Value);

    /// <summary>One positioning call: 0 SeekToFirst, 1 SeekToLast, 2 Seek, 3 SeekForPrev, 4 Next, 5 Prev.</summary>
    private readonly record struct Step(int Kind, string Target);

    private sealed record Session(string? Lower, string? Upper, List<Step> Steps);

    private static List<Op> RandomOps(Random rnd, int n, int keys)
    {
        var ops = new List<Op>();
        // 0 Put, 1 Delete, 2 Merge (SingleDelete + Merge on one key is undefined upstream, so it is left out).
        for (int i = 0; i < n; i++) ops.Add(new Op(rnd.Next(3), "k" + rnd.Next(keys).ToString("D2"), "v" + i));
        return ops;
    }

    private static string RandomTarget(Random rnd, int keys)
    {
        string t = "k" + rnd.Next(-1, keys + 1).ToString("D2");
        return rnd.Next(4) == 0 ? t + "x" : t; // sometimes between two keys
    }

    private static string Describe(bool valid, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        valid ? Encoding.UTF8.GetString(key) + "=" + Encoding.UTF8.GetString(value) : "-";

    private static unsafe string NativeState(IntPtr it)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        if (n.rocksdb_iter_valid(it) == 0) return "-";
        IntPtr k = n.rocksdb_iter_key(it, out UIntPtr klen);
        IntPtr v = n.rocksdb_iter_value(it, out UIntPtr vlen);
        return Describe(true, new ReadOnlySpan<byte>((void*)k, (int)klen), new ReadOnlySpan<byte>((void*)v, (int)vlen));
    }

    /// <summary>
    /// BaseDeltaIterator (Seek / SeekForPrev / SeekToFirst / SeekToLast / Next / Prev with direction changes; AdvanceDelta,
    /// AdvanceBase, UpdateCurrent) and WBWIIteratorImpl with iterate bounds (rocksdb_writebatch_wi_create_iterator_with_base_cf_readopts):
    /// random walks over a random batch on a random DB give the same position, key and value as native after every step.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void BaseDeltaIteratorRandomWalkMatchesNative(bool overwriteKey)
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        string ctx = "seed=" + seed + " overwrite=" + overwriteKey;
        const int Keys = 30;
        using var dir = new TempDir("audit4-wbwi");
        List<Op> flushedOps = RandomOps(rnd, 60, Keys);
        List<Op> memOps = RandomOps(rnd, 30, Keys);
        List<Op> batchOps = RandomOps(rnd, 70, Keys);

        var sessions = new List<Session>();
        for (int s = 0; s < 24; s++)
        {
            string? lower = null, upper = null;
            if (s >= 6)
            {
                int a = rnd.Next(Keys), b = rnd.Next(Keys);
                if (a > b) (a, b) = (b, a);
                if (rnd.Next(3) != 0) lower = "k" + a.ToString("D2");
                if (rnd.Next(3) != 0) upper = "k" + (b + 1).ToString("D2");
            }
            var steps = new List<Step>();
            for (int i = 0; i < 60; i++) steps.Add(new Step(rnd.Next(6), RandomTarget(rnd, Keys)));
            sessions.Add(new Session(lower, upper, steps));
        }

        var expected = new List<List<string>>();
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = Open(dir.Path, new RocksDbSharp.DbOptions(), op))
        {
            RocksDbSharp.ColumnFamilyHandle cf = db.GetDefaultColumnFamily();
            void Apply(Op o)
            {
                byte[] k = U(o.Key), v = U(o.Value);
                if (o.Kind == 1) db.Remove(k, cf);
                else if (o.Kind == 2) db.Merge(k, v, cf);
                else db.Put(k, v, cf);
            }
            foreach (Op o in flushedOps) Apply(o);
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            foreach (Op o in memOps) Apply(o);
            using var nb = new RocksDbSharp.WriteBatchWithIndex(0, overwriteKey);
            foreach (Op o in batchOps)
            {
                byte[] k = U(o.Key), v = U(o.Value);
                if (o.Kind == 1) nb.Delete(k, cf);
                else if (o.Kind == 2) nb.Merge(k, v, cf);
                else nb.Put(k, v, cf);
            }
            RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
            foreach (Session s in sessions)
            {
                var ro = new RocksDbSharp.ReadOptions();
                if (s.Lower is not null) ro.SetIterateLowerBound(U(s.Lower));
                if (s.Upper is not null) ro.SetIterateUpperBound(U(s.Upper));
                RocksDbSharp.Iterator baseIt = db.NewIterator(cf, ro);
                IntPtr it = n.rocksdb_writebatch_wi_create_iterator_with_base_cf_readopts(nb.Handle, baseIt.Detach(), cf.Handle, ro.Handle);
                var states = new List<string>();
                bool valid = false;
                try
                {
                    foreach (Step st in s.Steps)
                    {
                        byte[] t = U(st.Target);
                        switch (st.Kind)
                        {
                            case 0: n.rocksdb_iter_seek_to_first(it); break;
                            case 1: n.rocksdb_iter_seek_to_last(it); break;
                            case 2: n.rocksdb_iter_seek(it, t, (UIntPtr)t.Length); break;
                            case 3: n.rocksdb_iter_seek_for_prev(it, t, (UIntPtr)t.Length); break;
                            case 4:
                                if (valid) n.rocksdb_iter_next(it);
                                else n.rocksdb_iter_seek(it, t, (UIntPtr)t.Length);
                                break;
                            default:
                                if (valid) n.rocksdb_iter_prev(it);
                                else n.rocksdb_iter_seek_for_prev(it, t, (UIntPtr)t.Length);
                                break;
                        }
                        string state = NativeState(it);
                        valid = state != "-";
                        states.Add(state);
                    }
                    n.rocksdb_iter_get_error(it, out IntPtr err);
                    Assert.AreEqual(IntPtr.Zero, err, ctx + " native iterator error " + (err == IntPtr.Zero ? "" : Marshal.PtrToStringAnsi(err)));
                }
                finally
                {
                    n.rocksdb_iter_destroy(it);
                    GC.KeepAlive(ro);
                }
                expected.Add(states);
            }
        }

        var cfo = new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") };
        using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions(), [new("default", cfo), new("one", cfo), new("two", cfo)]);
        ColumnFamily mcf = mdb.DefaultColumnFamily;
        using var mb = new WriteBatchWithIndex(Comparator.Bytewise, 0, overwriteKey, 0);
        foreach (Op o in batchOps)
        {
            byte[] k = U(o.Key), v = U(o.Value);
            if (o.Kind == 1) mb.Delete(mcf, k);
            else if (o.Kind == 2) mb.Merge(mcf, k, v);
            else mb.Put(mcf, k, v);
        }
        for (int si = 0; si < sessions.Count; si++)
        {
            Session s = sessions[si];
            var ro = new ReadOptions();
            if (s.Lower is not null) ro.IterateLowerBound = U(s.Lower);
            if (s.Upper is not null) ro.IterateUpperBound = U(s.Upper);
            using BaseDeltaIterator it = mb.NewIteratorWithBase(mcf, mcf.NewIterator(ro), ro);
            bool valid = false;
            var trace = new StringBuilder();
            for (int i = 0; i < s.Steps.Count; i++)
            {
                Step st = s.Steps[i];
                byte[] t = U(st.Target);
                switch (st.Kind)
                {
                    case 0: it.SeekToFirst(); trace.Append("SeekToFirst "); break;
                    case 1: it.SeekToLast(); trace.Append("SeekToLast "); break;
                    case 2: it.Seek(t); trace.Append("Seek(" + st.Target + ") "); break;
                    case 3: it.SeekForPrev(t); trace.Append("SeekForPrev(" + st.Target + ") "); break;
                    case 4:
                        if (valid) { it.Next(); trace.Append("Next "); }
                        else { it.Seek(t); trace.Append("Seek(" + st.Target + ") "); }
                        break;
                    default:
                        if (valid) { it.Prev(); trace.Append("Prev "); }
                        else { it.SeekForPrev(t); trace.Append("SeekForPrev(" + st.Target + ") "); }
                        break;
                }
                valid = it.Valid;
                string state = valid ? Describe(true, it.Key, it.Value) : "-";
                Assert.AreEqual(expected[si][i], state,
                    ctx + " session " + si + " bounds [" + s.Lower + "," + s.Upper + ") step " + i + ": " + trace);
            }
        }
    }
}
