using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Merge;

/// <summary>
/// The built-in merge operators in a DB: port of utilities/merge_operators/string_append/stringappend_test.cc (run with
/// StringAppendOperator and, in place of upstream's TTL-DB variant, StringAppendTESTOperator), and a randomized model
/// test per operator covering merges in the memtable, in SST files and through compaction (PartialMergeMulti),
/// snapshots, deletes and reopen (cf. db/merge_test.cc).
/// </summary>
[TestClass]
public sealed class BuiltinMergeOperatorDbTests
{
    private static MergeOperator StringAppendFor(bool testOperator, string delim) =>
        testOperator ? new StringAppendTestOperator(delim) : new StringAppendOperator(delim);

    private static RocksDatabase OpenDb(MemoryFileSystem fs, MergeOperator op) =>
        RocksDatabase.Open(DbPath, NewOptions(fs), new ColumnFamilyOptions { MergeOperator = op });

    private static void Append(RocksDatabase db, string key, string val) => db.Merge(B(key), B(val));

    private static string Get(RocksDatabase db, string key) => GetString(db, key) ?? "";

    private static void CheckAppends(bool testOperator, string delim, string expected)
    {
        using RocksDatabase db = OpenDb(NewFs(), StringAppendFor(testOperator, delim));
        Append(db, "k1", "v1");
        Append(db, "k1", "v2");
        Append(db, "k1", "v3");
        Assert.AreEqual(expected, Get(db, "k1"));
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void IteratorTest(bool testOperator)
    {
        using RocksDatabase db = OpenDb(NewFs(), StringAppendFor(testOperator, ","));
        foreach (string v in new[] { "v1", "v2", "v3" }) Append(db, "k1", v);
        foreach (string v in new[] { "a1", "a2", "a3" }) Append(db, "k2", v);
        static List<string> Values(Iterator it)
        {
            var r = new List<string>();
            for (it.Seek(B("k1")); it.Valid; it.Next()) r.Add(S(it.Value.ToArray())!);
            return r;
        }
        using (Iterator it = db.NewIterator())
        {
            CollectionAssert.AreEqual(new[] { "v1,v2,v3", "a1,a2,a3" }, Values(it));
            Append(db, "k2", "a4");
            Append(db, "k1", "v4");
            // The iterator's implicit snapshot ignores a4 and v4.
            CollectionAssert.AreEqual(new[] { "v1,v2,v3", "a1,a2,a3" }, Values(it));
        }
        using (Iterator it = db.NewIterator())
        {
            CollectionAssert.AreEqual(new[] { "v1,v2,v3,v4", "a1,a2,a3,a4" }, Values(it));
            // Seek to k2 directly.
            it.Seek(B("k2"));
            Assert.AreEqual("a1,a2,a3,a4", S(it.Value.ToArray()));
        }
        Append(db, "k3", "g1");
        using (Iterator it = db.NewIterator())
        {
            it.Seek(B("k3"));
            Assert.AreEqual("g1", S(it.Value.ToArray()));
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void SimpleDelimiterTests(bool testOperator)
    {
        CheckAppends(testOperator, ",", "v1,v2,v3");      // SimpleTest
        CheckAppends(testOperator, "|", "v1|v2|v3");      // SimpleDelimiterTest
        CheckAppends(testOperator, "", "v1v2v3");         // EmptyDelimiterTest
        CheckAppends(testOperator, "<>", "v1<>v2<>v3");   // MultiCharDelimiterTest
        CheckAppends(testOperator, "\0", "v1\0v2\0v3");   // SimpleTestNullDelimiter

        // DelimiterIsDefensivelyCopiedTest: the operator copies the delimiter bytes.
        byte[] delimiter = "<>"u8.ToArray();
        MergeOperator op = testOperator ? new StringAppendTestOperator(delimiter) : new StringAppendOperator(delimiter);
        using (RocksDatabase db = OpenDb(NewFs(), op))
        {
            Append(db, "k1", "v1");
            Append(db, "k1", "v2");
            Array.Clear(delimiter);
            Append(db, "k1", "v3");
            Assert.AreEqual("v1<>v2<>v3", Get(db, "k1"));
        }

        // OneValueNoDelimiterTest
        using (RocksDatabase db = OpenDb(NewFs(), StringAppendFor(testOperator, "!")))
        {
            Append(db, "random_key", "single_val");
            Assert.AreEqual("single_val", Get(db, "random_key"));
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void VariousKeysAndPersistence(bool testOperator)
    {
        // VariousKeys + PersistentVariousKeys.
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = OpenDb(fs, StringAppendFor(testOperator, "\n")))
        {
            Append(db, "c", "asdasd");
            Append(db, "a", "x");
            Append(db, "b", "y");
            Append(db, "a", "t");
            Append(db, "a", "r");
            Append(db, "b", "2");
            Append(db, "c", "asdasd");
            Assert.AreEqual("x\nt\nr", Get(db, "a"));
            Assert.AreEqual("y\n2", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd", Get(db, "c"));
        }
        using (RocksDatabase db = OpenDb(fs, StringAppendFor(testOperator, "\n")))
        {
            Append(db, "c", "bbnagnagsx");
            Append(db, "a", "sa");
            Append(db, "b", "df");
            Append(db, "a", "gh");
            Append(db, "a", "jk");
            Append(db, "b", "l;");
            Append(db, "c", "rogosh");
            Assert.AreEqual("x\nt\nr\nsa\ngh\njk", Get(db, "a"));
            Assert.AreEqual("y\n2\ndf\nl;", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd\nbbnagnagsx\nrogosh", Get(db, "c"));
        }
        using (RocksDatabase db = OpenDb(fs, StringAppendFor(testOperator, "\n")))
        {
            Assert.AreEqual("x\nt\nr\nsa\ngh\njk", Get(db, "a"));
            Assert.AreEqual("y\n2\ndf\nl;", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd\nbbnagnagsx\nrogosh", Get(db, "c"));
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PersistentFlushAndCompaction(bool testOperator)
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = OpenDb(fs, StringAppendFor(testOperator, "\n")))
        {
            Append(db, "c", "asdasd");
            db.Flush();
            Assert.AreEqual("asdasd", Get(db, "c"));
            Append(db, "a", "x");
            Append(db, "b", "y");
            db.Flush();
            Append(db, "a", "t");
            Append(db, "a", "r");
            Append(db, "b", "2");
            Assert.AreEqual("x\nt\nr", Get(db, "a"));
            Assert.AreEqual("y\n2", Get(db, "b"));
            Append(db, "c", "asdasd");
            Append(db, "b", "monkey");
            Assert.AreEqual("x\nt\nr", Get(db, "a"));
            Assert.AreEqual("y\n2\nmonkey", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd", Get(db, "c"));
        }
        using (RocksDatabase db = OpenDb(fs, StringAppendFor(testOperator, "\n")))
        {
            Assert.AreEqual("x\nt\nr", Get(db, "a"));
            Append(db, "c", "bbnagnagsx");
            Append(db, "a", "sa");
            Append(db, "b", "df");
            db.CompactRange();
            Assert.AreEqual("x\nt\nr\nsa", Get(db, "a"));
            Assert.AreEqual("y\n2\nmonkey\ndf", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd\nbbnagnagsx", Get(db, "c"));
            Append(db, "a", "gh");
            Append(db, "a", "jk");
            Append(db, "b", "l;");
            Append(db, "c", "rogosh");
            db.CompactRange();
            Assert.AreEqual("x\nt\nr\nsa\ngh\njk", Get(db, "a"));
            Assert.AreEqual("y\n2\nmonkey\ndf\nl;", Get(db, "b"));
            Assert.AreEqual("asdasd\nasdasd\nbbnagnagsx\nrogosh", Get(db, "c"));
            Append(db, "b", "afcg");
            db.Flush();
            db.CompactRange();
            Assert.AreEqual("y\n2\nmonkey\ndf\nl;\nafcg", Get(db, "b"));
        }
    }

    [TestMethod]
    [DataRow(false, 1337, 30)]   // RandomMixGetAppend
    [DataRow(true, 1337, 30)]
    [DataRow(false, 9138204, 1000)] // BIGRandomMixGetAppend
    [DataRow(true, 9138204, 1000)]
    public void RandomMixGetAppend(bool testOperator, int seed, int queries)
    {
        string[] words = ["sdasd", "triejf", "fnjsdfn", "dfjisdfsf", "342839", "dsuha", "mabuais", "sadajsid", "jf9834hf", "2d9j89", "dj9823jd", "a", "dk02ed2dh", "$(jd4h984$(*", "mabz"];
        string[] keys = ["dhaiusdhu", "denidw", "daisda", "keykey", "muki", "shzassdianmd"];
        var parallel = new Dictionary<string, string>();
        using RocksDatabase db = OpenDb(NewFs(), StringAppendFor(testOperator, " "));
        var rnd = new Random(seed);
        for (int q = 0; q < queries; q++)
        {
            bool append = rnd.Next(2) == 0;
            string key = keys[rnd.Next(keys.Length)];
            string word = words[rnd.Next(words.Length)];
            if (append)
            {
                Append(db, key, word);
                parallel[key] = parallel.TryGetValue(key, out string? cur) && cur.Length > 0 ? cur + " " + word : word;
            }
            else
            {
                Assert.AreEqual(parallel.GetValueOrDefault(key, ""), Get(db, key));
            }
        }
    }

    // ---- randomized model test of every built-in ----------------------------------------------------------------------

    private static byte[] RandomOperand(MergeOperator op, Random rnd)
    {
        switch (op)
        {
            case SortListOperator:
            {
                int n = rnd.Next(1, 5);
                var sorted = new int[n];
                for (int i = 0; i < n; i++) sorted[i] = rnd.Next(-1000, 1000);
                Array.Sort(sorted);
                return B(string.Join(",", sorted));
            }
            case UInt64AddOperator:
            {
                byte[] b = new byte[8];
                Managed.Util.Coding.EncodeFixed64(b, (ulong)rnd.Next(0, 1000));
                return b;
            }
            default:
            {
                byte[] b = new byte[rnd.Next(0, 12)];
                for (int i = 0; i < b.Length; i++) b[i] = (byte)rnd.Next('a', 'z' + 1);
                return b;
            }
        }
    }

    /// <summary>Expected value of a key: FullMerge over its operands since the last Put / Delete.</summary>
    private sealed class KeyModel
    {
        public byte[]? Base;
        public bool Exists;
        public readonly List<byte[]> Operands = new();

        public KeyModel Clone()
        {
            var c = new KeyModel { Base = Base, Exists = Exists };
            c.Operands.AddRange(Operands);
            return c;
        }

        public string? Expected(MergeOperator op)
        {
            if (!Exists) return null;
            if (Operands.Count == 0) return Convert.ToHexString(Base!);
            using var buf = new MergeOperandBuffer();
            foreach (byte[] o in Operands) buf.PushNewer(o);
            var w = new ArrayBufferWriter<byte>();
            MergeHelper.TimedFullMerge(op, "k"u8, Base is not null, Base, buf.AsOperands(), w);
            return Convert.ToHexString(w.WrittenSpan);
        }
    }

    public static IEnumerable<object[]> AllOperators()
    {
        yield return ["max"];
        yield return ["put"];
        yield return ["put_v1"];
        yield return ["bytesxor"];
        yield return ["stringappend"];
        yield return ["stringappendtest"];
        yield return ["uint64add"];
    }

    [TestMethod]
    [DynamicData(nameof(AllOperators))]
    public void RandomizedAgainstModel(string id)
    {
        MergeOperator op = MergeOperator.CreateBuiltin(id)!;
        int seed = Environment.TickCount;
        string trace = "operator=" + id + " seed=" + seed;
        var rnd = new Random(seed);
        MemoryFileSystem fs = NewFs();
        const int Keys = 12;
        var model = new KeyModel[Keys];
        for (int k = 0; k < Keys; k++) model[k] = new KeyModel();
        var snapshots = new List<(Snapshot Snap, string?[] Expected)>();
        RocksDatabase db = OpenDb(fs, op);
        try
        {
            for (int step = 0; step < 1500; step++)
            {
                int k = rnd.Next(Keys);
                byte[] key = Key(k);
                int action = rnd.Next(100);
                if (action < 70)
                {
                    byte[] operand = RandomOperand(op, rnd);
                    db.Merge(key, operand);
                    model[k].Exists = true;
                    model[k].Operands.Add(operand);
                }
                else if (action < 78)
                {
                    byte[] v = RandomOperand(op, rnd);
                    db.Put(key, v);
                    model[k].Base = v;
                    model[k].Exists = true;
                    model[k].Operands.Clear();
                }
                else if (action < 82)
                {
                    db.Delete(key);
                    model[k] = new KeyModel();
                }
                else if (action < 86) db.Flush();
                else if (action < 88) db.CompactRange();
                else if (action < 90 && snapshots.Count < 4)
                {
                    var exp = new string?[Keys];
                    for (int i = 0; i < Keys; i++) exp[i] = model[i].Expected(op);
                    snapshots.Add((db.GetSnapshot(), exp));
                }
                else if (action < 91 && snapshots.Count > 0)
                {
                    snapshots[0].Snap.Dispose();
                    snapshots.RemoveAt(0);
                }
                else if (action < 92)
                {
                    foreach ((Snapshot s, _) in snapshots) s.Dispose();
                    snapshots.Clear();
                    db.Dispose();
                    db = OpenDb(fs, op);
                }
                else
                {
                    Assert.AreEqual(model[k].Expected(op), Hex(db.Get(key)), trace + " step=" + step);
                }
            }
            for (int i = 0; i < Keys; i++) Assert.AreEqual(model[i].Expected(op), Hex(db.Get(Key(i))), trace + " key=" + i);
            foreach ((Snapshot s, string?[] exp) in snapshots)
            {
                for (int i = 0; i < Keys; i++) Assert.AreEqual(exp[i], Hex(db.Get(new ReadOptions { Snapshot = s }, Key(i))), trace + " snapshot key=" + i);
            }
            db.Flush();
            db.CompactRange();
            for (int i = 0; i < Keys; i++) Assert.AreEqual(model[i].Expected(op), Hex(db.Get(Key(i))), trace + " compacted key=" + i);
            foreach ((Snapshot s, string?[] exp) in snapshots)
            {
                for (int i = 0; i < Keys; i++) Assert.AreEqual(exp[i], Hex(db.Get(new ReadOptions { Snapshot = s }, Key(i))), trace + " compacted snapshot key=" + i);
            }
            // Iteration merges the same way.
            using Iterator it = db.NewIterator();
            int seen = 0;
            for (it.SeekToFirst(); it.Valid; it.Next(), seen++)
            {
                int i = int.Parse(Encoding.UTF8.GetString(it.Key[3..]), System.Globalization.CultureInfo.InvariantCulture);
                Assert.AreEqual(model[i].Expected(op), Convert.ToHexString(it.Value), trace + " iterator key=" + i);
            }
            int exists = 0;
            foreach (KeyModel m in model) if (m.Exists) exists++;
            Assert.AreEqual(exists, seen, trace);
        }
        finally
        {
            foreach ((Snapshot s, _) in snapshots) s.Dispose();
            db.Dispose();
        }
    }

    private static string? Hex(byte[]? b) => b is null ? null : Convert.ToHexString(b);

    /// <summary>
    /// Compaction with a snapshot between operands: the older stripe reaches the start of the key's history and is
    /// fully merged into a value; the newer stripe cannot see it and is combined by PartialMergeMulti where the operator
    /// supports it (Max, Put, BytesXOR, StringAppend, UInt64Add) and kept as is otherwise (StringAppendTESTOperator,
    /// SortList, tested separately below). Reads at the snapshot and at the latest state are unchanged by the compaction.
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(AllOperators))]
    public void CompactionPartialMergeAboveSnapshot(string id)
    {
        MergeOperator op = MergeOperator.CreateBuiltin(id)!;
        bool partial = op is not StringAppendTestOperator;
        var rnd = new Random(Environment.TickCount);
        using RocksDatabase db = OpenDb(NewFs(), op);
        var older = new KeyModel { Exists = true };
        for (int i = 0; i < 2; i++)
        {
            byte[] o = RandomOperand(op, rnd);
            db.Merge(B("k"), o);
            older.Operands.Add(o);
        }
        KeyModel newer = older.Clone();
        using Snapshot snap = db.GetSnapshot();
        for (int i = 0; i < 3; i++)
        {
            byte[] o = RandomOperand(op, rnd);
            db.Merge(B("k"), o);
            newer.Operands.Add(o);
            db.Flush();
        }
        Assert.AreEqual(older.Expected(op), Hex(db.Get(new ReadOptions { Snapshot = snap }, B("k"))));
        Assert.AreEqual(newer.Expected(op), Hex(db.Get(B("k"))));
        db.CompactRange();
        using var r = new MergeOperandsResult();
        Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 100 }, r));
        Assert.AreEqual(partial ? 2 : 4, r.Count, id); // the merged base value + the newer stripe's operand(s)
        Assert.AreEqual(older.Expected(op), Convert.ToHexString(r[0]), id);
        Assert.AreEqual(older.Expected(op), Hex(db.Get(new ReadOptions { Snapshot = snap }, B("k"))));
        Assert.AreEqual(newer.Expected(op), Hex(db.Get(B("k"))));
    }

    /// <summary>
    /// SortList in a DB, with upstream's semantics: operands in the memtable and in SST files are merged into one sorted
    /// list; partial merges are declined (the operands survive compactions above a snapshot); and because
    /// SortList::FullMergeV2 ignores the existing value, a value produced by a full merge (here: a bottommost
    /// compaction) or written by Put does not take part in later merges.
    /// </summary>
    [TestMethod]
    public void SortListInDb()
    {
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = OpenDb(fs, SortListOperator.Instance))
        {
            Append(db, "k", "5,9");
            db.Flush();
            Append(db, "k", "1,7");
            Append(db, "k", "3");
            Assert.AreEqual("1,3,5,7,9", Get(db, "k"));
            using (Snapshot snap = db.GetSnapshot())
            {
                Append(db, "k", "2,4");
                Append(db, "k", "8");
                db.Flush();
                db.CompactRange();
                using var r = new MergeOperandsResult();
                Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 100 }, r));
                Assert.AreEqual(3, r.Count); // the older stripe's value + both newer operands
                Assert.AreEqual("1,3,5,7,9", S(r[0].ToArray()));
                Assert.AreEqual("1,3,5,7,9", GetString(db, "k", snap));
                Assert.AreEqual("2,4,8", Get(db, "k")); // the existing value is ignored
            }
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            Append(db, "k", "6");
            Assert.AreEqual("6", Get(db, "k"));
            db.Put(B("p"), B("1,2"));
            Append(db, "p", "3");
            Assert.AreEqual("3", Get(db, "p"));
        }
        using (RocksDatabase db = OpenDb(fs, SortListOperator.Instance)) Assert.AreEqual("6", Get(db, "k"));

        // A malformed operand fails the read with Corruption (std::stoi throws upstream).
        using (RocksDatabase db = OpenDb(NewFs(), SortListOperator.Instance))
        {
            Append(db, "bad", "1,,2");
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("bad")));
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
    }
}
