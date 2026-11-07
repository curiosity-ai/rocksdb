using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// DBWithTTL merges and column families (utilities/ttl/ttl_test.cc in spirit): TtlMergeOperator strips the write-time
/// suffix before the user operator and stamps results; per-column-family TTLs, CreateColumnFamilyWithTtl, SetTtl/GetTtl.
/// </summary>
[TestClass]
public sealed class TtlDatabaseTests
{
    private static ColumnFamilyOptions Cf(MergeOperator? op) => new() { MergeOperator = op, WriteBufferSize = 1 << 20 };

    private static byte[] U64(ulong v)
    {
        byte[] b = new byte[8];
        Coding.EncodeFixed64(b, v);
        return b;
    }

    /// <summary>The raw stored value (with its suffix) and its write time.</summary>
    private static int RawTimestamp(TtlDatabase db, ColumnFamily cf, byte[] key)
    {
        byte[]? raw = cf.Get(key);
        Assert.IsNotNull(raw);
        return TtlValue.GetTimestamp(raw);
    }

    [TestMethod]
    public void StringAppendMergesAcrossFlushAndCompaction()
    {
        MemoryFileSystem fs = NewFs();
        long t0 = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long clock = t0;
        using TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), Cf(MergeOperator.StringAppend(",")), 1000, () => Interlocked.Read(ref clock));
        Assert.IsInstanceOfType<TtlMergeOperator>(db.Database.DefaultColumnFamily.Data.MergeOperator);
        db.Put(B("k"), B("a"));
        db.Merge(B("k"), B("b"));
        db.Merge(B("m"), B("x"));        // no base value
        CollectionAssert.AreEqual(B("a,b"), db.Get(B("k")));
        db.Flush();
        db.Merge(B("k"), B("c"));        // memtable operand over an SST chain
        db.Merge(B("m"), B("y"));
        CollectionAssert.AreEqual(B("a,b,c"), db.Get(B("k")));
        CollectionAssert.AreEqual(B("x,y"), db.Get(B("m")));
        using (Iterator it = db.NewIterator())
        {
            it.Seek(B("k"));
            Assert.IsTrue(it.Valid);
            CollectionAssert.AreEqual(B("a,b,c"), it.Value.ToArray());
            it.Next();
            CollectionAssert.AreEqual(B("x,y"), it.Value.ToArray());
        }
        Interlocked.Exchange(ref clock, t0 + 7);
        db.Flush();
        db.CompactRange();
        CollectionAssert.AreEqual(B("a,b,c"), db.Get(B("k")));
        CollectionAssert.AreEqual(B("x,y"), db.Get(B("m")));
        // The compaction's merge result is a single value stamped with the time of the merge.
        Assert.AreEqual((int)(t0 + 7), RawTimestamp(db, db.Database.DefaultColumnFamily, B("k")));
        CollectionAssert.AreEqual(B("a,b,c"), db.Database.Get(B("k"))![..^TtlValue.TimestampLength]);
    }

    [TestMethod]
    public void UInt64AddMergesAcrossFlushAndCompaction()
    {
        MemoryFileSystem fs = NewFs();
        ulong expected = 0;
        using (TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), Cf(MergeOperator.UInt64Add), 1000))
        {
            for (int i = 1; i <= 50; i++)
            {
                db.Merge(B("sum"), U64((ulong)i));
                expected += (ulong)i;
                if (i % 20 == 0) db.Flush();
            }
            CollectionAssert.AreEqual(U64(expected), db.Get(B("sum")));
            db.CompactRange();
            CollectionAssert.AreEqual(U64(expected), db.Get(B("sum")));
            db.Merge(B("sum"), U64(5));
            expected += 5;
        }
        // Reopen: the WAL operand is merged over the compacted (stamped) value.
        using (TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), Cf(MergeOperator.UInt64Add), 1000))
        {
            CollectionAssert.AreEqual(U64(expected), db.Get(B("sum")));
            db.Flush();
            db.CompactRange();
            CollectionAssert.AreEqual(U64(expected), db.Get(B("sum")));
        }
    }

    [TestMethod]
    public void OperandOrValueWithoutTimestampIsCorruption()
    {
        MemoryFileSystem fs = NewFs();
        using TtlDatabase db = TtlDatabase.Open(DbPath, NewOptions(fs), Cf(MergeOperator.StringAppend(",")), 1000);
        // A raw operand shorter than the suffix (written around the TTL layer).
        db.Merge(B("k"), B("a"));
        db.Database.Merge(B("k"), B("xy"));
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("k")));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(StatusSubCode.MergeOperatorFailed, ex.SubCode);
        // A raw base value shorter than the suffix.
        db.Database.Put(B("v"), B("z"));
        db.Merge(B("v"), B("a"));
        ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("v")));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        Assert.AreEqual(StatusSubCode.MergeOperatorFailed, ex.SubCode);
    }

    [TestMethod]
    public void PartialMergeResultCarriesTimestamp()
    {
        long clock = 1_700_000_000;
        var op = new TtlMergeOperator(MergeOperator.UInt64Add, () => clock);
        using var data = new MergeOperandBuffer();
        byte[] a = new byte[12], b = new byte[12];
        Coding.EncodeFixed64(a, 3);
        Coding.EncodeFixed32(a.AsSpan(8), 1_600_000_000);
        Coding.EncodeFixed64(b, 4);
        Coding.EncodeFixed32(b.AsSpan(8), 1_600_000_001);
        data.PushNewer(a);
        data.PushNewer(b);
        var w = new ArrayBufferWriter<byte>();
        Assert.IsTrue(op.PartialMerge(B("k"), data.AsOperands(), w));
        Assert.AreEqual(12, w.WrittenCount);
        Assert.AreEqual(7UL, Coding.DecodeFixed64(w.WrittenSpan));
        Assert.AreEqual((int)clock, TtlValue.GetTimestamp(w.WrittenSpan));
        // An operand without a suffix fails the partial merge (operands are then kept as they are).
        data.PushNewer(B("ab"));
        w.Clear();
        Assert.IsFalse(op.PartialMerge(B("k"), data.AsOperands(), w));
        Assert.AreEqual("{id=TtlMergeOperator;user_operator=UInt64AddOperator;}", op.OptionsId);
    }

    [TestMethod]
    public void ColumnFamiliesHaveTheirOwnTtl()
    {
        MemoryFileSystem fs = NewFs();
        long t0 = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long clock = t0;
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs = [new("default", Cf(null)), new("short", Cf(MergeOperator.StringAppend(",")))];
        var ex = Assert.ThrowsExactly<RocksDbException>(() => TtlDatabase.Open(DbPath, o, cfs, [0]));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        using (TtlDatabase db = TtlDatabase.Open(DbPath, o, cfs, [0, 100], readOnly: false, () => Interlocked.Read(ref clock)))
        {
            TtlColumnFamily shortCf = db.GetColumnFamily("short");
            Assert.AreEqual(0, db.TtlSeconds);
            Assert.AreEqual(100, db.GetTtl(shortCf));
            TtlColumnFamily longCf = db.CreateColumnFamilyWithTtl("long", Cf(null), 10_000);
            Assert.AreEqual(10_000, longCf.TtlSeconds);
            db.Put(B("k"), B("forever"));
            shortCf.Put(B("k"), B("s"));
            shortCf.Merge(B("k"), B("t"));
            longCf.Put(B("k"), B("l"));
            CollectionAssert.AreEqual(B("s,t"), shortCf.Get(B("k")));
            using (Iterator it = shortCf.NewIterator())
            {
                it.SeekToFirst();
                CollectionAssert.AreEqual(B("s,t"), it.Value.ToArray());
            }
            // A compaction merges "s" and "t" into one value stamped now (a merge result gets the time of the merge).
            foreach (TtlColumnFamily c in new[] { db.DefaultColumnFamily, shortCf, longCf })
            {
                c.Flush();
                c.CompactRange();
            }
            Assert.AreEqual((int)t0, RawTimestamp(db, shortCf.ColumnFamily, B("k")));
            // Past the short TTL: only the short column family loses its value.
            Interlocked.Exchange(ref clock, t0 + 1000);
            foreach (TtlColumnFamily c in new[] { db.DefaultColumnFamily, shortCf, longCf }) c.CompactRange();
            CollectionAssert.AreEqual(B("forever"), db.Get(B("k")));
            Assert.IsNull(shortCf.Get(B("k")));
            CollectionAssert.AreEqual(B("l"), longCf.Get(B("k")));
            // SetTtl: later compactions of the long column family use the new TTL.
            db.SetTtl(longCf, 500);
            Assert.AreEqual(500, longCf.TtlSeconds);
            longCf.CompactRange();
            Assert.IsNull(longCf.Get(B("k")));
        }
        // Reopen with all column families and TTLs; the default column family's TTL can be set too.
        using (TtlDatabase db = TtlDatabase.Open(DbPath, o, [.. cfs, new("long", Cf(null))], [0, 100, 10_000], readOnly: false, () => Interlocked.Read(ref clock)))
        {
            CollectionAssert.AreEqual(B("forever"), db.Get(B("k")));
            db.SetTtl(1);
            Assert.AreEqual(1, db.TtlSeconds);
            db.CompactRange();
            Assert.IsNull(db.Get(B("k")));
        }
    }

    private sealed class DropKeyFilter(string drop) : CompactionFilter
    {
        public override string Name => "DropKeyFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType, ReadOnlySpan<byte> existingValue,
            IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) =>
            key.SequenceEqual(B(drop)) ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
    }

    private sealed class DropKeyFactory(string drop) : CompactionFilterFactory
    {
        public override string Name => "DropKeyFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context) => new DropKeyFilter(drop);
    }

    [TestMethod]
    public void UserCompactionFilterAndFactoryAreWrapped()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        ColumnFamilyDescriptor[] cfs =
        [
            new("default", new ColumnFamilyOptions { CompactionFilter = new DropKeyFilter("x") }),
            new("f", new ColumnFamilyOptions { CompactionFilterFactory = new DropKeyFactory("y") }),
        ];
        using TtlDatabase db = TtlDatabase.Open(DbPath, o, cfs, [1000, 1000]);
        TtlColumnFamily f = db.GetColumnFamily("f");
        // SanitizeOptions: a user filter is wrapped directly (no TTL factory: GetTtl fails, SetTtl is a no-op).
        Assert.IsInstanceOfType<TtlCompactionFilter>(db.Database.DefaultColumnFamily.Data.Options.CompactionFilter);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => _ = db.TtlSeconds);
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        Assert.AreEqual(1000, f.TtlSeconds);
        db.Put(B("x"), B("1"));
        db.Put(B("y"), B("2"));
        f.Put(B("x"), B("3"));
        f.Put(B("y"), B("4"));
        db.Flush();
        f.Flush();
        db.CompactRange();
        f.CompactRange();
        Assert.IsNull(db.Get(B("x")));
        CollectionAssert.AreEqual(B("2"), db.Get(B("y")));
        CollectionAssert.AreEqual(B("3"), f.Get(B("x")));
        Assert.IsNull(f.Get(B("y")));
    }
}
