using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;
using ValueType = RocksDb.Managed.Db.ValueType;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Formats WriteBatch records like write_batch_test.cc's PrintContents (in batch order, not memtable order).</summary>
internal struct PrintingHandler : IWriteBatchHandler
{
    public StringBuilder Sb;
    public ulong Seq;
    public int StopAfter;

    public PrintingHandler(ulong seq)
    {
        Sb = new StringBuilder();
        Seq = seq;
        StopAfter = int.MaxValue;
    }

    private static string S(ReadOnlySpan<byte> s) => Encoding.ASCII.GetString(s);
    private string Cf(uint cf) => cf == 0 ? "" : cf + ", ";
    private void Add(string s) { Sb.Append(s).Append('@').Append(Seq++); StopAfter--; }

    public bool Continue() => StopAfter > 0;
    public void Put(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add($"Put({Cf(cf)}{S(key)}, {S(value)})");
    public void TimedPut(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong t) => Add($"TimedPut({Cf(cf)}{S(key)}, {S(value)}, {t})");
    public void PutEntity(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Add($"PutEntity({Cf(cf)}{S(key)}, {Convert.ToHexString(entity)})");
    public void Delete(uint cf, ReadOnlySpan<byte> key) => Add($"Delete({Cf(cf)}{S(key)})");
    public void SingleDelete(uint cf, ReadOnlySpan<byte> key) => Add($"SingleDelete({Cf(cf)}{S(key)})");
    public void DeleteRange(uint cf, ReadOnlySpan<byte> b, ReadOnlySpan<byte> e) => Add($"DeleteRange({Cf(cf)}{S(b)}, {S(e)})");
    public void Merge(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add($"Merge({Cf(cf)}{S(key)}, {S(value)})");
    public void PutBlobIndex(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Add($"PutBlobIndex({Cf(cf)}{S(key)}, {S(value)})");
    public void LogData(ReadOnlySpan<byte> blob) { Sb.Append("LogData(").Append(S(blob)).Append(')'); StopAfter--; }
    public void MarkBeginPrepare(bool unprepare) { Sb.Append(unprepare ? "MarkBeginUnprepare()" : "MarkBeginPrepare()"); StopAfter--; }
    public void MarkEndPrepare(ReadOnlySpan<byte> xid) { Sb.Append("MarkEndPrepare(").Append(S(xid)).Append(')'); StopAfter--; }
    public void MarkNoop(bool emptyBatch) { Sb.Append("MarkNoop(").Append(emptyBatch).Append(')'); StopAfter--; }
    public void MarkCommit(ReadOnlySpan<byte> xid) { Sb.Append("MarkCommit(").Append(S(xid)).Append(')'); StopAfter--; }
    public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> ts) { Sb.Append("MarkCommitWithTimestamp(").Append(S(xid)).Append(", ").Append(S(ts)).Append(')'); StopAfter--; }
    public void MarkRollback(ReadOnlySpan<byte> xid) { Sb.Append("MarkRollback(").Append(S(xid)).Append(')'); StopAfter--; }
}

/// <summary>Port of the meaningful cases of db/write_batch_test.cc plus rep-format checks.</summary>
[TestClass]
public sealed class WriteBatchTests
{
    private static byte[] B(string s) => Encoding.ASCII.GetBytes(s);

    /// <summary>Iterates with the handler and, independently, with the reader; both must agree.</summary>
    internal static string PrintContents(ReadOnlySpan<byte> rep)
    {
        var h = new PrintingHandler(Coding.DecodeFixed64(rep));
        string viaHandler;
        try
        {
            WriteBatch.Iterate(rep, ref h);
            viaHandler = h.Sb.ToString();
        }
        catch (RocksDbException ex)
        {
            viaHandler = h.Sb.ToString() + ex.Message;
        }

        var sb = new StringBuilder();
        ulong seq = Coding.DecodeFixed64(rep);
        try
        {
            foreach (WriteBatchEntry e in new WriteBatchReader(rep))
            {
                string cf = e.ColumnFamilyId == 0 ? "" : e.ColumnFamilyId + ", ";
                string k = Encoding.ASCII.GetString(e.Key), v = Encoding.ASCII.GetString(e.Value);
                switch (e.Type)
                {
                    case ValueType.Value: sb.Append($"Put({cf}{k}, {v})@{seq++}"); break;
                    case ValueType.Deletion: sb.Append($"Delete({cf}{k})@{seq++}"); break;
                    case ValueType.SingleDeletion: sb.Append($"SingleDelete({cf}{k})@{seq++}"); break;
                    case ValueType.RangeDeletion: sb.Append($"DeleteRange({cf}{k}, {v})@{seq++}"); break;
                    case ValueType.Merge: sb.Append($"Merge({cf}{k}, {v})@{seq++}"); break;
                    case ValueType.BlobIndex: sb.Append($"PutBlobIndex({cf}{k}, {v})@{seq++}"); break;
                    case ValueType.WideColumnEntity: sb.Append($"PutEntity({cf}{k}, {Convert.ToHexString(e.Value)})@{seq++}"); break;
                    case ValueType.ValuePreferredSeqno: sb.Append($"TimedPut({cf}{k}, {v}, {e.WriteUnixTime})@{seq++}"); break;
                    case ValueType.LogData: sb.Append($"LogData({v})"); break;
                    case ValueType.BeginPrepareXID:
                    case ValueType.BeginPersistedPrepareXID: sb.Append("MarkBeginPrepare()"); break;
                    case ValueType.BeginUnprepareXID: sb.Append("MarkBeginUnprepare()"); break;
                    case ValueType.EndPrepareXID: sb.Append($"MarkEndPrepare({v})"); break;
                    case ValueType.CommitXID: sb.Append($"MarkCommit({v})"); break;
                    case ValueType.CommitXIDAndTimestamp: sb.Append($"MarkCommitWithTimestamp({v}, {k})"); break;
                    case ValueType.RollbackXID: sb.Append($"MarkRollback({v})"); break;
                    case ValueType.Noop: sb.Append("MarkNoop(?)"); break;
                }
            }
        }
        catch (RocksDbException ex)
        {
            sb.Append(ex.Message);
        }
        if (!viaHandler.Contains("MarkNoop", StringComparison.Ordinal)) Assert.AreEqual(viaHandler, sb.ToString());
        return viaHandler;
    }

    private static string PrintContents(WriteBatch b) => PrintContents(b.Data);

    [TestMethod]
    public void Empty()
    {
        using var batch = new WriteBatch();
        Assert.AreEqual("", PrintContents(batch));
        Assert.AreEqual(0, batch.Count);
        Assert.AreEqual(WriteBatch.HeaderSize, batch.DataSize);
        CollectionAssert.AreEqual(new byte[12], batch.Data.ToArray());
    }

    [TestMethod]
    public void Multiple()
    {
        using var batch = new WriteBatch();
        batch.Put("foo"u8, "bar"u8).Delete("box"u8).DeleteRange("bar"u8, "foo"u8).Put("baz"u8, "boo"u8);
        batch.Sequence = 100;
        Assert.AreEqual(100UL, batch.Sequence);
        Assert.AreEqual(4, batch.Count);
        Assert.AreEqual("Put(foo, bar)@100Delete(box)@101DeleteRange(bar, foo)@102Put(baz, boo)@103", PrintContents(batch));
        Assert.IsTrue(batch.HasPut && batch.HasDelete && batch.HasDeleteRange);
        Assert.IsFalse(batch.HasMerge || batch.HasSingleDelete);
    }

    [TestMethod]
    public void Corruption()
    {
        using var batch = new WriteBatch();
        batch.Put("foo"u8, "bar"u8).Delete("box"u8);
        batch.Sequence = 200;
        byte[] contents = batch.Data.ToArray();
        batch.SetContents(contents.AsSpan(0, contents.Length - 1));
        Assert.AreEqual("Put(foo, bar)@200Corruption: bad WriteBatch Delete", PrintContents(batch));
    }

    [TestMethod]
    public void WrongCountIsCorruption()
    {
        using var batch = new WriteBatch();
        batch.Put("a"u8, "b"u8);
        batch.SetCount(2);
        var ex = Assert.ThrowsExactly<RocksDbException>(() =>
        {
            var h = new PrintingHandler(0);
            WriteBatch.Iterate(batch.Data, ref h);
        });
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "WriteBatch has wrong count");
        var ex2 = Assert.ThrowsExactly<RocksDbException>(() => { foreach (WriteBatchEntry _ in new WriteBatchReader(batch.Data)) { } });
        Assert.AreEqual(StatusCode.Corruption, ex2.Code);
    }

    [TestMethod]
    public void TooSmallAndUnknownTag()
    {
        var ex = Assert.ThrowsExactly<RocksDbException>(() => new WriteBatchReader(new byte[11]));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        byte[] rep = new byte[13];
        rep[12] = 0x7E;
        Assert.AreEqual("Corruption: unknown WriteBatch tag: 126", PrintContents(rep));
    }

    [TestMethod]
    public void Append()
    {
        using var b1 = new WriteBatch();
        using var b2 = new WriteBatch();
        b1.Sequence = 200;
        b2.Sequence = 300;
        WriteBatch.Append(b1, b2);
        Assert.AreEqual("", PrintContents(b1));
        Assert.AreEqual(0, b1.Count);
        b2.Put("a"u8, "va"u8);
        WriteBatch.Append(b1, b2);
        Assert.AreEqual("Put(a, va)@200", PrintContents(b1));
        Assert.AreEqual(1, b1.Count);
        b2.Clear();
        b2.Put("b"u8, "vb"u8);
        WriteBatch.Append(b1, b2);
        Assert.AreEqual("Put(a, va)@200Put(b, vb)@201", PrintContents(b1));
        Assert.AreEqual(2, b1.Count);
        b2.Delete("foo"u8);
        WriteBatch.Append(b1, b2);
        Assert.AreEqual("Put(a, va)@200Put(b, vb)@201Put(b, vb)@202Delete(foo)@203", PrintContents(b1));
        Assert.AreEqual(4, b1.Count);
        Assert.AreEqual(b1.DataSize, WriteBatch.AppendedByteSize(b1.DataSize, WriteBatch.HeaderSize));
    }

    [TestMethod]
    public void SingleDeletion()
    {
        using var batch = new WriteBatch();
        batch.Sequence = 100;
        Assert.AreEqual("", PrintContents(batch));
        batch.Put("a"u8, "va"u8);
        Assert.AreEqual("Put(a, va)@100", PrintContents(batch));
        batch.SingleDelete("a"u8);
        Assert.AreEqual("Put(a, va)@100SingleDelete(a)@101", PrintContents(batch));
        Assert.AreEqual(2, batch.Count);
        Assert.IsTrue(batch.HasSingleDelete);
    }

    [TestMethod]
    public void Blob()
    {
        using var batch = new WriteBatch();
        batch.Put("k1"u8, "v1"u8).Put("k2"u8, "v2"u8).Put("k3"u8, "v3"u8).PutLogData("blob1"u8).Delete("k2"u8)
            .SingleDelete("k3"u8).PutLogData("blob2"u8).Merge("foo"u8, "bar"u8);
        Assert.AreEqual(6, batch.Count);
        Assert.AreEqual("Put(k1, v1)@0Put(k2, v2)@1Put(k3, v3)@2LogData(blob1)Delete(k2)@3SingleDelete(k3)@4LogData(blob2)Merge(foo, bar)@5",
            PrintContents(batch));
    }

    [TestMethod]
    public void PrepareCommit()
    {
        using var batch = new WriteBatch();
        batch.InsertNoop();
        batch.Put("k1"u8, "v1"u8).Put("k2"u8, "v2"u8);
        batch.SetSavePoint();
        batch.MarkEndPrepare("xid1"u8, writeAfterCommit: true, unpreparedBatch: false);
        Assert.IsFalse(batch.RollbackToSavePoint()); // save points are cleared
        batch.Put("k3"u8, "v3"u8);
        batch.MarkCommit("xid1"u8);
        batch.MarkRollback("xid1"u8);
        Assert.AreEqual(3, batch.Count);
        Assert.AreEqual("MarkBeginPrepare()Put(k1, v1)@0Put(k2, v2)@1MarkEndPrepare(xid1)Put(k3, v3)@2MarkCommit(xid1)MarkRollback(xid1)",
            PrintContents(batch));
        Assert.IsTrue(batch.HasBeginPrepare && batch.HasEndPrepare && batch.HasCommit && batch.HasRollback);
    }

    [TestMethod]
    public void UnpreparedAndCommitWithTimestamp()
    {
        using var batch = new WriteBatch();
        batch.InsertNoop();
        batch.Put("k"u8, "v"u8);
        batch.MarkEndPrepare("x"u8, writeAfterCommit: false, unpreparedBatch: true);
        batch.MarkCommitWithTimestamp("x"u8, "ts"u8);
        Assert.AreEqual((byte)ValueType.BeginUnprepareXID, batch.Data[12]);
        Assert.AreEqual("MarkBeginUnprepare()Put(k, v)@0MarkEndPrepare(x)MarkCommitWithTimestamp(x, ts)", PrintContents(batch));
        // Deferred content flags are computed from the rep.
        using var copy = new WriteBatch(batch.Data);
        Assert.IsTrue(copy.HasBeginPrepare && copy.HasEndPrepare && copy.HasCommit && copy.HasPut && !copy.HasDelete);
    }

    [TestMethod]
    public void NoopMarksBatchBoundaries()
    {
        using var batch = new WriteBatch();
        batch.InsertNoop();
        batch.Put("a"u8, "1"u8);
        batch.InsertNoop();
        var h = new PrintingHandler(0);
        batch.Iterate(ref h);
        Assert.AreEqual("MarkNoop(True)Put(a, 1)@0MarkNoop(False)", h.Sb.ToString());
    }

    [TestMethod]
    public void ContinueStopsMidBatch()
    {
        using var batch = new WriteBatch();
        batch.Put("a"u8, "1"u8).Put("b"u8, "2"u8).Put("c"u8, "3"u8);
        var h = new PrintingHandler(0) { StopAfter = 2 };
        batch.Iterate(ref h); // no "wrong count" error when the handler stops early
        Assert.AreEqual("Put(a, 1)@0Put(b, 2)@1", h.Sb.ToString());
    }

    [TestMethod]
    public void ColumnFamiliesBatch()
    {
        using var batch = new WriteBatch();
        batch.Put(0, "foo"u8, "bar"u8).Put(2, "twofoo"u8, "bar2"u8).Put(8, "eightfoo"u8, "bar8"u8).Delete(8, "eightfoo"u8)
            .SingleDelete(2, "twofoo"u8).DeleteRange(2, "3foo"u8, "4foo"u8).Merge(3, "omom"u8, "nom"u8).Merge(0, "omom"u8, "nom"u8)
            .Put(300, "big"u8, "cf"u8);
        Assert.AreEqual(9, batch.Count);
        Assert.AreEqual("Put(foo, bar)@0Put(2, twofoo, bar2)@1Put(8, eightfoo, bar8)@2Delete(8, eightfoo)@3SingleDelete(2, twofoo)@4" +
                        "DeleteRange(2, 3foo, 4foo)@5Merge(3, omom, nom)@6Merge(omom, nom)@7Put(300, big, cf)@8", PrintContents(batch));
    }

    [TestMethod]
    public void RepLayoutIsExact()
    {
        using var batch = new WriteBatch();
        batch.Sequence = 0x0102030405060708;
        batch.Put("k"u8, "v"u8);
        batch.Delete(200, "d"u8);
        byte[] expected =
        [
            8, 7, 6, 5, 4, 3, 2, 1, // sequence, fixed64
            2, 0, 0, 0,             // count, fixed32
            (byte)ValueType.Value, 1, (byte)'k', 1, (byte)'v',
            (byte)ValueType.ColumnFamilyDeletion, 0xC8, 0x01, 1, (byte)'d',
        ];
        CollectionAssert.AreEqual(expected, batch.Data.ToArray());
    }

    [TestMethod]
    public void TimedPutAndEntityAndBlobIndex()
    {
        using var batch = new WriteBatch();
        batch.TimedPut(0, "k"u8, "v"u8, 12345).TimedPut(4, "k2"u8, "v2"u8, ulong.MaxValue).PutEntitySerialized(5, "e"u8, [1, 0])
            .PutBlobIndex(0, "b"u8, "idx"u8);
        Assert.AreEqual(4, batch.Count);
        Assert.AreEqual("TimedPut(k, v, 12345)@0Put(4, k2, v2)@1PutEntity(5, e, 0100)@2PutBlobIndex(b, idx)@3", PrintContents(batch));
        Assert.IsTrue(batch.HasTimedPut && batch.HasPut && batch.HasPutEntity);
        // TimedPut value is packed as value | fixed64(write time).
        Assert.AreEqual((byte)ValueType.ValuePreferredSeqno, batch.Data[12]);
        Assert.AreEqual(9, batch.Data[15]);
    }

    [TestMethod]
    public void SavePointTest()
    {
        using var batch = new WriteBatch();
        batch.SetSavePoint();
        batch.Put("A"u8, "a"u8).Put("B"u8, "b"u8);
        batch.SetSavePoint();
        batch.Put("C"u8, "c"u8).Delete("A"u8);
        batch.SetSavePoint();
        batch.SetSavePoint();

        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("Put(A, a)@0Put(B, b)@1Put(C, c)@2Delete(A)@3", PrintContents(batch));

        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("Put(A, a)@0Put(B, b)@1", PrintContents(batch));

        batch.Delete("A"u8).Put("B"u8, "bb"u8);
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("", PrintContents(batch));
        Assert.IsFalse(batch.RollbackToSavePoint());
        Assert.AreEqual("", PrintContents(batch));

        batch.Put("D"u8, "d"u8).Delete("A"u8);
        batch.SetSavePoint();
        batch.Put("A"u8, "aaa"u8);
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("Put(D, d)@0Delete(A)@1", PrintContents(batch));

        batch.SetSavePoint();
        batch.Put("D"u8, "d"u8).Delete("A"u8);
        Assert.IsTrue(batch.RollbackToSavePoint());
        Assert.AreEqual("Put(D, d)@0Delete(A)@1", PrintContents(batch));
        Assert.IsFalse(batch.RollbackToSavePoint());

        using var batch2 = new WriteBatch();
        Assert.IsFalse(batch2.RollbackToSavePoint());
        batch2.Delete("A"u8);
        batch2.SetSavePoint();
        Assert.IsTrue(batch2.RollbackToSavePoint());
        Assert.AreEqual("Delete(A)@0", PrintContents(batch2));
        batch2.Clear();
        Assert.AreEqual("", PrintContents(batch2));
        batch2.SetSavePoint();
        batch2.Delete("B"u8);
        batch2.SetSavePoint();
        Assert.IsTrue(batch2.RollbackToSavePoint());
        Assert.AreEqual("Delete(B)@0", PrintContents(batch2));
        Assert.IsTrue(batch2.RollbackToSavePoint());
        Assert.AreEqual("", PrintContents(batch2));
        Assert.IsFalse(batch2.RollbackToSavePoint());

        using var batch3 = new WriteBatch();
        Assert.IsFalse(batch3.PopSavePoint());
        batch3.SetSavePoint();
        batch3.Delete("A"u8);
        Assert.IsTrue(batch3.PopSavePoint());
        Assert.AreEqual("Delete(A)@0", PrintContents(batch3));
    }

    [TestMethod]
    public void MemoryLimitTest()
    {
        // The header size is 12 bytes. The two Puts take 8 bytes which gives total of 12 + 8 * 2 = 28 bytes.
        using var batch = new WriteBatch(0, 28);
        batch.Put("a"u8, "...."u8).Put("b"u8, "...."u8);
        var ex = Assert.ThrowsExactly<RocksDbException>(() => batch.Put("c"u8, "...."u8));
        Assert.AreEqual(StatusCode.Aborted, ex.Code);
        Assert.AreEqual(StatusSubCode.MemoryLimit, ex.SubCode);
        Assert.AreEqual(2, batch.Count);
        Assert.AreEqual(28, batch.DataSize);
        Assert.AreEqual("Put(a, ....)@0Put(b, ....)@1", PrintContents(batch));
    }

    [TestMethod]
    public void ClearKeepsWorkingAndPoolingReuses()
    {
        WriteBatch b = WriteBatch.Rent();
        b.Put("x"u8, "y"u8);
        b.Sequence = 9;
        b.Dispose();
        WriteBatch b2 = WriteBatch.Rent();
        Assert.AreEqual(0, b2.Count);
        Assert.AreEqual(0UL, b2.Sequence);
        b2.Put("z"u8, "w"u8);
        Assert.AreEqual("Put(z, w)@0", PrintContents(b2));
        b2.Dispose();
    }

    [TestMethod]
    public void RandomizedRoundTrip()
    {
        int seed = Environment.TickCount;
        var rnd = new Random(seed);
        using var batch = new WriteBatch();
        var expected = new List<(ValueType, uint, byte[], byte[])>();
        for (int i = 0; i < 2000; i++)
        {
            uint cf = rnd.Next(3) == 0 ? (uint)rnd.Next(1, 100000) : 0;
            byte[] k = new byte[rnd.Next(0, 300)];
            byte[] v = new byte[rnd.Next(0, 3000)];
            rnd.NextBytes(k);
            rnd.NextBytes(v);
            switch (rnd.Next(5))
            {
                case 0: batch.Put(cf, k, v); expected.Add((ValueType.Value, cf, k, v)); break;
                case 1: batch.Delete(cf, k); expected.Add((ValueType.Deletion, cf, k, [])); break;
                case 2: batch.SingleDelete(cf, k); expected.Add((ValueType.SingleDeletion, cf, k, [])); break;
                case 3: batch.DeleteRange(cf, k, v); expected.Add((ValueType.RangeDeletion, cf, k, v)); break;
                default: batch.Merge(cf, k, v); expected.Add((ValueType.Merge, cf, k, v)); break;
            }
        }
        Assert.AreEqual(expected.Count, batch.Count, "seed=" + seed);
        int idx = 0;
        foreach (WriteBatchEntry e in batch)
        {
            (ValueType t, uint cf, byte[] k, byte[] v) = expected[idx++];
            Assert.AreEqual(t, e.Type, "seed=" + seed);
            Assert.AreEqual(cf, e.ColumnFamilyId, "seed=" + seed);
            Assert.IsTrue(e.Key.SequenceEqual(k), "seed=" + seed);
            Assert.IsTrue(e.Value.SequenceEqual(v), "seed=" + seed);
        }
        Assert.AreEqual(expected.Count, idx);

        // Every proper prefix of the rep is a valid batch with a wrong count or corrupt: always a Corruption error.
        byte[] rep = batch.Data.ToArray();
        for (int cut = WriteBatch.HeaderSize; cut < Math.Min(rep.Length, 400); cut++)
        {
            int c = cut;
            var ex = Assert.ThrowsExactly<RocksDbException>(() => { foreach (WriteBatchEntry _ in new WriteBatchReader(rep.AsSpan(0, c))) { } });
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
        }
    }
}
