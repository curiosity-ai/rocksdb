using System.Runtime.InteropServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Per key-value protection info. Ports of db/db_kv_checksum_test.cc (DbKvChecksumTest: NoCorruptionCase,
/// MemTableAddCorrupted[WithColumnFamily], WriteToWALCorrupted; DbMemtableKVChecksumTest: Get/Iterator/Flush with a
/// corrupt memtable entry) and of the block per key-value checksum tests (table/block_based/data_block_hash_index /
/// block_test BlockPerKVChecksumTest): corruptions are injected with the same named hooks as upstream's sync points
/// (DEBUG builds).
/// </summary>
[TestClass]
public sealed class KvProtectionTests
{
    public enum Op { Put, Delete, SingleDelete, DeleteRange, Merge, PutEntity, TimedPut }

    private int _corruptOffset;
    private int _entryLength = -1;
    private int _skipOffset = -1;

    [TestCleanup]
    public void Cleanup() => TestHooks.ClearAll();

    private static IEnumerable<object[]> Ops()
    {
        foreach (Op op in Enum.GetValues<Op>()) yield return [op];
    }

    private static void AddOp(WriteBatch b, Op op, uint cf)
    {
        switch (op)
        {
            case Op.Put: b.Put(cf, B("key"), B("value")); break;
            case Op.Delete: b.Delete(cf, B("key")); break;
            case Op.SingleDelete: b.SingleDelete(cf, B("key")); break;
            case Op.DeleteRange: b.DeleteRange(cf, B("key"), B("key2")); break;
            case Op.Merge: b.Merge(cf, B("key"), B("value")); break;
            case Op.PutEntity: b.PutEntity(cf, B("key"), [new WideColumn(B("col"), B("value"))]); break;
            case Op.TimedPut: b.TimedPut(cf, B("key"), B("value"), 12345); break;
        }
    }

    /// <summary>inplace_update_support requires allow_concurrent_memtable_write = false.</summary>
    private static DbOptions NoConcurrent(MemoryFileSystem fs)
    {
        DbOptions o = NewOptions(fs);
        o.AllowConcurrentMemtableWrite = false;
        return o;
    }

    private static ColumnFamilyOptions CfOptions(uint memtableProtection = 0) =>
        new() { MergeOperator = new StringAppendOperator(), MemtableProtectionBytesPerKey = memtableProtection };

    /// <summary>Hook body: add 1 to the byte at the current offset (CorruptNextByteCallBack), recording the length.</summary>
    private void CorruptNextByte(Span<byte> data)
    {
        _entryLength = data.Length;
        if (_corruptOffset < data.Length) data[_corruptOffset]++;
    }

    // ------------------------------------------------------------------------------------------------- write batch

    [TestMethod]
    [DynamicData(nameof(Ops))]
    public void NoCorruptionCase(Op op)
    {
        using var b = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        AddOp(b, op, 0);
        AddOp(b, op, 3);
        Assert.AreEqual(2, b.ProtectionInfo.Length);
        b.VerifyChecksum();
        // UpdateProtectionInfo on an unprotected batch computes the same protection.
        using var u = new WriteBatch();
        AddOp(u, op, 0);
        AddOp(u, op, 3);
        u.UpdateProtectionInfo(8);
        CollectionAssert.AreEqual(b.ProtectionInfo.ToArray(), u.ProtectionInfo.ToArray());
        u.VerifyChecksum();
    }

    [TestMethod]
    public void EveryCorruptedBatchByteIsDetected()
    {
        using var b = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        b.Put(B("key"), B("value"));
        b.Merge(5, B("mkey"), B("operand"));
        b.Delete(B("gone"));
        Span<byte> rep = MemoryMarshal.AsMemory(b.DataMemory).Span;
        // The first 8 bytes (sequence number) are not protected; the count is checked separately.
        for (int i = 8; i < rep.Length; i++)
        {
            rep[i]++;
            var ex = Assert.ThrowsExactly<RocksDbException>(() => b.VerifyChecksum(), "byte " + i);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            rep[i]--;
        }
        b.VerifyChecksum();
    }

    [TestMethod]
    public void SavePointsAppendAndTimestampsKeepProtectionInSync()
    {
        using var b = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        b.Put(B("a"), B("1"));
        b.SetSavePoint();
        b.Put(B("b"), B("2"));
        b.Delete(B("c"));
        Assert.IsTrue(b.RollbackToSavePoint());
        Assert.AreEqual(1, b.ProtectionInfo.Length);
        b.VerifyChecksum();

        using var dst = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        dst.Put(B("x"), B("y"));
        WriteBatch.Append(dst, b);
        Assert.AreEqual(2, dst.ProtectionInfo.Length);
        dst.VerifyChecksum();

        using var ts = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        ts.Put(B("k\0\0\0\0\0\0\0\0"), B("v"));
        ts.DeleteRange(B("a\0\0\0\0\0\0\0\0"), B("z\0\0\0\0\0\0\0\0"));
        ts.UpdateTimestamps(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, static _ => 8);
        ts.VerifyChecksum();
    }

    /// <summary>
    /// SetContents and pooled batches start unprotected; appending a protected batch to an unprotected non-empty one
    /// (a write group mixing both) leaves the destination unprotected.
    /// </summary>
    [TestMethod]
    public void ProtectionIsDroppedWhereRecordsAreNotProtected()
    {
        WriteBatch pooled = WriteBatch.Rent();
        pooled.Put(B("k"), B("v"));
        pooled.UpdateProtectionInfo(8);
        Assert.AreEqual(8, pooled.ProtectionBytesPerKey);
        pooled.Dispose();
        WriteBatch again = WriteBatch.Rent();
        Assert.AreEqual(0, again.ProtectionBytesPerKey);
        again.Dispose();

        using var p = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        p.Put(B("a"), B("1"));
        using var copy = new WriteBatch(0, 0, protectionBytesPerKey: 8);
        copy.SetContents(p.Data);
        Assert.AreEqual(0, copy.ProtectionBytesPerKey);
        copy.VerifyChecksum();

        using var plain = new WriteBatch();
        plain.Put(B("b"), B("2"));
        WriteBatch.Append(plain, p);
        Assert.AreEqual(0, plain.ProtectionBytesPerKey);
        plain.VerifyChecksum();

        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions());
        db.Put(new WriteOptions { ProtectionBytesPerKey = 8 }, B("x"), B("1"));
        using var raw = new WriteBatch();
        raw.SetContents(p.Data);
        db.Write(raw);
        db.Put(B("y"), B("2"));
        Assert.AreEqual("1", GetString(db, "a"));
        Assert.AreEqual("2", GetString(db, "y"));
    }

    [TestMethod]
    public void InvalidProtectionSizesAreRejected()
    {
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => new WriteBatch(0, 0, 4)).Code);
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions()))
        {
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Put(new WriteOptions { ProtectionBytesPerKey = 4 }, B("k"), B("v")));
            Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
            StringAssert.Contains(ex.Message, "must be zero or eight");
        }
        var bad = new ColumnFamilyOptions { MemtableProtectionBytesPerKey = 3 };
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), bad)).Code);
        var badBlock = new ColumnFamilyOptions { BlockProtectionBytesPerKey = 5 };
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), badBlock)).Code);
    }

    /// <summary>Port of DbKvChecksumTest.WriteToWALCorrupted: every byte of the logged batch after the sequence number.</summary>
    [TestMethod]
    [DynamicData(nameof(Ops))]
    public void WriteToWalCorrupted(Op op)
    {
        TestHooks.SetSpan("DBImpl::WriteToWAL:log_entry", CorruptNextByte);
        MemoryFileSystem fs = NewFs();
        // First 8 bytes are for sequence number which is not protected in write batch
        for (_corruptOffset = 8; _entryLength < 0 || _corruptOffset < _entryLength; _corruptOffset++)
        {
            // Corrupted write batch leads to read-only mode, so we have to reopen for every attempt.
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions());
            using var b = new WriteBatch(0, 0, protectionBytesPerKey: 8);
            AddOp(b, op, 0);
            long walBefore = WalBytes(fs);
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Write(new WriteOptions { ProtectionBytesPerKey = 8 }, b), "offset " + _corruptOffset);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            // Confirm that nothing was written to WAL
            Assert.AreEqual(walBefore, WalBytes(fs));
            Assert.AreEqual(StatusCode.Corruption, db.BackgroundError?.Code);
        }
        Assert.IsTrue(_entryLength > 8);
    }

    private static long WalBytes(MemoryFileSystem fs)
    {
        long n = 0;
        foreach (string c in fs.GetChildren(DbPath))
            if (c.EndsWith(".log", StringComparison.Ordinal)) n += fs.GetFileSize(DbPath + "/" + c);
        return n;
    }

    /// <summary>Port of DbKvChecksumTest.MemTableAddCorrupted / MemTableAddWithColumnFamilyCorrupted.</summary>
    [TestMethod]
    [DynamicData(nameof(Ops))]
    public void MemTableAddCorrupted(Op op)
    {
        TestHooks.SetSpan("MemTable::Add:Encoded", CorruptNextByte);
        MemoryFileSystem fs = NewFs();
        foreach (bool otherCf in (bool[])[false, true])
        {
            _entryLength = -1;
            for (_corruptOffset = 0; _entryLength < 0 || _corruptOffset < _entryLength; _corruptOffset++)
            {
                DbOptions o = NewOptions(fs);
                o.CreateMissingColumnFamilies = true;
                using RocksDatabase db = RocksDatabase.Open(DbPath, o, [new("default", CfOptions()), new("pikachu", CfOptions())]);
                uint cf = otherCf ? db.GetColumnFamily("pikachu")!.Id : 0;
                using var b = new WriteBatch();
                AddOp(b, op, cf);
                // WriteMode::kWriteOptionProtectedBatch: protection computed by DB::Write.
                var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Write(new WriteOptions { ProtectionBytesPerKey = 8 }, b), "offset " + _corruptOffset);
                Assert.AreEqual(StatusCode.Corruption, ex.Code);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------- memtable

    private static IEnumerable<object[]> MemtableCases()
    {
        // skip 1 byte checksum as it makes test flaky (1/256 chance of a matching corrupted checksum)
        foreach (uint bytes in (uint[])[2, 4, 8])
        {
            yield return [Op.Put, bytes];
            yield return [Op.Merge, bytes];
            yield return [Op.PutEntity, bytes];
            yield return [Op.TimedPut, bytes];
        }
    }

    /// <summary>Port of DbMemtableKVChecksumTest.GetWithCorruptAfterMemtableInsert (value bytes; Get and MultiGet).</summary>
    [TestMethod]
    [DynamicData(nameof(MemtableCases))]
    public void GetWithCorruptAfterMemtableInsert(Op op, uint protectionBytes)
    {
        int valueStart = -1;
        TestHooks.SetSpan("MemTable::Add:BeforeReturn:Encoded", data =>
        {
            // varint32(ikey len) | ikey | varint32(value len) | value
            int ikeyLen = data[0];
            valueStart = 1 + ikeyLen + 1;
            _entryLength = data.Length;
        });
        TestHooks.SetSpan("Memtable::SaveValue:Found:entry", data => data[_corruptOffset]++);
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db0 = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(protectionBytes)))
        {
            using var b = new WriteBatch();
            AddOp(b, op, 0);
            db0.Write(b);
        }
        Assert.IsTrue(valueStart > 0);
        for (_corruptOffset = valueStart; _corruptOffset < _entryLength; _corruptOffset++)
        {
            fs = NewFs();
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(protectionBytes));
            using var b = new WriteBatch();
            AddOp(b, op, 0);
            db.Write(b);
            var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(B("key")), "offset " + _corruptOffset);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            StringAssert.Contains(ex.Message, "per key-value checksum verification failed");
        }
    }

    /// <summary>
    /// Port of DbMemtableKVChecksumTest.IteratorWithCorruptAfterMemtableInsert and FlushWithCorruptAfterMemtableInsert:
    /// the entry is corrupted after it was encoded (here: user key, sequence/type and value bytes).
    /// </summary>
    [TestMethod]
    [DynamicData(nameof(MemtableCases))]
    public void IteratorAndFlushWithCorruptAfterMemtableInsert(Op op, uint protectionBytes)
    {
        TestHooks.SetSpan("MemTable::Add:BeforeReturn:Encoded", data =>
        {
            _entryLength = data.Length;
            int ikeyLen = data[0];
            // Skip the length varints (corrupting them makes the entry unparsable, as upstream's SkipNotToCorruptEntry).
            int offset = _corruptOffset;
            _skipOffset = 1 + ikeyLen;
            if (offset == 0 || offset == _skipOffset) return;
            if (offset < data.Length) data[offset]++;
        });
        for (_corruptOffset = 1; _entryLength < 0 || _corruptOffset < _entryLength; _corruptOffset++)
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(protectionBytes));
            using var b = new WriteBatch();
            AddOp(b, op, 0);
            db.Write(b);
            if (_corruptOffset == _skipOffset) continue;
            using Iterator it = db.NewIterator();
            var ex = Assert.ThrowsExactly<RocksDbException>(() => it.SeekToFirst(), "offset " + _corruptOffset);
            Assert.AreEqual(StatusCode.Corruption, ex.Code);
            if (_corruptOffset == 2)
            {
                // Not corrupting each byte like other tests since Flush() is relatively slow.
                var fex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush(), "flush");
                Assert.AreEqual(StatusCode.Corruption, fex.Code);
                // DB enters read-only state when flush reads corrupted data
                Assert.AreEqual(StatusCode.Corruption, db.BackgroundError?.Code);
            }
        }
    }

    /// <summary>With protection on and no corruption, every operation reads back (memtable and SST, after reopen).</summary>
    [TestMethod]
    public void ProtectedDbRoundTrip()
    {
        foreach (uint bytes in (uint[])[1, 2, 4, 8])
        {
            MemoryFileSystem fs = NewFs();
            ColumnFamilyOptions cfo = CfOptions(bytes);
            cfo.BlockProtectionBytesPerKey = bytes;
            cfo.InplaceUpdateSupport = bytes == 4;
            var wo = new WriteOptions { ProtectionBytesPerKey = 8 };
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NoConcurrent(fs), cfo))
            {
                for (int i = 0; i < 2000; i++) db.Put(wo, Key(i), Value(i, 0));
                for (int i = 0; i < 2000; i += 3) db.Put(wo, Key(i), Value(i % 7, 1)); // shorter values: in place with inplace_update_support
                db.Merge(wo, B("m"), B("a"));
                db.Merge(wo, B("m"), B("b"));
                db.DeleteRange(wo, Key(100), Key(110));
                Verify(db);
                db.Flush();
                Verify(db);
                db.Put(wo, Key(5), Value(5, 2));
                db.CompactRange();
                Assert.AreEqual(S(Value(5, 2)), GetString(db, S(Key(5))!));
            }
            using (RocksDatabase db = RocksDatabase.Open(DbPath, NoConcurrent(fs), cfo))
            {
                Assert.AreEqual(S(Value(5, 2)), GetString(db, S(Key(5))!));
                using Iterator it = db.NewIterator();
                int n = 0;
                for (it.SeekToLast(); it.Valid; it.Prev()) n++;
                Assert.AreEqual(2000 - 10 + 1, n);
            }
        }

        static void Verify(RocksDatabase db)
        {
            for (int i = 0; i < 2000; i += 11)
            {
                string? expected = i is >= 100 and < 110 ? null : S(i % 3 == 0 ? Value(i % 7, 1) : Value(i, 0));
                Assert.AreEqual(expected, GetString(db, S(Key(i))!), "key " + i);
            }
            Assert.AreEqual("a,b", GetString(db, "m"));
            using Iterator it = db.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(2000 - 10 + 1, n);
        }
    }

    // ------------------------------------------------------------------------------------------------- blocks

    /// <summary>
    /// Port of the block per key-value checksum corruption tests (BlockPerKVChecksumTest / DbKVChecksum block cases):
    /// a value byte of an in-memory data block is corrupted after its checksums were computed; Get, iteration and
    /// MultiGet report Corruption.
    /// </summary>
    [TestMethod]
    [DataRow(1u)]
    [DataRow(2u)]
    [DataRow(4u)]
    [DataRow(8u)]
    public void BlockEntryCorruptionIsDetected(uint bytes)
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyOptions cfo = CfOptions();
        cfo.BlockProtectionBytesPerKey = bytes;
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
        for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        // Every entry verifies (also after the checksums were computed once).
        for (int pass = 0; pass < 2; pass++)
        {
            using Iterator it = db.NewIterator();
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) n++;
            Assert.AreEqual(500, n);
        }
        // Corrupt the current entry's value once (all 8 bits of one byte, so even a 1-byte checksum must notice:
        // corrupted entries differ in the protected value).
        bool armed = true;
        TestHooks.SetSpan("BlockIter::UpdateKey::value", v =>
        {
            if (!armed || v.Length == 0) return;
            armed = false;
            v[0] ^= 0xff;
        });
        var ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(Key(250)));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Corrupted block entry: per key-value checksum verification failed.");
        // The corruption stays in the cached block: iteration hits it too.
        TestHooks.ClearAll();
        Assert.ThrowsExactly<RocksDbException>(() =>
        {
            using Iterator it = db.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next()) { }
        });
    }
}
