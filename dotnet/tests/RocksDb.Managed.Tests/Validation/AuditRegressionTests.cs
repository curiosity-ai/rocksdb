using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests for discrepancies found while auditing the port against the C++ sources (see
/// dotnet/docs/validation.md), plus upstream vectors from util/coding_test.cc and db/dbformat_test.cc.
/// </summary>
[TestClass]
public sealed class AuditRegressionTests
{
    // ---- db/dbformat.h ParseInternalKey / IsExtendedValueType ------------------------------------------------

    private static byte[] IKey(string userKey, ulong seq, EntryType type)
    {
        byte[] k = new byte[userKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(k, B(userKey), seq, type);
        return k;
    }

    [TestMethod]
    public void ParseInternalKeyAcceptsExactlyTheExtendedValueTypes()
    {
        // IsValueType + kTypeRangeDeletion + kTypeMaxValid; every WAL-only type is a corrupted key.
        var valid = new HashSet<EntryType>
        {
            EntryType.Deletion, EntryType.Value, EntryType.Merge, EntryType.SingleDeletion, EntryType.BlobIndex,
            EntryType.DeletionWithTimestamp, EntryType.WideColumnEntity, EntryType.ValuePreferredSeqno,
            EntryType.RangeDeletion, EntryType.MaxValid,
        };
        for (int t = 0; t <= 0xff; t++)
        {
            var type = (EntryType)t;
            bool expected = valid.Contains(type);
            Assert.AreEqual(expected, DbFormat.TryParseInternalKey(IKey("k", 7, type), out ParsedInternalKey p), "type " + t);
            Assert.AreEqual(expected, DbFormat.IsExtendedValueType(type), "type " + t);
            Assert.AreEqual(expected, VersionEdit.IsValidInternalKey(IKey("k", 7, type)), "type " + t);
            if (expected)
            {
                Assert.AreEqual(7UL, p.Sequence);
                Assert.AreEqual(type, p.Type);
                Assert.AreEqual(1, p.UserKeyLength);
            }
        }
        Assert.IsFalse(DbFormat.TryParseInternalKey(new byte[7], out _));
        Assert.IsTrue(InternalKey.FromEncoded(IKey("x", 1, EntryType.MaxValid)).Valid);
        Assert.IsFalse(InternalKey.FromEncoded(IKey("x", 1, EntryType.LogData)).Valid);
    }

    [TestMethod]
    public void IsValueTypeMatchesUpstream()
    {
        EntryType[] valueTypes =
        [
            EntryType.Deletion, EntryType.Value, EntryType.Merge, EntryType.SingleDeletion, EntryType.BlobIndex,
            EntryType.DeletionWithTimestamp, EntryType.WideColumnEntity, EntryType.ValuePreferredSeqno,
        ];
        for (int t = 0; t <= 0xff; t++)
        {
            Assert.AreEqual(Array.IndexOf(valueTypes, (EntryType)t) >= 0, DbFormat.IsValueType((EntryType)t), "type " + t);
        }
    }

    [TestMethod]
    public void VersionEditRejectsFileBoundaryWithWalOnlyType()
    {
        var f = new FileMetaData
        {
            FileNumber = 5,
            FileSize = 100,
            Smallest = IKey("a", 1, EntryType.Value),
            Largest = IKey("b", 2, EntryType.Value),
            SmallestSeqno = 1,
            LargestSeqno = 2,
            EpochNumber = 1,
        };
        var edit = new VersionEdit();
        edit.AddFile(0, f);
        byte[] encoded = edit.Encode(0);
        // Corrupt the type byte of the smallest key to kTypeLogData (WAL only): GetInternalKey -> !Valid().
        int at = encoded.AsSpan().IndexOf(IKey("a", 1, EntryType.Value));
        Assert.IsTrue(at > 0);
        encoded[at + 1] = (byte)EntryType.LogData;
        var ex = Assert.ThrowsExactly<RocksDbException>(() => VersionEdit.DecodeFrom(encoded));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }

    // ---- ColumnFamilyOptions::arena_block_size (advanced_options.h default 0 + SanitizeCfOptions) ----------

    [TestMethod]
    public void ArenaBlockSizeDefaultsToZeroAndIsDerivedFromWriteBufferSize()
    {
        var cfo = new ColumnFamilyOptions();
        Assert.AreEqual(0UL, cfo.ArenaBlockSize);
        Assert.AreEqual(1024 * 1024, new MemTableOptions { WriteBufferSize = 64L << 20 }.GetEffectiveArenaBlockSize());
        Assert.AreEqual(8 * 1024, new MemTableOptions { WriteBufferSize = 64 * 1024 }.GetEffectiveArenaBlockSize());
        // Below the 64 KiB clamp of write_buffer_size.
        Assert.AreEqual(8 * 1024, new MemTableOptions { WriteBufferSize = 4 * 1024 }.GetEffectiveArenaBlockSize());
        // Not a multiple of 4 KiB: aligned up.
        Assert.AreEqual(16 * 1024, new MemTableOptions { WriteBufferSize = 100 * 1024 }.GetEffectiveArenaBlockSize());
    }

    [TestMethod]
    public void SmallWriteBufferWithDefaultArenaBlockSizeOpensAndFlushes()
    {
        // Previously the 1 MiB default arena block exceeded a 64 KiB write buffer, so a fresh memtable already
        // wanted a flush (Debug.Assert in the MemTable constructor).
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, MaxWriteBufferNumber = 3 };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int i = 0; i < 3000; i++) db.Put(Key(i), Value(i, 0));
            db.Flush();
            int files = int.Parse(db.GetProperty("rocksdb.num-files-at-level0")!, System.Globalization.CultureInfo.InvariantCulture);
            Assert.IsTrue(files > 1, "files=" + files);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int i = 0; i < 3000; i += 11) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)), "key " + i);
        }
    }

    // ---- MemTable fragmented range tombstone cache (publication race) ----------------------------------------

    [TestMethod]
    public void MemTableRangeTombstoneCacheNeverReturnsStaleListForNewerVersion()
    {
        // A writer adds range tombstones with increasing seqnos and then publishes the seqno; readers that observed a
        // published seqno must see a covering tombstone at least that new. The cache used to publish the list and its
        // version separately, so a reader could pair the previous list with the new version.
        using var mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions(), 0);
        long published = 0;
        bool stop = false;
        int failures = 0;
        var readers = new Task[3];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                while (!Volatile.Read(ref stop))
                {
                    ulong seen = (ulong)Volatile.Read(ref published);
                    if (seen == 0) continue;
                    if (mem.MaxCoveringTombstoneSeqnum("k"u8, DbFormat.MaxSequenceNumber) < seen) Interlocked.Increment(ref failures);
                }
            });
        }
        for (ulong seq = 1; seq <= 2000; seq++)
        {
            Assert.IsTrue(mem.Add(seq, EntryType.RangeDeletion, B("a" + seq.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)), B("z")));
            Volatile.Write(ref published, (long)seq);
        }
        Volatile.Write(ref stop, true);
        Task.WaitAll(readers);
        Assert.AreEqual(0, failures);
        Assert.AreEqual(2000UL, mem.MaxCoveringTombstoneSeqnum("k"u8, DbFormat.MaxSequenceNumber));
    }

    // ---- util/coding_test.cc --------------------------------------------------------------------------------

    [TestMethod]
    public void CodingFixed32AndFixed64()
    {
        using var w = new PooledBufferWriter();
        for (uint v = 0; v < 100000; v++) Coding.PutFixed32(w, v);
        ReadOnlySpan<byte> p = w.WrittenSpan;
        for (uint v = 0; v < 100000; v++)
        {
            Assert.AreEqual(v, Coding.DecodeFixed32(p));
            p = p[4..];
        }
        w.Clear();
        for (int power = 0; power <= 63; power++)
        {
            ulong v = 1UL << power;
            Coding.PutFixed64(w, v - 1);
            Coding.PutFixed64(w, v);
            Coding.PutFixed64(w, v + 1);
        }
        p = w.WrittenSpan;
        for (int power = 0; power <= 63; power++)
        {
            ulong v = 1UL << power;
            Assert.AreEqual(v - 1, Coding.DecodeFixed64(p));
            Assert.AreEqual(v, Coding.DecodeFixed64(p[8..]));
            Assert.AreEqual(v + 1, Coding.DecodeFixed64(p[16..]));
            p = p[24..];
        }
    }

    [TestMethod]
    public void CodingEncodingOutputIsLittleEndian()
    {
        using var w = new PooledBufferWriter();
        Coding.PutFixed32(w, 0x04030201);
        CollectionAssert.AreEqual(new byte[] { 0x01, 0x02, 0x03, 0x04 }, w.WrittenSpan.ToArray());
        w.Clear();
        Coding.PutFixed64(w, 0x0807060504030201);
        CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, w.WrittenSpan.ToArray());
    }

    [TestMethod]
    public void CodingVarint32AllShifts()
    {
        using var w = new PooledBufferWriter();
        for (uint i = 0; i < 32 * 32; i++) Coding.PutVarint32(w, (i / 32) << (int)(i % 32));
        ReadOnlySpan<byte> p = w.WrittenSpan;
        for (uint i = 0; i < 32 * 32; i++)
        {
            uint expected = (i / 32) << (int)(i % 32);
            int before = p.Length;
            Assert.IsTrue(Coding.GetVarint32(ref p, out uint actual));
            Assert.AreEqual(expected, actual);
            Assert.AreEqual(Coding.VarintLength(actual), before - p.Length);
        }
        Assert.AreEqual(0, p.Length);
    }

    [TestMethod]
    public void CodingVarint64Values()
    {
        var values = new List<ulong> { 0, 100, ~0UL, ~0UL - 1 };
        for (int k = 0; k < 64; k++)
        {
            ulong power = 1UL << k;
            values.Add(power);
            values.Add(power - 1);
            values.Add(power + 1);
        }
        using var w = new PooledBufferWriter();
        foreach (ulong v in values) Coding.PutVarint64(w, v);
        ReadOnlySpan<byte> p = w.WrittenSpan;
        foreach (ulong v in values)
        {
            int before = p.Length;
            Assert.IsTrue(Coding.GetVarint64(ref p, out ulong actual));
            Assert.AreEqual(v, actual);
            Assert.AreEqual(Coding.VarintLength(actual), before - p.Length);
        }
        Assert.AreEqual(0, p.Length);
    }

    [TestMethod]
    public void CodingVarintOverflowAndTruncation()
    {
        // Varint32Overflow / Varint64Overflow
        Assert.AreEqual(0, Coding.DecodeVarint32([0x81, 0x82, 0x83, 0x84, 0x85, 0x11], out _));
        Assert.AreEqual(0, Coding.DecodeVarint64([0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11], out _));

        // Varint32Truncation / Varint64Truncation: every proper prefix fails, the whole encoding succeeds.
        Span<byte> buf = stackalloc byte[Coding.MaxVarint64Length];
        int n32 = Coding.EncodeVarint32(buf, uint.MaxValue);
        for (int len = 0; len < n32; len++) Assert.AreEqual(0, Coding.DecodeVarint32(buf[..len], out _), "len " + len);
        Assert.AreEqual(n32, Coding.DecodeVarint32(buf[..n32], out uint r32));
        Assert.AreEqual(uint.MaxValue, r32);

        ulong large = (1UL << 63) + 100;
        int n64 = Coding.EncodeVarint64(buf, large);
        for (int len = 0; len < n64; len++) Assert.AreEqual(0, Coding.DecodeVarint64(buf[..len], out _), "len " + len);
        Assert.AreEqual(n64, Coding.DecodeVarint64(buf[..n64], out ulong r64));
        Assert.AreEqual(large, r64);
    }

    [TestMethod]
    public void CodingLengthPrefixedStrings()
    {
        using var w = new PooledBufferWriter();
        Coding.PutLengthPrefixedSlice(w, ""u8);
        Coding.PutLengthPrefixedSlice(w, "foo"u8);
        Coding.PutLengthPrefixedSlice(w, "bar"u8);
        Coding.PutLengthPrefixedSlice(w, new byte[200].AsSpan());
        ReadOnlySpan<byte> input = w.WrittenSpan;
        Assert.IsTrue(Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> v));
        Assert.AreEqual(0, v.Length);
        Assert.IsTrue(Coding.GetLengthPrefixedSlice(ref input, out v));
        Assert.IsTrue(v.SequenceEqual("foo"u8));
        Assert.IsTrue(Coding.GetLengthPrefixedSlice(ref input, out v));
        Assert.IsTrue(v.SequenceEqual("bar"u8));
        Assert.IsTrue(Coding.GetLengthPrefixedSlice(ref input, out v));
        Assert.AreEqual(200, v.Length);
        Assert.AreEqual(0, input.Length);
        // Length larger than the remaining input fails without consuming.
        ReadOnlySpan<byte> bad = [3, (byte)'a', (byte)'b'];
        Assert.IsFalse(Coding.GetLengthPrefixedSlice(ref bad, out _));
    }

    [TestMethod]
    public void ZigZagMatchesUpstream()
    {
        // util/coding_test.cc / coding_lean.h i64ToZigzag
        (long Signed, ulong Zig)[] cases = [(0, 0), (-1, 1), (1, 2), (-2, 3), (2, 4), (long.MaxValue, ulong.MaxValue - 1), (long.MinValue, ulong.MaxValue)];
        foreach ((long s, ulong z) in cases)
        {
            Assert.AreEqual(z, Coding.ZigZagEncode64(s));
            Assert.AreEqual(s, Coding.ZigZagDecode64(z));
        }
    }
}
