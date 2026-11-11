using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice UtilMemtable: memtable (db/memtable.cc), skiplist iterator, comparators / internal key separators
/// and compression framing (util/compression.cc) against the C++ sources.
/// </summary>
[TestClass]
public sealed class Audit4UtilMemtableTests
{
    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static byte[] Packed(string value, ulong preferredSeqno)
    {
        byte[] v = B(value);
        byte[] r = new byte[v.Length + 8];
        v.CopyTo(r, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(r.AsSpan(v.Length), preferredSeqno);
        return r;
    }

    // ------------------------------------------------------------------ F1: TimedPut read under the in-place lock

    [TestMethod]
    [DataRow(EntryType.Value)]
    [DataRow(EntryType.ValuePreferredSeqno)]
    public void InplaceUpdateReadCopiesValueUnderTheReadLock(EntryType type)
    {
        // SaveValue (db/memtable.cc) takes GetLock(user_key)->ReadLock() for every entry of the key when
        // inplace_update_support is set and copies the value while holding it (HandleTypeValue for kTypeValue AND
        // kTypeValuePreferredSeqno). WriteBatch's PutCFImpl sends TimedPut entries through MemTable::Update, so they are
        // overwritten in place too: a value handed out as a pointer into the arena would change (or tear) under the
        // reader. The value read must not alias the entry.
        using var mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions { InplaceUpdateSupport = true }, 0);
        bool timed = type == EntryType.ValuePreferredSeqno;
        byte[] v1 = timed ? Packed("aaaa", 3) : B("aaaa");
        byte[] v2 = timed ? Packed("bbbb", 3) : B("bbbb");
        Assert.IsTrue(mem.Add(10, type, B("k"), v1));

        using var lk = new LookupKey(B("k"), 100, stackalloc byte[LookupKey.StackBufferSize]);
        ulong maxCovering = 0;
        MemTableGetResult r = mem.Get(in lk, out ReadOnlySpan<byte> value, out EntryType vt, out ulong seq, null, ref maxCovering);
        Assert.AreEqual(MemTableGetResult.Found, r);
        Assert.AreEqual(EntryType.Value, vt);
        Assert.AreEqual(10UL, seq);
        Assert.AreEqual("aaaa", System.Text.Encoding.UTF8.GetString(value));

        // Same size: MemTable::Update overwrites in place, the existing sequence number prevails.
        Assert.IsTrue(mem.Update(11, type, B("k"), v2));
        Assert.AreEqual(1L, mem.NumEntries, "updated in place");
        Assert.AreEqual("aaaa", System.Text.Encoding.UTF8.GetString(value), "the value read earlier must be a copy");

        r = mem.Get(in lk, out value, out _, out seq, null, ref maxCovering);
        Assert.AreEqual(MemTableGetResult.Found, r);
        Assert.AreEqual(10UL, seq);
        Assert.AreEqual("bbbb", System.Text.Encoding.UTF8.GetString(value));
    }

    // ------------------------------------------------------------------ F2: UpdateWriteBufferSize

    private static bool FullAfter(MemTableOptions options, long newWriteBufferSize, int kilobytes)
    {
        using var mem = new MemTable(InternalKeyComparator.Bytewise, options, 0);
        mem.UpdateWriteBufferSize(newWriteBufferSize);
        byte[] value = new byte[1000];
        Span<byte> key = stackalloc byte[8];
        for (int i = 0; i < kilobytes; i++)
        {
            BinaryPrimitives.WriteUInt64BigEndian(key, (ulong)i);
            Assert.IsTrue(mem.Add((ulong)i + 1, EntryType.Value, key, value));
        }
        return mem.ShouldFlushNow();
    }

    [TestMethod]
    public void UpdateWriteBufferSizeDoesNotGrowAMemtableWithBloom()
    {
        // MemTable::UpdateWriteBufferSize (db/memtable.h):
        //   if (bloom_filter_ == nullptr || new_write_buffer_size < write_buffer_size_) write_buffer_size_ = new;
        // The bloom filter was sized for the old write_buffer_size, so a memtable with one never grows.
        const long small = 256 << 10, large = 64L << 20;
        MemTableOptions WithBloom(long wbs) => new() { WriteBufferSize = wbs, ArenaBlockSize = 4096, WholeKeyFiltering = true, PrefixBloomBits = 4096 };
        MemTableOptions NoBloom(long wbs) => new() { WriteBufferSize = wbs, ArenaBlockSize = 4096 };

        Assert.IsTrue(FullAfter(WithBloom(small), small, 400), "baseline: 400 KB fill a 256 KB memtable");
        Assert.IsTrue(FullAfter(WithBloom(small), large, 400), "with a bloom filter the larger size is ignored");
        Assert.IsFalse(FullAfter(NoBloom(small), large, 400), "without a bloom filter the memtable grows");
        Assert.IsTrue(FullAfter(WithBloom(large), small, 400), "shrinking always applies");
        Assert.IsTrue(FullAfter(NoBloom(large), small, 400), "shrinking always applies");
    }

    // ------------------------------------------------------------------ extra coverage: UpdateCallback

    [TestMethod]
    public void UpdateCallbackShrinkAcrossVarintBoundaryKeepsEntryConsistent()
    {
        // MemTable::UpdateCallback, UPDATED_INPLACE with new_prev_size < prev_size: the value length varint is rewritten
        // and, when it got shorter (200 -> 100: 2 bytes -> 1 byte), the value is shifted down; the entry checksum is
        // recomputed for the existing sequence number (UpdateEntryChecksum(nullptr, ..., existing_seq, ...)).
        using var mem = new MemTable(InternalKeyComparator.Bytewise,
            new MemTableOptions { InplaceUpdateSupport = true, ProtectionBytesPerKey = 8 }, 0);
        byte[] big = new byte[200];
        Array.Fill(big, (byte)'x');
        Assert.IsTrue(mem.Add(5, EntryType.Value, B("k"), big));
        Assert.IsTrue(mem.Add(6, EntryType.Value, B("l"), B("next")));

        InplaceUpdateCallback shrink = static (Span<byte> existing, bool has, ref int size, ReadOnlySpan<byte> delta, IBufferWriter<byte> merged) =>
        {
            Assert.IsTrue(has);
            Assert.AreEqual(200, size);
            existing[..100].Fill((byte)'y');
            size = 100;
            return InplaceUpdateStatus.UpdatedInplace;
        };
        Assert.AreEqual(MemTable.UpdateCallbackResult.Done, mem.UpdateCallback(9, B("k"), B("d"), shrink));
        Assert.AreEqual(2L, mem.NumEntries);

        using var lk = new LookupKey(B("k"), 100, stackalloc byte[LookupKey.StackBufferSize]);
        ulong maxCovering = 0;
        // Get verifies the per-entry checksum (memtable_protection_bytes_per_key).
        Assert.AreEqual(MemTableGetResult.Found, mem.Get(in lk, out ReadOnlySpan<byte> value, out _, out ulong seq, null, ref maxCovering));
        Assert.AreEqual(5UL, seq, "the existing sequence number prevails");
        Assert.AreEqual(100, value.Length);
        Assert.IsTrue(value.IndexOfAnyExcept((byte)'y') < 0);

        // The iterator verifies every entry and must still step over the shrunk one.
        MemTableIterator it = mem.NewIterator();
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual(100, it.Value.Length);
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("next", System.Text.Encoding.UTF8.GetString(it.Value));

        // The newest entry is not a value: Status::NotFound (the caller then reads the DB).
        Assert.IsTrue(mem.Add(20, EntryType.Deletion, B("k"), default));
        Assert.AreEqual(MemTable.UpdateCallbackResult.NotFound, mem.UpdateCallback(21, B("k"), B("d"), shrink));
        Assert.AreEqual(MemTable.UpdateCallbackResult.NotFound, mem.UpdateCallback(22, B("zz"), B("d"), shrink));
    }

    [TestMethod]
    public void UpdateAddsWhenTypeDiffersOrValueGrows()
    {
        // MemTable::Update: in place only when the newest entry has the same type and new_size <= prev_size; otherwise
        // Add (a new entry at the new sequence number).
        using var mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions { InplaceUpdateSupport = true }, 0);
        Assert.IsTrue(mem.Update(1, EntryType.Value, B("k"), B("12345")));
        Assert.AreEqual(1L, mem.NumEntries);
        Assert.IsTrue(mem.Update(2, EntryType.Value, B("k"), B("123456"))); // grows -> Add
        Assert.AreEqual(2L, mem.NumEntries);
        Assert.IsTrue(mem.Update(3, EntryType.WideColumnEntity, B("k"), B("1"))); // other type -> Add
        Assert.AreEqual(3L, mem.NumEntries);
        Assert.IsTrue(mem.Update(4, EntryType.WideColumnEntity, B("k"), default)); // empty value fits in place
        Assert.AreEqual(3L, mem.NumEntries);

        using var lk = new LookupKey(B("k"), 100, stackalloc byte[LookupKey.StackBufferSize]);
        ulong maxCovering = 0;
        Assert.AreEqual(MemTableGetResult.Found, mem.Get(in lk, out ReadOnlySpan<byte> value, out EntryType vt, out ulong seq, null, ref maxCovering));
        Assert.AreEqual(EntryType.WideColumnEntity, vt);
        Assert.AreEqual(3UL, seq);
        Assert.AreEqual(0, value.Length);
        // A snapshot below the in-place entry still sees the older version.
        using var lk2 = new LookupKey(B("k"), 2, stackalloc byte[LookupKey.StackBufferSize]);
        Assert.AreEqual(MemTableGetResult.Found, mem.Get(in lk2, out value, out vt, out seq, null, ref maxCovering));
        Assert.AreEqual(2UL, seq);
        Assert.AreEqual("123456", System.Text.Encoding.UTF8.GetString(value));
    }

    // ------------------------------------------------------------------ extra coverage: memtable iterator vs model

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void MemTableIteratorSeekAndSeekForPrevMatchSortedModel(bool reverse)
    {
        // MemTableIterator::Seek / SeekForPrev / Prev over InlineSkipList (FindGreaterOrEqual, FindLessThan): compared
        // with a sorted list for random internal-key targets, including targets before the first and after the last key.
        var icmp = new InternalKeyComparator(reverse ? Comparator.ReverseBytewise : Comparator.Bytewise);
        using var mem = new MemTable(icmp, new MemTableOptions(), 0);
        var rnd = new Random(20240611);
        var model = new List<byte[]>();
        ulong seq = 1;
        for (int i = 0; i < 600; i++)
        {
            byte[] uk = new byte[rnd.Next(0, 4)];
            for (int j = 0; j < uk.Length; j++) uk[j] = (byte)(rnd.Next(4) == 0 ? 0xff : rnd.Next(3));
            Assert.IsTrue(mem.Add(seq, EntryType.Value, uk, B("v")));
            byte[] ik = new byte[uk.Length + 8];
            DbFormat.AppendInternalKey(ik, uk, seq, EntryType.Value);
            model.Add(ik);
            seq++;
        }
        model.Sort((a, b) => icmp.Compare(a, b));

        MemTableIterator it = mem.NewIterator();
        it.SeekToFirst();
        foreach (byte[] ik in model)
        {
            Assert.IsTrue(it.Valid);
            Assert.IsTrue(it.Key.SequenceEqual(ik));
            it.Next();
        }
        Assert.IsFalse(it.Valid);
        it.SeekToLast();
        for (int i = model.Count - 1; i >= 0; i--)
        {
            Assert.IsTrue(it.Valid);
            Assert.IsTrue(it.Key.SequenceEqual(model[i]));
            it.Prev();
        }
        Assert.IsFalse(it.Valid);

        Span<byte> target = stackalloc byte[16];
        for (int t = 0; t < 2000; t++)
        {
            int len = rnd.Next(0, 5);
            for (int j = 0; j < len; j++) target[j] = (byte)(rnd.Next(4) == 0 ? 0xff : rnd.Next(3));
            ulong tseq = rnd.Next(3) == 0 ? DbFormat.MaxSequenceNumber : (ulong)rnd.Next(0, (int)seq + 2);
            bool forPrev = (t & 1) != 0;
            int n = DbFormat.AppendInternalKey(target, target[..len], tseq, forPrev ? DbFormat.ValueTypeForSeekForPrev : DbFormat.ValueTypeForSeek);
            ReadOnlySpan<byte> tk = target[..n];
            int expected = -1;
            if (!forPrev)
            {
                for (int i = 0; i < model.Count; i++)
                {
                    if (icmp.Compare(model[i], tk) >= 0) { expected = i; break; }
                }
                it.Seek(tk);
            }
            else
            {
                for (int i = model.Count - 1; i >= 0; i--)
                {
                    if (icmp.Compare(model[i], tk) <= 0) { expected = i; break; }
                }
                it.SeekForPrev(tk);
            }
            Assert.AreEqual(expected >= 0, it.Valid, "target " + Convert.ToHexString(tk) + " forPrev=" + forPrev);
            if (expected >= 0) Assert.IsTrue(it.Key.SequenceEqual(model[expected]), "target " + Convert.ToHexString(tk) + " forPrev=" + forPrev);
        }
    }

    // ------------------------------------------------------------------ extra coverage: internal key separators

    /// <summary>Bytewise order behind a custom comparator: takes the generic (virtual) path of InternalKeyComparator.</summary>
    private sealed class WrappedBytewise : Comparator
    {
        public override string Name => "audit4.WrappedBytewise";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceCompareTo(b);
        public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result) =>
            Bytewise.FindShortestSeparator(start, limit, result);
        public override void FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result) => Bytewise.FindShortSuccessor(key, result);
    }

    [TestMethod]
    public void InternalKeySeparatorFastPathMatchesGenericPath()
    {
        // ShortenedIndexBuilder::FindShortestInternalKeySeparator / FindShortInternalKeySuccessor
        // (table/block_based/index_builder.cc) over BytewiseComparatorImpl::FindShortestSeparator / FindShortSuccessor
        // (util/comparator.cc). The bytewise fast path of the port must produce the bytes of the generic path (they are
        // persisted in index blocks), and start <= separator < limit.
        InternalKeyComparator fast = InternalKeyComparator.Bytewise;
        var generic = new InternalKeyComparator(new WrappedBytewise());
        var rnd = new Random(77);
        byte[] alphabet = [0x00, 0x01, 0x61, 0x62, 0xfe, 0xff];
        byte[] RandomKey()
        {
            byte[] k = new byte[rnd.Next(0, 6)];
            for (int i = 0; i < k.Length; i++) k[i] = alphabet[rnd.Next(alphabet.Length)];
            return k;
        }
        using var a = new PooledBufferWriter();
        using var b = new PooledBufferWriter();
        for (int t = 0; t < 20000; t++)
        {
            byte[] u1 = RandomKey(), u2 = RandomKey();
            ulong s1 = (ulong)rnd.Next(1, 100), s2 = (ulong)rnd.Next(1, 100);
            byte[] k1 = new byte[u1.Length + 8], k2 = new byte[u2.Length + 8];
            DbFormat.AppendInternalKey(k1, u1, s1, EntryType.Value);
            DbFormat.AppendInternalKey(k2, u2, s2, EntryType.Value);
            if (fast.Compare(k1, k2) > 0) (k1, k2) = (k2, k1);
            if (fast.Compare(k1, k2) == 0) continue;

            a.Clear();
            b.Clear();
            bool sa = fast.FindShortestSeparator(k1, k2, a);
            bool sb = generic.FindShortestSeparator(k1, k2, b);
            string ctx = Convert.ToHexString(k1) + " / " + Convert.ToHexString(k2);
            Assert.AreEqual(sb, sa, ctx);
            Assert.IsTrue(a.WrittenSpan.SequenceEqual(b.WrittenSpan), ctx);
            Assert.IsTrue(fast.Compare(k1, a.WrittenSpan) <= 0, ctx);
            Assert.IsTrue(fast.Compare(a.WrittenSpan, k2) < 0, ctx);
            Assert.IsTrue(a.WrittenCount <= k1.Length, ctx);

            a.Clear();
            b.Clear();
            sa = fast.FindShortSuccessor(k1, a);
            sb = generic.FindShortSuccessor(k1, b);
            Assert.AreEqual(sb, sa, ctx);
            Assert.IsTrue(a.WrittenSpan.SequenceEqual(b.WrittenSpan), ctx);
            Assert.IsTrue(fast.Compare(k1, a.WrittenSpan) <= 0, ctx);
        }

        // Explicit edge cases of BytewiseComparatorImpl::FindShortestSeparator.
        static string Sep(string startHex, string limitHex)
        {
            using var w = new PooledBufferWriter();
            Comparator.Bytewise.FindShortestSeparator(Convert.FromHexString(startHex), Convert.FromHexString(limitHex), w);
            return Convert.ToHexString(w.WrittenSpan);
        }
        Assert.AreEqual("", Sep("", "61"), "empty start is a prefix of limit: unchanged");
        Assert.AreEqual("6161", Sep("6161", "616161"), "prefix: unchanged");
        Assert.AreEqual("616161", Sep("616161", "6161"), "start > limit: unchanged");
        Assert.AreEqual("62", Sep("61FF", "63"), "byte + 1 < limit byte");
        Assert.AreEqual("61FFFF", Sep("61FFFF", "62"), "only 0xff after the differing byte: unchanged");
        Assert.AreEqual("61FF02", Sep("61FF0105", "62"), "skip the differing byte and 0xff, increment the next one");
        Assert.AreEqual("62", Sep("61FF", "6200"), "limit continues after the differing byte: increment it");
        Assert.AreEqual("FE", Sep("FD01", "FF"), "0xfd -> 0xfe below a 0xff limit byte");
        Assert.AreEqual("FE02", Sep("FE01", "FF"), "0xfe + 1 would equal the one-byte limit: increment the next byte");
        Assert.AreEqual("FEFF", Sep("FEFF", "FF"), "nothing left to increment: unchanged");
    }

    // ------------------------------------------------------------------ extra coverage: malformed compressed blocks

    [TestMethod]
    public void MalformedCompressedBlocksAreCorruptionNeverACrash()
    {
        // Decompressor::ExtractUncompressedSize + the *_DecompressBlock helpers (util/compression.cc): a missing or
        // truncated size prefix, an empty payload and a declared size that does not match the payload are
        // Status::Corruption. No other exception type may escape (the table reader maps only RocksDbException).
        CompressionType[] codecs = [CompressionType.Snappy, CompressionType.LZ4, CompressionType.LZ4HC, CompressionType.Zstd, CompressionType.Zlib];
        byte[] raw = new byte[3000];
        for (int i = 0; i < raw.Length; i++) raw[i] = (byte)("abcabcabd"[i % 9] + (i / 500));
        foreach (CompressionType type in codecs)
        {
            using var c = new PooledBufferWriter();
            Assert.IsTrue(BlockCompression.TryCompress(type, raw, 5, BlockCompression.DefaultCompressionLevel, c), type.ToString());
            byte[] good = c.WrittenSpan.ToArray();
            var inputs = new List<(string Name, byte[] Data)>
            {
                ("empty", []),
                ("truncated varint", [0x80]),
                ("unterminated varint", [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
                ("size only", good[..2]),
                ("one payload byte", good[..3]),
                ("garbage", [0x10, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02]),
            };
            // Zlib: see ZlibStreamWithoutEndMarkerIsCorruption.
            if (type != CompressionType.Zlib) inputs.Add(("payload cut by one", good[..^1]));
            if (type != CompressionType.Snappy)
            {
                // Format 2 prefix: varint32(3000) = B8 17. Declare one byte more / one byte less than the payload holds.
                Assert.AreEqual(0xB8, good[0]);
                Assert.AreEqual(0x17, good[1]);
                byte[] larger = (byte[])good.Clone();
                larger[0] = 0xB9;
                inputs.Add(("declared size too large", larger));
                byte[] smaller = (byte[])good.Clone();
                smaller[0] = 0xB7;
                inputs.Add(("declared size too small", smaller));
                byte[] zero = [0x00, .. good[2..]];
                inputs.Add(("declared size zero", zero));
            }
            foreach ((string name, byte[] data) in inputs)
            {
                using var d = new PooledBufferWriter();
                try
                {
                    BlockCompression.Decompress(type, data, 5, d);
                    Assert.Fail(type + " accepted malformed input: " + name);
                }
                catch (RocksDbException e)
                {
                    Assert.AreEqual(StatusCode.Corruption, e.Code, type + " " + name);
                }
                Assert.IsFalse(BlockCompression.TryGetUncompressedLength(type, [], 5, out _), type + " empty length");
            }
        }
    }

    [TestMethod]
    public void ZlibStreamWithoutEndMarkerIsCorruption()
    {
        // Zlib_DecompressBlock (util/compression.cc): `if (st != Z_STREAM_END) return Corruption("Failed zlib inflate")`
        // ("Z_OK is still corruption because it means we got the size wrong"). A raw deflate stream cut inside its last
        // byte (here only the tail of the end-of-block code and padding) still inflates to every output byte; upstream
        // rejects it because the stream did not end, the port accepts it (the bytes returned are the right ones).
        byte[] raw = new byte[3000];
        for (int i = 0; i < raw.Length; i++) raw[i] = (byte)("abcabcabd"[i % 9] + (i / 500));
        using var c = new PooledBufferWriter();
        Assert.IsTrue(BlockCompression.TryCompress(CompressionType.Zlib, raw, 5, BlockCompression.DefaultCompressionLevel, c));
        byte[] cut = c.WrittenSpan[..^1].ToArray();
        using var d = new PooledBufferWriter();
        var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockCompression.Decompress(CompressionType.Zlib, cut, 5, d));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
    }
}
