using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// A simple user-defined index for tests: the last user key of every data block, binary searched with the table's
/// comparator. Serialized as varint32 count, then per block varint32 key length, key, varint64 offset, varint64 size.
/// </summary>
internal sealed class SeparatorListIndexFactory : UserDefinedIndexFactory
{
    public int IteratorsCreated;
    public int KeysAdded;
    public override string Name => "test.SeparatorList";

    public override UserDefinedIndexBuilder NewBuilder(UserDefinedIndexOptions options) => new Builder(this);

    public override UserDefinedIndexReader NewReader(UserDefinedIndexOptions options, ReadOnlyMemory<byte> indexContents) =>
        new Reader(this, options.Comparator, indexContents);

    private sealed class Builder(SeparatorListIndexFactory factory) : UserDefinedIndexBuilder
    {
        private readonly List<(byte[] Key, BlockHandle Handle)> _entries = new();
        private readonly ArrayBufferWriter<byte> _out = new();

        public override ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNextBlock,
            BlockHandle blockHandle, in UserDefinedIndexEntryContext context)
        {
            _entries.Add((lastKeyInCurrentBlock.ToArray(), blockHandle));
            return lastKeyInCurrentBlock;
        }

        public override void OnKeyAdded(ReadOnlySpan<byte> userKey, UserDefinedIndexValueType type, ReadOnlySpan<byte> value) =>
            Interlocked.Increment(ref factory.KeysAdded);

        public override ReadOnlySpan<byte> Finish()
        {
            Coding.PutVarint32(_out, (uint)_entries.Count);
            foreach ((byte[] k, BlockHandle h) in _entries)
            {
                Coding.PutVarint32(_out, (uint)k.Length);
                _out.Write(k);
                h.EncodeTo(_out);
            }
            return _out.WrittenSpan;
        }

        public override ulong EstimatedSize => (ulong)_entries.Count * 32;
    }

    private sealed class Reader : UserDefinedIndexReader
    {
        private readonly SeparatorListIndexFactory _factory;
        private readonly Comparator _ucmp;
        public readonly List<(byte[] Key, BlockHandle Handle)> Entries = new();

        public Reader(SeparatorListIndexFactory factory, Comparator ucmp, ReadOnlyMemory<byte> contents)
        {
            _factory = factory;
            _ucmp = ucmp;
            ReadOnlySpan<byte> s = contents.Span;
            if (!Coding.GetVarint32(ref s, out uint n)) throw RocksDbException.Corruption("bad test index");
            for (uint i = 0; i < n; i++)
            {
                if (!Coding.GetVarint32(ref s, out uint len) || s.Length < len) throw RocksDbException.Corruption("bad test index");
                byte[] k = s[..(int)len].ToArray();
                s = s[(int)len..];
                if (!BlockHandle.TryDecode(ref s, out BlockHandle h)) throw RocksDbException.Corruption("bad test index");
                Entries.Add((k, h));
            }
        }

        public override UserDefinedIndexIterator NewIterator(ReadOptions? readOptions)
        {
            Interlocked.Increment(ref _factory.IteratorsCreated);
            return new Iter(this);
        }

        public override ulong ApproximateMemoryUsage => (ulong)Entries.Count * 48;

        private sealed class Iter(Reader r) : UserDefinedIndexIterator
        {
            private int _pos = -1;
            private bool At(int pos)
            {
                _pos = pos;
                return pos >= 0 && pos < r.Entries.Count;
            }

            public override bool Seek(ReadOnlySpan<byte> targetUserKey, ulong targetTag)
            {
                int lo = 0, hi = r.Entries.Count;
                while (lo < hi)
                {
                    int mid = (lo + hi) / 2;
                    if (r._ucmp.Compare(r.Entries[mid].Key, targetUserKey) < 0) lo = mid + 1;
                    else hi = mid;
                }
                return At(lo);
            }

            public override bool SeekToFirst() => At(0);
            public override bool SeekToLast() => At(r.Entries.Count - 1);
            public override bool Next() => At(_pos + 1);
            public override bool Prev() => At(_pos - 1);
            public override ReadOnlySpan<byte> Key => r.Entries[_pos].Key;
            public override BlockHandle Value => r.Entries[_pos].Handle;
        }
    }
}

/// <summary>
/// User-defined index of block-based tables (include/rocksdb/index_factory.h, user_defined_index_wrapper.h): building
/// with every index_mode, read routing (index_mode, ReadOptions.ReadIndex), the kCustomOnly footer feature and the
/// open-time errors of BlockBasedTable::Open, and a DB using kCustomOnly end to end.
/// </summary>
[TestClass]
public sealed class UserDefinedIndexTests
{
    private static List<(byte[] Key, byte[] Value)> Entries(int n)
    {
        var list = new List<(byte[], byte[])>();
        for (int i = 0; i < n; i++)
        {
            byte[] uk = Encoding.ASCII.GetBytes("key" + (i * 3).ToString("D6", System.Globalization.CultureInfo.InvariantCulture));
            byte[] ik = new byte[uk.Length + 8];
            DbFormat.AppendInternalKey(ik, uk, (ulong)(i + 1), EntryType.Value);
            list.Add((ik, Encoding.ASCII.GetBytes("value" + i + new string('v', i % 60))));
        }
        return list;
    }

    private static byte[] Build(TableFormatOptions format, List<(byte[] Key, byte[] Value)> entries)
    {
        var f = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(f, format, new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
        }
        return f.ToArray();
    }

    private static TableFormatOptions Format(SeparatorListIndexFactory? factory, BlockBasedIndexMode mode, int formatVersion = 7) => new()
    {
        BlockSize = 256,
        FormatVersion = formatVersion,
        UserDefinedIndexFactory = factory,
        IndexMode = mode,
    };

    private static void VerifyReads(BlockBasedTableReader reader, List<(byte[] Key, byte[] Value)> entries, TableReadFlags flags)
    {
        var buf = new ArrayBufferWriter<byte>();
        for (int i = 0; i < entries.Count; i += 7)
        {
            buf.Clear();
            byte[] uk = entries[i].Key[..^8];
            byte[] lk = new byte[uk.Length + 8];
            DbFormat.AppendInternalKey(lk, uk, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            Assert.IsTrue(reader.Get(lk, buf, flags).Found);
            CollectionAssert.AreEqual(entries[i].Value, buf.WrittenSpan.ToArray());
            // A missing key between two existing ones.
            byte[] missing = Encoding.ASCII.GetBytes(Encoding.ASCII.GetString(uk) + "x");
            byte[] mk = new byte[missing.Length + 8];
            DbFormat.AppendInternalKey(mk, missing, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            Assert.IsFalse(reader.Get(mk, null, flags).Found);
        }
        using BlockBasedTableIterator it = reader.NewIterator(flags);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) CollectionAssert.AreEqual(entries[n++].Key, it.Key.ToArray());
        Assert.AreEqual(entries.Count, n);
        it.Seek(entries[entries.Count / 2].Key);
        CollectionAssert.AreEqual(entries[entries.Count / 2].Key, it.Key.ToArray());
        it.SeekToLast();
        CollectionAssert.AreEqual(entries[^1].Key, it.Key.ToArray());
        it.Prev();
        CollectionAssert.AreEqual(entries[^2].Key, it.Key.ToArray());
    }

    private static BlockBasedTableReader Open(byte[] file, SeparatorListIndexFactory? factory, BlockBasedIndexMode mode) =>
        BlockBasedTableReader.Open(new MemRandomAccessFile(file), new TableReaderOptions { UserDefinedIndexFactory = factory, IndexMode = mode });

    [TestMethod]
    public void StandardDefault_BuildsBothIndexes_ReadIndexSelectsCustom()
    {
        var factory = new SeparatorListIndexFactory();
        List<(byte[] Key, byte[] Value)> entries = Entries(2000);
        byte[] file = Build(Format(factory, BlockBasedIndexMode.StandardDefault), entries);
        Assert.AreEqual(entries.Count, factory.KeysAdded);
        using BlockBasedTableReader reader = Open(file, factory, BlockBasedIndexMode.StandardDefault);
        Assert.IsTrue(reader.MetaBlocks.ContainsKey(UserDefinedIndexFactory.MetaBlockPrefix + factory.Name));
        Assert.AreEqual(0UL, reader.Properties.UdiIsPrimaryIndex);
        Assert.IsNotNull(reader.UserDefinedIndex);
        VerifyReads(reader, entries, TableReadFlags.None);
        Assert.AreEqual(0, factory.IteratorsCreated, "kDefault reads use the standard index");
        VerifyReads(reader, entries, TableReadFlags.PreferCustomIndex);
        Assert.IsTrue(factory.IteratorsCreated > 0);
        // Without a factory the file reads through the standard index.
        using BlockBasedTableReader plain = Open(file, null, BlockBasedIndexMode.StandardDefault);
        VerifyReads(plain, entries, TableReadFlags.None);
    }

    [TestMethod]
    public void CustomDefault_RoutesDefaultReadsThroughCustomIndex()
    {
        var factory = new SeparatorListIndexFactory();
        List<(byte[] Key, byte[] Value)> entries = Entries(1500);
        byte[] file = Build(Format(factory, BlockBasedIndexMode.CustomDefault), entries);
        using BlockBasedTableReader reader = Open(file, factory, BlockBasedIndexMode.CustomDefault);
        Assert.AreEqual(1UL, reader.Properties.UdiIsPrimaryIndex);
        Assert.AreEqual(0UL, reader.Properties.StandardIndexIsStub);
        VerifyReads(reader, entries, TableReadFlags.None);
        int created = factory.IteratorsCreated;
        Assert.IsTrue(created > 0);
        VerifyReads(reader, entries, TableReadFlags.BuiltinIndex);
        Assert.AreEqual(created, factory.IteratorsCreated, "kBuiltin reads use the standard index");
        // Rollback: lowering the mode sends reads back to the populated standard index.
        using BlockBasedTableReader rolledBack = Open(file, factory, BlockBasedIndexMode.StandardOnly);
        VerifyReads(rolledBack, entries, TableReadFlags.None);
    }

    [TestMethod]
    public void CustomOnly_FooterFeatureStubIndexAndOpenErrors()
    {
        var factory = new SeparatorListIndexFactory();
        List<(byte[] Key, byte[] Value)> entries = Entries(1200);
        byte[] file = Build(Format(factory, BlockBasedIndexMode.CustomOnly), entries);
        Footer footer = Footer.Decode(file.AsSpan(file.Length - Footer.EncodedLength), (ulong)(file.Length - Footer.EncodedLength));
        Assert.AreEqual(Footer.FeatureRequireUserDefinedIndex, footer.IncompatibleFeatures);
        using (BlockBasedTableReader reader = Open(file, factory, BlockBasedIndexMode.StandardDefault))
        {
            Assert.AreEqual(1UL, reader.Properties.StandardIndexIsStub);
            Assert.AreEqual(1UL, reader.Properties.UdiIsPrimaryIndex);
            // A stub standard index keeps routing reads to the custom index even with a lower index_mode.
            VerifyReads(reader, entries, TableReadFlags.None);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
                () => reader.Get(entries[0].Key, null, TableReadFlags.BuiltinIndex)).Code);
        }
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => Open(file, null, BlockBasedIndexMode.StandardDefault)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => Open(file, factory, BlockBasedIndexMode.StandardOnly)).Code);
        // kCustomOnly needs format_version >= 6 (the footer feature flag); kCustom* modes need a factory.
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => Build(Format(factory, BlockBasedIndexMode.CustomOnly, formatVersion: 5), entries)).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => Build(Format(null, BlockBasedIndexMode.CustomDefault), entries)).Code);
        // Tampering with the feature flags is caught by the footer checksum.
        byte[] bad = (byte[])file.Clone();
        Coding.EncodeFixed64(bad.AsSpan(bad.Length - Footer.EncodedLength + 33), 3);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(
            () => Footer.Decode(bad.AsSpan(bad.Length - Footer.EncodedLength), (ulong)(bad.Length - Footer.EncodedLength))).Code);
    }

    [TestMethod]
    public void StandardRequired_FailsWithoutCustomIndexBlock()
    {
        var factory = new SeparatorListIndexFactory();
        List<(byte[] Key, byte[] Value)> entries = Entries(300);
        byte[] file = Build(Format(null, BlockBasedIndexMode.StandardDefault), entries);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Open(file, factory, BlockBasedIndexMode.StandardRequired)).Code);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => Open(file, factory, BlockBasedIndexMode.CustomDefault)).Code);
        using BlockBasedTableReader ok = Open(file, factory, BlockBasedIndexMode.StandardDefault);
        Assert.IsNull(ok.UserDefinedIndex);
        VerifyReads(ok, entries, TableReadFlags.PreferCustomIndex);
    }

    [TestMethod]
    public void Db_CustomOnly_FlushCompactReopen()
    {
        var factory = new SeparatorListIndexFactory();
        var fs = DbTestUtil.NewFs();
        ColumnFamilyOptions Cf()
        {
            var cf = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024 };
            cf.TableOptions.BlockSize = 512;
            cf.TableOptions.UserDefinedIndexFactory = factory;
            cf.TableOptions.IndexMode = BlockBasedIndexMode.CustomOnly;
            return cf;
        }
        var rnd = new Random(5);
        var oracle = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int round = 0; round < 3; round++)
        {
            using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), Cf());
            for (int i = 0; i < 4000; i++)
            {
                string k = "k" + rnd.Next(3000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                if (rnd.Next(5) == 0)
                {
                    db.Delete(DbTestUtil.B(k));
                    oracle.Remove(k);
                }
                else
                {
                    string v = "v" + rnd.Next();
                    db.Put(DbTestUtil.B(k), DbTestUtil.B(v));
                    oracle[k] = v;
                }
            }
            db.Flush();
            if (round == 1) db.CompactRange();
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
            using Iterator it = db.NewIterator();
            var keys = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(Encoding.ASCII.GetString(it.Key));
            CollectionAssert.AreEqual(new List<string>(oracle.Keys), keys);
        }
        Assert.IsTrue(factory.IteratorsCreated > 0);
        // Reopening without the factory fails: the files require their custom index.
        var noFactory = new ColumnFamilyOptions();
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), noFactory).Dispose()).Code);
    }
}
