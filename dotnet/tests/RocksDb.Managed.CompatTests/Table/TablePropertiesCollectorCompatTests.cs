using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// User table properties collectors against native RocksDB 11.12: the "rocksdb.property.collectors" list and user
/// collected properties written by the managed builder (flush and SstFileWriter) are readable by native RocksDB (files
/// opened/ingested and read back), and native files written with the built-in CompactOnDeletionCollector record the same
/// collector list as the managed port of it. (The C API cannot create custom native collectors or read user properties
/// back, so the user property values themselves are verified through the managed reader.)
/// </summary>
[TestClass]
public sealed class TablePropertiesCollectorCompatTests
{
    private static byte[] Key(int i) => Encoding.ASCII.GetBytes("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));
    private static byte[] Value(int i) => Encoding.ASCII.GetBytes("value-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private sealed class SumFactory : TablePropertiesCollectorFactory
    {
        public override string Name => "SumFactory";
        public override TablePropertiesCollector CreateTablePropertiesCollector(TablePropertiesCollectorContext context) => new Sum();

        private sealed class Sum : TablePropertiesCollector
        {
            private ulong _keyBytes, _entries;
            public override string Name => "Sum";
            public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
            {
                _keyBytes += (ulong)key.Length;
                _entries++;
            }
            public override void Finish(IDictionary<string, byte[]> properties)
            {
                properties["sum.key_bytes"] = Encoding.ASCII.GetBytes(_keyBytes.ToString(System.Globalization.CultureInfo.InvariantCulture));
                properties["sum.entries"] = Encoding.ASCII.GetBytes(_entries.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
        }
    }

    private static RocksDbSharp.DbOptions NativeOptions(bool compactOnDeletion)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        o.SetCompression(RocksDbSharp.Compression.No);
        if (compactOnDeletion) RocksDbSharp.Native.Instance.rocksdb_options_add_compact_on_deletion_collector_factory(o.Handle, (UIntPtr)100, (UIntPtr)10);
        return o;
    }

    [TestMethod]
    public void CollectorNamesMatchNativeFlush()
    {
        using var dir = new TempDir("collector-native");
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(compactOnDeletion: true), dir.Path))
        {
            for (int i = 0; i < 300; i++) db.Put(Key(i), Value(i));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        using SstFile native = SstFile.OpenSingle(dir.Path, TableReaderOptions.Default);
        Assert.AreEqual("[CompactOnDeletionCollector]", native.Properties.PropertyCollectorsNames);

        using var mdir = new TempDir("collector-managed");
        var cfo = new ColumnFamilyOptions
        {
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            TablePropertiesCollectorFactories = { new CompactOnDeletionCollectorFactory(100, 10, 0, 0) },
        };
        using (RocksDatabase db = RocksDatabase.Open(mdir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            for (int i = 0; i < 300; i++) db.Put(Key(i), Value(i));
            db.Flush();
        }
        using SstFile managed = SstFile.OpenSingle(mdir.Path, TableReaderOptions.Default);
        Assert.AreEqual(native.Properties.PropertyCollectorsNames, managed.Properties.PropertyCollectorsNames);
    }

    /// <summary>A managed DB whose flushed tables carry user properties opens in native RocksDB and reads back.</summary>
    [TestMethod]
    public void ManagedFlushWithUserPropertiesReadByNative()
    {
        using var dir = new TempDir("collector-flush");
        var cfo = new ColumnFamilyOptions
        {
            Compression = RocksDb.Managed.Compression.CompressionType.None,
            TablePropertiesCollectorFactories = { new SumFactory(), new CompactOnDeletionCollectorFactory(100, 10, 0, 0) },
        };
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, cfo))
        {
            for (int i = 0; i < 500; i++) db.Put(Key(i), Value(i));
            for (int i = 0; i < 500; i += 2) db.Delete(Key(i));
            db.Flush();
            TableProperties p = db.GetPropertiesOfAllTables().Values.Single();
            Assert.AreEqual("[SumFactory,CompactOnDeletionCollector]", p.PropertyCollectorsNames);
            // Flush drops the puts hidden by the deletions: 250 puts + 250 deletions.
            Assert.AreEqual("500", p.GetUserPropertyString("sum.entries"));
            Assert.AreEqual((500 * 9).ToString(System.Globalization.CultureInfo.InvariantCulture), p.GetUserPropertyString("sum.key_bytes"));
        }
        // Native opens the DB (the MANIFEST marks the file for compaction) and reads every key.
        using (var db = RocksDbSharp.RocksDb.Open(NativeOptions(compactOnDeletion: true), dir.Path))
        {
            for (int i = 0; i < 500; i++)
            {
                if (i % 2 == 0) Assert.IsNull(db.Get(Key(i)));
                else CollectionAssert.AreEqual(Value(i), db.Get(Key(i)));
            }
        }
    }

    /// <summary>SstFileWriter with a user collector: native ingests the file; the managed reader sees the properties.</summary>
    [TestMethod]
    public void ManagedSstFileWriterUserPropertiesIngestedByNative()
    {
        using var dir = new TempDir("collector-sst");
        string sst = Path.Combine(dir.Path, "ext.sst");
        var cfo = new ColumnFamilyOptions { TablePropertiesCollectorFactories = { new SumFactory() } };
        using (var w = new SstFileWriter(cfo, LocalFileSystem.Instance))
        {
            w.Open(sst);
            for (int i = 0; i < 200; i++) w.Put(Key(i), Value(i));
            w.Finish();
        }
        using (SstFile f = SstFile.Open(sst, TableReaderOptions.Default))
        {
            Assert.AreEqual("[SumFactory]", f.Properties.PropertyCollectorsNames);
            Assert.AreEqual("200", f.Properties.GetUserPropertyString("sum.entries"));
        }
        using var db = RocksDbSharp.RocksDb.Open(NativeOptions(compactOnDeletion: false), Path.Combine(dir.Path, "db"));
        db.IngestExternalFiles([sst], new RocksDbSharp.IngestExternalFileOptions());
        for (int i = 0; i < 200; i += 3) CollectionAssert.AreEqual(Value(i), db.Get(Key(i)));
    }
}
