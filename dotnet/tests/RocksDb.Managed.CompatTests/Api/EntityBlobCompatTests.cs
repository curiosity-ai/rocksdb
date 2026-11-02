using System.Buffers;
using System.Globalization;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Wide-column entities with blob-backed columns (version 2 entities) across implementations. librocksdb 11.12 separates
/// large entity columns into blob files at flush/compaction (min_blob_size), so both sides write version 2 entities:
/// native-written ones are read, compacted and garbage collected by managed and vice versa. Entities are handed to native
/// as WriteBatch reps (the 11.12 C API has no PutEntity); native reads only the default column (Get / iterator value),
/// the other columns are verified by managed after every native step.
/// </summary>
[TestClass]
public sealed class EntityBlobCompatTests
{
    private const ulong MinBlobSize = 64;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static WideColumn C(string n, string v) => new(U(n), U(v));

    private static string Key(int i) => "e" + i.ToString("D4", CultureInfo.InvariantCulture);

    /// <summary>Entity i: a default column (large every other key), a small column and a large column.</summary>
    private static WideColumn[] Entity(int i, string tag) =>
    [
        C("", i % 2 == 0 ? tag + "-d" + i.ToString(CultureInfo.InvariantCulture) + new string('d', 100 + i % 50) : tag + "-small"),
        C("small", "s" + i.ToString(CultureInfo.InvariantCulture)),
        C("zlarge", tag + new string((char)('a' + i % 26), 80 + i % 120)),
    ];

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private static string Describe(WideColumn[] cols)
    {
        var sb = new StringBuilder();
        foreach (WideColumn c in cols) sb.Append(Encoding.UTF8.GetString(c.Name.Span)).Append('=').Append(Encoding.UTF8.GetString(c.Value.Span)).Append(';');
        return sb.ToString();
    }

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        RocksDbSharp.ColumnFamilyOptions o = CfOptions(op);
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_enable_blob_files(o.Handle, 1);
        n.rocksdb_options_set_min_blob_size(o.Handle, MinBlobSize);
        n.rocksdb_options_set_blob_file_size(o.Handle, 16 * 1024);
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        GC.KeepAlive(options);
        return RocksDbSharp.RocksDb.Open(options, path, new RocksDbSharp.ColumnFamilies(o));
    }

    private static RocksDatabase OpenManaged(string path) => RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
        [new("default", new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = MinBlobSize, BlobFileSize = 16 * 1024, MergeOperator = new StringAppend() })]);

    private static void NativeForceGcCompact(RocksDbSharp.RocksDb db)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr opt = n.rocksdb_compactoptions_create();
        try
        {
            n.rocksdb_compactoptions_set_blob_garbage_collection_policy(opt, 0); // kForce
            n.rocksdb_compactoptions_set_blob_garbage_collection_age_cutoff(opt, 1.0);
            n.rocksdb_compactoptions_set_bottommost_level_compaction(opt, 2); // kForce
            n.rocksdb_compact_range_opt(db.Handle, opt, (byte[]?)null, UIntPtr.Zero, (byte[]?)null, UIntPtr.Zero);
        }
        finally
        {
            n.rocksdb_compactoptions_destroy(opt);
        }
    }

    /// <summary>Counts the entities of the table files stored as version 2 with blob-backed columns.</summary>
    private static int CountV2EntitiesInTables(string dir)
    {
        IFileSystem fs = LocalFileSystem.Instance;
        int n = 0;
        foreach (string c in Directory.GetFiles(dir, "*.sst"))
        {
            using BlockBasedTableReader reader = BlockBasedTableReader.Open(fs.OpenRandomAccess(c), new TableReaderOptions());
            using BlockBasedTableIterator it = reader.NewIterator();
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if ((EntryType)(byte)DbFormat.ExtractFooter(it.Key) != EntryType.WideColumnEntity) continue;
                if (WideColumnSerialization.HasBlobColumns(it.Value)) n++;
            }
        }
        return n;
    }

    /// <summary>Model: key -> sorted columns description (null = deleted).</summary>
    private static void VerifyManaged(RocksDatabase db, Dictionary<string, WideColumn[]?> model, string phase)
    {
        foreach ((string k, WideColumn[]? cols) in model)
        {
            WideColumns? e = db.GetEntity(U(k));
            if (cols is null)
            {
                Assert.IsNull(e, phase + " " + k);
                continue;
            }
            Assert.AreEqual(Describe(cols), Describe(e!), phase + " " + k);
            Assert.AreEqual(Encoding.UTF8.GetString(cols[0].Value.Span), Encoding.UTF8.GetString(db.Get(U(k))!), phase + " get " + k);
        }
        int live = 0;
        foreach (WideColumn[]? v in model.Values) if (v is not null) live++;
        using Iterator it = db.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            WideColumn[] cols = model[Encoding.UTF8.GetString(it.Key)]!;
            Assert.AreEqual(Describe(cols), Describe(it.Columns), phase + " iter");
            n++;
        }
        Assert.AreEqual(live, n, phase);
    }

    private static void VerifyNative(RocksDbSharp.RocksDb db, Dictionary<string, WideColumn[]?> model, string phase)
    {
        foreach ((string k, WideColumn[]? cols) in model)
        {
            byte[]? v = db.Get(U(k));
            if (cols is null) Assert.IsNull(v, phase + " " + k);
            else Assert.AreEqual(Encoding.UTF8.GetString(cols[0].Value.Span), Encoding.UTF8.GetString(v!), phase + " native get " + k);
        }
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next())
        {
            WideColumn[] cols = model[Encoding.UTF8.GetString(it.Key())]!;
            Assert.AreEqual(Encoding.UTF8.GetString(cols[0].Value.Span), Encoding.UTF8.GetString(it.Value()), phase + " native iter");
        }
    }

    private static void PutEntities(RocksDbSharp.RocksDb db, Dictionary<string, WideColumn[]?> model, int from, int to, string tag)
    {
        using WriteBatch b = new();
        for (int i = from; i < to; i++)
        {
            WideColumn[] cols = Entity(i, tag);
            b.PutEntity(U(Key(i)), cols);
            model[Key(i)] = cols;
        }
        using var nb = new RocksDbSharp.WriteBatch(b.Data.ToArray());
        db.Write(nb);
    }

    private static void PutEntities(RocksDatabase db, Dictionary<string, WideColumn[]?> model, int from, int to, string tag)
    {
        for (int i = from; i < to; i++)
        {
            WideColumn[] cols = Entity(i, tag);
            db.PutEntity(U(Key(i)), cols);
            model[Key(i)] = cols;
        }
    }

    [TestMethod]
    public void NativeV2EntitiesReadAndGarbageCollectedByManaged()
    {
        using var dir = new TempDir("ent-blob-n");
        var model = new Dictionary<string, WideColumn[]?>();
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path))
        {
            PutEntities(db, model, 0, 200, "n1");
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            PutEntities(db, model, 0, 100, "n2"); // overwrites: garbage in the first blob files after compaction
            for (int i = 200; i < 210; i++)
            {
                db.Remove(U(Key(i - 100)));
                model[Key(i - 100)] = null;
            }
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.CompactRange((byte[]?)null, null);
            VerifyNative(db, model, "native");
        }
        // Native separated the large columns: the stored entities are version 2.
        Assert.IsTrue(CountV2EntitiesInTables(dir.Path) >= 100);
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            VerifyManaged(db, model, "managed reads native");
            Assert.IsTrue(ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture) > 0);
            db.CompactRange(new CompactRangeOptions
            {
                BlobGarbageCollectionPolicy = BlobGarbageCollectionPolicy.Force,
                BlobGarbageCollectionAgeCutoff = 1.0,
                BottommostLevelCompaction = BottommostLevelCompaction.Force,
            }, null, null);
            Assert.AreEqual(0UL, ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture));
            VerifyManaged(db, model, "managed after GC");
            PutEntities(db, model, 150, 250, "m1");
            db.Merge(U(Key(0)), U("op")); // merge on an entity with a blob-backed default column
            model[Key(0)] = [C("", Encoding.UTF8.GetString(model[Key(0)]![0].Value.Span) + ",op"), model[Key(0)]![1], model[Key(0)]![2]];
            db.Flush();
            VerifyManaged(db, model, "managed extended");
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path))
        {
            VerifyNative(db, model, "native reads managed GC output");
            NativeForceGcCompact(db);
            VerifyNative(db, model, "native after GC");
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            VerifyManaged(db, model, "managed final");
            Assert.IsTrue(CountV2EntitiesInTables(dir.Path) >= 100);
        }
    }

    [TestMethod]
    public void ManagedV2EntitiesReadAndGarbageCollectedByNative()
    {
        using var dir = new TempDir("ent-blob-m");
        var model = new Dictionary<string, WideColumn[]?>();
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            PutEntities(db, model, 0, 200, "m1");
            db.Flush();
            PutEntities(db, model, 50, 120, "m2");
            db.Delete(U(Key(5)));
            model[Key(5)] = null;
            db.Flush();
            db.CompactRange(); // garbage recorded by managed for entity blob references
            Assert.IsTrue(ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture) > 0);
            VerifyManaged(db, model, "managed");
        }
        Assert.IsTrue(CountV2EntitiesInTables(dir.Path) >= 100);
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path))
        {
            VerifyNative(db, model, "native reads managed");
            db.Merge(U(Key(2)), U("nop"));
            model[Key(2)] = [C("", Encoding.UTF8.GetString(model[Key(2)]![0].Value.Span) + ",nop"), model[Key(2)]![1], model[Key(2)]![2]];
            PutEntities(db, model, 180, 220, "n1");
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            NativeForceGcCompact(db); // native GC of managed-written entity blob columns (uses managed garbage metadata)
            VerifyNative(db, model, "native after GC");
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            VerifyManaged(db, model, "managed reads native GC output");
            Assert.AreEqual(0UL, ulong.Parse(db.GetProperty(DbProperties.LiveBlobFileGarbageSize)!, CultureInfo.InvariantCulture));
        }
    }

    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result)
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
}
