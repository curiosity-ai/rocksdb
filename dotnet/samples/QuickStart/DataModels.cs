using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Wide columns, user-defined timestamps, blob files and TTL.</summary>
internal static class DataModels
{
    public static Task WideColumns(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

        // <snippet:wide-columns>
        db.PutEntity("user:1"u8,
        [
            WideColumn.Default("alice"u8.ToArray()),                       // the default column = plain Get value
            new WideColumn("email"u8.ToArray(), "alice@example.com"u8.ToArray()),
            new WideColumn("city"u8.ToArray(), "Lisbon"u8.ToArray()),
        ]);

        WideColumns? columns = db.GetEntity("user:1"u8);
        if (columns is not null && columns.TryGetValue("city"u8, out ReadOnlySpan<byte> city))
            Console.WriteLine(System.Text.Encoding.UTF8.GetString(city));    // Lisbon
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("user:1"u8)!));   // alice (default column)

        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next())
            if (it.IsEntity) Console.WriteLine(it.Columns.Count);           // 3
        // </snippet:wide-columns>
        Check(columns is not null && columns.Count == 3, "three columns");
        return Task.CompletedTask;
    }

    public static Task AttributeGroups(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true },
            [new("default", new ColumnFamilyOptions()), new("profile", new ColumnFamilyOptions()),
             new("media", new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 1024 })]);
        ColumnFamily profile = db.GetColumnFamily("profile"), media = db.GetColumnFamily("media");

        // <snippet:attribute-groups>
        // One logical row split across column families, written atomically.
        db.PutAttributeGroups("user:1"u8,
        [
            new AttributeGroup(profile, new[] { new WideColumn("name"u8.ToArray(), "alice"u8.ToArray()) }),
            new AttributeGroup(media, new[] { new WideColumn("avatar"u8.ToArray(), new byte[4096]) }),
        ]);

        // Read the groups back (one consistent view); the result objects are reusable.
        PinnableAttributeGroup[] groups = [new(profile), new(media)];
        db.GetAttributeGroups(null, "user:1"u8, groups);
        Console.WriteLine(groups[1].Columns.Count);                        // 1

        // Batched entity reads on one column family.
        using var batch = new MultiGetBatch();
        batch.Add("user:1"u8);
        var columns = new WideColumns();
        profile.MultiGetEntity(batch);
        if (batch.TryGetEntity(0, columns)) Console.WriteLine(columns.Count);   // 1

        // Iterate rows across column families: columns of later families shadow earlier ones.
        using (CoalescingIterator rows = db.NewCoalescingIterator(null, [profile, media]))
            for (rows.SeekToFirst(); rows.Valid; rows.Next())
                Console.WriteLine(rows.Columns.Count);                     // 2

        // Lazy read: blob-backed columns are fetched only when pulled, in whole or by byte range.
        media.Flush();
        using LazyWideColumns? lazy = media.GetEntityLazy(null, "user:1"u8);
        Span<byte> header = stackalloc byte[16];
        int read = lazy!.ReadColumnRange(lazy.IndexOf("avatar"u8), 0, header);   // reads 16 bytes, not 4 KiB
        // </snippet:attribute-groups>
        Check(groups[0].Status == MultiGetStatus.Found && read == 16 && lazy.IsReference(0), "attribute groups and lazy read");
        return Task.CompletedTask;
    }

    public static Task UserDefinedTimestamps(string path)
    {
        // <snippet:user-defined-timestamps>
        static byte[] Ts(ulong t)                                            // 8-byte little-endian timestamps
        {
            byte[] b = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(b, t);
            return b;
        }

        var cfOptions = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts };
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);

        db.Put("k"u8, Ts(10), "v10"u8);
        db.Put("k"u8, Ts(20), "v20"u8);
        db.Delete("k"u8, Ts(30));

        // Reads name a timestamp and see the newest version at or below it.
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get(new ReadOptions { Timestamp = Ts(15) }, "k"u8)!));   // v10
        Console.WriteLine(db.Get(new ReadOptions { Timestamp = Ts(35) }, "k"u8) is null);                                  // True

        // TryGet can also return the timestamp of the version found.
        var value = new ArrayBufferWriter<byte>();
        var timestamp = new ArrayBufferWriter<byte>();
        db.TryGet(new ReadOptions { Timestamp = Ts(25) }, "k"u8, value, timestamp);   // v20 @ 20

        // History below full_history_ts_low may be garbage collected by compaction.
        db.IncreaseFullHistoryTsLow(Ts(20));
        // </snippet:user-defined-timestamps>
        ulong ts = BinaryPrimitives.ReadUInt64LittleEndian(timestamp.WrittenSpan);
        Line($"at 25: {Utf8(value.WrittenSpan)} @ {ts}");
        Check(Utf8(value.WrittenSpan) == "v20" && ts == 20, "timestamped read");
        return Task.CompletedTask;
    }

    public static Task BlobFiles(string path)
    {
        // <snippet:blob-files>
        var cfOptions = new ColumnFamilyOptions
        {
            EnableBlobFiles = true,                 // integrated BlobDB: large values go to .blob files at flush
            MinBlobSize = 512,
            BlobCompressionType = RocksDb.Managed.Compression.CompressionType.LZ4,
            EnableBlobGarbageCollection = true,
        };
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
        db.Put("big"u8, new byte[4096]);
        db.Put("small"u8, "inline"u8);
        db.Flush();
        Console.WriteLine(db.GetProperty(DbProperties.NumBlobFiles));        // 1
        Console.WriteLine(db.Get("big"u8)!.Length);                          // 4096, read through the blob file
        // </snippet:blob-files>
        Check(db.GetProperty(DbProperties.NumBlobFiles) == "1", "one blob file");
        return Task.CompletedTask;
    }

    public static Task Ttl(string path)
    {
        // <snippet:ttl>
        // Values carry a 4-byte write time (native DBWithTTL format); compaction drops expired entries.
        using TtlDatabase db = TtlDatabase.Open(path, new DbOptions { CreateIfMissing = true }, ttlSeconds: 3600);
        db.Put("session"u8, "token"u8);
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("session"u8)!));   // token (timestamp stripped)
        using (Iterator it = db.NewIterator())                                           // iterators strip it too
            for (it.SeekToFirst(); it.Valid; it.Next()) Console.WriteLine(it.Value.Length); // 5
        db.CompactRange();                                                               // expired entries are removed here
        // </snippet:ttl>
        Check(Utf8(db.Get("session"u8)) == "token", "ttl value");
        return Task.CompletedTask;
    }
}
