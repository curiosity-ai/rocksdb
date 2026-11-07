using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Iterators, snapshots, merge operators and prefix seek.</summary>
internal static class Reading
{
    public static async Task Iterators(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        foreach (string k in new[] { "a", "b", "c", "d", "e" })
            db.Put(System.Text.Encoding.UTF8.GetBytes(k), System.Text.Encoding.UTF8.GetBytes(k.ToUpperInvariant()));

        // <snippet:iterators>
        // Cursor style: Key/Value are spans into pinned blocks, valid until the iterator moves. No allocation per step.
        using (Iterator it = db.NewIterator(new ReadOptions { IterateUpperBound = "d"u8.ToArray() }))
        {
            for (it.Seek("b"u8); it.Valid; it.Next())
                Console.WriteLine($"{(char)it.Key[0]} = {(char)it.Value[0]}");      // b, c (d is the bound)
            for (it.SeekToLast(); it.Valid; it.Prev()) { /* reverse: c, b, a */ }
        }

        // foreach over a range [start, end) with a ref struct enumerator.
        using (Iterator it = db.NewIterator())
        {
            foreach ((ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) in it.Range("b"u8, "e"u8))
                Console.WriteLine($"{(char)key[0]} = {(char)value[0]}");            // b, c, d
        }

        // Async scan: block reads that miss the cache are awaited instead of blocking the thread.
        using (Iterator it = await db.NewIteratorAsync(null))
        {
            await foreach (KeyValue kv in it.ScanAsync("c"u8.ToArray(), null))     // null = unbounded
                Console.WriteLine(kv.KeySpan.Length);   // kv memory is reused: copy (kv.KeyToArray()) to keep it
        }
        // </snippet:iterators>
        using Iterator check = db.NewIterator();
        int n = 0;
        foreach (KeyValueRef _ in check.Range()) n++;
        Check(n == 5, "five keys");

        // <snippet:iterator-reuse>
        // One long-lived iterator for many short scans: SetBounds changes the range, Refresh moves it to the latest
        // state (or Refresh(snapshot)). Both keep the iterator's internal stack and allocate nothing.
        using Iterator scanner = db.NewIterator();
        scanner.SetBounds("a"u8.ToArray(), "c"u8.ToArray());
        for (scanner.SeekToFirst(); scanner.Valid; scanner.Next()) { /* a, b */ }
        db.Put("bb"u8, "BB"u8);
        scanner.Refresh();                                   // sees "bb" now; unpositioned until the next seek
        for (scanner.SeekToFirst(); scanner.Valid; scanner.Next()) { /* a, b, bb */ }
        // </snippet:iterator-reuse>
        int m = 0;
        for (scanner.SeekToFirst(); scanner.Valid; scanner.Next()) m++;
        Check(m == 3, "refreshed bounded scan");
    }

    public static Task Snapshots(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

        // <snippet:snapshots>
        db.Put("k"u8, "old"u8);
        using Snapshot snapshot = db.GetSnapshot();          // consistent view; Dispose releases it
        db.Put("k"u8, "new"u8);

        var atSnapshot = new ReadOptions { Snapshot = snapshot };
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get(atSnapshot, "k"u8)!));   // old
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("k"u8)!));               // new
        using Iterator it = db.NewIterator(atSnapshot);       // iterators honour the snapshot too
        // </snippet:snapshots>
        Line($"snapshot seq={snapshot.SequenceNumber}, latest={db.GetLatestSequenceNumber()}");
        Check(Utf8(db.Get(atSnapshot, "k"u8)) == "old", "snapshot read");
        return Task.CompletedTask;
    }

    // <snippet:merge-operator-class>
    /// <summary>Keeps the maximum of little-endian int64 operands.</summary>
    private sealed class Int64MaxOperator : MergeOperator
    {
        public override string Name => "Int64MaxOperator";  // persisted in OPTIONS; keep it stable and unique

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
            in MergeOperands operands, IBufferWriter<byte> result)
        {
            long max = hasExistingValue ? BinaryPrimitives.ReadInt64LittleEndian(existingValue) : long.MinValue;
            for (int i = 0; i < operands.Count; i++)
                max = Math.Max(max, BinaryPrimitives.ReadInt64LittleEndian(operands[i]));
            BinaryPrimitives.WriteInt64LittleEndian(result.GetSpan(8), max);
            result.Advance(8);
            return true;                                      // false = merge failure (Corruption)
        }
    }
    // </snippet:merge-operator-class>

    public static Task MergeOperators(string path)
    {
        // <snippet:merge-operators>
        var options = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true };
        using RocksDatabase db = RocksDatabase.Open(path, options,
        [
            // Built-ins share the native names (uint64add, stringappend, max, put, bytesxor, ...), so native RocksDB opens the DB too.
            new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { MergeOperator = MergeOperator.UInt64Add }),
            new ColumnFamilyDescriptor("tags", new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") }),
            new ColumnFamilyDescriptor("max", new ColumnFamilyOptions { MergeOperator = new Int64MaxOperator() }),
        ]);

        Span<byte> one = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(one, 1);
        db.Merge("hits"u8, one);
        db.Merge("hits"u8, one);
        ulong hits = db.Get("hits"u8, 0, static (v, _) => BinaryPrimitives.ReadUInt64LittleEndian(v));   // 2

        ColumnFamily tags = db.GetColumnFamily("tags");
        tags.Merge("post:1"u8, "db"u8);
        tags.Merge("post:1"u8, "dotnet"u8);
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(tags.Get("post:1"u8)!));   // db,dotnet
        // </snippet:merge-operators>

        ColumnFamily max = db.GetColumnFamily("max");
        Span<byte> v = stackalloc byte[8];
        foreach (long x in new long[] { 5, 42, 7 })
        {
            BinaryPrimitives.WriteInt64LittleEndian(v, x);
            max.Merge("m"u8, v);
        }
        db.Flush();
        long m = max.Get("m"u8, 0, static (s, _) => BinaryPrimitives.ReadInt64LittleEndian(s));
        Line($"hits={hits} tags={Utf8(tags.Get("post:1"u8))} max={m}");
        Check(hits == 2 && m == 42, "merge results");
        return Task.CompletedTask;
    }

    public static Task PrefixSeek(string path)
    {
        // <snippet:prefix-seek>
        var cfOptions = new ColumnFamilyOptions
        {
            PrefixExtractor = SliceTransform.CreateFixedPrefix(5),          // "user:" / "post:"; persisted as rocksdb.FixedPrefix.5
            TableOptions = { FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10) },   // prefix blooms
        };
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
        db.Put("user:1"u8, "a"u8);
        db.Put("user:2"u8, "b"u8);
        db.Put("post:1"u8, "c"u8);
        db.Flush();

        // PrefixSameAsStart: the iterator stops when the prefix of the key changes; filters skip files without it.
        using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
        int users = 0;
        for (it.Seek("user:"u8); it.Valid; it.Next()) users++;            // 2
        // TotalOrderSeek = true ignores the prefix extractor for one iterator.
        // </snippet:prefix-seek>
        Line($"users={users}");
        Check(users == 2, "prefix seek");
        return Task.CompletedTask;
    }

    public static Task TableFormats(string path)
    {
        // <snippet:table-formats>
        // PlainTable: for in-memory / tmpfs databases (no blocks, compression or checksums). With a prefix extractor,
        // lookups use a prefix hash index and iterators only support prefix seeks.
        var plain = new ColumnFamilyOptions
        {
            TableFactory = TableFactoryType.PlainTable,                    // table_factory=PlainTable
            PrefixExtractor = SliceTransform.CreateFixedPrefix(5),
            PlainTableOptions = { EncodingType = PlainTableEncodingType.Prefix, StoreIndexInFile = true },
        };
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "plain"), new DbOptions { CreateIfMissing = true }, plain))
        {
            db.Put("user:1"u8, "a"u8);
            db.Put("user:2"u8, "b"u8);
            db.Flush();                                                    // a plain table SST, readable by native RocksDB
            using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
            for (it.Seek("user:"u8); it.Valid; it.Next()) { /* user:1, user:2 */ }
        }

        // CuckooTable: fixed-size keys and values, one version per key, read memory-mapped (AllowMmapReads).
        var cuckoo = new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = { HashTableRatio = 0.9 } };
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "cuckoo"), new DbOptions { CreateIfMissing = true, AllowMmapReads = true }, cuckoo))
        {
            db.Put("key00001"u8, "val1"u8);
            db.Flush();
            byte[]? v = db.Get("key00001"u8);                              // "val1"
        }

        // AdaptiveTableFactory: reads block-based, plain and cuckoo files by their magic number; writes one format.
        var adaptive = new ColumnFamilyOptions
        {
            TableFactory = TableFactoryType.AdaptiveTable,
            AdaptiveTableWriteFactory = TableFactoryType.BlockBasedTable,
            PrefixExtractor = SliceTransform.CreateFixedPrefix(5),
        };
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "plain"), new DbOptions(), adaptive))
        {
            byte[]? v = db.Get("user:2"u8);                                // read from the plain table file
        }
        // </snippet:table-formats>
        using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "plain"), new DbOptions(), adaptive))
            Check(db.Get("user:2"u8) is [(byte)'b'], "adaptive read of a plain table");
        Line("plain + cuckoo + adaptive ok");
        return Task.CompletedTask;
    }
}
