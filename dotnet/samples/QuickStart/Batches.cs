using RocksDb.Managed;
using RocksDb.Managed.Db;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Atomic multi-key writes and batched point lookups.</summary>
internal static class Batches
{
    public static async Task WriteBatches(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        ColumnFamily audit = db.CreateColumnFamily("audit", new ColumnFamilyOptions());
        db.Put("stale"u8, "x"u8);

        // <snippet:write-batch>
        using (WriteBatch batch = db.CreateWriteBatch())               // pooled; Dispose returns it
        {
            batch.Put("k1"u8, "v1"u8)
                 .Delete("stale"u8)
                 .DeleteRange("tmp:"u8, "tmp;"u8)                      // [begin, end)
                 .Put(audit, "k1"u8, "created"u8);                     // another column family
            db.Write(batch);                                           // atomic
        }

        using (WriteBatch batch = db.CreateWriteBatch())
        {
            batch.Put("k2"u8, "v2"u8);
            batch.SetSavePoint();
            batch.Put("k3"u8, "oops"u8);
            batch.RollbackToSavePoint();                               // drops k3
            await db.WriteAsync(WriteOptions.SyncWrites, batch);       // fsync the WAL before completing
            Console.WriteLine(batch.Count);                            // 1
        }
        using (WriteBatch batch = db.CreateWriteBatch())
        {
            batch.Put("logged"u8, "1"u8);
            batch.MarkWalTerminationPoint();                           // later records skip the WAL (memtable only)
            batch.Put("cache-only"u8, "2"u8);
            db.Write(batch);
        }
        // The batch representation (batch.Data) is byte-identical to native rocksdb::WriteBatch::Data().
        // </snippet:write-batch>
        Line($"k1={Utf8(db.Get("k1"u8))} audit:k1={Utf8(audit.Get("k1"u8))} k3={Utf8(db.Get("k3"u8))}");
        Check(db.Get("stale"u8) is null && db.Get("k3"u8) is null && Utf8(db.Get("k2"u8)) == "v2", "batch applied");
    }

    public static async Task MultiGet(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        db.Put("a"u8, "1"u8);
        db.Put("c"u8, "3"u8);
        db.Flush();

        // <snippet:multiget>
        using var batch = new MultiGetBatch();          // reusable: no allocation once warmed up
        batch.Add("a"u8).Add("b"u8).Add("c"u8);
        db.MultiGet(batch);                             // or: await db.MultiGetAsync(batch)
        for (int i = 0; i < batch.Count; i++)
        {
            switch (batch.GetStatus(i))
            {
                case MultiGetStatus.Found:
                    Console.WriteLine($"{i}: {System.Text.Encoding.UTF8.GetString(batch.GetValue(i))}");
                    break;
                case MultiGetStatus.NotFound:
                    Console.WriteLine($"{i}: not found");
                    break;
                case MultiGetStatus.Error:
                    throw batch.GetError(i)!;
            }
        }
        batch.Clear();                                  // keep the buffers for the next batch
        // </snippet:multiget>
        batch.Add("c"u8);
        await db.MultiGetAsync(batch);
        Line($"async found={batch.FoundCount}");
        Check(batch.FoundCount == 1, "multiget async");

        // <snippet:multiget-spans>
        ReadOnlyMemory<byte>[] keys = ["a"u8.ToArray(), "b"u8.ToArray(), "c"u8.ToArray()];
        var values = new System.Buffers.ArrayBufferWriter<byte>();     // found values, back to back
        var ranges = new Range[keys.Length];                           // where each value is in `values`
        var statuses = new MultiGetStatus[keys.Length];
        db.MultiGet(null, keys, values, ranges, statuses);             // or: await db.MultiGetAsync(null, keys, values, ranges, statuses)
        for (int i = 0; i < keys.Length; i++)
        {
            if (statuses[i] == MultiGetStatus.Found) Console.WriteLine(System.Text.Encoding.UTF8.GetString(values.WrittenSpan[ranges[i]]));
        }
        // </snippet:multiget-spans>
        Line($"spans: {statuses[0]} {statuses[1]} {statuses[2]} {Utf8(values.WrittenSpan[ranges[2]].ToArray())}");
        Check(statuses[0] == MultiGetStatus.Found && statuses[1] == MultiGetStatus.NotFound && Utf8(values.WrittenSpan[ranges[2]].ToArray()) == "3",
            "multiget spans");
    }
}
