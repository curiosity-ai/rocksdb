using RocksDb.Managed;
using RocksDb.Managed.Db;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>WAL tailing (change data capture / replication) and read-only / secondary instances.</summary>
internal static class Replication
{
    public static async Task WalTailing(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

        // <snippet:wal-tailing>
        ulong start = db.GetLatestSequenceNumber() + 1;
        db.Put("a"u8, "1"u8);
        using (WriteBatch batch = db.CreateWriteBatch())
        {
            batch.Put("b"u8, "2"u8).Delete("a"u8);
            db.Write(batch);
        }

        // Batches already in the WAL. Each WalBatch is the native WriteBatch representation (feed it to a native
        // or managed replica with new WriteBatch(b.Span)); it is valid until the iteration moves on.
        using (TransactionLogIterator updates = db.GetUpdatesSince(start))
            await foreach (WalBatch b in updates)
                Console.WriteLine($"seq {b.Sequence}..{b.LastSequence}: {b.Count} ops");

        // Follow the WAL forever: awaits new writes (signalled by the write path, no polling) and WAL rotations.
        using var cts = new CancellationTokenSource();
        Task writer = Task.Run(async () => { await db.PutAsync("c"u8.ToArray(), "3"u8.ToArray()); });
        await foreach (WalBatch b in db.TailUpdatesAsync(start, cts.Token))
        {
            foreach (WriteBatchEntry e in b.GetReader())
                Console.WriteLine($"  {e.Type} {System.Text.Encoding.UTF8.GetString(e.Key)}");
            if (b.LastSequence >= start + 3) break;          // or cts.Cancel() from elsewhere
        }
        await writer;
        // </snippet:wal-tailing>
        Line($"latest sequence {db.GetLatestSequenceNumber()}");
        Check(db.GetLatestSequenceNumber() == start + 3, "four sequence numbers consumed");
    }

    public static async Task ReadOnlyAndSecondary(string root)
    {
        string path = Path.Combine(root, "primary");
        string secondaryPath = Path.Combine(root, "secondary");

        // <snippet:read-only-secondary>
        using RocksDatabase primary = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        primary.Put("k"u8, "v1"u8);

        // Read-only: no LOCK, nothing written; works next to a running primary (sees the state at open).
        using (RocksDatabase readOnly = RocksDatabase.OpenReadOnly(path, new DbOptions()))
            Console.WriteLine(System.Text.Encoding.UTF8.GetString(readOnly.Get("k"u8)!));   // v1

        // Secondary: follows the primary (managed or native) on demand.
        using RocksDatabase secondary = await RocksDatabase.OpenAsSecondaryAsync(path, secondaryPath, new DbOptions());
        primary.Put("k"u8, "v2"u8);
        await secondary.TryCatchUpWithPrimaryAsync();
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(secondary.Get("k"u8)!));      // v2
        // </snippet:read-only-secondary>
        Check(Utf8(secondary.Get("k"u8)) == "v2" && secondary.IsSecondary, "secondary caught up");
    }
}
