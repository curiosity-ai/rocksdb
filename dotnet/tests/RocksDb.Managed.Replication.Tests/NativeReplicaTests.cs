namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Wire compatibility of the replication stream with native RocksDB: a native replica (rocksdb-sharp) is seeded from a
/// managed checkpoint and then fed the managed source's merged WAL chunks as native WriteBatches - it must end up with
/// the same contents and the same sequence number.
/// </summary>
[TestClass]
public sealed class NativeReplicaTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    [TestMethod]
    public async Task NativeReplicaFollowsManagedSource()
    {
        using var dir = new TempDir("native");
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 3600; // the flush below archives the WAL the tail still needs
        await using RocksDatabase source = await RocksDatabase.OpenAsync(dir.Sub("source"), o);
        await WriteWorkloadAsync(source, 0, 500, 0);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 500, 700, 0);

        // Initial state: a managed checkpoint, shipped through the snapshot stream, opened natively.
        var src = new ReplicationSource(source);
        using (ReplicationSession session = await src.GetInitialStateAsync(dir.Sub("cp"), CancellationToken.None))
        {
            await SnapshotReceiver.ReceiveAsync(session.StreamSnapshotAsync([], 8192, CancellationToken.None), LocalFileSystem.Instance, dir.Sub("native"), [],
                CancellationToken.None);
        }
        var nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(nativeOptions);
        using RocksDbSharp.RocksDb native = RocksDbSharp.RocksDb.Open(nativeOptions, dir.Sub("native"));
        Assert.AreEqual(source.GetLatestSequenceNumber(), native.GetLatestSequenceNumber(), "native opened the managed checkpoint");

        // Stream: merged chunks applied as native write batches.
        await WriteWorkloadAsync(source, 700, 1500, 1);
        await source.FlushAsync();
        await WriteWorkloadAsync(source, 1500, 2000, 2);
        using WalTailer tailer = src.TailWal(native.GetLatestSequenceNumber() + 1, 8 * 1024);
        int chunks = 0, mergedBatches = 0;
        while (tailer.TryReadChunk(out WalChunk chunk))
        {
            Assert.AreEqual(native.GetLatestSequenceNumber() + 1, chunk.FirstSequenceNumber);
            using (var wb = new RocksDbSharp.WriteBatch(chunk.Data.ToArray()))
            {
                Assert.AreEqual(chunk.EntryCount, wb.Count(), "native parses the merged batch");
                native.Write(wb);
            }
            Assert.AreEqual(chunk.NextSequenceNumber - 1, native.GetLatestSequenceNumber());
            chunks++;
            mergedBatches += chunk.BatchCount;
        }
        Assert.IsTrue(chunks > 1 && mergedBatches > chunks, "chunks merge several WAL batches");
        Assert.AreEqual(source.GetLatestSequenceNumber(), native.GetLatestSequenceNumber());
        AssertSameContents(Contents(source), Contents(native), "native replica");
    }
}
