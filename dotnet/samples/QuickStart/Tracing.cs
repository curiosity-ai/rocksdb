using RocksDb.Managed;
using RocksDb.Managed.Trace;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Query tracing and replay, block cache and IO traces.</summary>
internal static class Tracing
{
    public static Task TraceAndReplay(string root)
    {
        string path = Path.Combine(root, "db");
        string replayPath = Path.Combine(root, "replayed");
        string trace = Path.Combine(root, "queries.trace");
        string blockTrace = Path.Combine(root, "block-cache.trace");
        // <snippet:tracing>
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }))
        {
            // Query trace (native trace file format: native trace_analyzer and replayers read it).
            db.StartTrace(new TraceOptions { SamplingFrequency = 1, Filter = TraceFilter.None }, trace);
            db.Put("k1"u8, "v1"u8);
            _ = db.Get("k1"u8);
            using (Iterator it = db.NewIterator()) it.Seek("k"u8);
            db.EndTrace();

            // Block cache trace: every block cache lookup of the table readers.
            db.Flush();
            db.StartBlockCacheTrace(new BlockCacheTraceOptions(), blockTrace);
            _ = db.Get("k1"u8);
            db.EndBlockCacheTrace();
        }

        using (var reader = new BlockCacheTraceReader(TraceReader.OpenFile(blockTrace)))
        {
            reader.ReadHeader();
            while (reader.TryReadAccess(out BlockCacheTraceRecord? access))
                Line($"{access!.Caller} {access.BlockType} hit={access.IsCacheHit}");
        }

        // Replay the query trace against another DB (here: twice as fast, 2 threads).
        using RocksDatabase target = RocksDatabase.Open(replayPath, new DbOptions { CreateIfMissing = true });
        using Replayer replayer = target.NewDefaultReplayer(TraceReader.OpenFile(trace));
        replayer.Prepare();
        int executed = 0;
        replayer.Replay(new ReplayOptions { NumThreads = 2, FastForward = 2.0 }, (error, result) => Interlocked.Increment(ref executed));
        // </snippet:tracing>
        Check(executed == 3, "three records replayed");
        Check(target.Get("k1"u8) is not null, "write replayed");
        return Task.CompletedTask;
    }
}
