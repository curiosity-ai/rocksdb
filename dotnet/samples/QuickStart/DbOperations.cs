using RocksDb.Managed;
using RocksDb.Managed.Table;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Column family export/import, remote compaction, merge operands, multi-range scans and file-level operations.</summary>
internal static class DbOperations
{
    public static async Task ExportImport(string root)
    {
        string source = Path.Combine(root, "source"), target = Path.Combine(root, "target"), exportDir = Path.Combine(root, "export");

        // <snippet:export-import>
        ExportImportFilesMetadata exported;
        using (RocksDatabase db = RocksDatabase.Open(source, new DbOptions { CreateIfMissing = true }))
        {
            ColumnFamily users = db.CreateColumnFamily("users", new ColumnFamilyOptions());
            users.Put("user:1"u8, "alice"u8);
            // Flushes the column family and hard-links (or copies) its table files into a new directory.
            exported = await users.ExportColumnFamilyAsync(exportDir);
        }

        // Import into another DB (managed or native: the metadata maps to rocksdb_export_import_files_metadata_t).
        using RocksDatabase other = RocksDatabase.Open(target, new DbOptions { CreateIfMissing = true });
        ColumnFamily imported = other.CreateColumnFamilyWithImport("users", new ColumnFamilyOptions(),
            new ImportColumnFamilyOptions { MoveFiles = true }, exported);      // or a list of non-overlapping exports
        Console.WriteLine(Utf8(imported.Get("user:1"u8)));                      // alice
        // </snippet:export-import>
        Check(Utf8(imported.Get("user:1"u8)) == "alice", "imported column family readable");
    }

    // <snippet:compaction-service-class>
    /// <summary>
    /// A compaction service whose "remote" worker runs in-process. A real one ships the input bytes to a worker host that
    /// can read the DB directory (shared storage) and returns the bytes OpenAndCompact produced there.
    /// </summary>
    private sealed class InProcessCompactionService(string dbPath) : CompactionService
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _jobs = new();

        public override string Name => "InProcess";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info,
            ReadOnlyMemory<byte> input, CancellationToken cancellationToken)
        {
            string jobId = $"{info.DbSessionId}-{info.JobId}";
            _jobs[jobId] = input.ToArray();          // byte-compatible with native CompactionServiceInput
            return ValueTask.FromResult(new CompactionServiceScheduleResponse(jobId, CompactionServiceJobStatus.Success));
        }

        public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string jobId, CancellationToken cancellationToken)
        {
            _jobs.TryRemove(jobId, out byte[]? input);
            try
            {
                // The worker side (native DB::OpenAndCompact works too); outputs go to <db>/<jobId>.
                byte[] result = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), dbPath, jobId, input!,
                    new CompactionServiceOptionsOverride(), cancellationToken);
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, result);
            }
            catch (OpenAndCompactException e)
            {
                return new CompactionServiceWaitResult(CompactionServiceJobStatus.Failure, e.Result);
            }
        }
    }
    // </snippet:compaction-service-class>

    public static async Task RemoteCompaction(string path)
    {
        // <snippet:remote-compaction>
        var options = new DbOptions { CreateIfMissing = true, CompactionService = new InProcessCompactionService(path) };
        using RocksDatabase db = RocksDatabase.Open(path, options);
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 100; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"key:{i:D3}"), "v"u8);
            db.Flush();
        }
        await db.CompactRangeAsync();     // compacted by the service; its outputs are moved into the DB and installed
        // </snippet:remote-compaction>
        Line($"files after remote compaction: {db.GetLiveFilesMetadata().Count}");
        Check(db.GetLiveFilesMetadata().TrueForAll(f => f.Level == 6) && db.Get("key:007"u8) is not null, "remote compaction installed");
    }

    public static async Task MergeOperandsAndScans(string path)
    {
        // <snippet:merge-operands-multiscan>
        var cfOptions = new ColumnFamilyOptions { MergeOperator = new StringAppendOperator(","u8) };
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
        db.Put("list"u8, "a"u8);
        db.Merge("list"u8, "b"u8);
        db.Merge("list"u8, "c"u8);

        // The operands without merging them (oldest first, base value included); reusable, pooled result.
        using var operands = new MergeOperandsResult();
        var limits = new GetMergeOperandsOptions
        {
            ExpectedMaxNumberOfOperands = 10,
            ContinueCallback = operand => !operand.SequenceEqual("b"u8),   // newest first: stop at "b"
        };
        if (db.TryGetMergeOperands("list"u8, limits, operands))
            for (int i = 0; i < operands.Count; i++) Console.WriteLine(Utf8(operands[i]));   // b, c

        // Several key ranges with one iterator; the data blocks of all ranges are prefetched up front.
        for (int i = 0; i < 100; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"row:{i:D3}"), "x"u8);
        var ranges = new MultiScanArgs().Add("row:010"u8.ToArray(), "row:013"u8.ToArray()).Add("row:090"u8.ToArray(), "row:092"u8.ToArray());
        using (MultiScan scan = db.NewMultiScan(ranges))
            while (await scan.MoveNextRangeAsync())
                for (; scan.Valid; await scan.NextAsync())
                    Console.WriteLine(Utf8(scan.Key));
        // </snippet:merge-operands-multiscan>
        Check(operands.Count == 2 && Utf8(operands[0]) == "b", "merge operands");
    }

    public static async Task FileLevelOperations(string path)
    {
        // <snippet:file-level-operations>
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
            new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int f = 0; f < 3; f++)
        {
            for (int i = 0; i < 10; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"k{f}{i}"), "v"u8);
            db.Flush();                               // three non-overlapping L0 files
        }
        db.PromoteL0(targetLevel: 1);                 // move them to L1 without rewriting
        db.SuggestCompactRange(null, null);           // mark files for compaction; returns at once
        db.DeleteFilesInRange("k1"u8.ToArray(), "k2"u8.ToArray(), includeEnd: false);   // drop whole files in a range
        db.ClipColumnFamily("k0"u8.ToArray(), "k2"u8.ToArray());                         // keep only [k0, k2)
        Dictionary<string, TableProperties> props =
            db.GetPropertiesOfTablesInRange([new KeyRange("k0"u8.ToArray(), "k1"u8.ToArray())]);

        // Freeze the DB (writes stop until disposed), e.g. to copy a consistent state.
        using (WalLock frozen = await db.LockWalAsync())
            db.CreateCheckpoint(Path.Combine(path, "..", "frozen-checkpoint"));

        db.DisableManualCompaction();                 // CompactRange now fails with Incomplete / ManualCompactionPaused
        db.EnableManualCompaction();
        ulong oldest = db.GetCreationTimeOfOldestFile();   // unix seconds (max_open_files = -1)
        // </snippet:file-level-operations>
        Line($"{props.Count} table(s) in range, oldest file created at {oldest}");
        Check(db.Get("k05"u8) is not null && db.Get("k25"u8) is null, "clip kept [k0, k2)");
    }
}
