using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit11;

/// <summary>
/// Audit #11, EventListener callbacks against native RocksDB 11.12 through the C API listener (rocksdb_eventlistener_create,
/// db/c.cc rocksdb_eventlistener_t: OnFlushBegin / OnFlushCompleted / OnCompactionBegin / OnCompactionCompleted /
/// OnSubcompactionBegin / OnSubcompactionCompleted). The same workload runs on both engines (four overlapping L0 files,
/// a manual compaction with max_subcompactions = 4) and the callbacks must agree: which fire, in which order, and their
/// info (flush reasons, compaction levels / reason / inputs / records, the subcompaction ids and each subcompaction's
/// records and output files, job ids, threads). The C API exposes no OnCompactionPreCommit, OnTableFileCreationStarted,
/// OnManualFlushScheduled, blob file or file I/O callbacks; those are pinned against the C++ source in
/// Audit11ListenerCallbackTests.
/// </summary>
[TestClass]
public sealed class Audit11ListenerCallbackCompatTests
{
    private static readonly ConcurrentBag<object> s_keepAlive = new();

    private sealed record Event(string Kind, int JobId, int SubId, int BaseLevel, int OutputLevel, uint Reason, ulong InputRecords,
        ulong OutputRecords, ulong OutputFiles, int InputFiles, string Cf, string? Status, ulong Thread, ulong RawKeyBytes = 0, ulong RawValueBytes = 0,
        ulong Deletions = 0);

    private static byte[] K(int i)
    {
        byte[] k = new byte[12];
        "key-"u8.CopyTo(k);
        BinaryPrimitives.WriteInt64BigEndian(k.AsSpan(4), i);
        return k;
    }

    /// <summary>Four overlapping L0 files of distinct keys, then a full manual compaction.</summary>
    private static void Workload(Action<byte[], byte[]> put, Action flush, Action compact)
    {
        var value = new byte[100];
        var rnd = new Random(31);
        for (int f = 0; f < 4; f++)
        {
            for (int i = 0; i < 2000; i++)
            {
                rnd.NextBytes(value);
                put(K(i * 4 + f), value);
            }
            flush();
        }
        compact();
    }

    // ---- native ----------------------------------------------------------------------------------------------------

    private static string Utf8(IntPtr p, UIntPtr size) => p == IntPtr.Zero ? "" : Marshal.PtrToStringUTF8(p, (int)size)!;

    private static List<Event> RunNative(string dir)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        var events = new ConcurrentQueue<Event>();
        RocksDbSharp.on_flush_begin_cb flushBegin = (_, _, info) => events.Enqueue(new Event("FlushBegin", n.rocksdb_flushjobinfo_job_id(info), -1, 0, 0,
            n.rocksdb_flushjobinfo_flush_reason(info), 0, 0, 0, 0, Utf8(n.rocksdb_flushjobinfo_cf_name(info, out UIntPtr s1), s1), null,
            n.rocksdb_flushjobinfo_thread_id(info)));
        RocksDbSharp.on_flush_completed_cb flushCompleted = (_, _, info) => events.Enqueue(new Event("FlushCompleted", n.rocksdb_flushjobinfo_job_id(info), -1, 0,
            0, n.rocksdb_flushjobinfo_flush_reason(info), 0, 0, 0, 0, Utf8(n.rocksdb_flushjobinfo_cf_name(info, out UIntPtr s1), s1), null,
            n.rocksdb_flushjobinfo_thread_id(info)));
        RocksDbSharp.on_compaction_begin_cb compactionBegin = (_, _, info) => events.Enqueue(Compaction(n, "CompactionBegin", info));
        RocksDbSharp.on_compaction_completed_cb compactionCompleted = (_, _, info) => events.Enqueue(Compaction(n, "CompactionCompleted", info));
        RocksDbSharp.on_subcompaction_begin_cb subBegin = (_, info) => events.Enqueue(Subcompaction(n, "SubcompactionBegin", info));
        RocksDbSharp.on_subcompaction_completed_cb subCompleted = (_, info) => events.Enqueue(Subcompaction(n, "SubcompactionCompleted", info));
        RocksDbSharp.on_external_file_ingested_cb ingested = (_, _, _) => { };
        RocksDbSharp.on_background_error_cb bgError = (_, _, _) => { };
        RocksDbSharp.on_stall_conditions_changed_cb stall = (_, _) => { };
        RocksDbSharp.on_memtable_sealed_cb sealed_ = (_, _) => { };
        RocksDbSharp.Destructor_Delegate destructor = _ => { };
        foreach (object d in new object[] { flushBegin, flushCompleted, compactionBegin, compactionCompleted, subBegin, subCompleted, ingested, bgError, stall, sealed_, destructor })
        {
            s_keepAlive.Add(d);
        }
        IntPtr listener = n.rocksdb_eventlistener_create(IntPtr.Zero, destructor, flushBegin, flushCompleted, compactionBegin, compactionCompleted, subBegin,
            subCompleted, ingested, bgError, stall, sealed_);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(o);
        n.rocksdb_options_add_eventlistener(o.Handle, listener);
        n.rocksdb_options_set_max_subcompactions(o.Handle, 4);
        n.rocksdb_options_set_target_file_size_base(o.Handle, 32 << 10);
        n.rocksdb_options_set_disable_auto_compactions(o.Handle, 1);
        n.rocksdb_options_set_compression(o.Handle, 0);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, dir))
        {
            var flushOptions = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
            Workload((k, v) => db.Put(k, v), () => db.Flush(flushOptions), () => db.CompactRange((byte[]?)null, null));
        }
        return events.ToList();
    }

    private static Event Compaction(RocksDbSharp.Native n, string kind, IntPtr info)
    {
        n.rocksdb_compactionjobinfo_status(info, out IntPtr err);
        return new Event(kind, n.rocksdb_compactionjobinfo_job_id(info), -1, n.rocksdb_compactionjobinfo_base_input_level(info),
            n.rocksdb_compactionjobinfo_output_level(info), n.rocksdb_compactionjobinfo_compaction_reason(info),
            kind == "CompactionBegin" ? 0 : n.rocksdb_compactionjobinfo_input_records(info), kind == "CompactionBegin" ? 0 : n.rocksdb_compactionjobinfo_output_records(info),
            (ulong)n.rocksdb_compactionjobinfo_output_files_count(info), (int)n.rocksdb_compactionjobinfo_input_files_count(info),
            Utf8(n.rocksdb_compactionjobinfo_cf_name(info, out UIntPtr s), s), err == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(err),
            n.rocksdb_compactionjobinfo_thread_id(info));
    }

    private static Event Subcompaction(RocksDbSharp.Native n, string kind, IntPtr info)
    {
        n.rocksdb_subcompactionjobinfo_status(info, out IntPtr err);
        IntPtr stats = n.rocksdb_subcompactionjobinfo_stats(info);
        return new Event(kind, n.rocksdb_subcompactionjobinfo_job_id(info), n.rocksdb_subcompactionjobinfo_subcompaction_job_id(info),
            n.rocksdb_subcompactionjobinfo_base_input_level(info), n.rocksdb_subcompactionjobinfo_output_level(info),
            n.rocksdb_subcompactionjobinfo_compaction_reason(info), n.rocksdb_compaction_job_stats_num_input_records(stats),
            n.rocksdb_compaction_job_stats_num_output_records(stats), (ulong)n.rocksdb_compaction_job_stats_num_output_files(stats), 0,
            Utf8(n.rocksdb_subcompactionjobinfo_cf_name(info, out UIntPtr s), s), err == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(err),
            n.rocksdb_subcompactionjobinfo_thread_id(info), n.rocksdb_compaction_job_stats_total_input_raw_key_bytes(stats),
            n.rocksdb_compaction_job_stats_total_input_raw_value_bytes(stats), n.rocksdb_compaction_job_stats_num_input_deletion_records(stats));
    }

    // ---- managed ---------------------------------------------------------------------------------------------------

    private sealed class ManagedRecorder : EventListener
    {
        public readonly ConcurrentQueue<Event> Events = new();

        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info) => Events.Enqueue(new Event("FlushBegin", info.JobId, -1, 0, 0,
            (uint)info.FlushReason, 0, 0, 0, 0, info.ColumnFamilyName, null, (ulong)info.ThreadId));

        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Events.Enqueue(new Event("FlushCompleted", info.JobId, -1, 0, 0,
            (uint)info.FlushReason, 0, 0, 0, 0, info.ColumnFamilyName, null, (ulong)info.ThreadId));

        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => Events.Enqueue(Compaction("CompactionBegin", info));
        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => Events.Enqueue(Compaction("CompactionCompleted", info));

        private static Event Compaction(string kind, CompactionJobInfo i) => new(kind, i.JobId, -1, i.BaseInputLevel, i.OutputLevel, (uint)i.CompactionReason,
            (ulong)i.NumInputRecords, (ulong)i.NumOutputRecords, (ulong)i.OutputFiles.Count, i.InputFiles.Count, i.ColumnFamilyName, i.Status?.Message,
            (ulong)i.ThreadId);

        public override void OnSubcompactionBegin(SubcompactionJobInfo info) => Events.Enqueue(Sub("SubcompactionBegin", info));
        public override void OnSubcompactionCompleted(SubcompactionJobInfo info) => Events.Enqueue(Sub("SubcompactionCompleted", info));

        // The subcompaction's stats carry no output counters (upstream reports 0 for them).
        private static Event Sub(string kind, SubcompactionJobInfo i) => new(kind, i.JobId, i.SubcompactionJobId, i.BaseInputLevel, i.OutputLevel,
            (uint)i.CompactionReason, (ulong)i.NumInputRecords, 0, 0, 0, i.ColumnFamilyName, i.Status?.Message, (ulong)i.ThreadId,
            (ulong)i.TotalInputRawKeyBytes, (ulong)i.TotalInputRawValueBytes, (ulong)i.NumInputDeletionRecords);
    }

    private static List<Event> RunManaged(string dir)
    {
        var l = new ManagedRecorder();
        var o = new DbOptions { CreateIfMissing = true, MaxSubcompactions = 4 };
        o.Listeners.Add(l);
        var cf = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            TargetFileSizeBase = 32 << 10,
            Compression = RocksDb.Managed.Compression.CompressionType.None,
        };
        using (RocksDatabase db = RocksDatabase.Open(dir, o, cf))
        {
            Workload((k, v) => db.Put(k, v), db.Flush, db.CompactRange);
        }
        return l.Events.ToList();
    }

    /// <summary>
    /// The events with their thread-dependent parts normalized: the subcompactions of one compaction run concurrently,
    /// so their begin / completed events are listed by subcompaction id (begins first) between the compaction's begin
    /// and completed; job ids are renumbered in order of appearance (each engine numbers its jobs differently).
    /// </summary>
    private static List<string> Normalize(List<Event> events)
    {
        var jobIds = new Dictionary<int, int>();
        int Job(int id)
        {
            if (!jobIds.TryGetValue(id, out int j)) jobIds[id] = j = jobIds.Count;
            return j;
        }
        var result = new List<string>();
        var pendingSubs = new List<Event>();
        foreach (Event e in events)
        {
            if (e.Kind.StartsWith("Subcompaction", StringComparison.Ordinal))
            {
                pendingSubs.Add(e);
                continue;
            }
            if (e.Kind == "CompactionCompleted")
            {
                foreach (Event s in pendingSubs.OrderBy(s => s.Kind == "SubcompactionBegin" ? 0 : 1).ThenBy(s => s.SubId))
                {
                    result.Add(s.Kind + " job " + Job(s.JobId) + " sub " + s.SubId + " L" + s.BaseLevel + "->L" + s.OutputLevel + " reason " + s.Reason
                        + " in " + s.InputRecords + " out " + s.OutputRecords + " files " + s.OutputFiles + " rawkey " + s.RawKeyBytes + " rawvalue "
                        + s.RawValueBytes + " dels " + s.Deletions + " cf " + s.Cf + " status " + (s.Status ?? "OK"));
                }
                pendingSubs.Clear();
            }
            result.Add(e.Kind switch
            {
                "FlushBegin" or "FlushCompleted" => e.Kind + " job " + Job(e.JobId) + " reason " + e.Reason + " cf " + e.Cf,
                _ => e.Kind + " job " + Job(e.JobId) + " L" + e.BaseLevel + "->L" + e.OutputLevel + " reason " + e.Reason + " inputs " + e.InputFiles
                    + " in " + e.InputRecords + " out " + e.OutputRecords + " files " + e.OutputFiles + " cf " + e.Cf + " status " + (e.Status ?? "OK"),
            });
        }
        return result;
    }

    /// <summary>The checks that hold for each engine on its own (thread ids are per engine: managed thread ids vs OS ids).</summary>
    private static void AssertConsistent(List<Event> events, string engine)
    {
        Event begin = events.Single(e => e.Kind == "CompactionBegin");
        Event done = events.Single(e => e.Kind == "CompactionCompleted");
        int bi = events.IndexOf(begin), di = events.IndexOf(done);
        List<Event> subs = events.Where(e => e.Kind.StartsWith("Subcompaction", StringComparison.Ordinal)).ToList();
        Assert.IsTrue(subs.Count >= 4, engine + ": " + subs.Count);
        foreach (Event s in subs)
        {
            int si = events.IndexOf(s);
            Assert.IsTrue(si > bi && si < di, engine + ": a subcompaction event outside its compaction");
            Assert.AreEqual(begin.JobId, s.JobId, engine);
        }
        foreach (Event b in subs.Where(s => s.Kind == "SubcompactionBegin"))
        {
            Event c = subs.Single(s => s.Kind == "SubcompactionCompleted" && s.SubId == b.SubId);
            Assert.IsTrue(events.IndexOf(b) < events.IndexOf(c), engine);
            Assert.AreEqual(b.Thread, c.Thread, engine + ": begin and completed of a subcompaction on one thread");
        }
        Assert.AreEqual(done.InputRecords, (ulong)subs.Where(s => s.Kind == "SubcompactionCompleted").Sum(s => (long)s.InputRecords), engine);
    }

    // CompactionJob::NotifyOnSubcompactionBegin / NotifyOnSubcompactionCompleted (compaction_job.cc), DBImpl::
    // NotifyOnCompactionBegin / NotifyOnCompactionCompleted, NotifyOnFlushBegin / NotifyOnFlushCompleted: same events, same
    // order, same info on both engines.
    [TestMethod]
    public void FlushCompactionAndSubcompactionCallbacksMatchNative()
    {
        using var nd = new TempDir("a11-listener-n");
        using var md = new TempDir("a11-listener-m");
        List<Event> native = RunNative(nd.Path);
        List<Event> managed = RunManaged(md.Path);
        AssertConsistent(native, "native");
        AssertConsistent(managed, "managed");
        List<string> n = Normalize(native);
        List<string> m = Normalize(managed);
        CollectionAssert.AreEqual(n, m, "native:\n" + string.Join("\n", n) + "\nmanaged:\n" + string.Join("\n", m));
    }
}
