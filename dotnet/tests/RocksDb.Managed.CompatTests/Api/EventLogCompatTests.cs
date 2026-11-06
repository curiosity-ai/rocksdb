using System.Text;
using System.Text.Json;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The EVENT_LOG_v1 records of the info LOG (EventLogger / JSONWriter, logging/event_logger.h): the same workload on
/// both sides writes the same event types with the same JSON keys in the same order.
/// </summary>
[TestClass]
public sealed class EventLogCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] K(int i) => Encoding.UTF8.GetBytes("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] V(int i, int round) => Encoding.UTF8.GetBytes(new string((char)('a' + round), 100) + i);

    /// <summary>The events this test checks (each must appear on both sides).</summary>
    private static readonly string[] s_events =
    [
        "table_file_deletion", "blob_file_creation", "blob_file_deletion", "trivial_move", "recovery_started", "recovery_finished",
        "ingest_finished", "flush_started", "flush_finished", "table_file_creation", "compaction_started", "compaction_finished",
    ];

    /// <summary>Event name -> key list of its first occurrence, from every LOG* file of the directory.</summary>
    internal static Dictionary<string, List<string>> ParseEvents(string dir)
    {
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (string file in Directory.GetFiles(dir, "LOG*"))
        {
            foreach (string line in File.ReadAllLines(file))
            {
                int at = line.IndexOf("EVENT_LOG_v1 ", StringComparison.Ordinal);
                if (at < 0) continue;
                using JsonDocument doc = JsonDocument.Parse(line[(at + "EVENT_LOG_v1 ".Length)..]);
                var keys = new List<string>();
                foreach (JsonProperty p in doc.RootElement.EnumerateObject()) keys.Add(p.Name);
                if (!doc.RootElement.TryGetProperty("event", out JsonElement e)) continue;
                result.TryAdd(e.GetString()!, keys);
                if (doc.RootElement.TryGetProperty("table_properties", out JsonElement tp))
                {
                    var nested = new List<string>();
                    foreach (JsonProperty p in tp.EnumerateObject()) nested.Add(p.Name);
                    result.TryAdd(e.GetString() + ".table_properties", nested);
                }
            }
        }
        return result;
    }

    private static string WriteSst(string path)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), RocksDb.Managed.Env.LocalFileSystem.Instance);
        w.Open(path);
        for (int i = 5000; i < 5010; i++) w.Put(K(i), V(i, 9));
        w.Finish();
        return path;
    }

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var o = new RocksDbSharp.ColumnFamilyOptions().SetDisableAutoCompactions(1).SetCompression(RocksDbSharp.Compression.No);
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_enable_blob_files(o.Handle, 1);
        n.rocksdb_options_set_min_blob_size(o.Handle, 50);
        n.rocksdb_options_set_enable_blob_gc(o.Handle, 1);
        n.rocksdb_options_set_blob_gc_age_cutoff(o.Handle, 1.0);
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(o);
        s_keepAlive.Add(options);
        return RocksDbSharp.RocksDb.Open(options, path, new RocksDbSharp.ColumnFamilies(o));
    }

    private static RocksDatabase OpenManaged(string path) => RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
        new ColumnFamilyOptions
        {
            DisableAutoCompactions = true, Compression = RocksDb.Managed.Compression.CompressionType.None, EnableBlobFiles = true, MinBlobSize = 50,
            EnableBlobGarbageCollection = true, BlobGarbageCollectionAgeCutoff = 1.0,
        });

    /// <summary>
    /// Flush (blob + table creation), CompactRange of a single L0 file (trivial move), an overwrite + CompactRange
    /// (table and blob file deletion through blob GC), an ingestion and a reopen that replays a WAL (recovery).
    /// </summary>
    [TestMethod]
    public void EventKeysMatchNative()
    {
        using var nativeDir = new TempDir("evlog-n");
        using var managedDir = new TempDir("evlog-m");
        string nativeSst = WriteSst(System.IO.Path.Combine(nativeDir.Path, "..", System.IO.Path.GetFileName(nativeDir.Path) + "-ingest.sst"));
        string managedSst = WriteSst(System.IO.Path.Combine(managedDir.Path, "..", System.IO.Path.GetFileName(managedDir.Path) + "-ingest.sst"));
        try
        {
            using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path))
            {
                var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
                for (int i = 0; i < 100; i++) db.Put(K(i), V(i, 0));
                db.Flush(flush);
                db.CompactRange((byte[]?)null, null);
                for (int i = 0; i < 100; i++) db.Put(K(i), V(i, 1));
                db.Flush(flush);
                db.CompactRange((byte[]?)null, null);
                db.IngestExternalFiles([nativeSst], new RocksDbSharp.IngestExternalFileOptions().SetMoveFiles(false));
                db.Put(K(7000), V(7000, 2));
            }
            using (RocksDbSharp.RocksDb db = OpenNative(nativeDir.Path)) Assert.IsNotNull(db.Get(K(7000)));

            using (RocksDatabase db = OpenManaged(managedDir.Path))
            {
                for (int i = 0; i < 100; i++) db.Put(K(i), V(i, 0));
                db.Flush();
                db.CompactRange(null, null);
                for (int i = 0; i < 100; i++) db.Put(K(i), V(i, 1));
                db.Flush();
                db.CompactRange(null, null);
                db.IngestExternalFiles([managedSst], new IngestExternalFileOptions());
                db.Put(K(7000), V(7000, 2));
            }
            using (RocksDatabase db = OpenManaged(managedDir.Path)) Assert.IsNotNull(db.Get(K(7000)));
        }
        finally
        {
            try { File.Delete(nativeSst); } catch (IOException) { }
            try { File.Delete(managedSst); } catch (IOException) { }
        }

        Dictionary<string, List<string>> native = ParseEvents(nativeDir.Path), managed = ParseEvents(managedDir.Path);
        foreach (string e in s_events)
        {
            Assert.IsTrue(native.ContainsKey(e), "native has no " + e + ": " + string.Join(", ", native.Keys));
            Assert.IsTrue(managed.ContainsKey(e), "managed has no " + e + ": " + string.Join(", ", managed.Keys));
            CollectionAssert.AreEqual(native[e], managed[e], e + ": " + string.Join(",", native[e]) + " vs " + string.Join(",", managed[e]));
        }
        // The TableProperties fields of table_file_creation; native then lists its user-collected properties (the block
        // based table's index type / filtering ones), which the managed event leaves out.
        List<string> nt = native["table_file_creation.table_properties"], mt = managed["table_file_creation.table_properties"];
        CollectionAssert.AreEqual(nt.Take(mt.Count).ToList(), mt, string.Join(",", nt) + " vs " + string.Join(",", mt));
        Assert.IsTrue(mt.Count >= 40, mt.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }
}
