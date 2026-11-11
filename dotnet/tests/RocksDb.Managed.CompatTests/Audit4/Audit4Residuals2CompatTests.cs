using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4 residuals (second pass): MANIFEST records of a managed DB compared record by record with the records
/// native RocksDB writes for the same operations (VersionSet::ProcessManifestWrites / LogAndApplyHelper /
/// LogAndApplyCFHelper, db/version_set.cc).
/// </summary>
[TestClass]
public sealed class Audit4Residuals2CompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string WriteSst(string path)
    {
        using var w = new SstFileWriter(new ColumnFamilyOptions(), LocalFileSystem.Instance);
        w.Open(path);
        for (int i = 0; i < 10; i++) w.Put(U("z" + i), U("ingested" + i));
        w.Finish();
        return path;
    }

    /// <summary>
    /// The fields LogAndApplyHelper / LogAndApplyCFHelper / ProcessManifestWrites stamp on an edit, plus the edit's
    /// payload shape. File sizes, checksums, unique ids and times are left out (they legitimately differ).
    /// </summary>
    private static string Shape(VersionEdit e)
    {
        var sb = new StringBuilder();
        sb.Append("cf=").Append(e.ColumnFamily);
        if (e.ColumnFamilyAddName is not null) sb.Append(" add=").Append(e.ColumnFamilyAddName);
        if (e.IsColumnFamilyDrop) sb.Append(" drop");
        if (e.IsInAtomicGroup) sb.Append(" atomic(").Append(e.RemainingEntries).Append(')');
        if (e.DbId is not null) sb.Append(" dbid");
        if (e.ComparatorName is not null) sb.Append(" cmp=").Append(e.ComparatorName);
        if (e.LogNumber is not null) sb.Append(" log");
        if (e.PrevLogNumber is ulong p) sb.Append(" prevlog=").Append(p);
        if (e.NextFileNumber is not null) sb.Append(" next");
        if (e.LastSequence is ulong s) sb.Append(" seq=").Append(s);
        if (e.MaxColumnFamily is uint m) sb.Append(" maxcf=").Append(m);
        if (e.MinLogNumberToKeep is not null) sb.Append(" minlog");
        if (e.CommittedOptionsFileNumber is not null) sb.Append(" optionsfile");
        if (e.LastCompactedManifestFileSize is not null) sb.Append(" lastcompactedmanifest");
        if (e.WalAdditions.Count > 0) sb.Append(" wal+").Append(e.WalAdditions.Count);
        if (e.WalDeletionBefore != 0) sb.Append(" wal-");
        foreach ((int level, ulong _) in e.DeletedFiles) sb.Append(" del@L").Append(level);
        foreach (NewFileEntry f in e.NewFiles) sb.Append(" new@L").Append(f.Level);
        if (e.CompactCursors.Count > 0) sb.Append(" cursors=").Append(e.CompactCursors.Count);
        return sb.ToString();
    }

    private static List<string> ManifestShapes(string dbPath, out List<int> sizes)
    {
        string manifestPath = FileNames.GetCurrentManifestPath(LocalFileSystem.Instance, dbPath, out _);
        using ManifestLogReader reader = ManifestLogReader.Open(LocalFileSystem.Instance, manifestPath);
        var shapes = new List<string>();
        sizes = new List<int>();
        while (reader.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            sizes.Add(record.Length);
            shapes.Add(Shape(VersionEdit.DecodeFrom(record.ToArray())));
        }
        return shapes;
    }

    /// <summary>
    /// Two flushes, a manual compaction, an ingestion and a column family creation / flush / drop produce the same
    /// MANIFEST records on both sides: the same number of records, each carrying the same stamped fields
    /// (LogAndApplyHelper: prev_log_number on every non column family edit, next_file, last_sequence;
    /// LogAndApplyCFHelper: next_file, last_sequence, max_column_family only on a drop).
    /// </summary>
    [TestMethod]
    public void ManifestRecordsMatchNativeRecordByRecord()
    {
        using var nativeDir = new TempDir("r2-manifest-n");
        using var managedDir = new TempDir("r2-manifest-m");
        using var sstDir = new TempDir("r2-manifest-sst");
        string sstN = WriteSst(Path.Combine(sstDir.Path, "n.sst"));
        string sstM = WriteSst(Path.Combine(sstDir.Path, "m.sst"));

        var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        var ncf = new RocksDbSharp.ColumnFamilies(new RocksDbSharp.ColumnFamilyOptions().SetDisableAutoCompactions(1));
        using (var db = RocksDbSharp.RocksDb.Open(nopts, nativeDir.Path, ncf))
        {
            var flush = new RocksDbSharp.FlushOptions();
            db.Put(U("a"), U("1"));
            db.Put(U("b"), U("1"));
            db.Flush(flush);
            db.Put(U("a"), U("2"));
            db.Remove(U("b"));
            db.Flush(flush);
            db.CompactRange((byte[])null!, (byte[])null!);
            db.IngestExternalFiles([sstN], new RocksDbSharp.IngestExternalFileOptions());
            RocksDbSharp.ColumnFamilyHandle extra = db.CreateColumnFamily(new RocksDbSharp.ColumnFamilyOptions(), "extra");
            db.Put(U("c"), U("3"), extra);
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, flush.Handle, extra.Handle);
            db.DropColumnFamily("extra");
        }

        using (RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true },
                   new ColumnFamilyOptions { DisableAutoCompactions = true }))
        {
            db.Put(U("a"), U("1"));
            db.Put(U("b"), U("1"));
            db.Flush();
            db.Put(U("a"), U("2"));
            db.Delete(U("b"));
            db.Flush();
            db.CompactRange(null, null);
            db.IngestExternalFiles([sstM], new IngestExternalFileOptions());
            ColumnFamily extra = db.CreateColumnFamily("extra", new ColumnFamilyOptions());
            extra.Put(U("c"), U("3"));
            extra.Flush();
            db.DropColumnFamily(extra);
        }

        AssertSameManifest(nativeDir.Path, managedDir.Path, "first open");
        // Not compared: last_sequence after unflushed writes (native records the largest value the MANIFEST holds so far,
        // the port the DB's current last sequence, which the WAL error recovery's sequence reservation relies on), and
        // the edits of a reopen that replays a WAL. Native writes the recovery as several edits (an empty
        // edit, the flush edit, min_log_number_to_keep), the port as one; the recovered state is the same.
    }

    private static void AssertSameManifest(string nativePath, string managedPath, string context)
    {
        List<string> native = ManifestShapes(nativePath, out List<int> nativeSizes);
        List<string> managed = ManifestShapes(managedPath, out List<int> managedSizes);
        string dump = "\n" + context;
        dump += "\nnative:\n  " + string.Join("\n  ", native) + "\nmanaged:\n  " + string.Join("\n  ", managed) +
                      "\nnative sizes: " + string.Join(",", nativeSizes) + "\nmanaged sizes: " + string.Join(",", managedSizes);
        Assert.AreEqual(native.Count, managed.Count, "record count" + dump);
        for (int i = 0; i < native.Count; i++) Assert.AreEqual(native[i], managed[i], "record " + i + dump);
    }
}
