using System.Runtime.InteropServices;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Soak;

/// <summary>DB file kinds (for fsync triggers, torn-write victims and the corruption nemesis).</summary>
internal enum DbFileKind
{
    Other,
    Wal,
    Manifest,
    Sst,
    Blob,
    Options,
    Current,
}

internal static class DbFiles
{
    public static DbFileKind KindOf(string name)
    {
        name = Path.GetFileName(name);
        if (name.EndsWith(".log", StringComparison.Ordinal) && char.IsDigit(name[0])) return DbFileKind.Wal;
        if (name.StartsWith("MANIFEST-", StringComparison.Ordinal)) return DbFileKind.Manifest;
        if (name.EndsWith(".sst", StringComparison.Ordinal)) return DbFileKind.Sst;
        if (name.EndsWith(".blob", StringComparison.Ordinal)) return DbFileKind.Blob;
        if (name.StartsWith("OPTIONS-", StringComparison.Ordinal)) return DbFileKind.Options;
        if (name == "CURRENT") return DbFileKind.Current;
        return DbFileKind.Other;
    }
}

/// <summary>How the in-process fault file system ends the process.</summary>
internal enum CacheFault
{
    /// <summary>LazyFS clear-cache: all unsynced data and unsynced directory entries are lost.</summary>
    ClearCache,
    /// <summary>LazyFS torn-op: some parts of one unsynced write persist, everything else unsynced is lost.</summary>
    TornOp,
    /// <summary>LazyFS torn-seq: a non-prefix subset of the unsynced writes of one file persists (gaps read as zeros).</summary>
    TornSeq,
}

/// <summary>
/// An in-process equivalent of LazyFS (and of upstream's FaultInjectionTestFS) over the real
/// <see cref="LocalFileSystem"/>, for the files directly inside one directory (the DB directory):
/// <list type="bullet">
/// <item>It tracks, per file, the length covered by the last successful fsync and the unsynced writes since then
/// (offset and length; for overwrites of synced data, as in WAL recycling, the old bytes are saved), and per
/// directory the listing as of the last directory fsync. Deleted or replaced files still named by that listing are
/// kept as hard links in a graveyard so they can come back.</item>
/// <item><see cref="Crash"/> stops all file system activity (and the history), rewrites the directory to what a
/// power loss would leave (<see cref="CacheFault"/>), and ends the process with <c>_exit</c>. Data, files and
/// directory entries that were synced are kept; with torn faults selected unsynced writes (or parts of one) persist,
/// with zeros in the gaps.</item>
/// <item>An fsync trigger: <see cref="ArmFsyncTrigger"/> crashes with clear-cache right after the N-th fsync of a
/// file kind (WAL, MANIFEST, SST) completes, before the DB sees it return.</item>
/// <item>Random I/O errors on appends, syncs and reads (<see cref="SetErrorRates"/>).</item>
/// <item>A test-only switch <see cref="SkipSync"/> that makes fsync a no-op for durability (to prove the checks
/// catch a missing fsync).</item>
/// </list>
/// Files outside the directory (checkpoints) pass through untracked.
/// </summary>
internal sealed class LazyFaultFileSystem : IFileSystem
{
    private readonly IFileSystem _inner = LocalFileSystem.Instance;
    private readonly string _root;
    private readonly string _graveyard;
    private readonly Lock _meta = new();
    private readonly Dictionary<string, Tracked> _current = new(StringComparer.Ordinal);
    private Dictionary<string, Tracked> _durable = new(StringComparer.Ordinal);
    private long _nextId;
    private int _inflight;
    private volatile bool _frozen;
    private readonly Action<string> _log;

    // I/O error injection (parts per million).
    private volatile int _writeErrorPpm;
    private volatile int _syncErrorPpm;
    private volatile int _readErrorPpm;
    private long _injectedErrors;

    // fsync trigger
    private DbFileKind _triggerKind;
    private int _triggerRemaining;
    private Action? _onTrigger;

    public LazyFaultFileSystem(string dbDir, string graveyard, Action<string> log)
    {
        _root = Normalize(dbDir);
        _graveyard = Normalize(graveyard);
        _log = log;
        Directory.CreateDirectory(_root);
        if (Directory.Exists(_graveyard)) Directory.Delete(_graveyard, recursive: true);
        Directory.CreateDirectory(_graveyard);
        // Everything that exists when the process starts is durable (we do not model a power loss across cycles).
        foreach (string f in Directory.GetFiles(_root))
        {
            var t = new Tracked(++_nextId) { SyncedLen = new FileInfo(f).Length };
            _current[Path.GetFileName(f)] = t;
        }
        _durable = new Dictionary<string, Tracked>(_current, StringComparer.Ordinal);
    }

    /// <summary>Test-only: fsync does not make data or directory entries durable.</summary>
    public bool SkipSync { get; set; }

    /// <summary>
    /// Test-only: fsync and directory fsync are not passed to the real file system either. With the DB on a LazyFS
    /// mount this is a port that never fsyncs; the next <c>lazyfs::clear-cache</c> must make the durability check fail.
    /// </summary>
    public bool SkipRealSync { get; set; }

    public long InjectedErrors => Interlocked.Read(ref _injectedErrors);

    /// <summary>Kind of injected I/O error: "plain" IOError, "retryable" (IOStatus retryable) or "nospace".</summary>
    private volatile string _errorKind = "plain";

    public void SetErrorRates(int writePpm, int syncPpm, int readPpm, string kind)
    {
        _errorKind = kind;
        _writeErrorPpm = writePpm;
        _syncErrorPpm = syncPpm;
        _readErrorPpm = readPpm;
    }

    /// <summary>Calls <paramref name="onTrigger"/> right after the <paramref name="n"/>-th successful fsync of a <paramref name="kind"/> file.</summary>
    public void ArmFsyncTrigger(DbFileKind kind, int n, Action onTrigger)
    {
        lock (_meta)
        {
            _triggerKind = kind;
            _triggerRemaining = n;
            _onTrigger = onTrigger;
        }
    }

    private static string Normalize(string p)
    {
        string full = Path.GetFullPath(p);
        return full.Length > 1 && full[^1] == Path.DirectorySeparatorChar ? full[..^1] : full;
    }

    private bool IsTracked(string path, out string name)
    {
        string full = Normalize(path);
        name = Path.GetFileName(full);
        return string.Equals(Path.GetDirectoryName(full), _root, StringComparison.Ordinal);
    }

    // ---- gate: every operation runs inside Enter/Exit; Freeze waits for in-flight operations and blocks new ones ----

    private void Enter()
    {
        while (true)
        {
            if (_frozen) Thread.Sleep(Timeout.Infinite);
            Interlocked.Increment(ref _inflight);
            if (!_frozen) return;
            Interlocked.Decrement(ref _inflight);
        }
    }

    private void Exit() => Interlocked.Decrement(ref _inflight);

    private void Freeze(int ownInflight)
    {
        _frozen = true;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (Volatile.Read(ref _inflight) > ownInflight)
        {
            if (sw.Elapsed > TimeSpan.FromSeconds(30))
            {
                _log("fault fs: in-flight operations did not drain in 30 s; crashing anyway (" + Volatile.Read(ref _inflight) + ")");
                break;
            }
            Thread.Sleep(1);
        }
    }

    private bool Roll(int ppm) => ppm > 0 && Random.Shared.Next(1_000_000) < ppm;

    private RocksDbException Injected(string what, string path)
    {
        Interlocked.Increment(ref _injectedErrors);
        string msg = "injected " + what + " error: " + path;
        return _errorKind switch
        {
            "retryable" => RocksDbException.RetryableIOError(msg),
            "nospace" => RocksDbException.NoSpace(msg),
            _ => new RocksDbException(StatusCode.IOError, "IO error: " + msg),
        };
    }

    // ---- tracked state ----

    private sealed class WriteRec(long offset, int length)
    {
        public long Offset = offset;
        public int Length = length;
        public long End => Offset + Length;
    }

    private sealed class Tracked(long id)
    {
        public readonly long Id = id;
        public readonly Lock L = new();
        public long SyncedLen;
        public readonly List<WriteRec> Unsynced = [];
        /// <summary>Old synced bytes overwritten since the last sync: offset -> bytes.</summary>
        public readonly SortedDictionary<long, byte[]> Undo = [];
        /// <summary>Overwrites below this offset already saved their old bytes.</summary>
        public long UndoCapturedTo;
        public string? GraveyardPath;
        public int SyncCount;
    }

    private bool IsDurablyReferenced(Tracked t)
    {
        foreach (Tracked d in _durable.Values)
            if (ReferenceEquals(d, t)) return true;
        return false;
    }

    private bool IsCurrentlyNamedElsewhere(Tracked t, string name)
    {
        foreach (KeyValuePair<string, Tracked> kv in _current)
            if (ReferenceEquals(kv.Value, t) && kv.Key != name) return true;
        return false;
    }

    /// <summary>Keeps the file's inode reachable (hard link in the graveyard) before its last name goes away.</summary>
    private void PreserveIfNeeded(Tracked t, string name)
    {
        if (t.GraveyardPath is null && IsDurablyReferenced(t) && !IsCurrentlyNamedElsewhere(t, name))
        {
            string g = Path.Combine(_graveyard, t.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _inner.LinkFile(Path.Combine(_root, name), g);
            t.GraveyardPath = g;
        }
    }

    // ---- IFileSystem ----

    public IWritableFile CreateWritable(string path, bool truncate = true) => CreateWritable(path, truncate, WritableFileOptions.Default);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
    {
        if (!IsTracked(path, out string name)) return _inner.CreateWritable(path, truncate, options);
        Enter();
        try
        {
            if (Roll(_writeErrorPpm)) throw Injected("create", path);
            Tracked t;
            lock (_meta)
            {
                if (_current.TryGetValue(name, out Tracked? existing))
                {
                    if (truncate)
                    {
                        // The old contents may still be needed after a power loss: keep a copy in the graveyard.
                        if (IsDurablyReferenced(existing) && !IsCurrentlyNamedElsewhere(existing, name) && existing.GraveyardPath is null)
                        {
                            string g = Path.Combine(_graveyard, existing.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
                            File.Copy(Path.Combine(_root, name), g);
                            existing.GraveyardPath = g;
                        }
                        t = new Tracked(++_nextId);
                        _current[name] = t;
                    }
                    else t = existing;
                }
                else
                {
                    t = new Tracked(++_nextId);
                    _current[name] = t;
                }
            }
            IWritableFile f = _inner.CreateWritable(path, truncate, options);
            return new Writable(this, f, t, Path.Combine(_root, name));
        }
        finally { Exit(); }
    }

    public IWritableFile ReopenWritable(string path, WritableFileOptions options)
    {
        if (!IsTracked(path, out string name)) return _inner.ReopenWritable(path, options);
        Enter();
        try
        {
            Tracked t;
            lock (_meta)
            {
                if (!_current.TryGetValue(name, out Tracked? existing))
                {
                    existing = new Tracked(++_nextId);
                    _current[name] = existing;
                }
                t = existing;
            }
            IWritableFile f = _inner.ReopenWritable(path, options);
            return new Writable(this, f, t, Path.Combine(_root, name));
        }
        finally { Exit(); }
    }

    public IRandomAccessFile OpenRandomAccess(string path) => OpenRandomAccess(path, RandomAccessFileOptions.Default);

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options)
    {
        IRandomAccessFile f = _inner.OpenRandomAccess(path, options);
        return IsTracked(path, out _) ? new RandomReader(this, f, path) : f;
    }

    public ISequentialFile OpenSequential(string path) => OpenSequential(path, SequentialFileOptions.Default);

    public ISequentialFile OpenSequential(string path, SequentialFileOptions options)
    {
        ISequentialFile f = _inner.OpenSequential(path, options);
        return IsTracked(path, out _) ? new SequentialReader(this, f, path) : f;
    }

    public bool FileExists(string path) => _inner.FileExists(path);
    public long GetFileSize(string path) => _inner.GetFileSize(path);
    public bool DirectoryExists(string path) => _inner.DirectoryExists(path);
    public IEnumerable<string> GetChildren(string directory) => _inner.GetChildren(directory);
    public IDisposable LockFile(string path) => _inner.LockFile(path);
    public DateTimeOffset GetFileModificationTime(string path) => _inner.GetFileModificationTime(path);
    public long GetFreeSpace(string path) => _inner.GetFreeSpace(path);
    public void CreateDirectoryIfMissing(string path) => _inner.CreateDirectoryIfMissing(path);
    public void DeleteDirectory(string path) => _inner.DeleteDirectory(path);
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => _inner.WriteAt(path, offset, data);

    public void DeleteFile(string path)
    {
        if (!IsTracked(path, out string name))
        {
            _inner.DeleteFile(path);
            return;
        }
        Enter();
        try
        {
            if (Roll(_writeErrorPpm)) throw Injected("delete", path);
            lock (_meta)
            {
                if (_current.TryGetValue(name, out Tracked? t))
                {
                    PreserveIfNeeded(t, name);
                    _current.Remove(name);
                }
                _inner.DeleteFile(path);
            }
        }
        finally { Exit(); }
    }

    public void RenameFile(string source, string target)
    {
        bool st = IsTracked(source, out string sname), tt = IsTracked(target, out string tname);
        if (!st && !tt)
        {
            _inner.RenameFile(source, target);
            return;
        }
        Enter();
        try
        {
            if (Roll(_writeErrorPpm)) throw Injected("rename", source);
            lock (_meta)
            {
                Tracked? moving = null;
                if (st && _current.TryGetValue(sname, out moving)) PreserveIfNeeded(moving, sname);
                if (tt && _current.TryGetValue(tname, out Tracked? replaced)) PreserveIfNeeded(replaced, tname);
                _inner.RenameFile(source, target);
                if (st) _current.Remove(sname);
                if (tt) _current[tname] = moving ?? new Tracked(++_nextId) { SyncedLen = _inner.GetFileSize(target) };
            }
        }
        finally { Exit(); }
    }

    public void LinkFile(string source, string target)
    {
        _inner.LinkFile(source, target);
        if (IsTracked(target, out string tname))
        {
            lock (_meta)
            {
                _current[tname] = IsTracked(source, out string sname) && _current.TryGetValue(sname, out Tracked? s)
                    ? s
                    : new Tracked(++_nextId) { SyncedLen = _inner.GetFileSize(target) };
            }
        }
    }

    public void SyncDirectory(string path)
    {
        if (Normalize(path) != _root)
        {
            if (!SkipRealSync) _inner.SyncDirectory(path);
            return;
        }
        Enter();
        try
        {
            if (Roll(_syncErrorPpm)) throw Injected("directory sync", path);
            if (!SkipRealSync) _inner.SyncDirectory(path);
            if (SkipSync) return;
            lock (_meta)
            {
                _durable = new Dictionary<string, Tracked>(_current, StringComparer.Ordinal);
            }
        }
        finally { Exit(); }
    }

    /// <summary>True when a WAL file with a durable directory entry has at least <paramref name="need"/> unsynced writes.</summary>
    public bool HasWalTornCandidate(int need)
    {
        lock (_meta)
        {
            foreach (KeyValuePair<string, Tracked> kv in _durable)
            {
                if (DbFiles.KindOf(kv.Key) != DbFileKind.Wal || !_current.TryGetValue(kv.Key, out Tracked? cur) || !ReferenceEquals(cur, kv.Value)) continue;
                lock (cur.L)
                    if (cur.Unsynced.Count >= need && cur.Undo.Count == 0) return true;
            }
        }
        return false;
    }

    // ---- the crash ----

    /// <summary>
    /// Simulates the power loss: <paramref name="beforeFreeze"/> (stop the history) runs first, then all file system
    /// activity is stopped, the directory is rewritten to what survives <paramref name="fault"/>, and the process ends
    /// with exit code 77. <paramref name="ownInflight"/> is 1 when called from inside a file operation (the fsync trigger).
    /// </summary>
    public void Crash(CacheFault fault, Random rng, Action beforeFreeze, int ownInflight)
    {
        beforeFreeze();
        Freeze(ownInflight);
        string summary;
        try
        {
            summary = Apply(fault, rng);
        }
        catch (Exception e)
        {
            summary = "fault application failed: " + e;
        }
        _log("CRASH " + fault + ": " + summary);
        Console.Out.Flush();
        NativeExit.Exit(77);
    }

    private sealed class Part(long offset, byte[] data)
    {
        public readonly long Offset = offset;
        public readonly byte[] Data = data;
    }

    /// <summary>Rewrites the directory; returns a description.</summary>
    internal string Apply(CacheFault fault, Random rng)
    {
        lock (_meta)
        {
            // Which identity is the torn victim, and which unsynced parts of it persist (read before anything changes).
            Tracked? victim = null;
            string victimName = "";
            var persist = new List<Part>();
            string tornInfo = "";
            if (fault != CacheFault.ClearCache)
            {
                var candidates = new List<KeyValuePair<string, Tracked>>();
                foreach (KeyValuePair<string, Tracked> kv in _durable)
                {
                    if (!_current.TryGetValue(kv.Key, out Tracked? cur) || !ReferenceEquals(cur, kv.Value)) continue;
                    int need = fault == CacheFault.TornSeq ? 2 : 1;
                    DbFileKind vk = DbFiles.KindOf(kv.Key);
                    // Torn writes hit table, blob and WAL files. A torn MANIFEST tail is reported as Corruption by
                    // upstream too (VersionSet::Recover fails on a bad record), so it only gets clear-cache.
                    if (vk is DbFileKind.Wal or DbFileKind.Sst or DbFileKind.Blob && kv.Value.Unsynced.Count >= need && kv.Value.Undo.Count == 0) candidates.Add(kv);
                }
                // Prefer WAL files (the interesting case for point-in-time recovery).
                var wal = candidates.FindAll(static kv => DbFiles.KindOf(kv.Key) == DbFileKind.Wal);
                if (wal.Count > 0 && rng.Next(4) != 0) candidates = wal;
                if (candidates.Count > 0)
                {
                    KeyValuePair<string, Tracked> pick = candidates[rng.Next(candidates.Count)];
                    victim = pick.Value;
                    victimName = pick.Key;
                    string full = Path.Combine(_root, victimName);
                    long diskLen = new FileInfo(full).Length;
                    using var fh = File.OpenHandle(full, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    if (fault == CacheFault.TornOp)
                    {
                        WriteRec w = victim.Unsynced[rng.Next(victim.Unsynced.Count)];
                        int parts = Math.Min(w.Length, rng.Next(2, 5));
                        var cuts = new SortedSet<int> { 0, w.Length };
                        while (cuts.Count < parts + 1 && w.Length > 1) cuts.Add(rng.Next(1, w.Length));
                        int[] c = [.. cuts];
                        int n = c.Length - 1;
                        int mask;
                        do mask = rng.Next(1, 1 << n); while (n > 1 && mask == (1 << n) - 1);
                        var kept = new List<string>();
                        for (int i = 0; i < n; i++)
                        {
                            if ((mask & (1 << i)) == 0) continue;
                            AddPart(fh, diskLen, w.Offset + c[i], c[i + 1] - c[i], persist);
                            kept.Add($"[{c[i]},{c[i + 1]})");
                        }
                        tornInfo = $" torn-op on {victimName}: write @{w.Offset}+{w.Length} split in {n}, kept {string.Join(" ", kept)}";
                    }
                    else
                    {
                        int n = Math.Min(victim.Unsynced.Count, 62);
                        int first = victim.Unsynced.Count - n;
                        var keep = new bool[n];
                        for (int i = 0; i < n; i++) keep[i] = rng.Next(2) == 0;
                        // Must not be a prefix: some write is lost while a later one persists.
                        bool prefix = true, seenGap = false;
                        for (int i = 0; i < n; i++)
                        {
                            if (!keep[i]) seenGap = true;
                            else if (seenGap) prefix = false;
                        }
                        if (prefix)
                        {
                            int i = rng.Next(0, n - 1), j = rng.Next(i + 1, n);
                            keep[i] = false;
                            keep[j] = true;
                        }
                        var kept = new List<int>();
                        for (int i = 0; i < n; i++)
                        {
                            if (!keep[i]) continue;
                            WriteRec w = victim.Unsynced[first + i];
                            AddPart(fh, diskLen, w.Offset, w.Length, persist);
                            kept.Add(i + 1);
                        }
                        tornInfo = $" torn-seq on {victimName}: {n} unsynced writes, kept #{string.Join(",#", kept)}";
                    }
                }
                else
                {
                    int unsynced = 0, notDurable = 0, recycled = 0;
                    foreach (KeyValuePair<string, Tracked> kv in _current)
                    {
                        if (kv.Value.Unsynced.Count == 0) continue;
                        unsynced++;
                        if (!_durable.TryGetValue(kv.Key, out Tracked? d) || !ReferenceEquals(d, kv.Value)) notDurable++;
                        if (kv.Value.Undo.Count > 0) recycled++;
                    }
                    var wals = new List<string>();
                    foreach (KeyValuePair<string, Tracked> kv in _current)
                    {
                        if (DbFiles.KindOf(kv.Key) != DbFileKind.Wal) continue;
                        bool durable = _durable.TryGetValue(kv.Key, out Tracked? d) && ReferenceEquals(d, kv.Value);
                        wals.Add($"{kv.Key}: synced {kv.Value.SyncedLen}, {kv.Value.Unsynced.Count} unsynced writes{(durable ? "" : ", entry not durable")}");
                    }
                    tornInfo = $" (no torn candidate: {unsynced} files with unsynced writes, {notDurable} of them without a durable" +
                        $" directory entry, {recycled} recycled; WALs: {string.Join("; ", wals)}; plain clear-cache)";
                }
            }

            // 1. Where does each durable entry's data come from?
            var staged = new Dictionary<string, string>(StringComparer.Ordinal); // name -> staging path
            string staging = Path.Combine(_graveyard, "staging");
            Directory.CreateDirectory(staging);
            int stagedCount = 0;
            foreach (KeyValuePair<string, Tracked> kv in _durable)
            {
                if (_current.TryGetValue(kv.Key, out Tracked? cur) && ReferenceEquals(cur, kv.Value)) continue;
                string? source = null;
                foreach (KeyValuePair<string, Tracked> c in _current)
                {
                    if (ReferenceEquals(c.Value, kv.Value))
                    {
                        source = Path.Combine(_root, c.Key);
                        break;
                    }
                }
                source ??= kv.Value.GraveyardPath;
                if (source is null || !File.Exists(source)) continue;
                string s = Path.Combine(staging, (stagedCount++).ToString(System.Globalization.CultureInfo.InvariantCulture));
                _inner.LinkFile(source, s);
                staged[kv.Key] = s;
            }
            // 2. Remove every entry that is not durable (or names another file now).
            int removed = 0;
            foreach (string f in Directory.GetFiles(_root))
            {
                string name = Path.GetFileName(f);
                if (_durable.TryGetValue(name, out Tracked? d) && _current.TryGetValue(name, out Tracked? cur) && ReferenceEquals(d, cur)) continue;
                File.Delete(f);
                removed++;
            }
            // 3. Restore replaced / renamed / deleted durable entries.
            foreach (KeyValuePair<string, string> kv in staged) File.Move(kv.Value, Path.Combine(_root, kv.Key));
            // 4. Contents: synced length, undo overwrites, then the persisted torn parts.
            int truncated = 0;
            var done = new HashSet<long>();
            foreach (KeyValuePair<string, Tracked> kv in _durable)
            {
                string full = Path.Combine(_root, kv.Key);
                if (!File.Exists(full) || !done.Add(kv.Value.Id)) continue;
                Tracked t = kv.Value;
                using var fs = new FileStream(full, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                if (fs.Length > t.SyncedLen)
                {
                    fs.SetLength(t.SyncedLen);
                    truncated++;
                }
                foreach (KeyValuePair<long, byte[]> u in t.Undo)
                {
                    fs.Position = u.Key;
                    fs.Write(u.Value);
                }
                if (ReferenceEquals(t, victim))
                {
                    foreach (Part p in persist)
                    {
                        fs.Position = p.Offset;
                        fs.Write(p.Data);
                    }
                }
                fs.Flush(flushToDisk: true);
            }
            Directory.Delete(_graveyard, recursive: true);
            return $"{removed} non-durable entries removed, {staged.Count} restored, {truncated} files cut to their synced length{tornInfo}";
        }
    }

    private static void AddPart(Microsoft.Win32.SafeHandles.SafeFileHandle fh, long diskLen, long offset, int length, List<Part> persist)
    {
        if (offset >= diskLen) return; // never reached the OS
        int n = (int)Math.Min(length, diskLen - offset);
        var data = new byte[n];
        int read = RandomAccess.Read(fh, data, offset);
        if (read < n) Array.Resize(ref data, read);
        persist.Add(new Part(offset, data));
    }

    // ---- file wrappers ----

    private sealed class Writable(LazyFaultFileSystem fs, IWritableFile inner, Tracked t, string path) : IWritableFile
    {
        public long Size => inner.Size;

        private void Record(long offset, int length)
        {
            if (length == 0) return;
            lock (t.L)
            {
                if (offset < t.SyncedLen)
                {
                    // Overwrite of synced data (recycled WAL): save the old bytes once.
                    long from = Math.Max(offset, t.UndoCapturedTo);
                    long to = Math.Min(offset + length, t.SyncedLen);
                    if (to > from)
                    {
                        var old = new byte[to - from];
                        using (var h = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                        {
                            int n = RandomAccess.Read(h, old, from);
                            if (n < old.Length) Array.Resize(ref old, n);
                        }
                        if (old.Length > 0) t.Undo[from] = old;
                        t.UndoCapturedTo = to;
                    }
                }
                List<WriteRec> list = t.Unsynced;
                if (list.Count >= 2048)
                {
                    // Bound the memory: merge the oldest adjacent pairs.
                    var merged = new List<WriteRec>(list.Count / 2 + 1);
                    for (int i = 0; i < list.Count; i += 2)
                    {
                        if (i + 1 < list.Count && list[i].End == list[i + 1].Offset)
                            merged.Add(new WriteRec(list[i].Offset, list[i].Length + list[i + 1].Length));
                        else
                        {
                            merged.Add(list[i]);
                            if (i + 1 < list.Count) merged.Add(list[i + 1]);
                        }
                    }
                    list.Clear();
                    list.AddRange(merged);
                }
                list.Add(new WriteRec(offset, length));
            }
        }

        public void Append(ReadOnlySpan<byte> data)
        {
            fs.Enter();
            try
            {
                if (fs.Roll(fs._writeErrorPpm)) throw fs.Injected("append", path);
                Record(inner.Size, data.Length);
                inner.Append(data);
            }
            finally { fs.Exit(); }
        }

        public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            fs.Enter();
            try
            {
                if (fs.Roll(fs._writeErrorPpm)) throw fs.Injected("append", path);
                Record(inner.Size, data.Length);
                await inner.AppendAsync(data, cancellationToken).ConfigureAwait(false);
            }
            finally { fs.Exit(); }
        }

        public void Flush()
        {
            fs.Enter();
            try { inner.Flush(); }
            finally { fs.Exit(); }
        }

        public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            fs.Enter();
            try { await inner.FlushAsync(cancellationToken).ConfigureAwait(false); }
            finally { fs.Exit(); }
        }

        public void Sync()
        {
            fs.Enter();
            bool trigger;
            try
            {
                if (fs.Roll(fs._syncErrorPpm)) throw fs.Injected("fsync", path);
                long size = inner.Size;
                if (!fs.SkipRealSync) inner.Sync();
                trigger = AfterSync(size);
                if (trigger) fs._onTrigger!();
            }
            finally { fs.Exit(); }
        }

        public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
        {
            fs.Enter();
            try
            {
                if (fs.Roll(fs._syncErrorPpm)) throw fs.Injected("fsync", path);
                long size = inner.Size;
                if (!fs.SkipRealSync) await inner.SyncAsync(cancellationToken).ConfigureAwait(false);
                if (AfterSync(size)) fs._onTrigger!();
            }
            finally { fs.Exit(); }
        }

        /// <summary>Marks the data synced; returns true when the fsync trigger fires.</summary>
        private bool AfterSync(long size)
        {
            if (!fs.SkipSync)
            {
                lock (t.L)
                {
                    if (size > t.SyncedLen) t.SyncedLen = size;
                    t.Unsynced.RemoveAll(w => w.End <= size);
                    t.Undo.Clear();
                    t.SyncCount++;
                }
            }
            lock (fs._meta)
            {
                if (fs._triggerRemaining > 0 && DbFiles.KindOf(path) == fs._triggerKind && --fs._triggerRemaining == 0) return true;
            }
            return false;
        }

        private void AfterClose()
        {
            // A truncate-on-close (WAL recycling) is durable like the data it removes (as MemoryFileSystem).
            long len;
            try { len = new FileInfo(path).Length; }
            catch (IOException) { return; }
            lock (t.L)
            {
                if (len < t.SyncedLen) t.SyncedLen = len;
                t.Unsynced.RemoveAll(w => w.Offset >= len);
            }
        }

        public void Dispose()
        {
            fs.Enter();
            try { inner.Dispose(); }
            finally { fs.Exit(); }
            AfterClose();
        }

        public async ValueTask DisposeAsync()
        {
            fs.Enter();
            try { await inner.DisposeAsync().ConfigureAwait(false); }
            finally { fs.Exit(); }
            AfterClose();
        }
    }

    private sealed class RandomReader(LazyFaultFileSystem fs, IRandomAccessFile inner, string path) : IRandomAccessFile
    {
        public long Length => inner.Length;

        public int Read(long offset, Span<byte> destination)
        {
            if (fs.Roll(fs._readErrorPpm)) throw fs.Injected("read", path);
            return inner.Read(offset, destination);
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            if (fs.Roll(fs._readErrorPpm)) throw fs.Injected("read", path);
            return inner.ReadAsync(offset, destination, cancellationToken);
        }

        public bool Prefetch(long offset, long length) => inner.Prefetch(offset, length);

        public void Dispose() => inner.Dispose();
    }

    private sealed class SequentialReader(LazyFaultFileSystem fs, ISequentialFile inner, string path) : ISequentialFile
    {
        public int Read(Span<byte> destination)
        {
            if (fs.Roll(fs._readErrorPpm)) throw fs.Injected("read", path);
            return inner.Read(destination);
        }

        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            if (fs.Roll(fs._readErrorPpm)) throw fs.Injected("read", path);
            return inner.ReadAsync(destination, cancellationToken);
        }

        public void Skip(long bytes) => inner.Skip(bytes);

        public void Dispose() => inner.Dispose();
    }
}

/// <summary>Ends the process immediately (no finalizers, no flushing): libc _exit on Unix.</summary>
internal static class NativeExit
{
    [DllImport("libc", EntryPoint = "_exit")]
    private static extern void UnixExit(int code);

    public static void Exit(int code)
    {
        if (OperatingSystem.IsWindows()) Environment.FailFast("soak: simulated crash " + code);
        UnixExit(code);
    }
}
