using System.Buffers;
using System.Diagnostics;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.Soak;

/// <summary>
/// The worker child process of a RUN phase: opens the DB, runs the workload threads, records every operation in the
/// history, runs online checks, and obeys supervisor commands on stdin (stop, crash, arm, ioerr). Exit codes:
/// 0 clean stop, 3 online check violation, 5 close failed, 77 simulated crash, 137 when SIGKILLed.
/// </summary>
internal sealed class Worker
{
    public const int ExitViolation = 3;
    public const int ExitCloseFailed = 5;
    public const int ExitCrash = 77;

    private readonly RunPaths _paths;
    private readonly RunConfig _cfg;
    private readonly CyclePlan _plan;
    private readonly Model _model;
    private readonly Layout _l;
    private readonly HistoryWriter _history;
    private readonly IFileSystem _fs;
    private readonly LazyFaultFileSystem? _faultFs;
    private OptimisticTransactionDatabase _occ = null!;
    private RocksDatabase _db = null!;
    private ColumnFamily _reg0 = null!, _reg1 = null!, _bank = null!, _ctr = null!, _rdel = null!;
    private readonly CancellationTokenSource _stop = new();
    private readonly WriteOptions _syncWrite = new() { Sync = true };
    private readonly WriteOptions _plainWrite = new();
    private readonly ulong _seed;

    // Online monotonic-read state, per register key: (version << 1) | deleted.
    private readonly long[] _acked;
    private readonly long[] _invoked;
    private long _errors;
    private int _scratchCount;
    // Probability of WriteOptions.Sync per write, per 100,000 (chosen per worker: 0, 0.05% or 0.5%). Sync writes
    // sync the whole WAL, so they must stay rare for the power-loss faults to find unsynced data.
    private readonly int _syncPer100k;

    public Worker(RunPaths paths, int part)
    {
        _paths = paths;
        _cfg = RunConfig.Load(paths.RunJson);
        _plan = CyclePlan.Load(paths.CycleJson);
        _model = Model.Load(paths.ModelFile);
        _l = _cfg.Layout;
        _seed = Keys.Mix(_plan.CycleSeed ^ (ulong)part * 0x51ED27);
        _syncPer100k = (int)(_seed % 3) switch { 0 => 0, 1 => 50, _ => 500 };
        _history = new HistoryWriter(paths.History(_plan.Cycle, part), (long)Environment.ProcessId << 32);
        if (_plan.FaultFs)
        {
            _faultFs = new LazyFaultFileSystem(paths.Db, Path.Combine(paths.Tmp, "graveyard-" + Environment.ProcessId), Log) { SkipSync = _plan.SkipSync, SkipRealSync = _plan.SkipRealSync, NoGraveyard = _plan.SkipRealSync };
            _fs = _faultFs;
        }
        else _fs = LocalFileSystem.Instance;
        int n = _l.Writers * _l.KeysPerWriter;
        _acked = new long[n];
        _invoked = new long[n];
        for (int i = 0; i < n; i++)
        {
            Cell c = _model.Registers[i];
            _acked[i] = _invoked[i] = Pack(c.Version, !c.Present);
        }
        // A later worker of the same cycle (after a crash during recovery) continues the versions of the earlier ones,
        // and their invoked writes may be in the DB.
        var earlier = new List<string>();
        for (int p = 0; p < part; p++) earlier.Add(paths.History(_plan.Cycle, p));
        foreach (HistoryRecord r in HistoryReader.ReadParts(earlier))
        {
            if (r.Phase != Phase.Invoke) continue;
            if (r.Type is OpType.RegPut or OpType.RegDel && r.Process < _l.Writers)
            {
                _model.NextVersion[r.Process] = Math.Max(_model.NextVersion[r.Process], r.V + 1);
                int idx = r.Process * _l.KeysPerWriter + r.A;
                if (r.V > VersionOf(_invoked[idx])) _invoked[idx] = Pack(r.V, r.Type == OpType.RegDel);
            }
            else if (r.Type is OpType.RdelPut or OpType.RdelRange)
                _model.RdelNextVersion = Math.Max(_model.RdelNextVersion, r.V + 1);
        }
    }

    private static long Pack(long version, bool deleted) => (version << 1) | (deleted ? 1L : 0L);
    private static long VersionOf(long packed) => packed >> 1;
    private static bool DeletedOf(long packed) => (packed & 1) != 0;

    private static void Log(string s) => Console.WriteLine(DateTime.UtcNow.ToString("HH:mm:ss.fff") + " " + s);

    private void Violation(string what)
    {
        Log("VIOLATION " + what);
        Console.Out.Flush();
        NativeExit.Exit(ExitViolation);
    }

    public async Task<int> RunAsync()
    {
        Log($"worker pid {Environment.ProcessId} cycle {_plan.Cycle} fault {_plan.Fault} fs {(_faultFs is null ? "local" : "fault-injecting")} sync writes {_syncPer100k / 1000.0:F2}% options: {_plan.Options}");
        // The workload mixes blocking (sync) and async calls on pool threads; enough threads so that blocked callers
        // do not starve the continuations of the async ones (thread injection is slow).
        ThreadPool.SetMinThreads(64, 64);
        var sw = Stopwatch.StartNew();
        BlockCache cache = BlockCache.CreateLru(_plan.Options.BlockCacheSize);
        List<ColumnFamilyDescriptor> cfs = DbSetup.Descriptors(_paths.Db, _fs, _cfg.Db, _plan.Options, cache);
        // ValidateSerial: the conflict check runs inside the write group, so it also sees the plain (non-transactional)
        // merge-batch transfers on the same accounts. With ValidateParallel (upstream's default) the check runs before
        // the write group and only excludes other optimistic transactions: a plain write landing between the check and
        // the write is lost, upstream as well, so that policy cannot be mixed with plain writes to the same keys.
        _occ = await OptimisticTransactionDatabase.OpenAsync(_paths.Db, DbSetup.DbOptions(_plan.Options, _fs),
                new OptimisticTransactionDbOptions { ValidatePolicy = OccValidationPolicy.ValidateSerial }, cfs)
            .ConfigureAwait(false);
        _db = _occ.Database;
        Console.WriteLine($"OPENED {sw.Elapsed.TotalMilliseconds:F0}");
        Console.Out.Flush();
        _reg0 = _db.GetColumnFamily(Keys.Reg0);
        _reg1 = _db.GetColumnFamily(Keys.Reg1);
        _bank = _db.GetColumnFamily(Keys.Bank);
        _ctr = _db.GetColumnFamily(Keys.Ctr);
        _rdel = _db.GetColumnFamily(Keys.Rdel);
        DropOldScratch();

        var tasks = new List<Task>();
        for (int w = 0; w < _l.Writers; w++)
        {
            int ww = w;
            tasks.Add(Task.Run(() => RegisterWriter(ww)));
        }
        for (int t = 0; t < Procs.BankThreads; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() => BankWorker(tt)));
        }
        for (int t = 0; t < Procs.CounterThreads; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() => CounterWorker(tt)));
        }
        tasks.Add(Task.Run(RdelWorker));
        for (int t = 0; t < Procs.ReaderThreads; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() => Reader(tt)));
        }
        tasks.Add(Task.Run(Admin));
        tasks.Add(Task.Run(Syncer));
        Task all = Task.WhenAll(tasks);

        // Supervisor commands.
        while (true)
        {
            Task<string?> line = Console.In.ReadLineAsync();
            Task done = await Task.WhenAny(line, all).ConfigureAwait(false);
            if (done == all)
            {
                Log("workload tasks ended unexpectedly: " + all.Exception);
                return ExitCloseFailed;
            }
            string? cmd = await line.ConfigureAwait(false);
            if (cmd is null || cmd == "stop") break;
            HandleCommand(cmd);
        }
        Log("stop: waiting for the workload threads");
        _stop.Cancel();
        try
        {
            await all.WaitAsync(TimeSpan.FromSeconds(120)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Log("workload did not stop cleanly: " + e);
            return ExitCloseFailed;
        }
        var close = Stopwatch.StartNew();
        try
        {
            await _occ.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Log("close failed: " + e);
            return ExitCloseFailed;
        }
        _history.Dispose();
        Log($"CLOSED in {close.Elapsed.TotalMilliseconds:F0} ms; {_errors} failed ops");
        return 0;
    }

    private void HandleCommand(string cmd)
    {
        string[] p = cmd.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        Log("command: " + cmd);
        switch (p[0])
        {
            case "crash":
            {
                CacheFault f = p[1] switch { "tornop" => CacheFault.TornOp, "tornseq" => CacheFault.TornSeq, _ => CacheFault.ClearCache };
                var rng = new Random(int.Parse(p[2], System.Globalization.CultureInfo.InvariantCulture));
                if (f != CacheFault.ClearCache)
                {
                    // Like choosing LazyFS's "occurrence": wait (up to 3 s) for a WAL with unsynced writes to tear.
                    var wait = Stopwatch.StartNew();
                    while (!_faultFs!.HasWalTornCandidate(f == CacheFault.TornSeq ? 3 : 1) && wait.Elapsed < TimeSpan.FromSeconds(3)) Thread.Sleep(1);
                }
                _faultFs!.Crash(f, rng, _history.Freeze, ownInflight: 0);
                break;
            }
            case "arm":
            {
                DbFileKind kind = p[1] switch { "manifest" => DbFileKind.Manifest, "sst" => DbFileKind.Sst, _ => DbFileKind.Wal };
                int n = int.Parse(p[2], System.Globalization.CultureInfo.InvariantCulture);
                var rng = new Random(n);
                LazyFaultFileSystem fs = _faultFs!;
                fs.ArmFsyncTrigger(kind, n, () =>
                {
                    Log($"fsync trigger: {n}-th fsync of a {kind} file completed");
                    fs.Crash(CacheFault.ClearCache, rng, _history.Freeze, ownInflight: 1);
                });
                break;
            }
            case "ioerr":
                _faultFs!.SetErrorRates(int.Parse(p[1], System.Globalization.CultureInfo.InvariantCulture),
                    int.Parse(p[2], System.Globalization.CultureInfo.InvariantCulture), int.Parse(p[3], System.Globalization.CultureInfo.InvariantCulture),
                    p.Length > 4 ? p[4] : "plain");
                break;
            case "ioerr-off":
                _faultFs!.SetErrorRates(0, 0, 0, "plain");
                Log($"I/O errors off ({_faultFs.InjectedErrors} injected); resuming");
                TryResume();
                break;
        }
    }

    /// <summary>
    /// Resume after the I/O error window: up to 20 failed attempts, 200 ms apart. While an automatic recovery runs
    /// (Busy) the attempt is not counted and Resume is retried for up to 60 s, so that a slow recovery is not reported
    /// as a failure to resume (the supervisor fails the cycle when retryable or NoSpace errors never resume). Ends with
    /// "RESUME ok" or "RESUME gave up", which the supervisor waits for before it ends the cycle.
    /// </summary>
    private void TryResume()
    {
        var busy = Stopwatch.StartNew();
        for (int failed = 0; failed < 20;)
        {
            try
            {
                _db.Resume();
                Log("RESUME ok");
                return;
            }
            catch (RocksDbException e) when (e.Code == StatusCode.Busy && busy.Elapsed < TimeSpan.FromSeconds(60))
            {
                Thread.Sleep(200);
            }
            catch (Exception e)
            {
                Log("RESUME failed: " + e.Message);
                failed++;
                Thread.Sleep(200);
            }
        }
        Log("RESUME gave up");
    }

    private async Task OnError(Exception e)
    {
        Interlocked.Increment(ref _errors);
        // The public API reports failures as RocksDbException; anything else (other than our own cancellation at stop)
        // is an engine bug, and so is a Corruption status while the files are intact.
        if (e is OperationCanceledException && _stop.IsCancellationRequested) return;
        if (e is not RocksDbException) Violation("unexpected exception from the engine: " + e);
        if (e is RocksDbException { Code: StatusCode.Corruption } c) Violation("Corruption status during the workload: " + c);
        await Task.Delay(5).ConfigureAwait(false);
    }

    private static Phase FailPhase(Exception e) =>
        e is RocksDbException { Code: StatusCode.Busy or StatusCode.TryAgain } ? Phase.Fail : Phase.Info;

    private static int PayloadLength(Random r, int max)
    {
        int x = r.Next(100);
        return x < 70 ? r.Next(0, 257) : x < 95 ? r.Next(256, 2049) : r.Next(2048, max + 1);
    }

    // ---- registers ----

    private async Task RegisterWriter(int w)
    {
        var r = new Random((int)(Keys.Mix(_seed ^ (ulong)w) & 0x7FFFFFFF));
        long version = _model.NextVersion[w];
        int K = _l.KeysPerWriter;
        CancellationToken ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            int i = r.Next(10) < 2 ? r.Next(Math.Min(32, K)) : r.Next(K);
            bool del = r.Next(100) < 12;
            bool sync = r.Next(100_000) < _syncPer100k;
            int api = r.Next(4);
            long v = version++;
            ColumnFamily cf = (i & 1) == 0 ? _reg0 : _reg1;
            byte[] key = Keys.Register(w, i);
            OpFlags flags = (sync ? OpFlags.Sync : 0) | (api >= 2 ? OpFlags.Async : 0);
            long id = _history.NewOpId();
            byte[]? value = null;
            uint crc = 0;
            int len = 0;
            if (!del)
            {
                len = PayloadLength(r, _l.MaxPayload);
                value = Keys.MakeValue((byte)'V', w, i, v, len, out crc);
            }
            int idx = w * K + i;
            _history.Append(Phase.Invoke, del ? OpType.RegDel : OpType.RegPut, w, flags, id, i, 0, v, crc, len);
            Volatile.Write(ref _invoked[idx], Pack(v, del));
            WriteOptions wo = sync ? _syncWrite : _plainWrite;
            try
            {
                switch (api)
                {
                    case 0:
                        if (del) cf.Delete(wo, key);
                        else cf.Put(wo, key, value);
                        break;
                    case 1:
                    {
                        using WriteBatch b = _db.CreateWriteBatch();
                        if (del) b.Delete(cf.Id, key);
                        else b.Put(cf.Id, key, value);
                        _db.Write(wo, b);
                        break;
                    }
                    case 2:
                        if (del) await cf.DeleteAsync(wo, key, ct).ConfigureAwait(false);
                        else await cf.PutAsync(wo, key, value, ct).ConfigureAwait(false);
                        break;
                    default:
                    {
                        using WriteBatch b = _db.CreateWriteBatch();
                        if (del) b.Delete(cf.Id, key);
                        else b.Put(cf.Id, key, value);
                        await _db.WriteAsync(wo, b, ct).ConfigureAwait(false);
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                _history.Append(FailPhase(e), del ? OpType.RegDel : OpType.RegPut, w, flags, id, i, 0, v, crc, len);
                await OnError(e).ConfigureAwait(false);
                continue;
            }
            _history.Append(Phase.Ok, del ? OpType.RegDel : OpType.RegPut, w, flags, id, i, 0, v, crc, len);
            Volatile.Write(ref _acked[idx], Pack(v, del));
        }
    }

    // ---- bank ----

    private async Task BankWorker(int t)
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0xBA4B ^ (ulong)t) & 0x7FFFFFFF));
        int proc = Procs.Bank0 + t;
        CancellationToken ct = _stop.Token;
        var tx = new OptimisticTransactionOptions();
        while (!ct.IsCancellationRequested)
        {
            int from = r.Next(_l.Accounts), to = r.Next(_l.Accounts - 1);
            if (to >= from) to++;
            long amount = r.Next(1, 50);
            bool occ = r.Next(2) == 0;
            bool sync = r.Next(100_000) < _syncPer100k;
            OpType type = occ ? OpType.BankTxn : OpType.BankBatch;
            OpFlags flags = sync ? OpFlags.Sync : 0;
            long id = _history.NewOpId();
            _history.Append(Phase.Invoke, type, proc, flags, id, from, to, amount, 0, 0);
            WriteOptions wo = sync ? _syncWrite : _plainWrite;
            try
            {
                if (occ)
                {
                    using Transaction txn = _occ.BeginTransaction(wo, tx);
                    byte[]? fv = txn.GetForUpdate(null, _bank, Keys.Account(from), exclusive: true);
                    byte[]? tv = txn.GetForUpdate(null, _bank, Keys.Account(to), exclusive: true);
                    if (fv is null || tv is null || !Keys.TryU64(fv, out ulong fb) || !Keys.TryU64(tv, out ulong tb))
                    {
                        Violation($"bank: account {from} or {to} missing or malformed inside a transaction");
                        return;
                    }
                    txn.Put(_bank, Keys.Account(from), Keys.U64(fb - (ulong)amount));
                    txn.Put(_bank, Keys.Account(to), Keys.U64(tb + (ulong)amount));
                    txn.Commit();
                }
                else
                {
                    using WriteBatch b = _db.CreateWriteBatch();
                    b.Merge(_bank.Id, Keys.Account(from), Keys.U64(unchecked((ulong)-amount)));
                    b.Merge(_bank.Id, Keys.Account(to), Keys.U64((ulong)amount));
                    if (r.Next(2) == 0) _db.Write(wo, b);
                    else await _db.WriteAsync(wo, b, ct).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _history.Append(FailPhase(e), type, proc, flags, id, from, to, amount, 0, 0);
                if (FailPhase(e) == Phase.Info) await OnError(e).ConfigureAwait(false);
                continue;
            }
            _history.Append(Phase.Ok, type, proc, flags, id, from, to, amount, 0, 0);
        }
    }

    // ---- counters ----

    private async Task CounterWorker(int t)
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0xC0C0 ^ (ulong)t) & 0x7FFFFFFF));
        int proc = Procs.Counter0 + t;
        CancellationToken ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            int c = r.Next(_l.Counters);
            long delta = r.Next(1, 10);
            bool sync = r.Next(100_000) < _syncPer100k;
            int api = r.Next(3);
            OpFlags flags = (sync ? OpFlags.Sync : 0) | (api == 1 ? OpFlags.Async : 0);
            long id = _history.NewOpId();
            _history.Append(Phase.Invoke, OpType.CtrAdd, proc, flags, id, c, 0, delta, 0, 0);
            WriteOptions wo = sync ? _syncWrite : _plainWrite;
            byte[] key = Keys.Counter(c), op = Keys.U64((ulong)delta);
            try
            {
                if (api == 0) _ctr.Merge(wo, key, op);
                else if (api == 1) await _ctr.MergeAsync(wo, key, op, ct).ConfigureAwait(false);
                else
                {
                    using WriteBatch b = _db.CreateWriteBatch();
                    b.Merge(_ctr.Id, key, op);
                    _db.Write(wo, b);
                }
            }
            catch (Exception e)
            {
                _history.Append(FailPhase(e), OpType.CtrAdd, proc, flags, id, c, 0, delta, 0, 0);
                await OnError(e).ConfigureAwait(false);
                continue;
            }
            _history.Append(Phase.Ok, OpType.CtrAdd, proc, flags, id, c, 0, delta, 0, 0);
        }
    }

    // ---- range deletes ----

    private async Task RdelWorker()
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0xDE1) & 0x7FFFFFFF));
        long version = _model.RdelNextVersion;
        CancellationToken ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            long v = version++;
            bool range = r.Next(100) < 15;
            bool sync = r.Next(100_000) < _syncPer100k;
            bool async = r.Next(2) == 0;
            WriteOptions wo = sync ? _syncWrite : _plainWrite;
            OpFlags flags = (sync ? OpFlags.Sync : 0) | (async ? OpFlags.Async : 0);
            long id = _history.NewOpId();
            if (range)
            {
                int a = r.Next(_l.RdelKeys), b = Math.Min(_l.RdelKeys, a + r.Next(1, 65));
                _history.Append(Phase.Invoke, OpType.RdelRange, Procs.Rdel, flags, id, a, b, v, 0, 0);
                try
                {
                    if (async) await _rdel.DeleteRangeAsync(wo, Keys.RdelKey(a), Keys.RdelKey(b), ct).ConfigureAwait(false);
                    else _rdel.DeleteRange(wo, Keys.RdelKey(a), Keys.RdelKey(b));
                }
                catch (Exception e)
                {
                    _history.Append(FailPhase(e), OpType.RdelRange, Procs.Rdel, flags, id, a, b, v, 0, 0);
                    await OnError(e).ConfigureAwait(false);
                    continue;
                }
                _history.Append(Phase.Ok, OpType.RdelRange, Procs.Rdel, flags, id, a, b, v, 0, 0);
            }
            else
            {
                int k = r.Next(_l.RdelKeys);
                int len = r.Next(0, 65);
                byte[] value = Keys.MakeValue((byte)'D', 0, k, v, len, out uint crc);
                _history.Append(Phase.Invoke, OpType.RdelPut, Procs.Rdel, flags, id, k, 0, v, crc, len);
                try
                {
                    if (async) await _rdel.PutAsync(wo, Keys.RdelKey(k), value, ct).ConfigureAwait(false);
                    else _rdel.Put(wo, Keys.RdelKey(k), value);
                }
                catch (Exception e)
                {
                    _history.Append(FailPhase(e), OpType.RdelPut, Procs.Rdel, flags, id, k, 0, v, crc, len);
                    await OnError(e).ConfigureAwait(false);
                    continue;
                }
                _history.Append(Phase.Ok, OpType.RdelPut, Procs.Rdel, flags, id, k, 0, v, crc, len);
            }
        }
    }

    // ---- syncs ----

    private async Task Syncer()
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0x5F) & 0x7FFFFFFF));
        CancellationToken ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(r.Next(100, 1500), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            int api = r.Next(4);
            long id = _history.NewOpId();
            _history.Append(Phase.Invoke, OpType.SyncWal, Procs.Syncer, api >= 2 ? OpFlags.Async : 0, id, api, 0, 0, 0, 0);
            try
            {
                switch (api)
                {
                    case 0: _db.SyncWal(); break;
                    case 1: _db.FlushWal(sync: true); break;
                    case 2: await _db.SyncWalAsync(ct).ConfigureAwait(false); break;
                    default: await _db.FlushWalAsync(sync: true, ct).ConfigureAwait(false); break;
                }
            }
            catch (Exception e)
            {
                _history.Append(Phase.Info, OpType.SyncWal, Procs.Syncer, 0, id, api, 0, 0, 0, 0);
                await OnError(e).ConfigureAwait(false);
                continue;
            }
            _history.Append(Phase.Ok, OpType.SyncWal, Procs.Syncer, 0, id, api, 0, 0, 0, 0);
        }
    }

    // ---- online checks ----

    private async Task Reader(int t)
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0x4EAD ^ (ulong)t) & 0x7FFFFFFF));
        int proc = Procs.Reader0 + t;
        CancellationToken ct = _stop.Token;
        var buf = new ArrayBufferWriter<byte>(8192);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                int x = r.Next(100);
                if (x < 65) await RegisterRead(r, proc, buf, ct).ConfigureAwait(false);
                else if (x < 80) await BankRead(r, proc, ct).ConfigureAwait(false);
                else if (x < 97) ScanCheck(r);
                else RefreshCheck(r);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                if (e is not RocksDbException { Code: StatusCode.IOError }) Log("reader error: " + e.GetType().Name + ": " + e.Message);
                await OnError(e).ConfigureAwait(false);
            }
        }
    }

    private async Task RegisterRead(Random r, int proc, ArrayBufferWriter<byte> buf, CancellationToken ct)
    {
        int w = r.Next(_l.Writers), i = r.Next(_l.KeysPerWriter);
        int idx = w * _l.KeysPerWriter + i;
        ColumnFamily cf = (i & 1) == 0 ? _reg0 : _reg1;
        byte[] key = Keys.Register(w, i);
        long before = Volatile.Read(ref _acked[idx]);
        long id = _history.NewOpId();
        // Every read is checked online; one in eight is also recorded (for export-history), to bound the history size.
        bool record = r.Next(8) == 0;
        if (record) _history.Append(Phase.Invoke, OpType.RegRead, proc, 0, id, w, i, 0, 0, 0);
        byte[]? value;
        switch (r.Next(4))
        {
            case 0:
                buf.Clear();
                value = cf.TryGet(key, buf) ? buf.WrittenSpan.ToArray() : null;
                break;
            case 1:
            {
                using PooledValue? pv = cf.GetPooled(key);
                value = pv?.ToArray();
                break;
            }
            case 2:
                value = await cf.GetAsync(key, ct).ConfigureAwait(false);
                break;
            default:
            {
                using var batch = new MultiGetBatch(1);
                batch.Add(key);
                await cf.MultiGetAsync(batch, ct).ConfigureAwait(false);
                if (batch.GetStatus(0) == MultiGetStatus.Error) throw batch.GetError(0)!;
                value = batch.GetStatus(0) == MultiGetStatus.Found ? batch.GetValue(0).ToArray() : null;
                break;
            }
        }
        long after = Volatile.Read(ref _invoked[idx]);
        long seen = -1;
        if (value is not null)
        {
            string? err = Keys.Decode((byte)'V', value, out Keys.ValueInfo info);
            if (err is not null) Violation($"register w{w} key {i}: bad value: {err}");
            if (info.Writer != w || info.Index != i) Violation($"register w{w} key {i}: value belongs to w{info.Writer} key {info.Index}");
            seen = info.Version;
            if (info.Version < VersionOf(before))
                Violation($"monotonic read: register w{w} key {i} read v{info.Version} after v{VersionOf(before)} was acknowledged");
            if (info.Version > VersionOf(after))
                Violation($"register w{w} key {i} read v{info.Version}, newer than any invoked write (v{VersionOf(after)})");
        }
        else if (!DeletedOf(before) && VersionOf(after) == VersionOf(before))
        {
            Violation($"monotonic read: register w{w} key {i} absent after v{VersionOf(before)} was acknowledged (no newer op in flight)");
        }
        if (record) _history.Append(Phase.Ok, OpType.RegRead, proc, 0, id, w, i, seen, 0, 0);
    }

    private async Task BankRead(Random r, int proc, CancellationToken ct)
    {
        using Snapshot snap = _db.GetSnapshot();
        var ro = new ReadOptions { Snapshot = snap };
        long id = _history.NewOpId();
        bool record = r.Next(8) == 0; // every snapshot read is checked; one in eight is recorded
        if (record) _history.Append(Phase.Invoke, OpType.BankRead, proc, 0, id, 0, 0, 0, 0, 0);
        var balances = new ulong[_l.Accounts];
        using (var batch = new MultiGetBatch(_l.Accounts))
        {
            for (int a = 0; a < _l.Accounts; a++) batch.Add(Keys.Account(a));
            if (r.Next(2) == 0) _bank.MultiGet(ro, batch);
            else await _bank.MultiGetAsync(ro, batch, ct).ConfigureAwait(false);
            for (int a = 0; a < _l.Accounts; a++)
            {
                if (batch.GetStatus(a) == MultiGetStatus.Error) throw batch.GetError(a)!;
                if (batch.GetStatus(a) != MultiGetStatus.Found || !Keys.TryU64(batch.GetValue(a), out balances[a]))
                    Violation($"bank: account {a} missing or malformed under a snapshot");
            }
        }
        ulong total = 0;
        foreach (ulong b in balances) total += b;
        if (total != _l.BankTotal) Violation($"bank: snapshot total {(long)total} != {_l.BankTotal} (MultiGet)");
        // The same snapshot through an iterator.
        using (Iterator it = _db.NewIterator(_bank, ro))
        {
            ulong itTotal = 0;
            int n = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                if (!Keys.TryParseIndexed(it.Key, (byte)'a', 4, out int a) || a >= _l.Accounts || !Keys.TryU64(it.Value, out ulong v))
                    Violation("bank: unexpected entry in the bank column family");
                else if (v != balances[a]) Violation($"bank: account {a} is {v} by iterator but {balances[a]} by MultiGet under the same snapshot");
                else itTotal += v;
                n++;
            }
            if (n != _l.Accounts || itTotal != _l.BankTotal) Violation($"bank: iterator saw {n} accounts, total {(long)itTotal}");
        }
        byte[] extra = new byte[8 * _l.Accounts];
        for (int a = 0; a < _l.Accounts; a++) System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(extra.AsSpan(a * 8), balances[a]);
        if (record) _history.Append(Phase.Ok, OpType.BankRead, proc, 0, id, 0, 0, (long)total, 0, 0, extra);
    }

    private void ScanCheck(Random r)
    {
        int which = r.Next(3);
        ColumnFamily cf = which == 0 ? _reg0 : which == 1 ? _reg1 : _rdel;
        byte tag = which == 2 ? (byte)'D' : (byte)'V';
        byte[] start = which == 2 ? Keys.RdelKey(r.Next(_l.RdelKeys)) : Keys.Register(r.Next(_l.Writers), r.Next(_l.KeysPerWriter));
        using Snapshot snap = _db.GetSnapshot();
        var ro = new ReadOptions { Snapshot = snap };
        var keys = new List<byte[]>();
        var values = new List<byte[]>();
        using (Iterator it = _db.NewIterator(cf, ro))
        {
            for (it.Seek(start); it.Valid && keys.Count < 200; it.Next())
            {
                if (keys.Count > 0 && it.Key.SequenceCompareTo(keys[^1]) <= 0)
                    Violation($"scan of {cf.Name}: keys not strictly increasing at {System.Text.Encoding.ASCII.GetString(it.Key)}");
                keys.Add(it.Key.ToArray());
                values.Add(it.Value.ToArray());
                string? err = Keys.Decode(tag, it.Value, out Keys.ValueInfo info);
                if (err is not null) Violation($"scan of {cf.Name}: key {System.Text.Encoding.ASCII.GetString(it.Key)}: {err}");
                if (which < 2 && (!Keys.TryParseRegister(it.Key, out int w, out int i) || w != info.Writer || i != info.Index))
                    Violation($"scan of {cf.Name}: key {System.Text.Encoding.ASCII.GetString(it.Key)} holds the value of w{info.Writer} key {info.Index}");
            }
            if (keys.Count == 0) return;
            // Reverse over the same range must give the same entries.
            int j = keys.Count - 1;
            for (it.SeekForPrev(keys[^1]); it.Valid && j >= 0; it.Prev(), j--)
            {
                if (!it.Key.SequenceEqual(keys[j]) || !it.Value.SequenceEqual(values[j]))
                    Violation($"scan of {cf.Name}: reverse scan differs from the forward scan at position {j}");
            }
            if (j >= 0) Violation($"scan of {cf.Name}: reverse scan ended early ({j + 1} entries missing)");
        }
        // Point reads under the same snapshot agree.
        for (int n = 0; n < 10; n++)
        {
            int p = r.Next(keys.Count);
            byte[]? v = cf.Get(ro, keys[p]);
            if (v is null || !v.AsSpan().SequenceEqual(values[p]))
                Violation($"scan of {cf.Name}: point read of {System.Text.Encoding.ASCII.GetString(keys[p])} disagrees with the scan under the same snapshot");
        }
    }

    private void RefreshCheck(Random r)
    {
        using Iterator it = _db.NewIterator(_rdel, new ReadOptions());
        for (int round = 0; round < 3; round++)
        {
            byte[]? prev = null;
            int n = 0;
            for (it.Seek(Keys.RdelKey(r.Next(_l.RdelKeys))); it.Valid && n < 100; it.Next(), n++)
            {
                if (prev is not null && it.Key.SequenceCompareTo(prev) <= 0) Violation("refreshed iterator: keys not increasing");
                if (Keys.Decode((byte)'D', it.Value, out _) is string err) Violation("refreshed iterator: " + err);
                prev = it.Key.ToArray();
            }
            it.Refresh();
        }
    }

    // ---- background / admin ----

    private async Task Admin()
    {
        var r = new Random((int)(Keys.Mix(_seed ^ 0xAD) & 0x7FFFFFFF));
        CancellationToken ct = _stop.Token;
        ColumnFamily[] cfs = [_reg0, _reg1, _bank, _ctr, _rdel];
        var lastCheckpoint = Stopwatch.StartNew();
        var lastScratch = Stopwatch.StartNew();
        int ckpt = 0;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(r.Next(50, 600), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            int x = r.Next(100);
            long id = _history.NewOpId();
            _history.Append(Phase.Invoke, OpType.Admin, Procs.Admin, 0, id, x, 0, 0, 0, 0);
            try
            {
                ColumnFamily cf = cfs[r.Next(cfs.Length)];
                if (x < 35)
                {
                    if (r.Next(2) == 0) cf.Flush();
                    else await cf.FlushAsync(ct).ConfigureAwait(false);
                }
                else if (x < 55)
                {
                    byte[]? a = null, b = null;
                    if (r.Next(3) != 0 && cf != _bank && cf != _ctr)
                    {
                        a = cf == _rdel ? Keys.RdelKey(r.Next(_l.RdelKeys)) : Keys.Register(r.Next(_l.Writers), 0);
                        b = cf == _rdel ? Keys.RdelKey(r.Next(_l.RdelKeys)) : Keys.Register(r.Next(_l.Writers), _l.KeysPerWriter - 1);
                        if (a.AsSpan().SequenceCompareTo(b) > 0) (a, b) = (b, a);
                    }
                    if (r.Next(2) == 0) cf.CompactRange(a, b);
                    else await cf.CompactRangeAsync(a, b, ct).ConfigureAwait(false);
                }
                else if (x < 70) CheckLiveFiles();
                else if (x < 85) CheckProperties(cf);
                else if (x < 93 && lastCheckpoint.Elapsed > TimeSpan.FromSeconds(20))
                {
                    lastCheckpoint.Restart();
                    await CheckpointCheck(ckpt++, ct).ConfigureAwait(false);
                }
                else if (lastScratch.Elapsed > TimeSpan.FromSeconds(15))
                {
                    lastScratch.Restart();
                    await ScratchColumnFamily(r, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                _history.Append(Phase.Info, OpType.Admin, Procs.Admin, 0, id, x, 0, 0, 0, 0);
                Log("admin op " + x + " failed: " + e.GetType().Name + ": " + e.Message);
                await OnError(e).ConfigureAwait(false);
                continue;
            }
            _history.Append(Phase.Ok, OpType.Admin, Procs.Admin, 0, id, x, 0, 0, 0, 0);
        }
    }

    private void CheckLiveFiles()
    {
        foreach (LiveFileMetadata f in _db.GetLiveFilesMetadata())
        {
            if (f.Level < 0 || f.Level >= 7) Violation($"live file {f.Name}: level {f.Level}");
            if (f.Size == 0) Violation($"live file {f.Name}: size 0");
            if (f.SmallestKey.AsSpan().SequenceCompareTo(f.LargestKey) > 0) Violation($"live file {f.Name}: smallest key > largest key");
            if (f.SmallestSeqno > f.LargestSeqno) Violation($"live file {f.Name}: smallest seqno > largest seqno");
            string path = Path.Combine(f.Directory, f.Name.TrimStart('/'));
            if (_fs.FileExists(path)) continue;
            // A compaction may have finished and deleted it since the listing: only a file that is still live counts.
            if (_db.GetLiveFilesMetadata().Exists(g => g.FileNumber == f.FileNumber) && !_fs.FileExists(path))
                Violation($"live file {path} does not exist");
        }
    }

    private void CheckProperties(ColumnFamily cf)
    {
        foreach (string p in new[] { "rocksdb.estimate-num-keys", "rocksdb.num-files-at-level0", "rocksdb.total-sst-files-size", "rocksdb.num-running-compactions" })
        {
            if (!cf.TryGetIntProperty(p, out _)) Violation($"property {p} of {cf.Name} is not available");
        }
        if (cf.GetProperty("rocksdb.levelstats") is null) Violation($"property rocksdb.levelstats of {cf.Name} is not available");
    }

    private async Task CheckpointCheck(int n, CancellationToken ct)
    {
        string dir = Path.Combine(_paths.Tmp, $"ckpt-{Environment.ProcessId}-{n}");
        if (Directory.Exists(dir)) Directory.Delete(dir, true);
        await _db.CreateCheckpointAsync(dir, ct).ConfigureAwait(false);
        BlockCache cache = BlockCache.CreateLru(1 << 20);
        List<ColumnFamilyDescriptor> cfs = DbSetup.Descriptors(dir, LocalFileSystem.Instance, _cfg.Db, _plan.Options, cache);
        using (RocksDatabase c = RocksDatabase.OpenReadOnly(dir, DbSetup.DbOptions(_plan.Options, LocalFileSystem.Instance), cfs, false))
        {
            ColumnFamily bank = c.GetColumnFamily(Keys.Bank);
            ulong total = 0;
            int count = 0;
            using (Iterator it = c.NewIterator(bank, null))
            {
                for (it.SeekToFirst(); it.Valid; it.Next())
                {
                    if (Keys.TryU64(it.Value, out ulong v)) total += v;
                    count++;
                }
            }
            if (count != _l.Accounts || total != _l.BankTotal) Violation($"checkpoint {dir}: bank has {count} accounts, total {(long)total}");
            using Iterator ri = c.NewIterator(c.GetColumnFamily(Keys.Reg0), null);
            for (ri.SeekToFirst(); ri.Valid; ri.Next())
            {
                if (Keys.Decode((byte)'V', ri.Value, out _) is string err) Violation($"checkpoint {dir}: {err}");
            }
        }
        Directory.Delete(dir, true);
    }

    private async Task ScratchColumnFamily(Random r, CancellationToken ct)
    {
        string name = $"{Keys.ScratchPrefix}{_plan.Cycle}-{Environment.ProcessId}-{_scratchCount++}";
        BlockCache cache = BlockCache.CreateLru(1 << 20);
        ColumnFamily cf = await _db.CreateColumnFamilyAsync(name, DbSetup.CfOptions(name, _cfg.Db, _plan.Options, cache), ct).ConfigureAwait(false);
        for (int i = 0; i < 20; i++) cf.Put(Keys.RdelKey(i), Keys.U64((ulong)i));
        if (r.Next(2) == 0) cf.Flush();
        if (r.Next(2) == 0) await _db.DropColumnFamilyAsync(cf, ct).ConfigureAwait(false);
        DropOldScratch();
    }

    /// <summary>Keeps at most two scratch column families.</summary>
    private void DropOldScratch()
    {
        var scratch = new List<ColumnFamily>();
        foreach (ColumnFamily cf in _db.ColumnFamilies)
            if (cf.Name.StartsWith(Keys.ScratchPrefix, StringComparison.Ordinal) && !cf.IsDropped) scratch.Add(cf);
        for (int i = 0; i < scratch.Count - 2; i++) _db.DropColumnFamily(scratch[i]);
    }
}
