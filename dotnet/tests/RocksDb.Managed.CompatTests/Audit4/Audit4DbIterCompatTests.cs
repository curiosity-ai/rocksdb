using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>
/// Audit #4, DbIter slice: the same random writes go to a native DB and to a managed DB (memtable entries with many
/// versions per key, flushes, a manual compaction, snapshots); every iterator configuration must then return the same
/// sequence of positions on both, for a scripted random walk (db/db_iter.cc: FindNextUserEntryInternal with its reseek,
/// MergeValuesNewToOld, ReverseToForward / ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek),
/// FindUserKeyBeforeSavedKey, the Seek / SeekForPrev clamps, prefix_same_as_start and max_skippable_internal_keys).
/// </summary>
[TestClass]
public sealed unsafe class Audit4DbIterCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static RocksDbSharp.MergeOperator NativeStringAppend() => RocksDbSharp.MergeOperators.Create(
        "StringAppendOperator",
        static (ReadOnlySpan<byte> key, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(default, false, operands);
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(existing, hasExisting, operands);
        });

    private static byte[] Join(ReadOnlySpan<byte> existing, bool hasExisting, RocksDbSharp.MergeOperators.OperandsEnumerator operands)
    {
        var result = new List<byte>();
        bool any = hasExisting;
        if (hasExisting) result.AddRange(existing.ToArray());
        for (int i = 0; i < operands.Count; i++)
        {
            if (any) result.Add((byte)',');
            result.AddRange(operands.Get(i).ToArray());
            any = true;
        }
        return result.ToArray();
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>Keys "pPPkKK": 4-byte prefix "pPPk"... the fixed prefix extractor of the prefix modes takes 3 bytes ("pPP").</summary>
    private static string Key(int i) => "p" + (i / 8).ToString("D2", CultureInfo.InvariantCulture) + "k" + (i % 8).ToString(CultureInfo.InvariantCulture);

    public enum Mode
    {
        /// <summary>No prefix extractor; flushes, a compaction, range deletions.</summary>
        TotalOrder,
        /// <summary>Everything stays in the memtable, no range deletions: max_skippable_internal_keys must count alike.</summary>
        MemtableOnlySkippable,
        /// <summary>Fixed(3) prefix extractor: prefix mode, prefix_same_as_start, total_order_seek.</summary>
        Prefix,
    }

    private readonly record struct Step(int Op, byte[] Target);

    private sealed record ReadConfig(int SnapshotIndex, string? Lower, string? Upper, ulong MaxSkippable, bool PrefixSameAsStart, bool TotalOrderSeek, bool PinData)
    {
        public override string ToString() => "snapshot#" + SnapshotIndex + " lower=" + Lower + " upper=" + Upper + " maxSkippable=" + MaxSkippable +
            " prefixSameAsStart=" + PrefixSameAsStart + " totalOrderSeek=" + TotalOrderSeek + " pinData=" + PinData;
    }

    private interface IIter
    {
        bool Valid { get; }
        string Current { get; }
        /// <summary>Runs the move; returns an error tag ("incomplete", ...) or null.</summary>
        string? Move(int op, byte[] target);
    }

    private sealed class NativeIter(RocksDbSharp.Iterator it) : IIter
    {
        public bool Valid => it.Valid();
        public string Current => Encoding.UTF8.GetString(it.GetKeySpan()) + "=" + Encoding.UTF8.GetString(it.GetValueSpan());

        public string? Move(int op, byte[] target)
        {
            switch (op)
            {
                case 0: it.Seek(target); break;
                case 1: it.SeekForPrev(target); break;
                case 2: it.Next(); break;
                case 3: it.Prev(); break;
                case 4: it.SeekToFirst(); break;
                default: it.SeekToLast(); break;
            }
            RocksDbSharp.Native.Instance.rocksdb_iter_get_error(it.Handle, out IntPtr err);
            if (err == IntPtr.Zero) return null;
            string msg = Marshal.PtrToStringUTF8(err)!;
            RocksDbSharp.Native.Instance.rocksdb_free(err);
            return msg.Contains("incomplete", StringComparison.OrdinalIgnoreCase) ? "Incomplete" : msg;
        }
    }

    private sealed class ManagedIter(Iterator it, bool async) : IIter
    {
        public bool Valid => it.Valid;
        public string Current => Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value);

        public string? Move(int op, byte[] target)
        {
            try
            {
                if (async)
                {
                    ValueTask t = op switch
                    {
                        0 => it.SeekAsync(target),
                        1 => it.SeekForPrevAsync(target),
                        2 => it.NextAsync(),
                        3 => it.PrevAsync(),
                        4 => it.SeekToFirstAsync(),
                        _ => it.SeekToLastAsync(),
                    };
                    t.AsTask().GetAwaiter().GetResult();
                    return null;
                }
                switch (op)
                {
                    case 0: it.Seek(target); break;
                    case 1: it.SeekForPrev(target); break;
                    case 2: it.Next(); break;
                    case 3: it.Prev(); break;
                    case 4: it.SeekToFirst(); break;
                    default: it.SeekToLast(); break;
                }
            }
            catch (RocksDbException e)
            {
                return e.Code.ToString();
            }
            return null;
        }
    }

    private static readonly string[] s_opNames = ["Seek", "SeekForPrev", "Next", "Prev", "SeekToFirst", "SeekToLast"];

    /// <summary>A full forward scan, a full reverse scan, then the scripted steps; every position is recorded.</summary>
    private static List<string> Run(IIter it, List<Step> steps)
    {
        var o = new List<string>();
        void Record(string what, string? err) => o.Add(what + " -> " + (err is not null ? "<" + err + ">" : it.Valid ? it.Current : "<invalid>"));
        string? e = it.Move(4, []);
        Record("SeekToFirst", e);
        while (e is null && it.Valid)
        {
            e = it.Move(2, []);
            Record("Next", e);
        }
        e = it.Move(5, []);
        Record("SeekToLast", e);
        while (e is null && it.Valid)
        {
            e = it.Move(3, []);
            Record("Prev", e);
        }
        bool failed = true; // Next / Prev need a valid iterator
        foreach (Step s in steps)
        {
            if (s.Op is 2 or 3 && (failed || !it.Valid)) continue;
            e = it.Move(s.Op, s.Target);
            failed = e is not null;
            Record(s_opNames[s.Op] + "(" + Encoding.UTF8.GetString(s.Target) + ")", e);
        }
        return o;
    }

    private static unsafe void NativeSingleDelete(RocksDbSharp.WriteBatch batch, byte[] key)
    {
        fixed (byte* p = key) RocksDbSharp.Native.Instance.rocksdb_writebatch_singledelete(batch.Handle, p, (UIntPtr)key.Length);
    }

    [TestMethod]
    [DataRow(Mode.TotalOrder, 0)]
    [DataRow(Mode.TotalOrder, 1)]
    [DataRow(Mode.TotalOrder, 3)]
    [DataRow(Mode.TotalOrder, 8)]
    [DataRow(Mode.MemtableOnlySkippable, 0)]
    [DataRow(Mode.MemtableOnlySkippable, 2)]
    [DataRow(Mode.MemtableOnlySkippable, 8)]
    [DataRow(Mode.Prefix, 0)]
    [DataRow(Mode.Prefix, 2)]
    [DataRow(Mode.Prefix, 8)]
    public void SameWritesGiveTheSameIteratorWalks(Mode mode, int maxSequentialSkip)
    {
        int rounds = int.TryParse(Environment.GetEnvironmentVariable("AUDIT4_DBITER_ROUNDS"), out int rr) ? rr : 3;
        for (int round = 0; round < rounds; round++)
        {
            int seed = Environment.TickCount ^ ((int)mode << 8) ^ maxSequentialSkip ^ (round << 16);
            RunOnce(mode, maxSequentialSkip, seed);
        }
    }

    /// <summary>Fixed seeds (deterministic vectors next to the time-seeded rounds).</summary>
    [TestMethod]
    [DataRow(Mode.TotalOrder, 0, 1)]
    [DataRow(Mode.TotalOrder, 1, 2)]
    [DataRow(Mode.MemtableOnlySkippable, 2, 3)]
    [DataRow(Mode.Prefix, 0, 4)]
    [DataRow(Mode.Prefix, 2, 5)]
    public void FixedSeeds(Mode mode, int maxSequentialSkip, int seed)
    {
        int before = s_incompleteSeen;
        RunOnce(mode, maxSequentialSkip, seed);
        // The max_skippable_internal_keys configurations must actually reach TooManyInternalKeysSkipped.
        if (mode == Mode.MemtableOnlySkippable) Assert.IsTrue(s_incompleteSeen > before, "no Incomplete position was produced");
    }

    private static void RunOnce(Mode mode, int maxSequentialSkip, int seed)
    {
        string ctx = "seed=" + seed.ToString(CultureInfo.InvariantCulture) + " mode=" + mode + " maxSkip=" + maxSequentialSkip;
        var rnd = new Random(seed);
        using var nativeDir = new TempDir("a4it-n");
        using var managedDir = new TempDir("a4it-m");
        const int keySpace = 48;
        bool memOnly = mode == Mode.MemtableOnlySkippable;

        RocksDbSharp.DbOptions no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        RocksDbSharp.MergeOperator nmo = NativeStringAppend();
        s_keepAlive.Add(nmo);
        no.SetMergeOperator(nmo);
        RocksDbSharp.Native.Instance.rocksdb_options_set_max_sequential_skip_in_iterations(no.Handle, (ulong)maxSequentialSkip);
        if (mode == Mode.Prefix) no.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(3));
        s_keepAlive.Add(no);

        var cfo = new ColumnFamilyOptions
        {
            DisableAutoCompactions = true,
            MergeOperator = new StringAppendOperator(),
            MaxSequentialSkipInIterations = (ulong)maxSequentialSkip,
        };
        if (mode == Mode.Prefix) cfo.PrefixExtractor = SliceTransform.CreateFixedPrefix(3);

        using RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.Open(no, nativeDir.Path);
        using RocksDatabase mdb = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo);

        var nSnaps = new List<RocksDbSharp.Snapshot>();
        var mSnaps = new List<Snapshot>();
        try
        {
            int sdNext = 0;
            int phases = memOnly ? 1 : 4;
            for (int phase = 0; phase < phases; phase++)
            {
                int ops = memOnly ? 500 : 250;
                for (int i = 0; i < ops; i++)
                {
                    int r = rnd.Next(100);
                    // A few hot keys get most of the writes, so they have long version chains (reseek paths).
                    int ki = rnd.Next(3) == 0 ? rnd.Next(4) * 9 : rnd.Next(keySpace);
                    byte[] k = B(rnd.Next(40) == 0 ? "" : rnd.Next(30) == 0 ? "p0" : Key(ki));
                    byte[] v = B("v" + phase + "_" + i);
                    using var batch = new RocksDbSharp.WriteBatch();
                    if (r < 35)
                    {
                        batch.Put(k, v);
                        ndb.Write(batch);
                        mdb.Put(k, v);
                    }
                    else if (r < 70)
                    {
                        batch.Merge(k, (ulong)k.Length, v, (ulong)v.Length);
                        ndb.Write(batch);
                        mdb.Merge(k, v);
                    }
                    else if (r < 88)
                    {
                        batch.Delete(k);
                        ndb.Write(batch);
                        mdb.Delete(k);
                    }
                    else if (r < 93 || memOnly)
                    {
                        // SingleDelete is only well defined on a key written exactly once.
                        byte[] sk = B("p" + rnd.Next(keySpace / 8).ToString("D2", CultureInfo.InvariantCulture) + "s" + (sdNext++).ToString("D4", CultureInfo.InvariantCulture));
                        ndb.Put(sk, v);
                        mdb.Put(sk, v);
                        if (rnd.Next(2) == 0)
                        {
                            NativeSingleDelete(batch, sk);
                            ndb.Write(batch);
                            mdb.SingleDelete(sk);
                        }
                    }
                    else
                    {
                        int s = rnd.Next(keySpace);
                        byte[] start = B(Key(s)), end = B(Key(Math.Min(keySpace - 1, s + 1 + rnd.Next(10))));
                        if (Encoding.UTF8.GetString(start).CompareTo(Encoding.UTF8.GetString(end)) >= 0) continue;
                        batch.DeleteRange(start, (ulong)start.Length, end, (ulong)end.Length);
                        ndb.Write(batch);
                        mdb.DeleteRange(start, end);
                    }
                    if (rnd.Next(memOnly ? 120 : 60) == 0 && nSnaps.Count < 6)
                    {
                        nSnaps.Add(ndb.CreateSnapshot());
                        mSnaps.Add(mdb.GetSnapshot());
                    }
                }
                if (memOnly) break;
                if (phase < 3)
                {
                    ndb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                    mdb.Flush();
                }
                if (phase == 1)
                {
                    ndb.CompactRange((byte[]?)null, null);
                    mdb.CompactRange();
                }
                // The last phase stays in the memtable.
            }

            // Snapshots must be the same points of the write history.
            for (int i = 0; i < nSnaps.Count; i++)
            {
                Assert.AreEqual(RocksDbSharp.Native.Instance.rocksdb_snapshot_get_sequence_number(nSnaps[i].Handle), mSnaps[i].SequenceNumber, ctx + " snapshot " + i);
            }

            var targets = new List<string> { "", "p", "p0", "p00", "p00k", "p03", "p03k9", "p05s", "p06", "q", "p02k3!", "p9" };
            for (int c = 0; c < 40; c++)
            {
                int si = rnd.Next(nSnaps.Count + 1); // == Count: latest
                string? lo = rnd.Next(3) == 0 ? (rnd.Next(4) == 0 ? targets[rnd.Next(targets.Count)] : Key(rnd.Next(keySpace))) : null;
                string? hi = rnd.Next(3) == 0 ? (rnd.Next(4) == 0 ? targets[rnd.Next(targets.Count)] : Key(rnd.Next(keySpace))) : null;
                ulong maxSkippable = memOnly && rnd.Next(2) == 0 ? (ulong)(1 + rnd.Next(12)) : 0;
                bool prefixSame = mode == Mode.Prefix && rnd.Next(2) == 0;
                bool totalOrder = mode == Mode.Prefix && rnd.Next(3) == 0;
                var rc = new ReadConfig(si, lo, hi, maxSkippable, prefixSame, totalOrder, rnd.Next(4) == 0);

                var steps = new List<Step>();
                for (int i = 0; i < 80; i++)
                {
                    int op = rnd.Next(20) == 0 ? 4 + rnd.Next(2) : rnd.Next(4);
                    string t = rnd.Next(4) == 0 ? targets[rnd.Next(targets.Count)] : Key(rnd.Next(keySpace)) + (rnd.Next(5) == 0 ? "!" : "");
                    steps.Add(new Step(op, B(t)));
                }

                byte[]? lb = lo is null ? null : B(lo), ub = hi is null ? null : B(hi);
                var nro = new RocksDbSharp.ReadOptions();
                if (si < nSnaps.Count) nro.SetSnapshot(nSnaps[si]);
                if (lb is not null) nro.SetIterateLowerBound(lb);
                if (ub is not null) nro.SetIterateUpperBound(ub);
                if (maxSkippable != 0) RocksDbSharp.Native.Instance.rocksdb_readoptions_set_max_skippable_internal_keys(nro.Handle, maxSkippable);
                if (prefixSame) nro.SetPrefixSameAsStart(true);
                if (totalOrder) nro.SetTotalOrderSeek(true);
                if (rc.PinData) nro.SetPinData(true);
                List<string> expected;
                using (RocksDbSharp.Iterator nit = ndb.NewIterator(null, nro))
                {
                    expected = Run(new NativeIter(nit), steps);
                }
                GC.KeepAlive(lb);
                GC.KeepAlive(ub);
                GC.KeepAlive(nro);

                string where = ctx + " config#" + c + " " + rc;
                var mro = new ReadOptions
                {
                    Snapshot = si < mSnaps.Count ? mSnaps[si] : null,
                    IterateLowerBound = lb is null ? (ReadOnlyMemory<byte>?)null : lb,
                    IterateUpperBound = ub is null ? (ReadOnlyMemory<byte>?)null : ub,
                    MaxSkippableInternalKeys = maxSkippable,
                    PrefixSameAsStart = prefixSame,
                    TotalOrderSeek = totalOrder,
                    PinData = rc.PinData,
                };
                List<string> got;
                using (Iterator mit = mdb.NewIterator(mro))
                {
                    got = Run(new ManagedIter(mit, false), steps);
                }
                AssertSame(expected, got, where + " [sync]");
                foreach (string line in expected)
                {
                    if (line.EndsWith("<Incomplete>", StringComparison.Ordinal)) s_incompleteSeen++;
                }

                // The async API (DbIter's async drivers share the step functions with the sync ones).
                using (Iterator mit = mdb.NewIterator(mro))
                {
                    got = Run(new ManagedIter(mit, true), steps);
                }
                AssertSame(expected, got, where + " [async]");

                // An iterator created without snapshot and bounds, then re-targeted: Refresh(snapshot) on the unchanged
                // super version (ArenaWrappedDBIter::Refresh: set_sequence) and SetBounds.
                var plain = new ReadOptions
                {
                    MaxSkippableInternalKeys = maxSkippable,
                    PrefixSameAsStart = prefixSame,
                    TotalOrderSeek = totalOrder,
                    PinData = rc.PinData,
                };
                using (Iterator mit = mdb.NewIterator(plain))
                {
                    new ManagedIter(mit, false).Move(rnd.Next(2) == 0 ? 4 : 5, []); // positioned (or failed) before the refresh
                    mit.Refresh(si < mSnaps.Count ? mSnaps[si] : null);
                    mit.SetBounds(lb is null ? (ReadOnlyMemory<byte>?)null : lb, ub is null ? (ReadOnlyMemory<byte>?)null : ub);
                    got = Run(new ManagedIter(mit, false), steps);
                }
                AssertSame(expected, got, where + " [refresh+setbounds]");
            }
        }
        finally
        {
            foreach (RocksDbSharp.Snapshot s in nSnaps) s.Dispose();
            foreach (Snapshot s in mSnaps) s.Dispose();
        }
    }

    private static int s_incompleteSeen;

    private static void AssertSame(List<string> expected, List<string> got, string where)
    {
        for (int i = 0; i < Math.Min(expected.Count, got.Count); i++)
        {
            if (expected[i] != got[i])
            {
                Assert.Fail(where + " at output " + i + ": native [" + expected[i] + "] managed [" + got[i] + "]; previous: " +
                    string.Join(" ; ", got.Skip(Math.Max(0, i - 4)).Take(4)));
            }
        }
        Assert.AreEqual(expected.Count, got.Count, where);
    }

    // ==================================================================== user-defined timestamps

    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly RocksDbSharp.DestructorDelegate s_destructor = static _ => { };
    private static readonly RocksDbSharp.CompareDelegate s_compare = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.Compare(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareTsDelegate s_compareTs = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.CompareTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareWithoutTsDelegate s_compareWithoutTs = static (_, a, alen, aHasTs, b, blen, bHasTs) =>
        Comparator.BytewiseWithU64Ts.CompareWithoutTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), aHasTs != 0,
            new ReadOnlySpan<byte>((void*)b, (int)blen), bHasTs != 0);
    private static readonly IntPtr s_name = Marshal.StringToHGlobalAnsi("leveldb.BytewiseComparator.u64ts");
    private static readonly RocksDbSharp.NameDelegate s_nameDelegate = static _ => s_name;
    private static readonly Lazy<IntPtr> s_comparator = new(static () =>
        N.rocksdb_comparator_with_ts_create(IntPtr.Zero, s_destructor, s_compare, s_compareTs, s_compareWithoutTs, s_nameDelegate, (UIntPtr)8));
    private static readonly RocksDbSharp.WriteOptions s_nativeWrite = new();

    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static void Check(IntPtr err)
    {
        if (err == IntPtr.Zero) return;
        string msg = Marshal.PtrToStringAnsi(err) ?? "";
        N.rocksdb_free(err);
        Assert.Fail("native error: " + msg);
    }

    private static string UdtEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ReadOnlySpan<byte> ts, bool deletion) =>
        Encoding.UTF8.GetString(key) + "=" + Encoding.UTF8.GetString(value) + "@" + BinaryPrimitives.ReadUInt64LittleEndian(ts) + (deletion ? "(del)" : "");

    private static bool IsDeletion(ReadOnlySpan<byte> internalKey)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out _, out EntryType t);
        return t is EntryType.Deletion or EntryType.DeletionWithTimestamp or EntryType.SingleDeletion;
    }

    private sealed class NativeUdtIter(RocksDbSharp.Iterator it, bool allVersions) : IIter
    {
        public bool Valid => it.Valid();

        public string Current
        {
            get
            {
                if (allVersions)
                {
                    // iter_start_ts: key() is the internal key (user key | timestamp | seq/type).
                    byte[] ik = it.Key();
                    ReadOnlySpan<byte> uk = DbFormat.ExtractUserKey(ik);
                    return UdtEntry(uk[..^8], it.Value(), uk[^8..], IsDeletion(ik));
                }
                IntPtr p = N.rocksdb_iter_timestamp(it.Handle, out UIntPtr len);
                return UdtEntry(it.Key(), it.Value(), new ReadOnlySpan<byte>((void*)p, (int)len), false);
            }
        }

        public string? Move(int op, byte[] target) => new NativeIter(it).Move(op, target);
    }

    private sealed class ManagedUdtIter(Iterator it, bool allVersions) : IIter
    {
        public bool Valid => it.Valid;
        public string Current => UdtEntry(it.Key, it.Value, it.Timestamp, allVersions && IsDeletion(it.InternalKey));
        public string? Move(int op, byte[] target) => new ManagedIter(it, false).Move(op, target);
    }

    /// <summary>
    /// db/db_iter.cc with a timestamp-aware comparator: the same timestamped writes (puts, deletes, range deletions;
    /// memtable, flushed and compacted files) on both sides, then random walks at random read timestamps, with and
    /// without iter_start_ts (all versions and tombstones, key() = internal key), bounds and small
    /// max_sequential_skip_in_iterations (the kTsMin / kTsMax / timestamp_ub_ reseek keys of FindNextUserEntryInternal,
    /// FindValueForCurrentKeyUsingSeek, FindUserKeyBeforeSavedKey and ReverseToForward; saved_timestamp_ in reverse;
    /// SetSavedKeyToSeekTarget / SetSavedKeyToSeekForPrevTarget with timestamps).
    /// </summary>
    [TestMethod]
    [DataRow(0, false)]
    [DataRow(1, false)]
    [DataRow(3, false)]
    [DataRow(8, false)]
    [DataRow(1, true)]
    [DataRow(8, true)]
    public void TimestampedIteratorWalksMatchNative(int maxSequentialSkip, bool rangeDeletions)
    {
        int rounds = int.TryParse(Environment.GetEnvironmentVariable("AUDIT4_DBITER_ROUNDS"), out int rr) ? rr : 2;
        for (int round = 0; round < rounds; round++)
        {
            RunUdtOnce(maxSequentialSkip, rangeDeletions, Environment.TickCount ^ (maxSequentialSkip << 8) ^ (round << 16) ^ (rangeDeletions ? 1 << 30 : 0));
        }
        RunUdtOnce(maxSequentialSkip, rangeDeletions, 11 + maxSequentialSkip);
    }

    /// <summary>
    /// Seeds that failed before the fixes of this audit: reverse movement with iter_start_ts over entries covered by
    /// range tombstones. Upstream's DBIter never sees covered entries (the MergingIterator drops them), and with
    /// iter_start_ts its reverse functions depend on exactly which entries they see: FindValueForCurrentKey also walks
    /// versions older than iter_start_ts and switches to FindValueForCurrentKeyUsingSeek after max_skip of them, and
    /// FindUserKeyBeforeSavedKey's reseek skips the newer versions of the key.
    /// </summary>
    [TestMethod]
    [DataRow(1, 12)]
    [DataRow(8, 19)]
    [DataRow(1, 1075116946)]
    [DataRow(8, 1075114754)]
    [DataRow(1, 1075208454)]
    [DataRow(8, 1075210322)]
    [DataRow(1, 1075653298)]
    [DataRow(8, 1073952262)]
    public void TimestampedWalksOverRangeDeletionsFixedSeeds(int maxSequentialSkip, int seed) => RunUdtOnce(maxSequentialSkip, true, seed);

    private static void RunUdtOnce(int maxSequentialSkip, bool rangeDeletions, int seed)
    {
        string ctx = "seed=" + seed.ToString(CultureInfo.InvariantCulture) + " maxSkip=" + maxSequentialSkip + " rangeDeletions=" + rangeDeletions;
        var rnd = new Random(seed);
        using var nativeDir = new TempDir("a4udt-n");
        using var managedDir = new TempDir("a4udt-m");
        const int keySpace = 24;
        static string K(int i) => "k" + i.ToString("D2", CultureInfo.InvariantCulture);

        var ncf = new RocksDbSharp.ColumnFamilyOptions();
        N.rocksdb_options_set_comparator(ncf.Handle, s_comparator.Value);
        ncf.SetDisableAutoCompactions(1);
        N.rocksdb_options_set_max_sequential_skip_in_iterations(ncf.Handle, (ulong)maxSequentialSkip);
        RocksDbSharp.DbOptions no = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(ncf);
        s_keepAlive.Add(no);

        var cfo = new ColumnFamilyOptions
        {
            Comparator = Comparator.BytewiseWithU64Ts,
            DisableAutoCompactions = true,
            MaxSequentialSkipInIterations = (ulong)maxSequentialSkip,
        };

        using RocksDbSharp.RocksDb ndb = RocksDbSharp.RocksDb.Open(no, nativeDir.Path, new RocksDbSharp.ColumnFamilies(ncf));
        using RocksDatabase mdb = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true }, cfo);

        ulong ts = 1;
        for (int phase = 0; phase < 3; phase++)
        {
            for (int i = 0; i < 140; i++)
            {
                // Hot keys get long version chains.
                int ki = rnd.Next(3) == 0 ? rnd.Next(3) * 7 : rnd.Next(keySpace);
                byte[] k = B(K(ki)), t = Ts(ts);
                int r = rnd.Next(100);
                if (r < 68)
                {
                    byte[] v = B("v" + ts.ToString(CultureInfo.InvariantCulture));
                    fixed (byte* pt = t)
                    {
                        N.rocksdb_put_with_ts(ndb.Handle, s_nativeWrite.Handle, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, v, (UIntPtr)v.Length, out IntPtr err);
                        Check(err);
                    }
                    mdb.Put(k, t, v);
                }
                else if (r < 96 || !rangeDeletions)
                {
                    fixed (byte* pt = t)
                    {
                        N.rocksdb_delete_with_ts(ndb.Handle, s_nativeWrite.Handle, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, out IntPtr err);
                        Check(err);
                    }
                    mdb.Delete(k, t);
                }
                else
                {
                    // DeleteRange with a timestamp (no C API): the managed batch rep is written to both sides.
                    int a = rnd.Next(keySpace - 1), b = Math.Min(keySpace, a + 1 + rnd.Next(4));
                    using var mb = new WriteBatch();
                    mb.DeleteRange(0, B(K(a)), B(K(b)), t);
                    byte[] rep = mb.Data.ToArray();
                    IntPtr wb = N.rocksdb_writebatch_create_from(rep, (UIntPtr)rep.Length);
                    try
                    {
                        N.rocksdb_write(ndb.Handle, s_nativeWrite.Handle, wb, out IntPtr err);
                        Check(err);
                    }
                    finally
                    {
                        N.rocksdb_writebatch_destroy(wb);
                    }
                    mdb.Write(mb);
                }
                ts += (ulong)(1 + rnd.Next(3));
            }
            if (phase < 2)
            {
                ndb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
                mdb.Flush();
            }
            if (phase == 0)
            {
                ndb.CompactRange((byte[]?)null, null);
                mdb.CompactRange();
            }
        }

        IntPtr tsBuf = Marshal.AllocHGlobal(16);
        try
        {
            for (int c = 0; c < 40; c++)
            {
                ulong readTs = rnd.Next(4) == 0 ? ulong.MaxValue : (ulong)rnd.NextInt64(0, (long)ts + 2);
                ulong? startTs = rnd.Next(3) == 0 ? (ulong)rnd.NextInt64(0, (long)Math.Min(readTs, ts + 1) + 1) : null;
                string? lo = rnd.Next(4) == 0 ? K(rnd.Next(keySpace)) : null;
                string? hi = rnd.Next(4) == 0 ? K(rnd.Next(keySpace)) : null;
                // No upper bound together with iter_start_ts: upstream's FindValueForCurrentKeyUsingSeek then seeks to
                // (key, iter_start_ts), which can land past the key's versions; native table iterators turn invalid
                // beyond iterate_upper_bound (memtable iterators do not), which ends the whole reverse step there, so the
                // native result depends on whether the next key is in a file or in the memtable. The managed inner
                // iterators do not enforce the bound (DbIter does), a documented difference of the stack.
                if (startTs is not null) hi = null;
                var steps = new List<Step>();
                for (int i = 0; i < 70; i++)
                {
                    int op = rnd.Next(20) == 0 ? 4 + rnd.Next(2) : rnd.Next(4);
                    steps.Add(new Step(op, B(rnd.Next(6) == 0 ? "" : K(rnd.Next(keySpace)) + (rnd.Next(5) == 0 ? "!" : ""))));
                }
                string where = ctx + " config#" + c + " readTs=" + readTs + " startTs=" + startTs + " lower=" + lo + " upper=" + hi;

                byte[]? lb = lo is null ? null : B(lo), ub = hi is null ? null : B(hi);
                Ts(readTs).CopyTo(new Span<byte>((void*)tsBuf, 8));
                var nro = new RocksDbSharp.ReadOptions();
                N.rocksdb_readoptions_set_timestamp(nro.Handle, tsBuf, (UIntPtr)8);
                if (startTs is ulong st)
                {
                    Ts(st).CopyTo(new Span<byte>((void*)(tsBuf + 8), 8));
                    N.rocksdb_readoptions_set_iter_start_ts(nro.Handle, tsBuf + 8, (UIntPtr)8);
                }
                if (lb is not null) nro.SetIterateLowerBound(lb);
                if (ub is not null) nro.SetIterateUpperBound(ub);
                List<string> expected;
                using (RocksDbSharp.Iterator nit = ndb.NewIterator(null, nro))
                {
                    expected = Run(new NativeUdtIter(nit, startTs is not null), steps);
                }
                GC.KeepAlive(lb);
                GC.KeepAlive(ub);
                GC.KeepAlive(nro);

                var mro = new ReadOptions
                {
                    Timestamp = Ts(readTs),
                    IterStartTs = startTs is ulong s2 ? Ts(s2) : (ReadOnlyMemory<byte>?)null,
                    IterateLowerBound = lb is null ? (ReadOnlyMemory<byte>?)null : lb,
                    IterateUpperBound = ub is null ? (ReadOnlyMemory<byte>?)null : ub,
                };
                List<string> got;
                using (Iterator mit = mdb.NewIterator(mro))
                {
                    got = Run(new ManagedUdtIter(mit, startTs is not null), steps);
                }
                AssertSame(expected, got, where);
            }
        }
        finally
        {
            Marshal.FreeHGlobal(tsBuf);
        }
    }
}
