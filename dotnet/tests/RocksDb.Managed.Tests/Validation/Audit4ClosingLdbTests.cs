using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tools;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests of the audit #4 closing pass over the ldb tool (tools/ldb_cmd.cc, tools/ldb_tool.cc), the trie index
/// reader (utilities/trie_index), and a few small files (env_mirror, lazy wide columns, SstPartitioner, PerfContext).
/// </summary>
[TestClass]
public sealed partial class Audit4ClosingLdbTests
{
    private static (int Rc, string Out, string Err) Tool(IFileSystem fs, params string[] args)
    {
        var o = new StringWriter { NewLine = "\n" };
        var e = new StringWriter { NewLine = "\n" };
        int rc = ToolMain.Run(args, fs, o, e);
        return (rc, o.ToString(), e.ToString());
    }

    private static string Ldb(IFileSystem fs, params string[] args)
    {
        (int rc, string o, string e) = Tool(fs, ["ldb", "--db=" + DbPath, .. args]);
        Assert.AreEqual(0, rc, e);
        return o;
    }

    private static (int Rc, string Out, string Err) LdbRaw(IFileSystem fs, params string[] args) => Tool(fs, ["ldb", "--db=" + DbPath, .. args]);

    private static MemoryFileSystem NewLdbDb()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a.1"), B("v"));
        db.Put(B("a.2"), B("v"));
        db.Put(B("b.1"), B("v"));
        db.Flush();
        return fs;
    }

    [TestMethod]
    public void LdbWithASingleArgumentPrintsTheHelp()
    {
        // LDBCommandRunner::RunCommand (tools/ldb_tool.cc:158-173): "argc <= 2" and the argument is neither --version nor
        // --help -> the help on stderr, exit 1 (the command is not even looked at).
        MemoryFileSystem fs = NewLdbDb();
        (int rc, string o, string e) = Tool(fs, "ldb", "list_column_families");
        Assert.AreEqual(1, rc);
        Assert.AreEqual("", o);
        StringAssert.StartsWith(e, "ldb - RocksDB Tool");
    }

    [TestMethod]
    public void LdbRejectsOptionsTheCommandDoesNotDeclare()
    {
        // LDBCommand::ValidateCmdLineOptions (tools/ldb_cmd.cc:1345): an option / flag outside the command's
        // valid_cmd_line_options prints "Invalid command-line option|flag X" (no "Failed: "), exit 1; then a command that
        // opens the DB needs --db or --path.
        MemoryFileSystem fs = NewLdbDb();
        (int rc, string o, string e) = LdbRaw(fs, "scan", "--bogus=1");
        Assert.AreEqual((1, "", "Invalid command-line option bogus\n"), (rc, o, e));
        // GetCommand's list has no no_value (ScanCommand's does).
        (rc, o, e) = LdbRaw(fs, "get", "a.1", "--no_value");
        Assert.AreEqual((1, "", "Invalid command-line flag no_value\n"), (rc, o, e));
        // CreateColumnFamilyCommand passes {ARG_DB} only: the common options (--column_family...) are not valid there.
        (rc, o, e) = LdbRaw(fs, "create_column_family", "x", "--column_family=default");
        Assert.AreEqual((1, "", "Invalid command-line option column_family\n"), (rc, o, e));
        // Options are checked before flags, in map order.
        (rc, o, e) = LdbRaw(fs, "scan", "--zflag", "--yopt=1", "--xopt=1");
        Assert.AreEqual((1, "", "Invalid command-line option xopt\n"), (rc, o, e));
        (rc, o, e) = Tool(fs, "ldb", "scan", "--no_value");
        Assert.AreEqual((1, "", "Either --db or --path must be specified.\n"), (rc, o, e));
        // Common options stay accepted (BuildCmdLineOptions), also when the port ignores them.
        Assert.AreEqual("a.1\na.2\nb.1\n", Ldb(fs, "scan", "--no_value", "--ignore_unknown_options", "--block_size=4096"));
    }

    [TestMethod]
    public void LdbOptionWithATrailingEqualsSignParsesLikeStringSplit()
    {
        // LDBCommand::ParseSingleParam (tools/ldb_cmd.cc:234): StringSplit drops a trailing empty piece, so
        // "--count_delim=" is the FLAG count_delim (delimiter ".") and "--max_keys=1=" is max_keys -> "1".
        MemoryFileSystem fs = NewLdbDb();
        Assert.AreEqual("a => count:2\tsize:8\nb => count:1\tsize:4\n", Ldb(fs, "dump", "--count_delim="));
        Assert.AreEqual("a.1\n", Ldb(fs, "scan", "--no_value", "--max_keys=1="));
    }

    [TestMethod]
    public void LdbIntegerOptionsParseLikeStdStoi()
    {
        // ScanCommand / DBDumperCommand constructors: std::stoi (trailing text ignored), "max_keys has an invalid value" /
        // "max_keys has a value out-of-range"; LDBCommand::ParseIntOption (idump, reduce_levels) ends both with a dot.
        MemoryFileSystem fs = NewLdbDb();
        Assert.AreEqual("a.1\na.2\n", Ldb(fs, "scan", "--no_value", "--max_keys=2x"));
        Assert.AreEqual("a.1 ==> v\nKeys in range: 1\n", Ldb(fs, "dump", "--max_keys= +1"));
        (int rc, _, string e) = LdbRaw(fs, "scan", "--max_keys=x");
        Assert.AreEqual((1, "Failed: max_keys has an invalid value\n"), (rc, e));
        (rc, _, e) = LdbRaw(fs, "dump", "--max_keys=99999999999");
        Assert.AreEqual((1, "Failed: max_keys has a value out-of-range\n"), (rc, e));
        (rc, _, e) = LdbRaw(fs, "idump", "--max_keys=x");
        Assert.AreEqual((1, "Failed: max_keys has an invalid value.\n"), (rc, e));
        (rc, _, e) = LdbRaw(fs, "reduce_levels", "--new_levels=-99999999999");
        Assert.AreEqual((1, "Failed: new_levels has a value out-of-range.\n"), (rc, e));
    }

    [TestMethod]
    public void LdbReduceLevelsWithoutNewLevelsNamesTheOption()
    {
        // ReduceDBLevelsCommand's constructor (tools/ldb_cmd.cc:2700): new_levels_ <= 0 (the default -1 included) fails with
        // " Use --new_levels to specify a new level number\n"; DoCommand's "Invalid number of levels.\n" is for 1 only.
        MemoryFileSystem fs = NewLdbDb();
        (int rc, _, string e) = LdbRaw(fs, "reduce_levels");
        Assert.AreEqual((1, "Failed:  Use --new_levels to specify a new level number\n\n"), (rc, e));
        (rc, _, e) = LdbRaw(fs, "reduce_levels", "--new_levels=0");
        Assert.AreEqual((1, "Failed:  Use --new_levels to specify a new level number\n\n"), (rc, e));
        (rc, _, e) = LdbRaw(fs, "reduce_levels", "--new_levels=1");
        Assert.AreEqual((1, "Failed: Invalid number of levels.\n\n"), (rc, e));
    }

    [TestMethod]
    public void LdbIdumpCountDelimGroupsInternalKeys()
    {
        // InternalDumpCommand::DoCommand (tools/ldb_cmd.cc:2280-2355): with count_delim no rows and no "Internal keys in
        // range" line are printed; keys are grouped by the prefix before the delimiter, the size is the length of the
        // encoded internal key up to its first 0x00 / 0x01 byte (the type byte of a value) plus the value's.
        MemoryFileSystem fs = NewLdbDb();
        Assert.AreEqual("a => count:2\tsize:8\nb => count:1\tsize:4\n", Ldb(fs, "idump", "--count_delim"));
        Assert.AreEqual("a.1 => count:1\tsize:4\na.2 => count:1\tsize:4\nb.1 => count:1\tsize:4\n", Ldb(fs, "idump", "--count_delim=,"));
    }

    [TestMethod]
    public void LdbManifestDumpReportsAnUnreadableFileOnStderrAndSucceeds()
    {
        // DumpManifestFile (tools/ldb_cmd.cc:1670): a failed VersionSet::DumpManifest prints "Error in processing file
        // <file> <status>" on stderr; exec_state_ is untouched, so the exit code is 0 and stdout stays empty.
        MemoryFileSystem fs = NewLdbDb();
        (int rc, string o, string e) = Tool(fs, "ldb", "manifest_dump", "--path=/nowhere/MANIFEST-000001");
        Assert.AreEqual(0, rc, e);
        Assert.AreEqual("", o);
        StringAssert.StartsWith(e, "Error in processing file /nowhere/MANIFEST-000001 ");
    }

    [TestMethod]
    public void LdbDumpOfATableFileDecodesHexRangeKeys()
    {
        // DBDumperCommand's constructor hex-decodes from_ / to_ when is_key_hex_ before DoCommand hands them to DumpSstFile
        // (tools/ldb_cmd.cc:2425, 2500).
        MemoryFileSystem fs = NewLdbDb();
        string sst = "";
        foreach (string f in fs.GetChildren(DbPath))
        {
            if (f.EndsWith(".sst", StringComparison.Ordinal)) sst = DbPath + "/" + f;
        }
        (int rc, string o, string e) = Tool(fs, "ldb", "dump", "--path=" + sst, "--hex", "--from=0x612E32", "--to=0x622E31");
        Assert.AreEqual(0, rc, e);
        StringAssert.Contains(o, "'612E32' seq:2, type:1 => 76\n");
        Assert.IsFalse(o.Contains("'612E31' seq", StringComparison.Ordinal), o);
        Assert.IsFalse(o.Contains("'622E31' seq", StringComparison.Ordinal), o);
    }

    [TestMethod]
    public void LdbDumpWalFailsOnATimedPutWithoutPrintingTheRow()
    {
        // InMemoryHandler (tools/ldb_cmd.cc:2989) does not override TimedPutCF, so WriteBatch::Handler's default returns
        // InvalidArgument("TimedPutCF not implemented"); DumpWalFile then sets "Print write batch error: ..." and breaks
        // before "std::cout << row.str()" (ldb_cmd.cc:3336-3346).
        MemoryFileSystem fs = NewFs();
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("a"), B("1"));
            using WriteBatch batch = db.CreateWriteBatch();
            batch.TimedPut(db.DefaultColumnFamily, B("t"), B("2"), 12345);
            db.Write(batch);
        }
        (int rc, string o, string e) = Tool(fs, "ldb", "dump_wal", "--walfile=" + DbPath);
        Assert.AreEqual(1, rc);
        Assert.AreEqual("Failed: Print write batch error: Invalid argument: TimedPutCF not implemented\n", e);
        StringAssert.Contains(o, "1,1,");
        Assert.IsFalse(o.Contains("error:", StringComparison.Ordinal), o);
        Assert.IsFalse(o.Contains("2,1,", StringComparison.Ordinal), o);
    }

    [TestMethod]
    public void LdbDumpWalOfAnEmptyDirectoryTreatsItAsAFileName()
    {
        // DumpWalFiles (tools/ldb_cmd.cc:3133): "!st.ok() || filenames.empty()" takes the single-file branch, where a name
        // not ending in ".log" is "<name>: Invalid log file name".
        MemoryFileSystem fs = NewFs();
        fs.CreateDirectoryIfMissing("/emptywal");
        (int rc, _, string e) = Tool(fs, "ldb", "dump_wal", "--walfile=/emptywal");
        Assert.AreEqual((1, "Failed: /emptywal: Invalid log file name\n"), (rc, e));
    }

    // ---- trie index reader -------------------------------------------------------------------------------------

    private static byte[] BuildFuzzTrie(bool seqno, int shape)
    {
        var keys = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        for (int i = 0; i < 200; i++)
        {
            string k = shape switch
            {
                // Long shared runs -> path-compression chains; a few prefix keys.
                0 => "k" + (i * 37 % 1000).ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + "_0000000000_" + (i % 5),
                // Short random-ish keys: dense upper levels, prefix keys ("a" / "ab" / "abc").
                1 => new string((char)('a' + i % 7), 1 + i % 4) + (char)('a' + i * 5 % 26),
                _ => ((i * 2654435761u) % 100000).ToString(System.Globalization.CultureInfo.InvariantCulture),
            };
            keys.Add(B(k));
        }
        using var b = new RocksDb.Managed.Table.TrieIndex.LoudsTrieBuilder();
        b.SetHasSeqnoEncoding(seqno);
        int n = 0;
        foreach (byte[] k in keys)
        {
            uint count = seqno && n % 10 == 0 ? 3u : 1u;
            b.AddKeyWithSeqno(k, new RocksDb.Managed.Table.TrieIndex.TrieBlockHandle((ulong)n * 100, 50), (ulong)n, count);
            for (uint j = 1; j < count; j++) b.AddOverflowBlock(new RocksDb.Managed.Table.TrieIndex.TrieBlockHandle(999, 1), 1);
            n++;
        }
        b.Finish();
        return b.SerializedData.ToArray();
    }

    /// <summary>Runs every iterator entry point over a (possibly corrupt) trie; returns the number of positions visited.</summary>
    private static int ExerciseTrie(ReadOnlySpan<byte> data, Random rnd)
    {
        var trie = RocksDb.Managed.Table.TrieIndex.LoudsTrie.Read(data);
        var it = new RocksDb.Managed.Table.TrieIndex.LoudsTrieIterator(trie);
        int visited = 0;
        const int Limit = 2000;
        void Touch()
        {
            // What TrieIndexIterator reads at a position: the handle and, with the seqno side-table, the leaf's run.
            _ = it.Key.Length;
            _ = it.Value;
            if (trie.HasSeqnoEncoding)
            {
                _ = trie.GetLeafSeqno(it.LeafIndex);
                uint count = trie.GetLeafBlockCount(it.LeafIndex);
                uint first = trie.GetOverflowBase(it.LeafIndex);
                for (uint j = 0; j + 1 < count && j < 8; j++)
                {
                    _ = trie.GetOverflowHandle(first + j);
                    _ = trie.GetOverflowSeqno(first + j);
                }
            }
            visited++;
        }
        for (bool ok = it.SeekToFirst(); ok && visited < Limit; ok = it.Next()) Touch();
        int before = visited;
        for (bool ok = it.SeekToLast(); ok && visited < before + Limit; ok = it.Prev()) Touch();
        byte[] target = new byte[24];
        for (int i = 0; i < 40; i++)
        {
            int len = rnd.Next(0, target.Length);
            for (int j = 0; j < len; j++) target[j] = (byte)(rnd.Next(4) == 0 ? rnd.Next(256) : "k0123456789_ab"[rnd.Next(14)]);
            if (!it.Seek(target.AsSpan(0, len))) continue;
            Touch();
            if (rnd.Next(2) == 0 ? it.Next() : it.Prev()) Touch();
        }
        return visited;
    }

    [TestMethod]
    [Timeout(60000, CooperativeCancellation = true)]
    public void TrieReaderFuzzCorruptBlocksGiveCorruptionOrAHarmlessResult()
    {
        // LoudsTrie::InitFromData / LoudsTrieIterator (utilities/trie_index/louds_trie.cc): upstream validates the block's
        // sections and then trusts them (out-of-range reads are undefined behaviour there). The managed reader is bounds
        // checked, so a block whose bits were flipped or that was cut short must either parse to some (wrong but
        // harmless) answer or fail with Corruption -- never IndexOutOfRange / ArgumentOutOfRange / overflow, never a hang.
        var rnd = new Random(20261111);
        int corrupt = 0, harmless = 0;
        foreach (bool seqno in new[] { false, true })
        {
            for (int shape = 0; shape < 3; shape++)
            {
                byte[] good = BuildFuzzTrie(seqno, shape);
                Assert.IsTrue(ExerciseTrie(good, new Random(1)) > 100, "the intact trie iterates");
                for (int round = 0; round < 1500; round++)
                {
                    byte[] bad = (byte[])good.Clone();
                    int len = bad.Length;
                    int kind = rnd.Next(10);
                    if (kind == 0)
                    {
                        len = rnd.Next(0, bad.Length);
                    }
                    else
                    {
                        int flips = 1 + rnd.Next(3);
                        for (int f = 0; f < flips; f++)
                        {
                            // Half of the flips land in the header / first bitvectors, where they matter most.
                            int at = rnd.Next(2) == 0 ? rnd.Next(Math.Min(bad.Length, 512)) : rnd.Next(bad.Length);
                            bad[at] = kind < 6 ? (byte)(bad[at] ^ (1 << rnd.Next(8))) : (byte)rnd.Next(256);
                        }
                    }
                    string ctx = "seqno=" + seqno + " shape=" + shape + " round=" + round;
                    try
                    {
                        ExerciseTrie(bad.AsSpan(0, len), rnd);
                        harmless++;
                    }
                    catch (RocksDbException e)
                    {
                        Assert.AreEqual(StatusCode.Corruption, e.Code, ctx);
                        corrupt++;
                    }
                    catch (Exception e) when (e is not AssertFailedException)
                    {
                        Assert.Fail(ctx + ": " + e.GetType().Name + ": " + e.Message + "\n" + e.StackTrace);
                    }
                }
            }
        }
        Assert.IsTrue(corrupt > 100 && harmless > 100, "corrupt=" + corrupt + " harmless=" + harmless);
    }

    // ---- MirrorFileSystem --------------------------------------------------------------------------------------

    /// <summary>Forwards IFileSystem to a MemoryFileSystem; its random access files return at most three bytes per read.</summary>
    public class ShortReadFileSystemProxy : System.Reflection.DispatchProxy
    {
        public IFileSystem Inner { get; set; } = null!;

        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
        {
            object? r = targetMethod!.Invoke(Inner, args);
            return r is IRandomAccessFile f ? new ShortReadFile(f) : r;
        }
    }

    private sealed class ShortReadFile(IRandomAccessFile inner) : IRandomAccessFile
    {
        public long Length => inner.Length;
        public int Read(long offset, Span<byte> destination) => inner.Read(offset, destination[..Math.Min(3, destination.Length)]);
        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(offset, destination[..Math.Min(3, destination.Length)], cancellationToken);
        public void Dispose() => inner.Dispose();
    }

    [TestMethod]
    public async Task MirrorRandomReadAcceptsShortReadsOfEitherFileSystem()
    {
        // RandomAccessFileMirror::Read (utilities/env_mirror.cc:68-87): after A returned result->size() bytes, B is read
        // "while (left)" from offset + off, so a file system that returns short reads is not a divergence; and only the
        // bytes A returned are requested from B.
        var memA = new MemoryFileSystem();
        var memB = new MemoryFileSystem();
        byte[] content = B("0123456789abcdef");
        foreach (MemoryFileSystem m in new[] { memA, memB })
        {
            m.CreateDirectoryIfMissing("/d");
            using IWritableFile w = m.CreateWritable("/d/f");
            w.Append(content);
            w.Sync();
        }
        IFileSystem Short(MemoryFileSystem m)
        {
            IFileSystem p = System.Reflection.DispatchProxy.Create<IFileSystem, ShortReadFileSystemProxy>();
            ((ShortReadFileSystemProxy)(object)p).Inner = m;
            return p;
        }
        byte[] buf = new byte[10];
        // B short-reads: the mirror loops until it has A's bytes.
        using (IRandomAccessFile f = new MirrorFileSystem(memA, Short(memB)).OpenRandomAccess("/d/f"))
        {
            Assert.AreEqual(10, f.Read(2, buf));
            CollectionAssert.AreEqual(content[2..12], buf);
            Assert.AreEqual(10, await f.ReadAsync(6, buf));
            CollectionAssert.AreEqual(content[6..16], buf);
        }
        // A short-reads: the result is A's (3 bytes), and B is only asked for those.
        using (IRandomAccessFile f = new MirrorFileSystem(Short(memA), memB).OpenRandomAccess("/d/f"))
        {
            Assert.AreEqual(3, f.Read(2, buf));
            CollectionAssert.AreEqual(content[2..5], buf[..3]);
            Assert.AreEqual(3, await f.ReadAsync(6, buf));
        }
        // A real difference is still reported.
        using (IWritableFile w = memB.CreateWritable("/d/g")) w.Append(B("0123456789abcdeX"));
        using (IWritableFile w = memA.CreateWritable("/d/g")) w.Append(content);
        using IRandomAccessFile g = new MirrorFileSystem(memA, Short(memB)).OpenRandomAccess("/d/g");
        Assert.ThrowsExactly<FileSystemMirrorMismatchException>(() => g.Read(8, buf.AsSpan(0, 8)));
    }

    // ---- IO tracer (R50) ---------------------------------------------------------------------------------------

    [TestMethod]
    public void IoTraceReaderIgnoresAnUnknownIoOpDataBit()
    {
        // IOTraceReader::ReadIOOp (trace_replay/io_tracer.cc:209-236): the switch over the set bits of io_op_data reads a
        // fixed64 for kIOFileSize / kIOLen / kIOOffset only; any other bit is "default: assert(false)" -- nothing is
        // consumed and, in a release build, the record is returned (the next field read is trace_data).
        MemoryFileSystem fs = NewFs();
        var tracer = new RocksDb.Managed.Tracing.IoTracer();
        tracer.StartTrace(new RocksDb.Managed.Tracing.TraceOptions(), RocksDb.Managed.Tracing.TraceWriter.CreateFile(fs, "/iotrace"));
        tracer.WriteIoOp("Append", System.Diagnostics.Stopwatch.GetTimestamp(), null, "/db/000001.log",
            RocksDb.Managed.Tracing.IoTraceOp.Length | (RocksDb.Managed.Tracing.IoTraceOp)(1UL << 5), length: 77, offset: 0, fileSize: 0);
        tracer.WriteIoOp("Read", System.Diagnostics.Stopwatch.GetTimestamp(), null, "/db/000002.sst",
            RocksDb.Managed.Tracing.IoTraceOp.Length | RocksDb.Managed.Tracing.IoTraceOp.Offset, length: 5, offset: 9, fileSize: 0);
        tracer.EndTrace();
        using var reader = new RocksDb.Managed.Tracing.IoTraceReader(RocksDb.Managed.Tracing.TraceReader.OpenFile(fs, "/iotrace"));
        reader.ReadHeader();
        Assert.IsTrue(reader.TryReadIoOp(out RocksDb.Managed.Tracing.IoTraceRecord? r));
        Assert.AreEqual("Append", r!.FileOperation);
        Assert.AreEqual(77UL, r.Length);
        Assert.AreEqual(0UL, r.TraceData);
        Assert.IsTrue(reader.TryReadIoOp(out r));
        Assert.AreEqual((5UL, 9UL), (r!.Length, r.Offset));
        Assert.IsFalse(reader.TryReadIoOp(out _));
    }
}
