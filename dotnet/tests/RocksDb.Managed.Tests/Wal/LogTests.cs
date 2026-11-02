using RocksDb.Managed.Compression;
using RocksDb.Managed.Wal;
using static RocksDb.Managed.Tests.Wal.LogHarness;

namespace RocksDb.Managed.Tests.Wal;

/// <summary>Port of the LogTest cases in db/log_test.cc, run for legacy/recyclable formats and sync/async APIs.</summary>
[TestClass]
public sealed class LogTests
{
    private const int BlockSize = LogFormat.BlockSize;

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)] [DataRow(true, true)]
    public void Empty(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)] [DataRow(true, true)]
    public void ReadWrite(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        t.Write("foo");
        t.Write("bar");
        t.Write("");
        t.Write("xxxx");
        Assert.AreEqual("foo", t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("", t.Read());
        Assert.AreEqual("xxxx", t.Read());
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual("EOF", t.Read()); // Make sure reads at eof work
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ReadWriteWithTimestampSize(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(1u, 8)]);
        t.Write("foo");
        t.Write("bar");
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(2u, 1)]);
        t.Write("");
        t.Write("xxxx");
        Assert.AreEqual("foo", t.Read());
        CollectionAssert.AreEquivalent(new Dictionary<uint, int> { [1] = 8 }, t.Reader.RecordedTimestampSizes.ToDictionary());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("", t.Read());
        // User-defined timestamp size records are accumulated and applied to subsequent records.
        CollectionAssert.AreEquivalent(new Dictionary<uint, int> { [1] = 8, [2] = 1 }, t.Reader.RecordedTimestampSizes.ToDictionary());
        Assert.AreEqual("xxxx", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ReadWriteWithTimestampSizeZeroTimestampIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(1u, 8)]);
        t.Write("foo");
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(1u, 8), new(2u, 0)]);
        t.Write("bar");
        Assert.AreEqual("foo", t.Read());
        Assert.AreEqual("bar", t.Read());
        CollectionAssert.AreEquivalent(new Dictionary<uint, int> { [1] = 8 }, t.Reader.RecordedTimestampSizes.ToDictionary());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)]
    public void ManyBlocks(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        for (int i = 0; i < 100000; i++) t.Write(NumberString(i));
        for (int i = 0; i < 100000; i++) Assert.AreEqual(NumberString(i), t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)] [DataRow(true, true)]
    public void Fragmentation(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        t.Write("small");
        t.Write(BigString("medium", 50000));
        t.Write(BigString("large", 100000));
        Assert.AreEqual("small", t.Read());
        Assert.AreEqual(BigString("medium", 50000), t.Read());
        Assert.AreEqual(BigString("large", 100000), t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void MarginalTrailer(bool recyclable)
    {
        // Make a trailer that is exactly the same length as an empty record.
        using var t = new LogHarness(recyclable);
        int n = BlockSize - 2 * t.HeaderSize;
        t.Write(BigString("foo", n));
        Assert.AreEqual(BlockSize - t.HeaderSize, t.WrittenBytes);
        t.Write("");
        t.Write("bar");
        Assert.AreEqual(BigString("foo", n), t.Read());
        Assert.AreEqual("", t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void MarginalTrailer2(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        int n = BlockSize - 2 * t.HeaderSize;
        t.Write(BigString("foo", n));
        Assert.AreEqual(BlockSize - t.HeaderSize, t.WrittenBytes);
        t.Write("bar");
        Assert.AreEqual(BigString("foo", n), t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(0, t.DroppedBytes);
        Assert.AreEqual("", t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ShortTrailer(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        int n = BlockSize - 2 * t.HeaderSize + 4;
        t.Write(BigString("foo", n));
        Assert.AreEqual(BlockSize - t.HeaderSize + 4, t.WrittenBytes);
        t.Write("");
        t.Write("bar");
        Assert.AreEqual(BigString("foo", n), t.Read());
        Assert.AreEqual("", t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void AlignedEof(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        int n = BlockSize - 2 * t.HeaderSize + 4;
        t.Write(BigString("foo", n));
        Assert.AreEqual(BlockSize - t.HeaderSize + 4, t.WrittenBytes);
        Assert.AreEqual(BigString("foo", n), t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)]
    public void RandomRead(bool recyclable, bool async)
    {
        const int N = 500;
        using var t = new LogHarness(recyclable, async);
        var writeRnd = new Random(301);
        for (int i = 0; i < N; i++) t.Write(RandomSkewedString(i, writeRnd));
        var readRnd = new Random(301);
        for (int i = 0; i < N; i++) Assert.AreEqual(RandomSkewedString(i, readRnd), t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    private static string RandomSkewedString(int i, Random rnd) => BigString(NumberString(i), rnd.Next(1 << rnd.Next(18)));

    // Tests of all the error paths in log_reader.cc follow:

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)]
    public void ReadError(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        t.Write("foo");
        t.ForceError();
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(BlockSize, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("read error"), t.ReportMessage);
        Assert.IsTrue(t.Reader.HasReadError);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void BadRecordType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        // Type is stored in header[6]
        t.IncrementByte(6, 100);
        t.FixChecksum(0, 3, false);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(3, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("unknown record type"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void IgnorableRecordType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.SetByte(6, LogFormat.RecordTypeSafeIgnoreMask + 100);
        t.FixChecksum(0, 3, false);
        Assert.AreEqual("EOF", t.Read());
        // The new type has value 228 and masked to be ignorable if unknown
        Assert.AreEqual(0, t.DroppedBytes);
        Assert.AreEqual("", t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void TruncatedTrailingRecordIsIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.ShrinkSize(4); // Drop all payload as well as a header byte
        Assert.AreEqual("EOF", t.Read());
        // Truncated last record is ignored, not treated as an error
        Assert.AreEqual(0, t.DroppedBytes);
        Assert.AreEqual("", t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void TruncatedTrailingRecordIsNotIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.ShrinkSize(4);
        Assert.AreEqual("EOF", t.Read(WalRecoveryMode.AbsoluteConsistency));
        Assert.IsTrue(t.DroppedBytes > 0);
        Assert.IsTrue(t.MatchError("Corruption: truncated header"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void BadLength(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        int payloadSize = BlockSize - t.HeaderSize;
        t.Write(BigString("bar", payloadSize));
        t.Write("foo");
        // Least significant size byte is stored in header[4].
        t.IncrementByte(4, 1);
        if (!recyclable)
        {
            Assert.AreEqual("foo", t.Read());
            Assert.AreEqual(BlockSize, t.DroppedBytes);
            Assert.IsTrue(t.MatchError("bad record length"), t.ReportMessage);
        }
        else
        {
            Assert.AreEqual("EOF", t.Read());
        }
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void BadLengthAtEndIsIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.ShrinkSize(1);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(0, t.DroppedBytes);
        Assert.AreEqual("", t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void BadLengthAtEndIsNotIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.ShrinkSize(1);
        Assert.AreEqual("EOF", t.Read(WalRecoveryMode.AbsoluteConsistency));
        Assert.IsTrue(t.DroppedBytes > 0);
        Assert.IsTrue(t.MatchError("Corruption: truncated record body"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ChecksumMismatch(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foooooo");
        t.IncrementByte(0, 14);
        Assert.AreEqual("EOF", t.Read());
        if (!recyclable)
        {
            Assert.AreEqual(14, t.DroppedBytes);
            Assert.IsTrue(t.MatchError("checksum mismatch"), t.ReportMessage);
        }
        else
        {
            Assert.AreEqual(0, t.DroppedBytes);
            Assert.AreEqual("", t.ReportMessage);
        }
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void UnexpectedMiddleType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.SetByte(6, (byte)(recyclable ? LogRecordType.RecyclableMiddle : LogRecordType.Middle));
        t.FixChecksum(0, 3, recyclable);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(3, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("missing start"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void UnexpectedLastType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.SetByte(6, (byte)(recyclable ? LogRecordType.RecyclableLast : LogRecordType.Last));
        t.FixChecksum(0, 3, recyclable);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(3, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("missing start"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void UnexpectedFullType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.Write("bar");
        t.SetByte(6, (byte)(recyclable ? LogRecordType.RecyclableFirst : LogRecordType.First));
        t.FixChecksum(0, 3, recyclable);
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(3, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("partial record without end"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void UnexpectedFirstType(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.Write(BigString("bar", 100000));
        t.SetByte(6, (byte)(recyclable ? LogRecordType.RecyclableFirst : LogRecordType.First));
        t.FixChecksum(0, 3, recyclable);
        Assert.AreEqual(BigString("bar", 100000), t.Read());
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(3, t.DroppedBytes);
        Assert.IsTrue(t.MatchError("partial record without end"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void MissingLastIsIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write(BigString("bar", BlockSize));
        // Remove the LAST block, including header.
        t.ShrinkSize(14);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual("", t.ReportMessage);
        Assert.AreEqual(0, t.DroppedBytes);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void MissingLastIsNotIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write(BigString("bar", BlockSize));
        t.ShrinkSize(14);
        Assert.AreEqual("EOF", t.Read(WalRecoveryMode.AbsoluteConsistency));
        Assert.IsTrue(t.DroppedBytes > 0);
        Assert.IsTrue(t.MatchError("Corruption: error reading trailing data"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void PartialLastIsIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write(BigString("bar", BlockSize));
        // Cause a bad record length in the LAST block.
        t.ShrinkSize(1);
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual("", t.ReportMessage);
        Assert.AreEqual(0, t.DroppedBytes);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void PartialLastIsNotIgnored(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write(BigString("bar", BlockSize));
        t.ShrinkSize(1);
        Assert.AreEqual("EOF", t.Read(WalRecoveryMode.AbsoluteConsistency));
        Assert.IsTrue(t.DroppedBytes > 0);
        Assert.IsTrue(t.MatchError("Corruption: truncated record body"), t.ReportMessage);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ErrorJoinsRecords(bool recyclable)
    {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear. We do not want first(R1),last(R2) to get joined and returned as
        // a valid record.
        using var t = new LogHarness(recyclable);
        t.Write(BigString("foo", BlockSize));
        t.Write(BigString("bar", BlockSize));
        t.Write("correct");
        // Wipe the middle block
        for (int offset = BlockSize; offset < 2 * BlockSize; offset++) t.SetByte(offset, (byte)'x');
        if (!recyclable)
        {
            Assert.AreEqual("correct", t.Read());
            Assert.AreEqual("EOF", t.Read());
            long dropped = t.DroppedBytes;
            Assert.IsTrue(dropped <= 2 * BlockSize + 100);
            Assert.IsTrue(dropped >= 2 * BlockSize);
        }
        else
        {
            Assert.AreEqual("EOF", t.Read());
        }
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)] [DataRow(true, true)]
    public void ClearEofSingleBlock(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async);
        t.Write("foo");
        t.Write("bar");
        t.ForceEof(3 + t.HeaderSize + 2);
        Assert.AreEqual("foo", t.Read());
        t.UnmarkEof();
        Assert.AreEqual("bar", t.Read());
        Assert.IsTrue(t.Reader.IsEof);
        Assert.AreEqual("EOF", t.Read());
        t.Write("xxx");
        t.UnmarkEof();
        Assert.AreEqual("xxx", t.Read());
        Assert.IsTrue(t.Reader.IsEof);
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)]
    public void ClearEofMultiBlock(bool recyclable, bool async)
    {
        const int numFullBlocks = 5;
        using var t = new LogHarness(recyclable, async);
        int n = (BlockSize - t.HeaderSize) * numFullBlocks + 25;
        t.Write(BigString("foo", n));
        t.Write(BigString("bar", n));
        t.ForceEof(n + numFullBlocks * t.HeaderSize + t.HeaderSize + 3);
        Assert.AreEqual(BigString("foo", n), t.Read());
        Assert.IsTrue(t.Reader.IsEof);
        t.UnmarkEof();
        Assert.AreEqual(BigString("bar", n), t.Read());
        Assert.IsTrue(t.Reader.IsEof);
        t.Write(BigString("xxx", n));
        t.UnmarkEof();
        Assert.AreEqual(BigString("xxx", n), t.Read());
        Assert.IsTrue(t.Reader.IsEof);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ClearEofError(bool recyclable)
    {
        // If an error occurs during Read() in UnmarkEOF(), the records contained in the buffer should be returned on
        // subsequent calls of ReadRecord() until no more full records are left, whereafter ReadRecord() should return
        // false to indicate that it cannot read any further.
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.Write("bar");
        t.UnmarkEof();
        Assert.AreEqual("foo", t.Read());
        Assert.IsTrue(t.Reader.IsEof);
        t.Write("xxx");
        t.ForceError(0);
        t.UnmarkEof();
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ClearEofError2(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        t.Write("foo");
        t.Write("bar");
        t.UnmarkEof();
        Assert.AreEqual("foo", t.Read());
        t.Write("xxx");
        t.ForceError(3);
        t.UnmarkEof();
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
        // Difference from C++: a failed read cannot also return partial data (it throws), so the 3 bytes read before
        // the error are not reported as dropped.
        Assert.IsTrue(t.Reader.HasReadError);
    }

    [TestMethod]
    public void Recycle()
    {
        using var t = new LogHarness(recyclable: true);
        t.Write("foo");
        t.Write("bar");
        t.Write("baz");
        t.Write("bif");
        t.Write("blitz");
        while (t.Contents.Count < BlockSize * 2) t.Write("xxxxxxxxxxxxxxxx");
        using (var recycleWriter = new LogWriter(new MemoryWritableFile(t.Contents, overwrite: true), LogNumber, recycleLogFiles: true))
        {
            recycleWriter.AddRecord("foooo"u8);
            recycleWriter.AddRecord("bar"u8);
        }
        Assert.IsTrue(t.Contents.Count >= BlockSize * 2);
        Assert.AreEqual("foooo", t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    public void RecycleWithTimestampSize()
    {
        using var t = new LogHarness(recyclable: true);
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(1u, 4)]);
        t.Write("foo");
        t.Write("bar");
        while (t.Contents.Count < BlockSize * 2) t.Write("xxxxxxxxxxxxxxxx");
        using (var recycleWriter = new LogWriter(new MemoryWritableFile(t.Contents, overwrite: true), LogNumber, recycleLogFiles: true))
        {
            recycleWriter.MaybeAddUserDefinedTimestampSizeRecord([new(2u, 8)]);
            recycleWriter.AddRecord("foooo"u8);
            recycleWriter.AddRecord("bar"u8);
        }
        Assert.AreEqual("foooo", t.Read());
        CollectionAssert.AreEquivalent(new Dictionary<uint, int> { [2] = 8 }, t.Reader.RecordedTimestampSizes.ToDictionary());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    public void RecycledLogWithOldLogNumberIsEof()
    {
        // A record written with a different log number is a leftover from the previous user of the file.
        using var t = new LogHarness(recyclable: true);
        using (var other = new LogWriter(new MemoryWritableFile(t.Contents, overwrite: false), LogNumber + 1, recycleLogFiles: true))
        {
            other.AddRecord("old"u8);
        }
        Assert.AreEqual("EOF", t.Read(WalRecoveryMode.PointInTimeRecovery));
        Assert.AreEqual(1, t.Report.OldRecords);
        Assert.AreEqual(0, t.DroppedBytes);
    }

    /// <summary>MaybeAddUserDefinedTimestampSizeRecord pads the tail of a block if there's not enough space for the record.</summary>
    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void TimestampSizeRecordPadding(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        int dataLen = BlockSize - 2 * t.HeaderSize;
        string first = BigString("foo", dataLen);
        t.Write(first);
        t.Writer.MaybeAddUserDefinedTimestampSizeRecord([new(2u, 8)]);
        Assert.IsTrue(t.Writer.BlockOffset < BlockSize);
        string second = BigString("bar", 1000);
        t.Write(second);
        Assert.AreEqual(first, t.Read());
        Assert.AreEqual(second, t.Read());
        CollectionAssert.AreEquivalent(new Dictionary<uint, int> { [2] = 8 }, t.Reader.RecordedTimestampSizes.ToDictionary());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void PredecessorWalInfoRoundTrip(bool recyclable)
    {
        using var t = new LogHarness(recyclable);
        var info = new PredecessorWalInfo(7, 1234, 99);
        t.Writer.AddPredecessorWalInfo(info);
        t.Write("foo");
        Assert.AreEqual("foo", t.Read());
        Assert.AreEqual("", t.ReportMessage);
        Assert.IsTrue(PredecessorWalInfo.TryDecode(t.Contents.GetRange(t.HeaderSize, 24).ToArray(), out PredecessorWalInfo decoded));
        Assert.AreEqual(info, decoded);
        Assert.AreEqual(recyclable ? (byte)LogRecordType.RecyclePredecessorWalInfo : (byte)LogRecordType.PredecessorWalInfo, t.Contents[6]);
    }

    [TestMethod]
    public void PredecessorWalInfoMismatchIsReported()
    {
        var contents = new List<byte>();
        using (var w = new LogWriter(new MemoryWritableFile(contents, overwrite: false), 10, recycleLogFiles: false))
        {
            w.AddPredecessorWalInfo(new PredecessorWalInfo(9, 100, 50));
            w.AddRecord("x"u8);
        }
        var report = new ReportCollector();
        using var reader = new LogReader(new MemorySequentialFile(contents), report, checksum: true, 10)
        {
            TrackAndVerifyWals = true,
            ObservedPredecessorWalInfo = new PredecessorWalInfo(9, 100, 51),
            RecoveryMode = WalRecoveryMode.PointInTimeRecovery,
        };
        Assert.IsTrue(reader.TryReadRecord(out _));
        Assert.IsTrue(report.Message.ToString().Contains("Mismatched last sequence number", StringComparison.Ordinal), report.Message.ToString());
    }

    [TestMethod]
    public void HeaderLayoutAndChecksum()
    {
        // Validate the exact byte layout of a legacy and a recyclable record.
        foreach (bool recyclable in new[] { false, true })
        {
            var contents = new List<byte>();
            using (var w = new LogWriter(new MemoryWritableFile(contents, overwrite: false), 0x1_0000_0002UL, recyclable))
            {
                w.AddRecord("abc"u8);
            }
            byte[] b = contents.ToArray();
            int hs = recyclable ? 11 : 7;
            Assert.AreEqual(hs + 3, b.Length);
            Assert.AreEqual(3, b[4]);
            Assert.AreEqual(0, b[5]);
            Assert.AreEqual(recyclable ? 5 : 1, b[6]);
            if (recyclable) Assert.AreEqual(2u, Managed.Util.Coding.DecodeFixed32(b.AsSpan(7))); // low 32 bits of log number
            uint expected = Managed.Util.Crc32C.Mask(Managed.Util.Crc32C.Value(b.AsSpan(6, hs - 6 + 3)));
            Assert.AreEqual(expected, Managed.Util.Coding.DecodeFixed32(b));
        }
    }

    [TestMethod]
    public void ManualFlushDoesNotFlushPerRecord()
    {
        var contents = new List<byte>();
        var file = new MemoryWritableFile(contents, overwrite: false);
        using var w = new LogWriter(file, 1, recycleLogFiles: false, manualFlush: true, CompressionType.None, initialBlockOffset: 0);
        w.AddRecord("a"u8);
        w.AddRecord("b"u8);
        Assert.AreEqual(0, file.FlushCount);
        w.WriteBuffer();
        Assert.AreEqual(1, file.FlushCount);
    }

    [TestMethod]
    public void LastSequenceRecorded()
    {
        using var t = new LogHarness(recyclable: false);
        t.Writer.AddRecord("a"u8, 10);
        t.Writer.AddRecord("b"u8, 5);
        Assert.AreEqual(10UL, t.Writer.LastSequenceRecorded);
    }
}

/// <summary>Port of CompressionLogTest (db/log_test.cc): streaming ZSTD WAL compression.</summary>
[TestClass]
public sealed class CompressionLogTests
{
    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)]
    public void Empty(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async, CompressionType.Zstd);
        Assert.AreEqual("EOF", t.Read());
        Assert.IsTrue(t.Reader.IsCompressedAndEmptyFile);
    }

    [TestMethod]
    [DataRow(false, false)] [DataRow(true, false)] [DataRow(false, true)] [DataRow(true, true)]
    public void ReadWrite(bool recyclable, bool async)
    {
        using var t = new LogHarness(recyclable, async, CompressionType.Zstd);
        t.Write("foo");
        t.Write("bar");
        t.Write("");
        t.Write("xxxx");
        Assert.AreEqual("foo", t.Read());
        Assert.AreEqual("bar", t.Read());
        Assert.AreEqual("", t.Read());
        Assert.AreEqual("xxxx", t.Read());
        Assert.AreEqual("EOF", t.Read());
        Assert.AreEqual(CompressionType.Zstd, t.Reader.CompressionType);
        Assert.AreEqual((byte)LogRecordType.SetCompressionType, t.Contents[6]);
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void ManyBlocks(bool recyclable)
    {
        using var t = new LogHarness(recyclable, false, CompressionType.Zstd);
        for (int i = 0; i < 20000; i++) t.Write(NumberString(i));
        for (int i = 0; i < 20000; i++) Assert.AreEqual(NumberString(i), t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    [DataRow(false)] [DataRow(true)]
    public void Fragmentation(bool recyclable)
    {
        using var t = new LogHarness(recyclable, false, CompressionType.Zstd);
        var rnd = new Random(301);
        // Incompressible data so that compressed records span blocks.
        byte[] big = new byte[3 * LogFormat.BlockSize + 77];
        rnd.NextBytes(big);
        string bigStr = Convert.ToBase64String(big);
        t.Write("small");
        t.Write(BigString("medium", 50000));
        t.Write(bigStr);
        Assert.AreEqual("small", t.Read());
        Assert.AreEqual(BigString("medium", 50000), t.Read());
        Assert.AreEqual(bigStr, t.Read());
        Assert.AreEqual("EOF", t.Read());
    }

    [TestMethod]
    public void ChecksumMismatch()
    {
        using var t = new LogHarness(false, false, CompressionType.Zstd);
        t.Write("foooooo");
        int offset = LogFormat.HeaderSize + CompressionTypeRecord.EncodedLength;
        t.IncrementByte(offset, 14);
        Assert.AreEqual("EOF", t.Read());
        Assert.IsTrue(t.MatchError("checksum mismatch"), t.ReportMessage);
    }
}
