using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Port of db/version_edit_test.cc.</summary>
[TestClass]
public sealed class VersionEditTests
{
    private const ulong Big = 1UL << 50;
    private const uint Big32Bit = 1U << 30;

    internal static byte[] IKey(string userKey, ulong seq, ValueType type)
    {
        byte[] u = Encoding.UTF8.GetBytes(userKey);
        var k = new byte[u.Length + 8];
        DbFormat.AppendInternalKey(k, u, seq, type);
        return k;
    }

    internal static FileMetaData File(ulong number, uint pathId, ulong size, byte[] smallest, byte[] largest, ulong sseq, ulong lseq, ulong epoch)
        => new()
        {
            FileNumber = number, PathId = pathId, FileSize = size, Smallest = smallest, Largest = largest,
            SmallestSeqno = sseq, LargestSeqno = lseq, EpochNumber = epoch,
        };

    /// <summary>TestEncodeDecode: encode, decode, re-encode must produce identical bytes.</summary>
    private static VersionEdit EncodeDecode(VersionEdit edit, int tsSize = 0)
    {
        byte[] encoded = edit.Encode(tsSize);
        VersionEdit parsed = VersionEdit.DecodeFrom(encoded);
        byte[] encoded2 = parsed.Encode(tsSize);
        CollectionAssert.AreEqual(encoded, encoded2);
        return parsed;
    }

    [TestMethod]
    public void EncodeDecode()
    {
        var edit = new VersionEdit();
        for (int i = 0; i < 4; i++)
        {
            EncodeDecode(edit);
            edit.AddFile(3, File(Big + 300 + (ulong)i, 0, Big + 400 + (ulong)i,
                IKey("foo", Big + 500 + (ulong)i, ValueType.Value), IKey("zoo", Big + 600 + (ulong)i, ValueType.Deletion),
                Big + 500 + (ulong)i, Big + 600 + (ulong)i, Big + 300 + (ulong)i));
            edit.DeleteFile(4, Big + 700 + (ulong)i);
        }
        edit.ComparatorName = "foo";
        edit.PersistUserDefinedTimestamps = true;
        edit.LogNumber = Big + 100;
        edit.NextFileNumber = Big + 200;
        edit.LastSequence = Big + 1000;
        edit.MaxColumnFamily = Big32Bit;
        VersionEdit p = EncodeDecode(edit);
        Assert.AreEqual(4, p.NewFiles.Count);
        Assert.AreEqual(4, p.DeletedFiles.Count);
        Assert.AreEqual(Big32Bit, p.MaxColumnFamily);
        Assert.AreEqual(Big + 1000, p.LastSequence);
    }

    [TestMethod]
    public void EncodeDecodeNewFile4()
    {
        var edit = new VersionEdit();
        FileMetaData f1 = File(300, 3, 100, IKey("foo", Big + 500, ValueType.Value), IKey("zoo", Big + 600, ValueType.Deletion), Big + 500, Big + 600, 300);
        f1.MarkedForCompaction = true;
        edit.AddFile(3, f1);
        FileMetaData f2 = File(301, 3, 100, IKey("foo", Big + 501, ValueType.Value), IKey("zoo", Big + 601, ValueType.Deletion), Big + 501, Big + 601, 301);
        edit.AddFile(4, f2);
        FileMetaData f3 = File(302, 0, 100, IKey("foo", Big + 502, ValueType.Value), IKey("zoo", Big + 602, ValueType.Deletion), Big + 502, Big + 602, 302);
        f3.MarkedForCompaction = true;
        f3.OldestBlobFileNumber = 666;
        f3.OldestAncestorTime = 888;
        f3.FileCreationTime = 678;
        f3.FileChecksum = "234"u8.ToArray();
        f3.FileChecksumFuncName = "crc32c";
        f3.UniqueId = new UniqueId64x2(123, 456);
        f3.CompensatedRangeDeletionSize = 789;
        f3.TailSize = 4096;
        f3.Temperature = Temperature.Cold;
        f3.MinTimestamp = "t1"u8.ToArray();
        f3.MaxTimestamp = "t2"u8.ToArray();
        f3.FileOpenMetadata = "meta"u8.ToArray();
        edit.AddFile(5, f3);
        edit.DeleteFile(4, 700);
        edit.ComparatorName = "foo";
        edit.PersistUserDefinedTimestamps = false;
        edit.LogNumber = Big + 100;
        edit.NextFileNumber = Big + 200;
        edit.LastSequence = Big + 1000;
        edit.MinLogNumberToKeep = 77;

        VersionEdit parsed = EncodeDecode(edit);
        List<NewFileEntry> nf = parsed.NewFiles;
        Assert.IsTrue(nf[0].Meta.MarkedForCompaction);
        Assert.IsFalse(nf[1].Meta.MarkedForCompaction);
        Assert.IsTrue(nf[2].Meta.MarkedForCompaction);
        Assert.AreEqual(3u, nf[0].Meta.PathId);
        Assert.AreEqual(3u, nf[1].Meta.PathId);
        Assert.AreEqual(0u, nf[2].Meta.PathId);
        Assert.AreEqual(FileMetaData.InvalidBlobFileNumber, nf[0].Meta.OldestBlobFileNumber);
        Assert.AreEqual(666UL, nf[2].Meta.OldestBlobFileNumber);
        Assert.AreEqual(888UL, nf[2].Meta.OldestAncestorTime);
        Assert.AreEqual(678UL, nf[2].Meta.FileCreationTime);
        Assert.AreEqual("crc32c", nf[2].Meta.FileChecksumFuncName);
        CollectionAssert.AreEqual("234"u8.ToArray(), nf[2].Meta.FileChecksum);
        Assert.AreEqual(new UniqueId64x2(123, 456), nf[2].Meta.UniqueId);
        Assert.AreEqual(789UL, nf[2].Meta.CompensatedRangeDeletionSize);
        Assert.AreEqual(4096UL, nf[2].Meta.TailSize);
        Assert.AreEqual(Temperature.Cold, nf[2].Meta.Temperature);
        CollectionAssert.AreEqual("t1"u8.ToArray(), nf[2].Meta.MinTimestamp);
        CollectionAssert.AreEqual("meta"u8.ToArray(), nf[2].Meta.FileOpenMetadata);
        Assert.AreEqual(false, parsed.PersistUserDefinedTimestamps);
        Assert.AreEqual(77UL, parsed.MinLogNumberToKeep);
        Assert.AreEqual(Big + 502, nf[2].Meta.SmallestSeqno);
        Assert.AreEqual(Big + 602, nf[2].Meta.LargestSeqno);
    }

    [TestMethod]
    public void EncodeDecodeNewFile4HandleFileBoundary()
    {
        const int tsSize = 16;
        byte[] minTs = new byte[tsSize];
        var edit = new VersionEdit();
        FileMetaData f = File(300, 3, 100, IKey("foo" + Encoding.Latin1.GetString(minTs), Big + 500, ValueType.Value),
            IKey("zoo" + Encoding.Latin1.GetString(minTs), Big + 600, ValueType.Deletion), Big + 500, Big + 600, 300);
        f.UserDefinedTimestampsPersisted = false;
        edit.AddFile(3, f);
        edit.ComparatorName = "foo";
        edit.PersistUserDefinedTimestamps = false;
        edit.LogNumber = Big + 100;
        edit.NextFileNumber = Big + 200;
        edit.LastSequence = Big + 1000;

        VersionEdit parsed = VersionEdit.DecodeFrom(edit.Encode(tsSize));
        // Timestamps are stripped from file boundaries when they are not persisted.
        CollectionAssert.AreEqual(IKey("foo", Big + 500, ValueType.Value), parsed.NewFiles[0].Meta.Smallest);
        CollectionAssert.AreEqual(IKey("zoo", Big + 600, ValueType.Deletion), parsed.NewFiles[0].Meta.Largest);
        Assert.IsFalse(parsed.NewFiles[0].Meta.UserDefinedTimestampsPersisted);
    }

    /// <summary>Encodes a single-file edit and splices extra custom fields before the NewFile4 terminator.</summary>
    private static byte[] EncodeWithCustomFields(VersionEdit edit, Action<IBufferWriter<byte>> customFields)
    {
        byte[] encoded = edit.Encode(0);
        // With a single new file and no column-family/atomic/WAL fields, the record ends with the kTerminate tag.
        Assert.AreEqual((byte)NewFileCustomTag.Terminate, encoded[^1]);
        var w = new ArrayBufferWriter<byte>();
        w.Write(encoded.AsSpan(0, encoded.Length - 1));
        customFields(w);
        w.Write(encoded.AsSpan(encoded.Length - 1));
        return w.WrittenSpan.ToArray();
    }

    private static VersionEdit SingleFileEdit(bool markedForCompaction)
    {
        var edit = new VersionEdit();
        FileMetaData f = File(300, 3, 100, IKey("foo", Big + 500, ValueType.Value), IKey("zoo", Big + 600, ValueType.Deletion), Big + 500, Big + 600, 300);
        f.MarkedForCompaction = markedForCompaction;
        edit.DeleteFile(4, 700);
        edit.ComparatorName = "foo"; // (persist_user_defined_timestamps would be encoded after the files)
        edit.LogNumber = Big + 100;
        edit.NextFileNumber = Big + 200;
        edit.AddFile(3, f);
        edit.LastSequence = Big + 1000;
        return edit;
    }

    [TestMethod]
    public void ForwardCompatibleNewFile4()
    {
        byte[] encoded = EncodeWithCustomFields(SingleFileEdit(true), static w =>
        {
            Coding.PutVarint32(w, 33);
            Coding.PutLengthPrefixedSlice(w, "random_string"u8);
            Coding.PutVarint32(w, 22);
            Coding.PutLengthPrefixedSlice(w, "s"u8);
        });
        VersionEdit parsed = VersionEdit.DecodeFrom(encoded);
        Assert.IsTrue(parsed.NewFiles[0].Meta.MarkedForCompaction);
        Assert.AreEqual(3u, parsed.NewFiles[0].Meta.PathId);
        Assert.AreEqual(1, parsed.DeletedFiles.Count);
        // Unknown ignorable custom fields are dropped: re-encoding gives the original encoding.
        CollectionAssert.AreEqual(SingleFileEdit(true).Encode(0), parsed.Encode(0));
    }

    [TestMethod]
    public void NewFile4NotSupportedField()
    {
        // A custom tag with the kCustomTagNonSafeIgnoreMask bit set must fail.
        byte[] encoded = EncodeWithCustomFields(SingleFileEdit(true), static w =>
        {
            Coding.PutVarint32(w, (uint)NewFileCustomTag.CustomTagNonSafeIgnoreMask + 10);
            Coding.PutLengthPrefixedSlice(w, "s"u8);
        });
        var ex = Assert.ThrowsExactly<RocksDbException>(() => VersionEdit.DecodeFrom(encoded));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);

        // Malformed: a bare length-prefixed slice where a tag is expected (C++ test variant).
        encoded = EncodeWithCustomFields(SingleFileEdit(true), static w => Coding.PutLengthPrefixedSlice(w, "s"u8));
        Assert.IsFalse(VersionEdit.TryDecodeFrom(encoded, out _, out string? error));
        Assert.IsNotNull(error);
    }

    [TestMethod]
    public void EncodeEmptyFile()
    {
        var edit = new VersionEdit();
        edit.AddFile(0, new FileMetaData { SmallestSeqno = 0, LargestSeqno = 0 });
        Assert.IsFalse(edit.TryEncodeTo(new ArrayBufferWriter<byte>(), 0));
    }

    [TestMethod]
    public void ColumnFamilyTest()
    {
        var edit = new VersionEdit { ColumnFamily = 2 };
        edit.AddColumnFamily("column_family");
        edit.MaxColumnFamily = 5;
        VersionEdit p = EncodeDecode(edit);
        Assert.AreEqual("column_family", p.ColumnFamilyAddName);
        Assert.AreEqual(2u, p.ColumnFamily);

        edit = new VersionEdit { ColumnFamily = 3 };
        edit.DropColumnFamily();
        p = EncodeDecode(edit);
        Assert.IsTrue(p.IsColumnFamilyDrop);
        Assert.AreEqual(3u, p.ColumnFamily);
    }

    [TestMethod]
    public void MinLogNumberToKeep()
    {
        Assert.AreEqual(13UL, EncodeDecode(new VersionEdit { MinLogNumberToKeep = 13 }).MinLogNumberToKeep);
        Assert.AreEqual(23UL, EncodeDecode(new VersionEdit { MinLogNumberToKeep = 23 }).MinLogNumberToKeep);
    }

    [TestMethod]
    public void AtomicGroupTest()
    {
        var edit = new VersionEdit();
        edit.MarkAtomicGroup(1);
        VersionEdit p = EncodeDecode(edit);
        Assert.IsTrue(p.IsInAtomicGroup);
        Assert.AreEqual(1u, p.RemainingEntries);
    }

    [TestMethod]
    public void IgnorableField()
    {
        // Size of ignorable field is too large.
        var w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, 2);
        Coding.PutVarint64(w, 66);
        Coding.PutVarint32(w, 0x2710);
        Coding.PutVarint64(w, 5);
        w.Write("abc"u8);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => VersionEdit.DecodeFrom(w.WrittenSpan.ToArray())).Code);

        // Unidentified tag that is not ignorable.
        w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, 2);
        Coding.PutVarint64(w, 66);
        Coding.PutVarint32(w, 666);
        Coding.PutVarint64(w, 3);
        w.Write("abc"u8);
        Coding.PutVarint32(w, 3);
        Coding.PutVarint64(w, 88);
        Assert.IsFalse(VersionEdit.TryDecodeFrom(w.WrittenSpan, out _, out _));

        // Safely ignore an identified but ignorable entry.
        w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, 2);
        Coding.PutVarint64(w, 66);
        Coding.PutVarint32(w, 0x2710);
        Coding.PutVarint64(w, 3);
        w.Write("abc"u8);
        Coding.PutVarint32(w, 3);
        Coding.PutVarint64(w, 88);
        VersionEdit ve = VersionEdit.DecodeFrom(w.WrittenSpan);
        Assert.AreEqual(66UL, ve.LogNumber);
        Assert.AreEqual(88UL, ve.NextFileNumber);
    }

    [TestMethod]
    public void DbId()
    {
        Assert.AreEqual("ab34-cd12-435f-er00", EncodeDecode(new VersionEdit { DbId = "ab34-cd12-435f-er00" }).DbId);
        Assert.AreEqual("34ba-cd12-435f-er01", EncodeDecode(new VersionEdit { DbId = "34ba-cd12-435f-er01" }).DbId);
    }

    [TestMethod]
    public void BlobFileAdditionAndGarbage()
    {
        var edit = new VersionEdit();
        for (ulong i = 1; i <= 5; i++)
        {
            edit.BlobFileAdditions.Add(new BlobFileAddition(i, i * 10, i * 1000, "Hash" + i, Encoding.UTF8.GetBytes("Value" + i)));
            edit.BlobFileGarbages.Add(new BlobFileGarbage(i, i * 3, i * 300));
        }
        VersionEdit p = EncodeDecode(edit);
        Assert.AreEqual(5, p.BlobFileAdditions.Count);
        Assert.AreEqual("Hash3", p.BlobFileAdditions[2].ChecksumMethod);
        Assert.AreEqual(900UL, p.BlobFileGarbages[2].GarbageBlobBytes);
    }

    [TestMethod]
    public void AddWalEncodeDecode()
    {
        var edit = new VersionEdit();
        for (ulong n = 1; n <= 20; n++)
        {
            edit.WalAdditions.Add(new WalAddition(n, n % 2 == 0 ? n * 100 : null));
        }
        VersionEdit p = EncodeDecode(edit);
        Assert.AreEqual(20, p.WalAdditions.Count);
        Assert.IsTrue(p.IsWalAddition);
        Assert.IsTrue(p.IsWalManipulation);
        Assert.AreEqual(400UL, p.WalAdditions[3].SyncedSizeInBytes);
        Assert.IsNull(p.WalAdditions[2].SyncedSizeInBytes);
    }

    [TestMethod]
    public void AddWalDecodeBadLogNumber()
    {
        var w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, (uint)VersionEditTag.WalAddition2);
        Coding.PutVarint32(w, 1);
        w.Write(new byte[] { 0x80 }); // truncated varint
        Assert.IsFalse(VersionEdit.TryDecodeFrom(w.WrittenSpan, out _, out string? err));
        StringAssert.Contains(err, "Error decoding WAL log number");
    }

    [TestMethod]
    public void AddWalDecodeBadTag()
    {
        var inner = new ArrayBufferWriter<byte>();
        Coding.PutVarint64(inner, 100);
        Coding.PutVarint32(inner, 2); // kSyncedSize
        Coding.PutVarint64(inner, 100);
        Coding.PutVarint32(inner, 1 << 10); // unknown tag
        var w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, (uint)VersionEditTag.WalAddition2);
        Coding.PutLengthPrefixedSlice(w, inner.WrittenSpan);
        Assert.IsFalse(VersionEdit.TryDecodeFrom(w.WrittenSpan, out _, out string? err));
        StringAssert.Contains(err, "Unknown tag");
    }

    [TestMethod]
    public void AddWalDecodeNoSize()
    {
        var inner = new ArrayBufferWriter<byte>();
        Coding.PutVarint64(inner, 100);
        Coding.PutVarint32(inner, 2); // kSyncedSize but no size
        var w = new ArrayBufferWriter<byte>();
        Coding.PutVarint32(w, (uint)VersionEditTag.WalAddition2);
        Coding.PutLengthPrefixedSlice(w, inner.WrittenSpan);
        Assert.IsFalse(VersionEdit.TryDecodeFrom(w.WrittenSpan, out _, out string? err));
        StringAssert.Contains(err, "Error decoding WAL file size");
    }

    [TestMethod]
    public void DeleteWalEncodeDecode()
    {
        VersionEdit p = EncodeDecode(new VersionEdit { WalDeletionBefore = 42 });
        Assert.AreEqual(42UL, p.WalDeletionBefore);
        Assert.IsTrue(p.IsWalDeletion);
        // An empty WAL deletion (number 0) encodes to nothing, like WriteCurrentStateToManifest's record.
        Assert.AreEqual(0, new VersionEdit { WalDeletionBefore = 0 }.Encode(0).Length);
    }

    [TestMethod]
    public void FullHistoryTsLow()
    {
        byte[] ts = BitConverter.GetBytes(0UL);
        VersionEdit p = EncodeDecode(new VersionEdit { FullHistoryTsLow = ts });
        CollectionAssert.AreEqual(ts, p.FullHistoryTsLow);
    }

    [TestMethod]
    public void OptionsFileNumbersAndManifestSize()
    {
        VersionEdit p = EncodeDecode(new VersionEdit { CommittedOptionsFileNumber = 12345, LastCompactedManifestFileSize = 999 });
        Assert.AreEqual(12345UL, p.CommittedOptionsFileNumber);
        Assert.AreEqual(999UL, p.LastCompactedManifestFileSize);
    }

    [TestMethod]
    public void SubcompactionProgressIsPreservedVerbatim()
    {
        VersionEdit p = EncodeDecode(new VersionEdit { SubcompactionProgress = [4, 1, 0, 1] });
        CollectionAssert.AreEqual(new byte[] { 4, 1, 0, 1 }, p.SubcompactionProgress);
    }

    [TestMethod]
    public void CompactCursorsAndPrevLog()
    {
        var edit = new VersionEdit { PrevLogNumber = 9 };
        edit.CompactCursors.Add(new CompactCursor(2, IKey("cursor", 5, ValueType.Value)));
        VersionEdit p = EncodeDecode(edit);
        Assert.AreEqual(9UL, p.PrevLogNumber);
        Assert.AreEqual(2, p.CompactCursors[0].Level);
    }

    [TestMethod]
    public void IgnorableTags()
    {
        // All forward-compatible tags written by the encoder can be skipped generically via their length.
        // Emulate an older reader by re-tagging every known ignorable record with an unknown ignorable tag.
        var edit = new VersionEdit
        {
            FullHistoryTsLow = [1, 2, 3],
            CommittedOptionsFileNumber = 7,
            LogNumber = 5,
        };
        byte[] encoded = edit.Encode(0);
        ReadOnlySpan<byte> input = encoded;
        var w = new ArrayBufferWriter<byte>();
        while (Coding.GetVarint32(ref input, out uint tag))
        {
            if (tag > (uint)VersionEditTag.TagSafeIgnoreMask)
            {
                Assert.IsTrue(Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> payload));
                Coding.PutVarint32(w, (uint)VersionEditTag.TagSafeIgnoreMask + 1000);
                Coding.PutLengthPrefixedSlice(w, payload);
            }
            else
            {
                Assert.AreEqual((uint)VersionEditTag.LogNumber, tag);
                Assert.IsTrue(Coding.GetVarint64(ref input, out ulong v));
                Coding.PutVarint32(w, tag);
                Coding.PutVarint64(w, v);
            }
        }
        VersionEdit p = VersionEdit.DecodeFrom(w.WrittenSpan);
        Assert.AreEqual(5UL, p.LogNumber);
        Assert.IsNull(p.FullHistoryTsLow);
        Assert.IsNull(p.CommittedOptionsFileNumber);
    }

    [TestMethod]
    public void LegacyNewFileFormats()
    {
        byte[] smallest = IKey("a", 1, ValueType.Value), largest = IKey("b", 2, ValueType.Value);
        var w = new ArrayBufferWriter<byte>();
        // kNewFile: level, number, size, smallest, largest
        Coding.PutVarint32(w, (uint)VersionEditTag.NewFile);
        Coding.PutVarint32(w, 1); Coding.PutVarint64(w, 10); Coding.PutVarint64(w, 100);
        Coding.PutLengthPrefixedSlice(w, smallest); Coding.PutLengthPrefixedSlice(w, largest);
        // kNewFile2: + smallest/largest seqno
        Coding.PutVarint32(w, (uint)VersionEditTag.NewFile2);
        Coding.PutVarint32(w, 2); Coding.PutVarint64(w, 11); Coding.PutVarint64(w, 101);
        Coding.PutLengthPrefixedSlice(w, smallest); Coding.PutLengthPrefixedSlice(w, largest);
        Coding.PutVarint64(w, 1); Coding.PutVarint64(w, 2);
        // kNewFile3: + path id after the number
        Coding.PutVarint32(w, (uint)VersionEditTag.NewFile3);
        Coding.PutVarint32(w, 3); Coding.PutVarint64(w, 12); Coding.PutVarint32(w, 2); Coding.PutVarint64(w, 102);
        Coding.PutLengthPrefixedSlice(w, smallest); Coding.PutLengthPrefixedSlice(w, largest);
        Coding.PutVarint64(w, 3); Coding.PutVarint64(w, 4);

        VersionEdit p = VersionEdit.DecodeFrom(w.WrittenSpan);
        Assert.AreEqual(3, p.NewFiles.Count);
        Assert.AreEqual(1, p.NewFiles[0].Level);
        Assert.AreEqual(DbFormat.MaxSequenceNumber, p.NewFiles[0].Meta.SmallestSeqno);
        Assert.AreEqual(11UL, p.NewFiles[1].Meta.FileNumber);
        Assert.AreEqual(2UL, p.NewFiles[1].Meta.LargestSeqno);
        Assert.AreEqual(2u, p.NewFiles[2].Meta.PathId);
        Assert.AreEqual(102UL, p.NewFiles[2].Meta.FileSize);
    }

    [TestMethod]
    public void TruncatedInputIsCorruption()
    {
        byte[] encoded = SingleFileEdit(false).Encode(0);
        for (int len = 1; len < encoded.Length; len++)
        {
            // Every strict prefix either decodes to a smaller edit or fails cleanly; it must never throw anything else.
            VersionEdit.TryDecodeFrom(encoded.AsSpan(0, len), out _, out _);
        }
        Assert.IsTrue(VersionEdit.TryDecodeFrom(encoded, out _, out _));
    }
}
