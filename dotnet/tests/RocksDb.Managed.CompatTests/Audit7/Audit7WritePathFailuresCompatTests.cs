using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit7;

/// <summary>
/// Audit #7, the write / flush / compaction path after a shutdown began, checked against native 11.12:
/// DBImpl::CancelAllBackgroundWork leaves the DB open; a flush then fails with ShutdownInProgress
/// (DBImpl::WaitForFlushMemTables), WaitForCompact too (DBImpl::WaitForCompact), and plain writes keep working.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class Audit7WritePathFailuresCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string? TakeError(IntPtr err)
    {
        if (err == IntPtr.Zero) return null;
        return Marshal.PtrToStringUTF8(err)!; // a few bytes leaked by the test (rocksdb_free is not bound)
    }

    private static string NativeStatus(Action action)
    {
        try
        {
            action();
            return "OK";
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return e.Message;
        }
    }

    private static string ManagedStatus(Action action)
    {
        try
        {
            action();
            return "OK";
        }
        catch (RocksDbException e)
        {
            return e.Message;
        }
        catch (ObjectDisposedException)
        {
            return "ObjectDisposed";
        }
    }

    /// <summary>
    /// DBImpl::CancelAllBackgroundWork(wait = true), then Put (OK), Flush with data in the memtable (WaitForFlushMemTables:
    /// "if (shutting_down_) return Status::ShutdownInProgress()") and WaitForCompact
    /// (ShutdownInProgress once shutting_down_ is set). Both engines give the same status for each call.
    /// </summary>
    [TestMethod]
    public void FlushAndWaitForCompactAfterCancelAllBackgroundWorkMatchNative()
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        using var nativeDir = new TempDir("a7-cancel-native");
        string nPut, nFlush, nWait, nCompact;
        {
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path);
            db.Put(U("a"), U("1"));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            db.Put(U("a"), U("1b"));
            n.rocksdb_cancel_all_background_work(db.Handle, 1);
            nPut = NativeStatus(() => db.Put(U("b"), U("2")));
            nFlush = NativeStatus(() => db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)));
            IntPtr wo = n.rocksdb_wait_for_compact_options_create();
            n.rocksdb_wait_for_compact(db.Handle, wo, out IntPtr err);
            n.rocksdb_wait_for_compact_options_destroy(wo);
            nWait = TakeError(err) ?? "OK";
            // (rocksdb_compact_range discards CompactRange's status, so native's CompactRange cannot be compared here.)
            nCompact = "n/a";
            Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(U("b"))!));
        }
        using var managedDir = new TempDir("a7-cancel-managed");
        string mPut, mFlush, mWait, mCompact;
        {
            using RocksDatabase db = RocksDatabase.Open(managedDir.Path, new DbOptions { CreateIfMissing = true });
            db.Put(U("a"), U("1"));
            db.Flush();
            db.Put(U("a"), U("1b"));
            db.CancelAllBackgroundWork(wait: true);
            mPut = ManagedStatus(() => db.Put(U("b"), U("2")));
            mFlush = ManagedStatus(db.Flush);
            mWait = ManagedStatus(db.WaitForCompact);
            mCompact = ManagedStatus(db.CompactRange);
            Assert.AreEqual("2", Encoding.UTF8.GetString(db.Get(U("b"))!));
        }
        Console.WriteLine("native: put=" + nPut + " flush=" + nFlush + " wait=" + nWait + " compact=" + nCompact);
        Console.WriteLine("managed: put=" + mPut + " flush=" + mFlush + " wait=" + mWait + " compact=" + mCompact);
        Assert.AreEqual("OK", nPut);
        Assert.AreEqual("OK", mPut);
        // Native's message carries an empty state ("Shutdown in progress: "); the status code is what must agree.
        foreach (string status in new[] { nFlush, nWait, mFlush, mWait, mCompact }) StringAssert.StartsWith(status, "Shutdown in progress");
    }
}
