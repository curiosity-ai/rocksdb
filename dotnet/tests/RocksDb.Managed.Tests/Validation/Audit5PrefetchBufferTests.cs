using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// <see cref="AsyncPrefetchBuffer"/> with prefetches that complete on another thread. The completion used to mark the
/// prefetch done before it set the reader's event, so a reader could skip the wait, dispose the buffer and leave the
/// completion to set an event that was gone: a NullReferenceException on a thread-pool thread, which ends the process
/// (seen once in a full unit run). The reader now always waits for the event of an asynchronous prefetch.
/// </summary>
[TestClass]
public sealed class Audit5PrefetchBufferTests
{
    /// <summary>A file whose asynchronous reads complete on a thread-pool thread.</summary>
    private sealed class ThreadPoolFile(byte[] content) : IRandomAccessFile
    {
        public long Length => content.Length;

        public int Read(long offset, Span<byte> destination)
        {
            int n = (int)Math.Max(0, Math.Min(destination.Length, content.Length - offset));
            content.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }

        public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return Read(offset, destination.Span);
        }

        public void Dispose()
        {
        }
    }

    [TestMethod]
    public void ReadsThenDisposesWhilePrefetchesCompleteOnOtherThreads()
    {
        var content = new byte[64 * 1024];
        new Random(5).NextBytes(content);
        using var file = new ThreadPoolFile(content);

        for (int round = 0; round < 2000; round++)
        {
            var buffer = new AsyncPrefetchBuffer(1024, 4096);
            buffer.Reset(file);

            //Read a varying stretch sequentially, so the round ends with a prefetch in flight at a different point
            int end = 512 + (round * 97) % (content.Length - 1024);

            for (int offset = 0; offset < end; offset += 256)
            {
                Assert.IsTrue(buffer.TryRead(offset, 256, out ReadOnlySpan<byte> data), "round " + round + ", offset " + offset);
                Assert.IsTrue(data.SequenceEqual(content.AsSpan(offset, 256)), "round " + round + ", offset " + offset);
            }

            buffer.Dispose();
        }
    }
}
