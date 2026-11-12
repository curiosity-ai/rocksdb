using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using MagicOnion.Client;
using MessagePack;
using RocksDb.Managed.Cluster;
using RocksDb.Managed.Replication.Hosting;
using RocksDb.Managed.Replication.MagicOnion;
using RocksDb.Managed.Replication.Switchover;

namespace RocksDb.Managed.Replication.Tests;

/// <summary>
/// Audit #4, large payloads on the wire. Part A: a write batch larger than the WAL stream's chunk size is sent to a
/// follower that announced <see cref="WalStreamCapabilities.Fragments"/> as fragments of at most the chunk size and
/// applied only once it is complete. Part B: a client write larger than a chunk is uploaded to a node in fragments
/// and applied as one write.
/// </summary>
[TestClass]
public sealed class Audit4ReplicationFragmentTests
{
    private const int Chunk = 16 * 1024;

    // ---------------------------------------------------------------------------------------------------
    // Helpers

    /// <summary>A batch of <paramref name="entries"/> puts of <paramref name="valueBytes"/> random letters each ("big-{tag}-{i}").</summary>
    private static WriteBatch LargeBatch(string tag, int entries, int valueBytes)
    {
        var rnd = new Random(tag.GetHashCode(StringComparison.Ordinal) ^ valueBytes);
        var batch = new WriteBatch();
        for (int i = 0; i < entries; i++)
        {
            byte[] v = new byte[valueBytes];
            for (int j = 0; j < v.Length; j++) v[j] = (byte)('a' + rnd.Next(26));
            batch.Put(U("big-" + tag + "-" + i), v);
        }
        return batch;
    }

    private static async Task WriteLargeAsync(RocksDatabase db, string tag, int entries, int valueBytes)
    {
        using WriteBatch b = LargeBatch(tag, entries, valueBytes);
        await db.WriteAsync(b);
    }

    private static async Task<(RocksDatabase Source, RocksDatabase Replica)> OpenPlainAsync(TempDir dir)
    {
        DbOptions o = DbOpts(null);
        o.WalTtlSeconds = 600;
        return (await RocksDatabase.OpenAsync(dir.Sub("primary"), o), await RocksDatabase.OpenAsync(dir.Sub("replica"), DbOpts(null)));
    }

    private readonly record struct Seen(ulong FirstSequence, int Offset, int Total, int Length, int EntryCount, ulong ReplicaSeq);

    /// <summary>
    /// An in-process transport for plain replication: messages are copied (as serialization would), optionally
    /// rewritten, and recorded with the replica's position at the time they arrive.
    /// </summary>
    private sealed class DirectClient(ReplicationPrimary primary, RocksDatabase replica, bool carriesCapabilities) : IReplicationSourceClient
    {
        public readonly ConcurrentQueue<Seen> Messages = new();
        public readonly ConcurrentQueue<ulong> Reports = new();
        public Func<WalStreamMessage, WalStreamMessage>? Rewrite;
        public int Streams;

        public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) => primary.StreamInitialStateAsync(cancellationToken);

        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, CancellationToken cancellationToken) =>
            StreamUpdatesAsync(startSeq, WalStreamCapabilities.None, cancellationToken);

        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, WalStreamCapabilities capabilities,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Streams);
            if (!carriesCapabilities) capabilities = WalStreamCapabilities.None;
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(startSeq, capabilities, cancellationToken))
            {
                WalStreamMessage d = m.Detach();
                if (Rewrite is not null) d = Rewrite(d);
                if (!d.ResyncRequired) Messages.Enqueue(new Seen(d.FirstSequence, d.FragmentOffset, d.TotalLength, d.Data.Length, d.EntryCount, replica.GetLatestSequenceNumber()));
                yield return d;
            }
        }

        public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken)
        {
            Reports.Enqueue(appliedThrough);
            primary.ReportProgress(replicaIndex, appliedThrough);
            return Task.CompletedTask;
        }
    }

    /// <summary>A transport written before capabilities existed: it implements only the old stream call.</summary>
    private sealed class LegacyClient(ReplicationPrimary primary) : IReplicationSourceClient
    {
        public int LargestMessage;
        public int Fragments;

        public IAsyncEnumerable<SnapshotChunk> StreamInitialStateAsync(CancellationToken cancellationToken) => primary.StreamInitialStateAsync(cancellationToken);

        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(ulong startSeq, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(startSeq, cancellationToken))
            {
                WalStreamMessage d = m.Detach();
                LargestMessage = Math.Max(LargestMessage, d.Data.Length);
                if (d.IsFragment) Fragments++;
                yield return d;
            }
        }

        public Task ReportProgressAsync(int replicaIndex, ulong appliedThrough, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>Follows until the replica reaches <paramref name="target"/> (signalled), then stops the stream. A failure of the follower fails the test.</summary>
    private static async Task FollowUntilAsync(IReplicationSourceClient client, RocksDatabase replica, ReplicaFollowerOptions options, ulong target)
    {
        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, options, cts.Token);
        Task<bool> reached = replica.WaitForSequenceAsync(target, cts.Token).AsTask();
        Task first = await Task.WhenAny(follow, reached);
        if (first == follow) await follow;
        Assert.IsTrue(await reached, "replica closed");
        await cts.CancelAsync();
        try
        {
            await follow;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static void AssertSameDatabase(RocksDatabase source, RocksDatabase replica, string what)
    {
        AssertReplicaEquals(source, replica, what);
        Assert.AreEqual(DatabaseDigest.Compute(source), DatabaseDigest.Compute(replica), what + ": digest");
    }

    /// <summary>
    /// Checks the fragments of one batch as a follower saw them: contiguous from 0 to the total, none larger than the
    /// chunk size, the entry count on the last only, and the follower's log ending right before the batch throughout.
    /// </summary>
    private static void AssertFragmentsOfOneBatch(IReadOnlyList<Seen> fragments, int expectedEntries)
    {
        Assert.IsTrue(fragments.Count > 1, "the batch was sent in " + fragments.Count + " message(s)");
        int offset = 0;
        for (int i = 0; i < fragments.Count; i++)
        {
            Seen f = fragments[i];
            Assert.AreEqual(fragments[0].FirstSequence, f.FirstSequence);
            Assert.AreEqual(fragments[0].Total, f.Total);
            Assert.AreEqual(offset, f.Offset, "fragment " + i + " offset");
            Assert.IsTrue(f.Length > 0 && f.Length <= Chunk, "fragment " + i + " carries " + f.Length + " bytes");
            Assert.AreEqual(i == fragments.Count - 1 ? expectedEntries : 0, f.EntryCount, "entry count of fragment " + i);
            Assert.AreEqual(f.FirstSequence - 1, f.ReplicaSeq, "the follower's log moved while fragment " + i + " arrived");
            offset += f.Length;
        }
        Assert.AreEqual(fragments[0].Total, offset);
    }

    // ---------------------------------------------------------------------------------------------------
    // Part A: fragments on the WAL stream

    /// <summary>
    /// Batches of 200 KB and 100 KB between small ones, chunk size 16 KB: no message is larger than a chunk, the large
    /// batches arrive as fragments, the replica's log never ends inside one (nor is such a position reported), and the
    /// replica equals the source at the same sequence number. Failed before: the batch was one 200 KB message.
    /// </summary>
    [TestMethod]
    public async Task LargeBatchIsStreamedInFragmentsAndAppliedAtomically()
    {
        using var dir = new TempDir("frag-plain");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 60, 0);
        ulong big1 = source.GetLatestSequenceNumber() + 1;
        await WriteLargeAsync(source, "one", 5, 40_000);
        await WriteWorkloadAsync(source, 60, 120, 1);
        ulong big2 = source.GetLatestSequenceNumber() + 1;
        await WriteLargeAsync(source, "two", 1, 100_000);
        await WriteWorkloadAsync(source, 120, 150, 2);

        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null) { MaxChunkBytes = Chunk };
        var client = new DirectClient(primary, replica, carriesCapabilities: true);
        await FollowUntilAsync(client, replica, new ReplicaFollowerOptions(), source.GetLatestSequenceNumber());

        AssertSameDatabase(source, replica, "after the stream");
        Seen[] seen = [.. client.Messages];
        foreach (Seen m in seen) Assert.IsTrue(m.Length <= Chunk, "a message of " + m.Length + " bytes at sequence " + m.FirstSequence);
        AssertFragmentsOfOneBatch([.. seen.Where(m => m.Total != 0 && m.FirstSequence == big1)], 5);
        AssertFragmentsOfOneBatch([.. seen.Where(m => m.Total != 0 && m.FirstSequence == big2)], 1);
        Assert.AreEqual(2, seen.Where(m => m.Total != 0).Select(m => m.FirstSequence).Distinct().Count(), "only the two large batches are fragmented");
        Assert.IsTrue(seen.Any(m => m.Total == 0 && m.FirstSequence < big1) && seen.Any(m => m.Total == 0 && m.FirstSequence > big2), "small batches before and after");
        // Reports name applied batches only: never a position inside a large batch.
        foreach (ulong r in client.Reports)
        {
            Assert.IsFalse(r >= big1 && r < big1 + 4, "progress report " + r + " inside the batch at " + big1);
        }
        Assert.AreEqual(1, client.Streams);
    }

    /// <summary>
    /// With <see cref="WalTailer.StrictChunkLimit"/> a chunk is larger than the chunk size only when it is one single
    /// batch; without it (the stream for followers that take no fragments) a large batch is merged with what precedes it.
    /// </summary>
    [TestMethod]
    public async Task StrictChunkLimitKeepsALargeBatchOnItsOwn()
    {
        using var dir = new TempDir("frag-tailer");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 20, 0);
        await WriteLargeAsync(source, "t", 1, 50_000);
        await WriteWorkloadAsync(source, 20, 40, 0);

        using (WalTailer strict = new ReplicationSource(source).TailWal(1, Chunk))
        {
            strict.StrictChunkLimit = true;
            int large = 0;
            while (strict.TryReadChunk(out WalChunk c))
            {
                if (c.Length <= Chunk) continue;
                large++;
                Assert.AreEqual(1, c.BatchCount, "a chunk above the limit is one batch");
            }
            Assert.AreEqual(1, large);
            Assert.AreEqual(source.GetLatestSequenceNumber() + 1, strict.NextSequenceNumber);
        }
        using WalTailer loose = new ReplicationSource(source).TailWal(1, Chunk);
        Assert.IsTrue(loose.TryReadChunk(out WalChunk first));
        Assert.IsTrue(first.Length > Chunk && first.BatchCount > 1, "the default tailer merges the large batch into the chunk it does not fit in");
    }

    /// <summary>
    /// A pair with <see cref="PairWriteAck.Replica"/>: the write of a large batch is acknowledged (the replica reported
    /// it), the replica received it in fragments, and neither the replica's log nor the position it reported was ever
    /// inside the batch.
    /// </summary>
    [TestMethod]
    public async Task PairWithReplicaAckAppliesAFragmentedBatchAtomically()
    {
        await using var h = new PairHarness { WriteAck = PairWriteAck.Replica };
        var seen = new ConcurrentQueue<Seen>();
        var ackedInside = new ConcurrentQueue<string>();
        await h.StartAsync("A");
        await h.StartAsync("B");
        h.BtoA.OnStreamMessage = m =>
        {
            if (m.ResyncRequired) return;
            seen.Enqueue(new Seen(m.FirstSequence, m.FragmentOffset, m.TotalLength, m.Data.Length, m.EntryCount, h["B"].Db.GetLatestSequenceNumber()));
            ulong acked = h["A"].ReplicaAckedSequence;
            if (m.IsFragment && acked >= m.FirstSequence) ackedInside.Enqueue("acked " + acked + " while fragment " + m.FragmentOffset + " of " + m.FirstSequence + " arrived");
        };

        async Task<PairWriteResult> WriteAsync(WriteBatch b)
        {
            using (b) return await h["A"].WriteAsync(b, TimeSpan.FromSeconds(2), CancellationToken.None);
        }
        WriteBatch Small(int i)
        {
            var b = new WriteBatch();
            FillBatch(b, i, 0);
            return b;
        }
        for (int i = 0; i < 20; i++) Assert.AreEqual(PairWriteStatus.Applied, (await WriteAsync(Small(i))).Status);
        PairWriteResult big = await WriteAsync(LargeBatch("pair", 4, 50_000));
        Assert.AreEqual(PairWriteStatus.Applied, big.Status, "the replica reported the large batch\n" + h.LogText);
        Assert.AreEqual(big.Sequence, h["B"].Db.GetLatestSequenceNumber(), "acknowledged: the replica holds all of it");
        for (int i = 20; i < 40; i++) Assert.AreEqual(PairWriteStatus.Applied, (await WriteAsync(Small(i))).Status);

        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual(0, h["B"].Resyncs, h.LogText);
        ulong first = big.Sequence - 3;
        AssertFragmentsOfOneBatch([.. seen.Where(m => m.Total != 0 && m.FirstSequence == first)], 4);
        Assert.AreEqual(0, ackedInside.Count, string.Join("\n", ackedInside));
        foreach (Seen m in seen) Assert.IsTrue(m.Length <= Chunk);
    }

    /// <summary>
    /// A Raft cluster: a large batch written through the leader commits, the followers received it in fragments (with
    /// the leader's term on every one) and hold it under the same term at the same sequence numbers.
    /// </summary>
    [TestMethod]
    public async Task ClusterCommitsAFragmentedBatchWithItsTerm()
    {
        var seen = new ConcurrentQueue<(string Follower, WalStreamMessage Message)>();
        await using var c = new ClusterHarness(3, null)
        {
            WrapClient = (node, peer, inner) => new RecordingClusterClient(inner, m => seen.Enqueue((node, m))),
        };
        ClusterNodeHost n1 = await c.StartAsync("n1");
        for (int i = 0; i < 30; i++) Assert.IsTrue(await n1.TryWriteAsLeaderAsync(U(K(i)), U(V(i, 0)), CancellationToken.None));
        await c.StartAsync("n2");
        await c.StartAsync("n3");
        await WaitUntilAsync(() => c.Statuses().TrueForAll(s => s.Mode == ClusterMode.Cluster), "Cluster mode\n" + c.LogText);
        ClusterNodeHost leader = await c.WaitForLeaderAsync(null);

        using (WriteBatch big = LargeBatch("raft", 3, 60_000))
        {
            Assert.AreEqual(ClusterWriteStatus.Committed, await leader.WriteCommittedAsync(big, TimeSpan.FromSeconds(5), CancellationToken.None), c.LogText);
        }
        ulong last = leader.Db.GetLatestSequenceNumber(), first = last - 2;
        Assert.IsTrue(await leader.WriteCommittedAsync(U("after"), U("x"), TimeSpan.FromSeconds(5), CancellationToken.None));
        await c.AssertConvergedAsync(leader);

        long term = leader.Node.GetTermForSeq(first);
        Assert.IsTrue(term > 0);
        foreach (ClusterNodeHost h in c.Running)
        {
            Assert.AreEqual(0, h.Resyncs, c.LogText);
            for (ulong s = first; s <= last; s++) Assert.AreEqual(term, h.Node.GetTermForSeq(s), h.NodeId + " term of " + s);
        }
        foreach (string follower in new[] { "n2", "n3" })
        {
            WalStreamMessage[] fragments = [.. seen.Where(x => x.Follower == follower && x.Message.IsFragment && x.Message.FirstSequence == first).Select(x => x.Message)];
            Assert.IsTrue(fragments.Length > 1, follower + " received " + fragments.Length + " fragments");
            foreach (WalStreamMessage f in fragments)
            {
                Assert.AreEqual(leader.NodeId, f.SourceId);
                Assert.AreEqual(term, f.SourceTerm);
                Assert.AreEqual(term, f.EntryTerm);
                Assert.IsTrue(f.Data.Length <= Chunk);
            }
            Assert.AreEqual(3, fragments[^1].EntryCount);
            Assert.IsTrue(fragments[^1].IsLastFragment);
        }
    }

    private sealed class RecordingClusterClient(IClusterNodeClient inner, Action<WalStreamMessage> onMessage) : IClusterNodeClient
    {
        public string NodeId => inner.NodeId;
        public Task<RequestVoteResult?> RequestVoteAsync(RequestVotePayload request, CancellationToken cancellationToken) => inner.RequestVoteAsync(request, cancellationToken);
        public Task<AppendEntriesResult?> HeartbeatAsync(HeartbeatPayload payload, CancellationToken cancellationToken) => inner.HeartbeatAsync(payload, cancellationToken);
        public Task<ClusterNodeStatus?> GetStatusAsync(CancellationToken cancellationToken) => inner.GetStatusAsync(cancellationToken);
        public Task<ClusterAttachResult?> CheckLogConsistencyAsync(string followerId, ulong latestSeq, long termAtLatest, CancellationToken cancellationToken) =>
            inner.CheckLogConsistencyAsync(followerId, latestSeq, termAtLatest, cancellationToken);
        public IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, CancellationToken cancellationToken) =>
            StreamUpdatesAsync(followerId, startSeq, WalStreamCapabilities.None, cancellationToken);
        public async IAsyncEnumerable<WalStreamMessage> StreamUpdatesAsync(string followerId, ulong startSeq, WalStreamCapabilities capabilities,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (WalStreamMessage m in inner.StreamUpdatesAsync(followerId, startSeq, capabilities, cancellationToken))
            {
                onMessage(m);
                yield return m;
            }
        }
        public Task ReportProgressAsync(string followerId, ulong appliedThrough, long leaderTerm, CancellationToken cancellationToken) =>
            inner.ReportProgressAsync(followerId, appliedThrough, leaderTerm, cancellationToken);
        public Task<IReadOnlyDictionary<string, string>> GetFileHashesAsync(IReadOnlyList<ReplicationFileInfo> candidates, CancellationToken cancellationToken) =>
            inner.GetFileHashesAsync(candidates, cancellationToken);
        public IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyList<ReplicationFileInfo> verifiedInventory, CancellationToken cancellationToken) =>
            inner.StreamSnapshotAsync(verifiedInventory, cancellationToken);
        public Task<string?> GetTermsAsync(CancellationToken cancellationToken) => inner.GetTermsAsync(cancellationToken);
    }

    /// <summary>
    /// The connection drops after the third fragment of a batch. The replica discards what it had, attaches again at
    /// its last applied sequence number and receives the batch from offset 0; nothing of the first attempt is applied
    /// and no rebuild is needed.
    /// </summary>
    [TestMethod]
    public async Task ConnectionDroppedInsideABatchResumesFromTheLastAppliedSequence()
    {
        await using var h = new PairHarness();
        var offsets = new ConcurrentQueue<(int Offset, ulong ReplicaSeq)>();
        int dropped = 0;
        h.BtoA.RewriteStream = m =>
        {
            if (!m.IsFragment) return m;
            if (m.FragmentOffset == 2 * Chunk && Interlocked.Exchange(ref dropped, 1) == 0) throw new IOException("connection dropped");
            offsets.Enqueue((m.FragmentOffset, h["B"].Db.GetLatestSequenceNumber()));
            return m;
        };
        await h.StartAsync("A");
        await h.StartAsync("B");
        using (WriteBatch small = new WriteBatch().Put(U("before"), U("1")))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(small, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        ulong before = h["A"].Db.GetLatestSequenceNumber();
        using (WriteBatch big = LargeBatch("drop", 2, 60_000))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(big, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        using (WriteBatch small = new WriteBatch().Put(U("after"), U("2")))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(small, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);

        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual(1, dropped);
        Assert.AreEqual(0, h["B"].Resyncs, h.LogText);
        (int Offset, ulong ReplicaSeq)[] got = [.. offsets];
        // Two fragments of the first attempt, then the whole batch again from the start.
        Assert.AreEqual(0, got[0].Offset);
        Assert.AreEqual(Chunk, got[1].Offset);
        Assert.AreEqual(0, got[2].Offset, "the second attempt starts at offset 0");
        Assert.IsTrue(got.Length > 5);
        foreach ((int _, ulong replicaSeq) in got) Assert.AreEqual(before, replicaSeq, "the replica's log moved inside the batch");
        StringAssert.Contains(h.LogText, "connection dropped");
    }

    private static WalStreamMessage Frag(ulong seq, int offset, int total, int count, byte[] all, int length, string source, long term, long entryTerm) =>
        WalStreamMessage.Fragment(seq, offset, total, count, all.AsMemory(Math.Min(offset, all.Length), Math.Min(length, all.Length - Math.Min(offset, all.Length))), source, term, entryTerm);

    /// <summary>
    /// Every way a stream can break the fragment protocol is refused, the partial batch is discarded, and the same
    /// assembler then takes the batch from offset 0 and returns exactly its bytes.
    /// </summary>
    [TestMethod]
    public void EveryFragmentInconsistencyIsRefusedAndDiscardsThePartialBatch()
    {
        byte[] all;
        using (WriteBatch b = LargeBatch("asm", 3, 2000))
        {
            b.Sequence = 77;
            all = b.Data.ToArray();
        }
        int total = all.Length;
        const int Piece = 1024;
        Assert.IsTrue(total > 3 * Piece);
        WalStreamMessage Good(int offset) => Frag(77, offset, total, offset + Piece >= total ? 3 : 0, all, Piece, "A", 4, 2);
        byte[] other = new byte[40];

        (string Name, bool AfterFirst, WalStreamMessage Bad, string Expect)[] cases =
        [
            ("unexpected offset (a gap)", true, Good(2 * Piece), "does not continue it"),
            ("unexpected offset (a repeat)", true, Good(0), "does not continue it"),
            ("different first sequence", true, Frag(78, Piece, total, 0, all, Piece, "A", 4, 2), "was incomplete"),
            ("different source id", true, Frag(77, Piece, total, 0, all, Piece, "B", 4, 2), "was incomplete"),
            ("different source term", true, Frag(77, Piece, total, 0, all, Piece, "A", 5, 2), "was incomplete"),
            ("different entry term", true, Frag(77, Piece, total, 0, all, Piece, "A", 4, 3), "was incomplete"),
            ("total length changing", true, Frag(77, Piece, total + 1, 0, all, Piece, "A", 4, 2), "was incomplete"),
            ("an ordinary message while a batch is incomplete", true, new WalStreamMessage(77, 1, other, "A", 4, 2), "unfragmented chunk"),
            ("an entry count before the end", true, Frag(77, Piece, total, 3, all, Piece, "A", 4, 2), "entry count"),
            ("an empty fragment", true, Frag(77, Piece, total, 0, all, 0, "A", 4, 2), "does not continue it"),
            ("a fragment with no batch in progress and offset != 0", false, Good(Piece), "without the beginning"),
            ("offset + length > total", false, WalStreamMessage.Fragment(77, 0, 100, 0, all.AsMemory(0, 200), "A", 4, 2), "carries 200 bytes of 100"),
            ("a total length that cannot hold a batch", false, WalStreamMessage.Fragment(77, 0, 12, 1, all.AsMemory(0, 12), "A", 4, 2), "total length of 12"),
            ("a negative total length", false, WalStreamMessage.Fragment(77, 0, -5, 1, all.AsMemory(0, 12), "A", 4, 2), "total length of -5"),
        ];
        using var assembler = new WalStreamAssembler(1 << 20);
        foreach ((string name, bool afterFirst, WalStreamMessage bad, string expect) in cases)
        {
            if (afterFirst)
            {
                Assert.AreEqual(WalAssemblyStatus.Incomplete, assembler.Accept(Good(0), out _), name);
                Assert.IsTrue(assembler.HasPartialBatch);
            }
            Assert.AreEqual(WalAssemblyStatus.Invalid, assembler.Accept(bad, out ReadOnlyMemory<byte> none), name);
            Assert.IsTrue(none.IsEmpty, name);
            Assert.IsFalse(assembler.HasPartialBatch, name + ": the partial batch is discarded");
            StringAssert.Contains(assembler.LastError, expect, name);

            // What a re-attached stream sends: the batch from offset 0.
            WalAssemblyStatus status = WalAssemblyStatus.Incomplete;
            ReadOnlyMemory<byte> chunk = default;
            for (int offset = 0; offset < total; offset += Piece)
            {
                Assert.AreEqual(WalAssemblyStatus.Incomplete, status, name);
                status = assembler.Accept(Good(offset), out chunk);
            }
            Assert.AreEqual(WalAssemblyStatus.Complete, status, name);
            Assert.IsTrue(chunk.Span.SequenceEqual(all), name + ": the assembled bytes");
            Assert.IsFalse(assembler.HasPartialBatch);
        }

        // The last fragment's entry count, and the announced first sequence, must be the assembled batch's own.
        Assert.AreEqual(WalAssemblyStatus.Incomplete, assembler.Accept(Frag(77, 0, total, 0, all, total - 10, "A", 4, 2), out _));
        Assert.AreEqual(WalAssemblyStatus.Invalid, assembler.Accept(Frag(77, total - 10, total, 4, all, 10, "A", 4, 2), out _));
        StringAssert.Contains(assembler.LastError, "its fragments announced");
        Assert.AreEqual(WalAssemblyStatus.Incomplete, assembler.Accept(Frag(76, 0, total, 0, all, total - 10, "A", 4, 2), out _));
        Assert.AreEqual(WalAssemblyStatus.Invalid, assembler.Accept(Frag(76, total - 10, total, 3, all, 10, "A", 4, 2), out _));

        // An ordinary message passes through as it is (no copy), also right after a completed batch.
        Assert.AreEqual(WalAssemblyStatus.Complete, assembler.Accept(new WalStreamMessage(5, 1, other, "A", 4, 2), out ReadOnlyMemory<byte> same));
        Assert.IsTrue(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(same, out ArraySegment<byte> segment) && ReferenceEquals(segment.Array, other));
        // A batch in one fragment is a batch.
        Assert.AreEqual(WalAssemblyStatus.Complete, assembler.Accept(Frag(77, 0, total, 3, all, total, "A", 4, 2), out ReadOnlyMemory<byte> whole));
        Assert.IsTrue(whole.Span.SequenceEqual(all));
    }

    /// <summary>
    /// A replica follows a source that sends the second fragment of a batch at the wrong offset. Once (a fault of the
    /// connection): the replica attaches again and receives the batch; no rebuild. Every time (the source is wrong):
    /// the second failure at the same position rebuilds the replica from a snapshot, which contains the batch - it does
    /// not attach again and again.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task FragmentInconsistencyReattachesOnceThenRebuilds(bool always)
    {
        await using var h = new PairHarness();
        int corrupted = 0;
        h.BtoA.RewriteStream = m =>
        {
            if (!m.IsFragment || m.FragmentOffset != Chunk) return m;
            if (!always && Volatile.Read(ref corrupted) > 0) return m;
            Interlocked.Increment(ref corrupted);
            return WalStreamMessage.Fragment(m.FirstSequence, m.FragmentOffset + 1, m.TotalLength, m.EntryCount, m.Data, m.SourceId, m.SourceTerm, m.EntryTerm);
        };
        await h.StartAsync("A");
        await h.StartAsync("B");
        using (WriteBatch small = new WriteBatch().Put(U("before"), U("1")))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(small, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B to follow");
        using (WriteBatch big = LargeBatch("bad", 2, 60_000))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(big, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);

        if (always) await WaitUntilAsync(() => h["B"].Resyncs == 1, "B to rebuild\n" + h.LogText);
        await h.AssertConvergedAsync("A", "B");
        StringAssert.Contains(h.LogText, "B: dropping the stream from A");
        StringAssert.Contains(h.LogText, "does not continue it at offset " + Chunk);
        if (always)
        {
            Assert.AreEqual(2, corrupted, "one re-attachment, then the rebuild\n" + h.LogText);
            Assert.AreEqual(1, h["B"].Resyncs, h.LogText);
            StringAssert.Contains(h.LogText, "B: resync: A's stream cannot be followed from");
        }
        else
        {
            Assert.AreEqual(1, corrupted);
            Assert.AreEqual(0, h["B"].Resyncs, h.LogText);
        }
    }

    /// <summary>
    /// A batch announced larger than <see cref="PairNodeHostOptions.MaxBatchBytes"/> is refused before anything is
    /// allocated for it, with an error that names the option; the replica rebuilds from a snapshot (the batch arrives as
    /// files) and follows on. The plain follower ends with <see cref="ReplicationResyncRequiredException"/>; an
    /// unfragmented message above the limit is refused the same way.
    /// </summary>
    [TestMethod]
    public async Task BatchLargerThanMaxBatchBytesIsRefusedAndTheReplicaRebuilds()
    {
        await using (var h = new PairHarness { MaxBatchBytes = 64 * 1024 })
        {
            int fragmentsOfTheBatch = 0;
            h.BtoA.OnStreamMessage = m =>
            {
                if (m.IsFragment) Interlocked.Increment(ref fragmentsOfTheBatch);
            };
            await h.StartAsync("A");
            await h.StartAsync("B");
            using (WriteBatch small = new WriteBatch().Put(U("before"), U("1")))
                Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(small, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
            await WaitUntilAsync(() => h["B"].Db.GetLatestSequenceNumber() == h["A"].Db.GetLatestSequenceNumber(), "B to follow");
            using (WriteBatch big = LargeBatch("cap", 2, 60_000))
                Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(big, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
            await WaitUntilAsync(() => h["B"].Resyncs == 1, "B to rebuild\n" + h.LogText);
            await h.AssertConvergedAsync("A", "B");
            Assert.AreEqual(1, h["B"].Resyncs, h.LogText);
            StringAssert.Contains(h.LogText, "more than this node accepts from a WAL stream (MaxBatchBytes = 65536)");
            // Refused at the first fragment; the replica did not wait for the rest.
            Assert.IsTrue(fragmentsOfTheBatch < 8, "fragments read of the refused batch: " + fragmentsOfTheBatch);
        }

        using var dir = new TempDir("frag-cap");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 10, 0);
        ulong before = source.GetLatestSequenceNumber();
        await WriteLargeAsync(source, "cap", 1, 100_000);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null) { MaxChunkBytes = Chunk };
        foreach (bool fragments in new[] { true, false })
        {
            using var cts = new CancellationTokenSource(TestUtil.Timeout);
            ReplicationResyncRequiredException e = await Assert.ThrowsExactlyAsync<ReplicationResyncRequiredException>(() => ReplicaFollower.FollowAsync(
                new DirectClient(primary, replica, carriesCapabilities: true), replica, 0,
                new ReplicaFollowerOptions { MaxBatchBytes = 64 * 1024, AcceptFragmentedBatches = fragments }, cts.Token));
            StringAssert.Contains(e.Message, "MaxBatchBytes = 65536");
            Assert.AreEqual(before, replica.GetLatestSequenceNumber(), "nothing of the refused batch was applied");
        }

        using var assembler = new WalStreamAssembler(64 * 1024);
        Assert.AreEqual(WalAssemblyStatus.TooLarge, assembler.Accept(WalStreamMessage.Fragment(9, 0, 64 * 1024 + 1, 0, new byte[100], "A", 1, 1), out _));
        Assert.IsFalse(assembler.HasPartialBatch);
        Assert.AreEqual(WalAssemblyStatus.Incomplete, assembler.Accept(WalStreamMessage.Fragment(9, 0, 64 * 1024, 0, new byte[100], "A", 1, 1), out _), "the limit itself is accepted");
    }

    /// <summary>
    /// The plain follower on a stream that breaks the protocol: <see cref="WalStreamProtocolException"/> with the
    /// replica's position, nothing applied; <see cref="FollowerResyncPolicy.ShouldRebuildAfterStreamFault"/> tells the
    /// first fault at a position (follow again) from a repeated one (rebuild).
    /// </summary>
    [TestMethod]
    public async Task PlainFollowerReportsAProtocolViolationWithItsPosition()
    {
        using var dir = new TempDir("frag-violation");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 10, 0);
        ulong before = source.GetLatestSequenceNumber();
        await WriteLargeAsync(source, "v", 1, 100_000);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null) { MaxChunkBytes = Chunk };
        var client = new DirectClient(primary, replica, carriesCapabilities: true)
        {
            Rewrite = m => m.IsFragment && m.FragmentOffset == 3 * Chunk
                ? WalStreamMessage.Fragment(m.FirstSequence, m.FragmentOffset, m.TotalLength - 1, m.EntryCount, m.Data, m.SourceId, m.SourceTerm, m.EntryTerm)
                : m,
        };
        var policy = new FollowerResyncPolicy();
        for (int attempt = 0; attempt < 2; attempt++)
        {
            using var cts = new CancellationTokenSource(TestUtil.Timeout);
            WalStreamProtocolException e = await Assert.ThrowsExactlyAsync<WalStreamProtocolException>(() =>
                ReplicaFollower.FollowAsync(client, replica, 0, new ReplicaFollowerOptions(), cts.Token));
            Assert.AreEqual(before, e.AppliedThrough);
            Assert.AreEqual(before, replica.GetLatestSequenceNumber());
            Assert.AreEqual(attempt == 1, policy.ShouldRebuildAfterStreamFault(e.AppliedThrough), "attempt " + attempt);
        }
        Assert.IsFalse(policy.ShouldRebuildAfterStreamFault(before + 1), "a fault at a new position is a first fault");
    }

    /// <summary>
    /// Compatibility of the stream. A follower that announces nothing - one built before fragments existed, a host with
    /// <see cref="PairNodeHostOptions.AcceptFragmentedBatches"/> off, a transport that implements only the old stream
    /// call - is sent the large batch as one message, as before (it would apply a fragment as a whole batch). A new
    /// follower on such a stream (an old sender) follows as before.
    /// </summary>
    [TestMethod]
    public async Task AFollowerThatDidNotAnnounceFragmentsGetsOneLargeMessage()
    {
        using var dir = new TempDir("frag-compat");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 30, 0);
        await WriteLargeAsync(source, "old", 3, 50_000);
        await WriteWorkloadAsync(source, 30, 60, 0);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null) { MaxChunkBytes = Chunk };

        // New sender, old replica: the stream call without capabilities.
        using (var cts = new CancellationTokenSource(TestUtil.Timeout))
        {
            int largest = 0;
            ulong next = 1;
            await foreach (WalStreamMessage m in primary.StreamUpdatesAsync(1, cts.Token))
            {
                Assert.IsFalse(m.IsFragment, "a fragment was sent to a follower that did not ask for fragments");
                Assert.AreEqual(next, m.FirstSequence);
                next += (ulong)m.EntryCount;
                largest = Math.Max(largest, m.Data.Length);
                if (next > source.GetLatestSequenceNumber()) break;
            }
            Assert.IsTrue(largest > 150_000, "the large batch is one message of " + largest + " bytes");
        }

        // New replica, old sender / old transport: no fragments arrive, the replica follows.
        var legacy = new LegacyClient(primary);
        await FollowUntilAsync(legacy, replica, new ReplicaFollowerOptions(), source.GetLatestSequenceNumber());
        AssertSameDatabase(source, replica, "over a transport without capabilities");
        Assert.AreEqual(0, legacy.Fragments);
        Assert.IsTrue(legacy.LargestMessage > 150_000);

        // A host that does not announce fragments.
        await using var h = new PairHarness { AcceptFragmentedBatches = false };
        int fragments = 0, largestToB = 0;
        h.BtoA.OnStreamMessage = m =>
        {
            if (m.IsFragment) Interlocked.Increment(ref fragments);
            if (!m.ResyncRequired) largestToB = Math.Max(largestToB, m.Data.Length);
        };
        await h.StartAsync("A");
        await h.StartAsync("B");
        using (WriteBatch big = LargeBatch("host", 2, 60_000))
            Assert.AreEqual(PairWriteStatus.Applied, (await h["A"].WriteAsync(big, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
        await h.AssertConvergedAsync("A", "B");
        Assert.AreEqual(0, fragments);
        Assert.IsTrue(largestToB > 100_000);
    }

    /// <summary>The <see cref="WalChunkDto"/> of the version before fragments (keys 0..8).</summary>
    [MessagePackObject]
    public sealed class WalChunkDtoBeforeFragments
    {
        [Key(0)] public ulong FirstSequence { get; set; }
        [Key(1)] public int EntryCount { get; set; }
        [Key(2)] public ReadOnlyMemory<byte> Data { get; set; }
        [Key(3)] public string SourceId { get; set; } = "";
        [Key(4)] public long SourceTerm { get; set; }
        [Key(5)] public long EntryTerm { get; set; }
        [Key(6)] public bool ResyncRequired { get; set; }
        [Key(7)] public string? Reason { get; set; }
        [Key(8)] public bool SourceFault { get; set; }
    }

    /// <summary>
    /// Wire compatibility of <see cref="WalChunkDto"/> in both directions: a message of the previous version reads as
    /// "not a fragment"; the previous version reads a message of this version (the two appended fields are skipped) -
    /// for an ordinary message with every field it knows unchanged. The capability header survives the round trip,
    /// and an absent, unreadable or unknown one announces nothing.
    /// </summary>
    [TestMethod]
    public void WalChunkDtoAndCapabilityHeaderAreCompatibleWithThePreviousVersion()
    {
        byte[] data = [1, 2, 3, 4, 5];
        var old = new WalChunkDtoBeforeFragments { FirstSequence = 42, EntryCount = 3, Data = data, SourceId = "A", SourceTerm = 7, EntryTerm = 6 };
        WalChunkDto read = MessagePackSerializer.Deserialize<WalChunkDto>(MessagePackSerializer.Serialize(old));
        Assert.AreEqual(0, read.TotalLength);
        Assert.AreEqual(0, read.FragmentOffset);
        WalStreamMessage m = read.ToMessage();
        Assert.IsFalse(m.IsFragment);
        Assert.AreEqual(42UL, m.FirstSequence);
        Assert.AreEqual(3, m.EntryCount);
        Assert.IsTrue(m.Data.Span.SequenceEqual(data));

        var plain = new WalStreamMessage(42, 3, data, "A", 7, 6);
        WalChunkDtoBeforeFragments byOld = MessagePackSerializer.Deserialize<WalChunkDtoBeforeFragments>(MessagePackSerializer.Serialize(WalChunkDto.From(plain)));
        Assert.AreEqual(42UL, byOld.FirstSequence);
        Assert.AreEqual(3, byOld.EntryCount);
        Assert.IsTrue(byOld.Data.Span.SequenceEqual(data));
        Assert.AreEqual("A", byOld.SourceId);
        Assert.AreEqual(7, byOld.SourceTerm);
        Assert.AreEqual(6, byOld.EntryTerm);
        Assert.IsFalse(byOld.ResyncRequired);

        WalStreamMessage resync = MessagePackSerializer.Deserialize<WalChunkDto>(MessagePackSerializer.Serialize(
            new WalChunkDtoBeforeFragments { ResyncRequired = true, Reason = "gone", SourceFault = true, SourceId = "A" })).ToMessage();
        Assert.IsTrue(resync.ResyncRequired && resync.SourceFault && !resync.IsFragment);

        WalStreamMessage fragment = WalStreamMessage.Fragment(42, 16, 100, 0, data, "A", 7, 6);
        WalStreamMessage back = MessagePackSerializer.Deserialize<WalChunkDto>(MessagePackSerializer.Serialize(WalChunkDto.From(fragment))).ToMessage();
        Assert.IsTrue(back.IsFragment && !back.IsLastFragment);
        Assert.AreEqual(16, back.FragmentOffset);
        Assert.AreEqual(100, back.TotalLength);
        Assert.AreEqual(0, back.EntryCount);
        Assert.IsTrue(back.Detach().IsFragment, "a copied message stays a fragment");
        // What an old replica would make of a fragment: a whole batch - which is why it is never sent one.
        Assert.AreEqual(5, MessagePackSerializer.Deserialize<WalChunkDtoBeforeFragments>(MessagePackSerializer.Serialize(WalChunkDto.From(fragment))).Data.Length);

        Assert.IsNull(WalStreamCapabilityHeader.Create(WalStreamCapabilities.None), "an old follower's call carries no header");
        Assert.AreEqual(WalStreamCapabilities.Fragments, WalStreamCapabilityHeader.Read(WalStreamCapabilityHeader.Create(WalStreamCapabilities.Fragments)));
        Assert.AreEqual(WalStreamCapabilities.None, WalStreamCapabilityHeader.Read(null));
        Assert.AreEqual(WalStreamCapabilities.None, WalStreamCapabilityHeader.Read([]));
        Assert.AreEqual(WalStreamCapabilities.None, WalStreamCapabilityHeader.Read(new Grpc.Core.Metadata { { WalStreamCapabilityHeader.Name, "yes" } }));
        Assert.AreEqual(WalStreamCapabilities.Fragments, WalStreamCapabilityHeader.Read(new Grpc.Core.Metadata { { WalStreamCapabilityHeader.Name, "255" } }),
            "flags of a later version are dropped");
    }

    private static Grpc.Net.Client.GrpcChannelOptions DefaultReceiveLimit() =>
        // One byte below gRPC's default of 4 MiB: GrpcTransportDefaults.Apply lifts the limit only while it is exactly the default.
        new() { MaxReceiveMessageSize = 4 * 1024 * 1024 - 1 };

    /// <summary>
    /// Real gRPC, with the receive limit of gRPC's default forced back onto the replica's channel: a 6 MB batch reaches
    /// the replica (in fragments of the default 256 KiB). The same replica without the announcement cannot receive it -
    /// the situation before the limit was lifted and before fragments.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task BatchAboveFourMegabytesPassesAChannelWithTheDefaultReceiveLimit(bool acceptFragments)
    {
        using var dir = new TempDir("grpc-frag");
        int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
        using var toB = new GrpcPairClient("B", "http://127.0.0.1:" + portB, TimeSpan.FromSeconds(2), DefaultReceiveLimit());
        using var toA = new GrpcPairClient("A", "http://127.0.0.1:" + portA, TimeSpan.FromSeconds(2), DefaultReceiveLimit());
        var log = new ConcurrentQueue<string>();
        PairNodeHost Host(string id, IPairNodeClient peer) => new(new PairNodeHostOptions
        {
            NodeId = id,
            PeerId = id == "A" ? "B" : "A",
            DbPath = dir.Sub(id),
            InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
            DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
            RetryDelay = TimeSpan.FromMilliseconds(50),
            AcceptFragmentedBatches = acceptFragments,
            Log = log.Enqueue,
        }, peer);
        PairNodeHost a = Host("A", toB), b = Host("B", toA);
        await using GrpcNodeServer serverA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(a, System.Net.IPAddress.Loopback, portA, CancellationToken.None);
        await using GrpcNodeServer serverB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(b, System.Net.IPAddress.Loopback, portB, CancellationToken.None);
        try
        {
            await a.StartAsync(CancellationToken.None);
            await b.StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => b.GetStatus().AttachedFromSeq > 0, "B following");
            ulong before = a.Db.GetLatestSequenceNumber();
            using (WriteBatch batch = LargeBatch("grpc", 3, 2 * 1024 * 1024))
                Assert.AreEqual(PairWriteStatus.Applied, (await a.WriteAsync(batch, TimeSpan.FromSeconds(2), CancellationToken.None)).Status);
            if (acceptFragments)
            {
                await WaitUntilAsync(() => b.Db.GetLatestSequenceNumber() == a.Db.GetLatestSequenceNumber(), "B to receive a 6 MB batch\n" + string.Join("\n", log));
                Assert.AreEqual(a.Checksum(), b.Checksum());
                Assert.AreEqual(0, b.Resyncs);
            }
            else
            {
                await WaitUntilAsync(() => log.Count(l => l.Contains("B: stream from A failed", StringComparison.Ordinal)) >= 2, "B to fail on the message twice");
                Assert.AreEqual(before, b.Db.GetLatestSequenceNumber(), "one 6 MB message does not pass a 4 MB limit");
                StringAssert.Contains(string.Join("\n", log), "ResourceExhausted");
            }
        }
        finally
        {
            // The hosts first: a server waits for its open streams when it stops.
            await b.DisposeAsync();
            await a.DisposeAsync();
        }
    }

    /// <summary>
    /// Plain replication over real gRPC with the default receive limit on the replica's channel: the capability header
    /// reaches <see cref="ReplicationService"/> and a 5 MB batch arrives in fragments.
    /// </summary>
    [TestMethod]
    public async Task PlainReplicationOverGrpcFragmentsABatchAboveTheReceiveLimit()
    {
        using var dir = new TempDir("grpc-frag-repl");
        (RocksDatabase source, RocksDatabase replica) = await OpenPlainAsync(dir);
        await using RocksDatabase _s = source, _r = replica;
        await WriteWorkloadAsync(source, 0, 50, 0);
        await WriteLargeAsync(source, "g", 1, 5 * 1024 * 1024);
        await WriteWorkloadAsync(source, 50, 100, 0);
        var primary = new ReplicationPrimary(source, dir.Sub("checkpoints"), null);
        await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<ReplicationService, ReplicationPrimary>(primary, CancellationToken.None);
        using var client = new GrpcReplicationClient(server.Endpoint, TimeSpan.FromSeconds(5), DefaultReceiveLimit());

        using var cts = new CancellationTokenSource(TestUtil.Timeout);
        Task<long> follow = ReplicaFollower.FollowAsync(client, replica, 0, cts.Token);
        Task<bool> reached = replica.WaitForSequenceAsync(source.GetLatestSequenceNumber(), cts.Token).AsTask();
        if (await Task.WhenAny(follow, reached) == follow) await follow;
        Assert.IsTrue(await reached);
        AssertSameDatabase(source, replica, "over gRPC");
        await cts.CancelAsync();
        try
        {
            await follow;
        }
        catch (OperationCanceledException)
        {
        }
        catch (Grpc.Core.RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Cancelled)
        {
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Part B: large client writes

    /// <summary>A pair over real gRPC: A primary, B replica, each behind a <see cref="GrpcNodeServer"/> with <paramref name="serverOptions"/>.</summary>
    private sealed class GrpcPair : IAsyncDisposable
    {
        private readonly TempDir _dir = new("grpc-write");
        public readonly ConcurrentQueue<string> Log = new();
        public GrpcPairClient ToA = null!, ToB = null!;
        public PairNodeHost A = null!, B = null!;
        public GrpcNodeServer ServerA = null!, ServerB = null!;

        public static async Task<GrpcPair> StartAsync(GrpcNodeServerOptions serverOptions)
        {
            var p = new GrpcPair();
            int portA = GrpcNodeServer.FindFreePort(), portB = GrpcNodeServer.FindFreePort();
            // The replica's channel keeps gRPC's default receive limit: the large write must also replicate in pieces.
            p.ToB = new GrpcPairClient("B", "http://127.0.0.1:" + portB, TimeSpan.FromSeconds(2), DefaultReceiveLimit());
            p.ToA = new GrpcPairClient("A", "http://127.0.0.1:" + portA, TimeSpan.FromSeconds(2), DefaultReceiveLimit());
            PairNodeHost Host(string id, IPairNodeClient peer) => new(new PairNodeHostOptions
            {
                NodeId = id,
                PeerId = id == "A" ? "B" : "A",
                DbPath = p._dir.Sub(id),
                InitialRole = id == "A" ? ReplicationRole.Primary : ReplicationRole.Replica,
                DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
                RetryDelay = TimeSpan.FromMilliseconds(50),
                Log = p.Log.Enqueue,
            }, peer);
            p.A = Host("A", p.ToB);
            p.B = Host("B", p.ToA);
            p.ServerA = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(p.A, System.Net.IPAddress.Loopback, portA, serverOptions, CancellationToken.None);
            p.ServerB = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(p.B, System.Net.IPAddress.Loopback, portB, serverOptions, CancellationToken.None);
            await p.A.StartAsync(CancellationToken.None);
            await p.B.StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => p.B.GetStatus().AttachedFromSeq > 0, "B following");
            return p;
        }

        public string? Read(PairNodeHost host, string key)
        {
            using PooledValue? v = host.Db.GetPooled(U(key));
            return v is null ? null : System.Text.Encoding.UTF8.GetString(v.Span);
        }

        public async ValueTask DisposeAsync()
        {
            // The hosts first: a server waits for its open streams when it stops.
            await B.DisposeAsync();
            await A.DisposeAsync();
            await ServerB.DisposeAsync();
            await ServerA.DisposeAsync();
            ToA.Dispose();
            ToB.Dispose();
            _dir.Dispose();
        }
    }

    private static string Letters(int length, int seed)
    {
        var rnd = new Random(seed);
        return string.Create(length, rnd, static (span, r) =>
        {
            for (int i = 0; i < span.Length; i++) span[i] = (char)('a' + r.Next(26));
        });
    }

    private static void AssertSameResult(PairWriteResultDto expected, PairWriteResultDto actual, ulong sequenceDelta)
    {
        Assert.AreEqual(expected.Applied, actual.Applied);
        Assert.AreEqual(expected.Status, actual.Status);
        Assert.AreEqual(expected.PrimaryHint, actual.PrimaryHint);
        Assert.AreEqual(expected.Sequence + sequenceDelta, actual.Sequence);
    }

    /// <summary>
    /// What a client write above the server's receive limit did before: the unary RPC is refused by the server with
    /// ResourceExhausted ("Received message exceeds the maximum configured message size.") and nothing is applied. Now:
    /// <see cref="GrpcPairClient.WriteAsync(string, string, CancellationToken)"/> uploads the 6 MB write in pieces to a
    /// server with the default 4 MiB limit; it is applied as one write, readable, and replicated (to a replica whose
    /// channel has the default limit too). A small write still is the unary RPC, and both RPCs give the same result.
    /// </summary>
    [TestMethod]
    public async Task ClientWriteAboveFourMegabytesIsUploadedInFragmentsAndReplicated()
    {
        await using GrpcPair p = await GrpcPair.StartAsync(new GrpcNodeServerOptions());
        string large = Letters(6 * 1024 * 1024, 1);

        // Before (and still, for a client that uses the unary RPC): refused by the server, nothing applied.
        ulong before = p.A.Db.GetLatestSequenceNumber();
        Grpc.Core.RpcException refused = await Assert.ThrowsExactlyAsync<Grpc.Core.RpcException>(async () => await p.ToA.Service.WriteAsync("large", large));
        Assert.AreEqual(Grpc.Core.StatusCode.ResourceExhausted, refused.StatusCode);
        StringAssert.Contains(refused.Status.Detail, "Received message exceeds the maximum configured message size");
        Assert.AreEqual(before, p.A.Db.GetLatestSequenceNumber());

        PairWriteResultDto r = await p.ToA.WriteAsync("large", large, CancellationToken.None);
        Assert.IsTrue(r.Applied);
        Assert.AreEqual(PairWriteStatus.Applied, r.ToStatus());
        Assert.AreEqual(before + 1, r.Sequence, "one write, one sequence number");
        Assert.AreEqual(before + 1, p.A.Db.GetLatestSequenceNumber());
        Assert.AreEqual(large, p.Read(p.A, "large"));
        Assert.AreEqual(1, p.ServerA.FragmentedWrites.UploadsStarted);
        Assert.AreEqual(1, p.ServerA.FragmentedWrites.UploadsCompleted);
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.PendingBytes, "the buffer was given back");

        await WaitUntilAsync(() => p.B.Db.GetLatestSequenceNumber() == p.A.Db.GetLatestSequenceNumber(), "B to receive the write\n" + string.Join("\n", p.Log));
        Assert.AreEqual(large, p.Read(p.B, "large"));
        Assert.AreEqual(p.A.Checksum(), p.B.Checksum());
        Assert.AreEqual(0, p.B.Resyncs);

        // A small write is the unary RPC: no upload is started.
        PairWriteResultDto small = await p.ToA.WriteAsync("small", "v", CancellationToken.None);
        Assert.IsTrue(small.Applied);
        Assert.AreEqual(1, p.ServerA.FragmentedWrites.UploadsStarted, "a small write does not use the fragment RPC");
        // The same write through both RPCs: the same result (the next sequence number).
        PairWriteResultDto unary = await p.ToA.Service.WriteAsync("same", "value-0123456789");
        PairWriteResultDto fragmented = await p.ToA.WriteAsync("same", "value-0123456789", new GrpcWriteOptions { MaxChunkBytes = 8 }, CancellationToken.None);
        Assert.AreEqual(2, p.ServerA.FragmentedWrites.UploadsStarted);
        AssertSameResult(unary, fragmented, 1);
        Assert.AreEqual("value-0123456789", p.Read(p.A, "same"));
    }

    /// <summary>
    /// A large write sent to the node that is not the primary is refused exactly as the unary one: not applied, with
    /// the primary's id as hint.
    /// </summary>
    [TestMethod]
    public async Task FragmentedWriteToANonPrimaryIsRefusedLikeTheUnaryOne()
    {
        await using GrpcPair p = await GrpcPair.StartAsync(new GrpcNodeServerOptions());
        ulong before = p.B.Db.GetLatestSequenceNumber();
        PairWriteResultDto unary = await p.ToB.Service.WriteAsync("k", "v");
        PairWriteResultDto fragmented = await p.ToB.WriteAsync("k", Letters(600_000, 2), CancellationToken.None);
        Assert.IsFalse(fragmented.Applied);
        Assert.AreEqual(PairWriteStatus.NotPrimary, fragmented.ToStatus());
        Assert.AreEqual("A", fragmented.PrimaryHint);
        AssertSameResult(unary, fragmented, 0);
        Assert.AreEqual(before, p.B.Db.GetLatestSequenceNumber());
        Assert.IsNull(p.Read(p.B, "k"));
        Assert.AreEqual(1, p.ServerB.FragmentedWrites.UploadsCompleted);
        Assert.AreEqual(0, p.ServerB.FragmentedWrites.PendingBytes);
    }

    /// <summary>
    /// The node's limits. A write above <see cref="FragmentedWriteOptions.MaxWriteBytes"/> is refused at its first
    /// piece, before anything is allocated, with an error that names the option. While the node holds
    /// <see cref="FragmentedWriteOptions.MaxPendingBytes"/> of unfinished uploads, another one is refused as busy - and
    /// accepted once the first is done. Nothing of a refused write is applied and no buffer stays behind.
    /// </summary>
    [TestMethod]
    public async Task FragmentedWriteAboveTheNodesLimitsIsRefusedBeforeAnythingIsAllocated()
    {
        await using GrpcPair p = await GrpcPair.StartAsync(new GrpcNodeServerOptions
        {
            FragmentedWrites = new FragmentedWriteOptions { MaxWriteBytes = 1024 * 1024, MaxPendingBytes = 1024 * 1024 },
        });
        ulong before = p.A.Db.GetLatestSequenceNumber();
        LargeWriteException tooLarge = await Assert.ThrowsExactlyAsync<LargeWriteException>(() => p.ToA.WriteAsync("big", Letters(2 * 1024 * 1024, 3), CancellationToken.None));
        Assert.AreEqual(LargeWriteError.TooLarge, tooLarge.Error);
        StringAssert.Contains(tooLarge.Message, "more than this node accepts (MaxWriteBytes = 1048576)");
        Assert.AreEqual(Grpc.Core.StatusCode.ResourceExhausted, ((Grpc.Core.RpcException)tooLarge.InnerException!).StatusCode);
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.UploadsStarted, "refused before a buffer was rented");
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.PendingBytes);
        Assert.AreEqual(before, p.A.Db.GetLatestSequenceNumber());
        Assert.IsNull(p.Read(p.A, "big"));

        // An upload of 600 KB that stays open after its first piece.
        byte[] payload = U("held" + Letters(600_000, 4));
        using (global::MagicOnion.ClientStreamingResult<WriteFragmentDto, PairWriteResultDto> held = await p.ToA.Service.WriteFragmentsAsync())
        {
            await held.RequestStream.WriteAsync(new WriteFragmentDto { TotalLength = payload.Length, KeyLength = 4, Offset = 0, Data = payload.AsMemory(0, 100_000) });
            await WaitUntilAsync(() => p.ServerA.FragmentedWrites.PendingBytes == payload.Length, "the node to hold the first upload");

            LargeWriteException busy = await Assert.ThrowsExactlyAsync<LargeWriteException>(() => p.ToA.WriteAsync("second", Letters(600_000, 5), CancellationToken.None));
            Assert.AreEqual(LargeWriteError.ServerBusy, busy.Error);
            StringAssert.Contains(busy.Message, "MaxPendingBytes = 1048576");
            Assert.AreEqual(payload.Length, p.ServerA.FragmentedWrites.PendingBytes, "the refused upload reserved nothing");
            Assert.AreEqual(before, p.A.Db.GetLatestSequenceNumber());

            await held.RequestStream.WriteAsync(new WriteFragmentDto { TotalLength = payload.Length, KeyLength = 4, Offset = 100_000, Data = payload.AsMemory(100_000) });
            await held.RequestStream.CompleteAsync();
            Assert.IsTrue((await held.ResponseAsync).Applied);
        }
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.PendingBytes);
        Assert.IsTrue((await p.ToA.WriteAsync("second", Letters(600_000, 5), CancellationToken.None)).Applied, "accepted once the first upload is done");
        Assert.AreEqual(before + 2, p.A.Db.GetLatestSequenceNumber());
    }

    /// <summary>
    /// An upload that is cancelled, whose connection goes away, or that stalls, is never applied - not even partly - and
    /// its buffer is given back.
    /// </summary>
    [TestMethod]
    [DataRow("cancelled")]
    [DataRow("connection dropped")]
    [DataRow("stalled")]
    public async Task UnfinishedUploadIsNeverAppliedAndLeavesNoBuffer(string how)
    {
        await using GrpcPair p = await GrpcPair.StartAsync(new GrpcNodeServerOptions
        {
            FragmentedWrites = new FragmentedWriteOptions { IdleTimeout = how == "stalled" ? TimeSpan.FromMilliseconds(300) : TimeSpan.FromSeconds(30) },
        });
        ulong before = p.A.Db.GetLatestSequenceNumber();
        byte[] payload = U("gone" + Letters(600_000, 6));
        using var client = new GrpcPairClient("A", p.ServerA.Endpoint, TimeSpan.FromSeconds(2));
        using var cts = new CancellationTokenSource();
        global::MagicOnion.ClientStreamingResult<WriteFragmentDto, PairWriteResultDto> call =
            await client.Service.WithCancellationToken(cts.Token).WriteFragmentsAsync();
        for (int offset = 0; offset < 500_000; offset += 100_000)
        {
            await call.RequestStream.WriteAsync(new WriteFragmentDto { TotalLength = payload.Length, KeyLength = 4, Offset = offset, Data = payload.AsMemory(offset, 100_000) });
        }
        await WaitUntilAsync(() => p.ServerA.FragmentedWrites.PendingBytes == payload.Length, "the node to hold the upload");
        switch (how)
        {
            case "cancelled":
                await cts.CancelAsync();
                break;
            case "connection dropped":
                client.Dispose();
                break;
            default:
                Grpc.Core.RpcException e = await Assert.ThrowsExactlyAsync<Grpc.Core.RpcException>(async () => await call.ResponseAsync);
                Assert.AreEqual(Grpc.Core.StatusCode.DeadlineExceeded, e.StatusCode);
                StringAssert.Contains(e.Status.Detail, "no piece of the upload arrived within");
                break;
        }
        await WaitUntilAsync(() => p.ServerA.FragmentedWrites.PendingBytes == 0, "the node to give the buffer back");
        call.Dispose();
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.UploadsCompleted);
        Assert.AreEqual(before, p.A.Db.GetLatestSequenceNumber(), "nothing of an unfinished upload is applied");
        Assert.IsNull(p.Read(p.A, "gone"));
        // The node still serves writes.
        Assert.IsTrue((await p.ToA.WriteAsync("after", Letters(300_000, 7), CancellationToken.None)).Applied);
    }

    /// <summary>
    /// Every malformed upload is refused with InvalidArgument (the client's <see cref="LargeWriteError.Rejected"/>),
    /// applies nothing and leaves no buffer: a piece at the wrong offset, a total or key length that changes, a first
    /// piece not at 0, a key longer than the payload, an empty payload, bytes past the announced total, and a request
    /// stream that ends before the payload is whole.
    /// </summary>
    [TestMethod]
    public async Task MalformedUploadsAreRefusedAndApplyNothing()
    {
        await using GrpcPair p = await GrpcPair.StartAsync(new GrpcNodeServerOptions());
        ulong before = p.A.Db.GetLatestSequenceNumber();
        byte[] payload = U("keyvalue-0123456789");
        WriteFragmentDto Piece(int total, int keyLength, int offset, int length) =>
            new() { TotalLength = total, KeyLength = keyLength, Offset = offset, Data = payload.AsMemory(Math.Min(offset, payload.Length), length) };
        int n = payload.Length;
        (string Name, WriteFragmentDto[] Pieces, string Expect)[] cases =
        [
            ("a gap", [Piece(n, 3, 0, 5), Piece(n, 3, 6, 5)], "does not continue the upload at offset 5"),
            ("a repeat", [Piece(n, 3, 0, 5), Piece(n, 3, 0, 5)], "does not continue the upload at offset 5"),
            ("an empty piece", [Piece(n, 3, 0, 5), Piece(n, 3, 5, 0)], "does not continue the upload"),
            ("a changing total", [Piece(n, 3, 0, 5), Piece(n + 1, 3, 5, 5)], "the upload began with " + n),
            ("a changing key length", [Piece(n, 3, 0, 5), Piece(n, 4, 5, 5)], "the upload began with " + n),
            ("a first piece not at offset 0", [Piece(n, 3, 5, 5)], "starts at offset 5"),
            ("a key longer than the payload", [Piece(n, n + 1, 0, 5)], "with a key of " + (n + 1)),
            ("an empty payload", [Piece(0, 0, 0, 0)], "a write of 0 bytes"),
            ("bytes past the total", [Piece(n - 4, 3, 0, 5), Piece(n - 4, 3, 5, n - 5)], "does not continue the upload"),
            ("an upload that ends early", [Piece(n, 3, 0, 5), Piece(n, 3, 5, 5)], "the upload ended after 10 of " + n),
            ("no piece at all", [], "the upload ended after 0 of 0"),
        ];
        foreach ((string name, WriteFragmentDto[] pieces, string expect) in cases)
        {
            using global::MagicOnion.ClientStreamingResult<WriteFragmentDto, PairWriteResultDto> call = await p.ToA.Service.WriteFragmentsAsync();
            try
            {
                foreach (WriteFragmentDto piece in pieces) await call.RequestStream.WriteAsync(piece);
                await call.RequestStream.CompleteAsync();
            }
            catch (Exception e) when (e is not AssertFailedException)
            {
                // The node may end the call while pieces are still being written; the status below says why.
            }
            Grpc.Core.RpcException refused = await Assert.ThrowsExactlyAsync<Grpc.Core.RpcException>(async () => await call.ResponseAsync, name);
            Assert.AreEqual(Grpc.Core.StatusCode.InvalidArgument, refused.StatusCode, name);
            StringAssert.Contains(refused.Status.Detail, expect, name);
            await WaitUntilAsync(() => p.ServerA.FragmentedWrites.PendingBytes == 0, name + ": the buffer to be given back");
        }
        Assert.AreEqual(0, p.ServerA.FragmentedWrites.UploadsCompleted);
        Assert.AreEqual(before, p.A.Db.GetLatestSequenceNumber());
        Assert.IsNull(p.Read(p.A, "key"));

        // The same payload, well-formed, in three pieces.
        using global::MagicOnion.ClientStreamingResult<WriteFragmentDto, PairWriteResultDto> good = await p.ToA.Service.WriteFragmentsAsync();
        await good.RequestStream.WriteAsync(Piece(n, 3, 0, 5));
        await good.RequestStream.WriteAsync(Piece(n, 3, 5, 5));
        await good.RequestStream.WriteAsync(Piece(n, 3, 10, n - 10));
        await good.RequestStream.CompleteAsync();
        Assert.IsTrue((await good.ResponseAsync).Applied);
        Assert.AreEqual("value-0123456789", p.Read(p.A, "key"));
    }

    /// <summary>A node as it was built before <c>WriteFragmentsAsync</c>: the same service name, only the unary write.</summary>
    public sealed class OldPairBackend
    {
        public readonly ConcurrentDictionary<string, int> ValueLengths = new();
        public int Writes;
    }

    /// <summary>
    /// A client of this version against a node built before large writes: the fragment RPC is Unimplemented, so the
    /// write goes out as the unary RPC - applied if it passes the node's receive limit, and otherwise a
    /// <see cref="LargeWriteException"/> that says the server does not support large writes. The server's limit is an
    /// option of <see cref="GrpcNodeServer"/> (default gRPC's 4 MiB): raised, the old node takes the large unary write.
    /// </summary>
    [TestMethod]
    public async Task OldServerWithoutTheFragmentRpcGetsTheUnaryWrite()
    {
        var backend = new OldPairBackend();
        await using (GrpcNodeServer old = await GrpcNodeServer.StartAsync<LegacyWire.OldPairService, OldPairBackend>(backend, CancellationToken.None))
        {
            using var client = new GrpcPairClient("old", old.Endpoint, TimeSpan.FromSeconds(2));
            // Above a chunk, below the receive limit: falls back and is applied once.
            PairWriteResultDto r = await client.WriteAsync("medium", Letters(1024 * 1024, 8), CancellationToken.None);
            Assert.IsTrue(r.Applied);
            Assert.AreEqual(1, backend.Writes);
            Assert.AreEqual(1024 * 1024, backend.ValueLengths["medium"]);
            // Remembered: the next large write is the unary RPC at once.
            Assert.IsTrue((await client.WriteAsync("medium2", Letters(1024 * 1024, 9), CancellationToken.None)).Applied);
            Assert.AreEqual(2, backend.Writes);

            foreach (GrpcPairClient c in new[] { client, new GrpcPairClient("old", old.Endpoint, TimeSpan.FromSeconds(2)) })
            {
                LargeWriteException e = await Assert.ThrowsExactlyAsync<LargeWriteException>(() => c.WriteAsync("large", Letters(5 * 1024 * 1024, 10), CancellationToken.None));
                Assert.AreEqual(LargeWriteError.NotSupportedByServer, e.Error);
                StringAssert.Contains(e.Message, "the server does not support large writes");
                StringAssert.Contains(e.Message, "Received message exceeds the maximum configured message size");
                if (!ReferenceEquals(c, client)) c.Dispose();
            }
            Assert.AreEqual(2, backend.Writes, "nothing of the refused write was applied");
            Assert.IsTrue((await client.WriteAsync("small", "v", CancellationToken.None)).Applied);
        }

        await using GrpcNodeServer unlimited = await GrpcNodeServer.StartAsync<LegacyWire.OldPairService, OldPairBackend>(backend, System.Net.IPAddress.Loopback, 0,
            new GrpcNodeServerOptions { MaxReceiveMessageSize = null }, CancellationToken.None);
        using var toUnlimited = new GrpcPairClient("old", unlimited.Endpoint, TimeSpan.FromSeconds(2));
        Assert.IsTrue((await toUnlimited.WriteAsync("large", Letters(5 * 1024 * 1024, 10), CancellationToken.None)).Applied);
        Assert.AreEqual(5 * 1024 * 1024, backend.ValueLengths["large"]);
        Assert.AreEqual(GrpcNodeServerOptions.DefaultMaxReceiveMessageSize, new GrpcNodeServerOptions().MaxReceiveMessageSize, "the default is gRPC's");
    }

    /// <summary>
    /// A Raft cluster over real gRPC (every channel with the default receive limit): a 5 MB write sent to the leader
    /// through <see cref="GrpcClusterClient.WriteAsync(string, string, CancellationToken)"/> is committed and reaches
    /// the followers; sent to a follower it is refused like the unary one (false, nothing written).
    /// </summary>
    [TestMethod]
    public async Task ClusterCommitsALargeClientWriteOverGrpc()
    {
        using var dir = new TempDir("grpc-cluster-write");
        var members = new List<ClusterMember>();
        for (int i = 1; i <= 3; i++) members.Add(new ClusterMember("n" + i, "http://127.0.0.1:" + GrpcNodeServer.FindFreePort()));
        var clients = new Dictionary<string, GrpcClusterClient>();
        foreach (ClusterMember m in members) clients[m.NodeId] = new GrpcClusterClient(m, TimeSpan.FromSeconds(1), DefaultReceiveLimit());
        var hosts = new Dictionary<string, ClusterNodeHost>();
        var servers = new Dictionary<string, GrpcNodeServer>();
        var log = new ConcurrentQueue<string>();
        try
        {
            foreach (ClusterMember m in members)
            {
                var host = new ClusterNodeHost(new ClusterNodeHostOptions
                {
                    NodeId = m.NodeId,
                    Endpoint = m.Endpoint,
                    DbPath = dir.Sub(m.NodeId),
                    Members = members,
                    BootstrapPrimaryId = "n1",
                    DbOptionsFactory = static () => new DbOptions { WalTtlSeconds = 600 },
                    ElectionTimeoutMinMs = 400,
                    ElectionTimeoutMaxMs = 800,
                    HeartbeatIntervalMs = 60,
                    RetryDelay = TimeSpan.FromMilliseconds(50),
                    Log = log.Enqueue,
                }, peer => clients[peer.NodeId]);
                hosts[m.NodeId] = host;
                servers[m.NodeId] = await GrpcNodeServer.StartAsync<ClusterService, ClusterNodeHost>(host, System.Net.IPAddress.Loopback,
                    new Uri(m.Endpoint).Port, CancellationToken.None);
            }
            await hosts["n1"].StartAsync(CancellationToken.None);
            Assert.IsTrue(await hosts["n1"].TryWriteAsLeaderAsync(U("first"), U("1"), CancellationToken.None));
            await hosts["n2"].StartAsync(CancellationToken.None);
            await hosts["n3"].StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => hosts.Values.All(h => h.Node.Mode == ClusterMode.Cluster), "Cluster mode over gRPC");

            string large = Letters(5 * 1024 * 1024, 11);
            ulong before = hosts["n1"].Db.GetLatestSequenceNumber();
            Assert.IsFalse(await clients["n2"].WriteAsync("large", large, CancellationToken.None), "a follower refuses the write");
            Assert.IsFalse(await clients["n2"].Service.WriteAsync("k", "v"), "as it refuses the unary one");
            Assert.AreEqual(before, hosts["n1"].Db.GetLatestSequenceNumber());

            Assert.IsTrue(await clients["n1"].WriteAsync("large", large, CancellationToken.None), "committed\n" + string.Join("\n", log));
            Assert.AreEqual(before + 1, hosts["n1"].Db.GetLatestSequenceNumber(), "one log entry");
            Assert.IsTrue(await clients["n1"].WriteAsync("small", "v", CancellationToken.None));
            Assert.AreEqual(1, servers["n1"].FragmentedWrites.UploadsCompleted);
            Assert.AreEqual(0, servers["n1"].FragmentedWrites.PendingBytes);
            await WaitUntilAsync(() => hosts.Values.All(h => h.Db.GetLatestSequenceNumber() == hosts["n1"].Db.GetLatestSequenceNumber()), "followers converge\n" + string.Join("\n", log));
            DigestDto d1 = await clients["n1"].Service.ChecksumAsync(), d2 = await clients["n2"].Service.ChecksumAsync(), d3 = await clients["n3"].Service.ChecksumAsync();
            Assert.AreEqual(d1.ToDigest(), d2.ToDigest());
            Assert.AreEqual(d1.ToDigest(), d3.ToDigest());
            foreach (ClusterNodeHost h in hosts.Values) Assert.AreEqual(0, h.Resyncs);
        }
        finally
        {
            foreach (ClusterNodeHost h in hosts.Values) await h.DisposeAsync();
            foreach (GrpcNodeServer s in servers.Values) await s.DisposeAsync();
            foreach (GrpcClusterClient c in clients.Values) c.Dispose();
        }
    }
}
