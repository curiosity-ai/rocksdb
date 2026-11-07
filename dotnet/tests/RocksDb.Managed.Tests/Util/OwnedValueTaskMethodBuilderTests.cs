using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

/// <summary>
/// <see cref="OwnedValueTaskMethodBuilder{TResult}"/>: the state machine box of a suspended call lives in the caller's slot
/// and is reused by the next call with that slot (DbImpl.LeadWriteGroupAsync's box is owned by its leader writer).
/// </summary>
[TestClass]
public sealed class OwnedValueTaskMethodBuilderTests
{
    /// <summary>A reusable completion source; continuations run on the thread that calls <see cref="Set"/>.</summary>
    private sealed class Gate : IValueTaskSource<int>
    {
        private ManualResetValueTaskSourceCore<int> _core;

        public ValueTask<int> WaitAsync()
        {
            _core.Reset();
            return new ValueTask<int>(this, _core.Version);
        }

        public void Set(int value) => _core.SetResult(value);
        public void Fail(Exception e) => _core.SetException(e);
        public int GetResult(short token) => _core.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    private static readonly AsyncLocal<int> s_local = new();

    [AsyncMethodBuilder(typeof(OwnedValueTaskMethodBuilder<>))]
    private static async ValueTask<int> TwoAwaitsAsync(Gate first, Gate second, int add)
    {
        int a = await first.WaitAsync().ConfigureAwait(false);
        int b = await second.WaitAsync().ConfigureAwait(false);
        return a + b + add + s_local.Value;
    }

    [AsyncMethodBuilder(typeof(OwnedValueTaskMethodBuilder<>))]
    private static async ValueTask<int> MaybeSuspendAsync(Gate? gate, int value, bool fail)
    {
        if (gate is not null) value += await gate.WaitAsync().ConfigureAwait(false);
        if (fail) throw new InvalidOperationException("failed " + value);
        return value;
    }

    private static ValueTask<int> Call(OwnedAsyncBoxSlot<int> slot, Gate first, Gate second, int add)
    {
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        return TwoAwaitsAsync(first, second, add);
    }

    /// <summary>
    /// What the compiler generates for <see cref="TwoAwaitsAsync"/> in an optimized build (a struct state machine; Debug
    /// builds, like this test project's, emit a class allocated per call), so the allocation test does not depend on
    /// the build configuration.
    /// </summary>
    private struct TwoAwaitsMachine : IAsyncStateMachine
    {
        public OwnedValueTaskMethodBuilder<int> Builder;
        public int State;
        public Gate First, Second;
        public int Add, A;
        private ConfiguredValueTaskAwaitable<int>.ConfiguredValueTaskAwaiter _awaiter;

        public void MoveNext()
        {
            int result;
            try
            {
                ConfiguredValueTaskAwaitable<int>.ConfiguredValueTaskAwaiter awaiter;
                if (State == 0)
                {
                    awaiter = First.WaitAsync().ConfigureAwait(false).GetAwaiter();
                    if (!awaiter.IsCompleted)
                    {
                        State = 1;
                        _awaiter = awaiter;
                        Builder.AwaitUnsafeOnCompleted(ref awaiter, ref this);
                        return;
                    }
                    A = awaiter.GetResult();
                    awaiter = Second.WaitAsync().ConfigureAwait(false).GetAwaiter();
                }
                else if (State == 1)
                {
                    awaiter = _awaiter;
                    _awaiter = default;
                    A = awaiter.GetResult();
                    awaiter = Second.WaitAsync().ConfigureAwait(false).GetAwaiter();
                }
                else
                {
                    awaiter = _awaiter;
                    _awaiter = default;
                    result = A + awaiter.GetResult() + Add;
                    State = -2;
                    Builder.SetResult(result);
                    return;
                }
                if (!awaiter.IsCompleted)
                {
                    State = 2;
                    _awaiter = awaiter;
                    Builder.AwaitUnsafeOnCompleted(ref awaiter, ref this);
                    return;
                }
                result = A + awaiter.GetResult() + Add;
            }
            catch (Exception e)
            {
                State = -2;
                Builder.SetException(e);
                return;
            }
            State = -2;
            Builder.SetResult(result);
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine) => Builder.SetStateMachine(stateMachine);
    }

    private static ValueTask<int> CallOptimized(OwnedAsyncBoxSlot<int> slot, Gate first, Gate second, int add)
    {
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        var sm = new TwoAwaitsMachine { Builder = OwnedValueTaskMethodBuilder<int>.Create(), First = first, Second = second, Add = add };
        sm.Builder.Start(ref sm);
        return sm.Builder.Task;
    }

    [TestMethod]
    public void SuspendedCallsReuseTheSlotsBoxWithoutAllocating()
    {
        var slot = new OwnedAsyncBoxSlot<int>();
        var g1 = new Gate();
        var g2 = new Gate();
        int Round(int i)
        {
            ValueTask<int> t = CallOptimized(slot, g1, g2, i);
            g1.Set(1);
            g2.Set(2);
            return t.GetAwaiter().GetResult();
        }
        // Warm up (JIT, the slot's box).
        for (int i = 0; i < 100; i++) Assert.AreEqual(3 + i, Round(i));
        object? box = slot.Box;
        Assert.IsNotNull(box);
        int[] results = new int[1000];
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < results.Length; i++) results[i] = Round(i);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        for (int i = 0; i < results.Length; i++) Assert.AreEqual(3 + i, results[i]);
        Assert.AreSame(box, slot.Box, "the slot keeps its box");
        Assert.AreEqual(0L, allocated, "bytes allocated by " + results.Length + " suspended calls");
    }

    [TestMethod]
    public void SynchronousCompletionAndExceptions()
    {
        var slot = new OwnedAsyncBoxSlot<int>();
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        ValueTask<int> t = MaybeSuspendAsync(null, 5, fail: false);
        Assert.IsTrue(t.IsCompletedSuccessfully);
        Assert.AreEqual(5, t.Result);
        Assert.IsNull(slot.Box, "a call that never suspends takes no box");

        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        t = MaybeSuspendAsync(null, 5, fail: true);
        Assert.IsTrue(t.IsFaulted);
        Assert.AreEqual("failed 5", Assert.ThrowsExactly<InvalidOperationException>(() => t.GetAwaiter().GetResult()).Message);

        // Fault after suspension, then reuse of the same box.
        var gate = new Gate();
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        t = MaybeSuspendAsync(gate, 5, fail: true);
        Assert.IsFalse(t.IsCompleted);
        gate.Set(2);
        Assert.AreEqual("failed 7", Assert.ThrowsExactly<InvalidOperationException>(() => t.GetAwaiter().GetResult()).Message);
        object? box = slot.Box;
        Assert.IsNotNull(box);
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        t = MaybeSuspendAsync(gate, 5, fail: false);
        gate.Set(3);
        Assert.AreEqual(8, t.GetAwaiter().GetResult());
        Assert.AreSame(box, slot.Box);

        // An awaited exception from the awaited source propagates.
        OwnedValueTaskMethodBuilder<int>.SetNextSlot(slot);
        t = MaybeSuspendAsync(gate, 5, fail: false);
        gate.Fail(new TimeoutException("gate"));
        Assert.AreEqual("gate", Assert.ThrowsExactly<TimeoutException>(() => t.GetAwaiter().GetResult()).Message);
    }

    [TestMethod]
    public void BoxStillInUseOrMissingSlotFallsBackToAFreshBox()
    {
        var slot = new OwnedAsyncBoxSlot<int>();
        Gate a1 = new(), a2 = new(), b1 = new(), b2 = new();
        // Misuse: a second call with the slot while the first is suspended must not share its box.
        ValueTask<int> first = Call(slot, a1, a2, 10);
        object? box = slot.Box;
        ValueTask<int> second = Call(slot, b1, b2, 20);
        Assert.AreSame(box, slot.Box, "the slot keeps the box of the call in progress");
        b1.Set(1);
        a1.Set(1);
        b2.Set(1);
        a2.Set(1);
        Assert.AreEqual(12, first.GetAwaiter().GetResult());
        Assert.AreEqual(22, second.GetAwaiter().GetResult());

        // No slot handed in: a fresh box, still correct; the hand-off is consumed by one call only.
        ValueTask<int> unowned = TwoAwaitsAsync(a1, a2, 30);
        a1.Set(0);
        a2.Set(0);
        Assert.AreEqual(30, unowned.GetAwaiter().GetResult());
    }

    [TestMethod]
    public async Task ExecutionContextFlowsAcrossThreads()
    {
        var slot = new OwnedAsyncBoxSlot<int>();
        Gate g1 = new(), g2 = new();
        for (int round = 0; round < 3; round++)
        {
            s_local.Value = 100 * (round + 1);
            ValueTask<int> t = Call(slot, g1, g2, round);
            s_local.Value = 0;
            // Complete from thread-pool threads without the caller's AsyncLocal value.
            await Task.Run(() => g1.Set(1));
            await Task.Run(() => g2.Set(2));
            Assert.AreEqual(100 * (round + 1) + 3 + round, await t);
        }
    }

    [TestMethod]
    public async Task ConcurrentCallsWithTheirOwnSlots()
    {
        const int Workers = 8, Rounds = 2000;
        var tasks = new Task[Workers];
        for (int w = 0; w < Workers; w++)
        {
            int worker = w;
            tasks[w] = Task.Run(async () =>
            {
                var slot = new OwnedAsyncBoxSlot<int>();
                Gate g1 = new(), g2 = new();
                for (int i = 0; i < Rounds; i++)
                {
                    ValueTask<int> t = Call(slot, g1, g2, worker);
                    // Wake the call from another thread-pool thread.
                    ThreadPool.UnsafeQueueUserWorkItem(static s => { s.g1.Set(1); s.g2.Set(1); }, (g1, g2), preferLocal: false);
                    Assert.AreEqual(2 + worker, await t);
                }
            });
        }
        await Task.WhenAll(tasks);
    }
}
