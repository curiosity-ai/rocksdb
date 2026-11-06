using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace RocksDb.Managed.Util;

/// <summary>
/// Holds the reusable state machine box of an async method built with <see cref="OwnedValueTaskMethodBuilder{TResult}"/>.
/// The owner (e.g. a pooled <c>WriteThread.Writer</c>) runs at most one such call at a time and awaits it to completion
/// before starting the next one, so one box per owner is enough.
/// </summary>
internal sealed class OwnedAsyncBoxSlot<TResult>
{
    internal OwnedAsyncBox<TResult>? Box;
}

/// <summary>
/// Async method builder for a <see cref="ValueTask{TResult}"/>-returning method whose state machine box, when the call
/// suspends, is the box kept in an <see cref="OwnedAsyncBoxSlot{TResult}"/> handed in by the caller instead of one from
/// the runtime's pool. <see cref="PoolingAsyncValueTaskMethodBuilder{TResult}"/> keeps one box per thread and per core:
/// when several calls are suspended at once and resume on other threads (the write group leaders of concurrent async
/// writers), its cache misses and boxes are allocated. Not a port of RocksDB code: managed infrastructure for the
/// write path (db/write_thread.cc's writers live on the stack of their writing thread upstream).
/// <para>
/// Usage: call <see cref="SetNextSlot"/> on the calling thread right before invoking the method (the slot is taken by
/// the method's <see cref="Create"/>); a call without a slot, or whose slot's box is still in use, uses a fresh box.
/// The method must not be called concurrently with the same slot, and its result must be consumed (awaited) before
/// the slot is used again.
/// </para>
/// </summary>
[StructLayout(LayoutKind.Auto)]
internal struct OwnedValueTaskMethodBuilder<TResult>
{
    [ThreadStatic] private static OwnedAsyncBoxSlot<TResult>? t_nextSlot;

    private OwnedAsyncBoxSlot<TResult>? _slot;
    private OwnedAsyncBox<TResult>? _box;
    private Exception? _exception;
    private TResult _result;

    /// <summary>Hands <paramref name="slot"/> to the next call of a method using this builder on this thread.</summary>
    public static void SetNextSlot(OwnedAsyncBoxSlot<TResult> slot) => t_nextSlot = slot;

    public static OwnedValueTaskMethodBuilder<TResult> Create()
    {
        OwnedAsyncBoxSlot<TResult>? slot = t_nextSlot;
        t_nextSlot = null;
        return new OwnedValueTaskMethodBuilder<TResult> { _slot = slot };
    }

    // AsyncValueTaskMethodBuilder.Start is stateless (AsyncMethodBuilderCore.Start: runs MoveNext and restores the
    // caller's ExecutionContext / SynchronizationContext).
    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine =>
        default(AsyncValueTaskMethodBuilder<TResult>).Start(ref stateMachine);

    public void SetStateMachine(IAsyncStateMachine stateMachine) => ArgumentNullException.ThrowIfNull(stateMachine);

    public void SetResult(TResult result)
    {
        if (_box is null) _result = result;
        else _box.SetResult(result);
    }

    public void SetException(Exception exception)
    {
        if (_box is null) _exception = exception;
        else _box.SetException(exception);
    }

    public ValueTask<TResult> Task
    {
        get
        {
            if (_box is { } box) return new ValueTask<TResult>(box, box.Version);
            if (_exception is { } e) return ValueTask.FromException<TResult>(e);
            return new ValueTask<TResult>(_result);
        }
    }

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        OwnedAsyncBox<TStateMachine, TResult> box = GetBox(ref stateMachine);
        awaiter.OnCompleted(box.MoveNextAction);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion where TStateMachine : IAsyncStateMachine
    {
        OwnedAsyncBox<TStateMachine, TResult> box = GetBox(ref stateMachine);
        awaiter.UnsafeOnCompleted(box.MoveNextAction);
    }

    private OwnedAsyncBox<TStateMachine, TResult> GetBox<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        ExecutionContext? context = ExecutionContext.Capture();
        // Later awaits run on the box's copy of the state machine, whose builder already references the box.
        if (_box is OwnedAsyncBox<TStateMachine, TResult> current)
        {
            if (current.Context != context) current.Context = context;
            return current;
        }
        Debug.Assert(_box is null);
        OwnedAsyncBox<TStateMachine, TResult>? box = _slot?.Box as OwnedAsyncBox<TStateMachine, TResult>;
        if (box is null || box.InUse)
        {
            box = new OwnedAsyncBox<TStateMachine, TResult>();
            if (_slot is not null && _slot.Box is null or { InUse: false }) _slot.Box = box;
        }
        box.Rent();
        // Set the field before copying the state machine, so that the box's copy references the box too.
        _box = box;
        box.StateMachine = stateMachine;
        box.Context = context;
        return box;
    }
}

/// <summary>The completion source part of a box built by <see cref="OwnedValueTaskMethodBuilder{TResult}"/>.</summary>
internal abstract class OwnedAsyncBox<TResult> : IValueTaskSource<TResult>
{
    // Continuations run synchronously, like the runtime's state machine boxes (the awaiting method resumes on the thread
    // that completed this one).
    private ManualResetValueTaskSourceCore<TResult> _core;
    private bool _inUse;

    public short Version => _core.Version;

    /// <summary>Between the suspension that took the box and the GetResult of its ValueTask.</summary>
    public bool InUse => Volatile.Read(ref _inUse);

    public void Rent()
    {
        _core.Reset();
        _inUse = true;
    }

    public void SetResult(TResult result) => _core.SetResult(result);

    public void SetException(Exception exception) => _core.SetException(exception);

    private protected abstract void ClearStateMachine();

    public TResult GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            // Release what the finished state machine references; the box may be reused from here on.
            ClearStateMachine();
            Volatile.Write(ref _inUse, false);
        }
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}

internal sealed class OwnedAsyncBox<TStateMachine, TResult> : OwnedAsyncBox<TResult> where TStateMachine : IAsyncStateMachine
{
    private static readonly ContextCallback s_moveNext = static s => ((OwnedAsyncBox<TStateMachine, TResult>)s!).StateMachine!.MoveNext();

    public TStateMachine? StateMachine;
    public ExecutionContext? Context;
    public readonly Action MoveNextAction;

    public OwnedAsyncBox() => MoveNextAction = MoveNext;

    private void MoveNext()
    {
        ExecutionContext? context = Context;
        if (context is null) StateMachine!.MoveNext();
        else ExecutionContext.Run(context, s_moveNext, this);
    }

    private protected override void ClearStateMachine()
    {
        StateMachine = default;
        Context = null;
    }
}
