using System.Buffers.Binary;

namespace RocksDb.Managed.Replication.Hosting;

/// <summary>What <see cref="WalStreamAssembler.Accept"/> made of a message.</summary>
public enum WalAssemblyStatus
{
    /// <summary>A whole chunk is ready to be applied (an ordinary message, or the last fragment of a batch).</summary>
    Complete = 0,
    /// <summary>A fragment was stored; the batch is not complete. Nothing to apply, nothing to report.</summary>
    Incomplete = 1,
    /// <summary>
    /// The message does not continue the stream (see <see cref="WalStreamAssembler"/>). The partial batch was discarded;
    /// <see cref="WalStreamAssembler.LastError"/> says why. The follower drops the stream and attaches again at its
    /// last applied sequence number.
    /// </summary>
    Invalid = 2,
    /// <summary>
    /// The batch is larger than the follower accepts (<see cref="WalStreamAssembler.MaxBatchBytes"/>). Nothing was
    /// allocated for it. The stream cannot get past it: the follower rebuilds from a snapshot.
    /// </summary>
    TooLarge = 3,
}

/// <summary>
/// Follower side of a WAL stream that may carry fragmented write batches (<see cref="WalStreamMessage.IsFragment"/>):
/// hands an ordinary message through untouched and collects the fragments of a large batch in one pooled buffer of the
/// announced length (<see cref="FragmentBuffer"/>), so that the follower applies the batch exactly as if it had
/// arrived in one message - and only once all of it arrived. The follower's applied sequence number never stands
/// inside a batch.
/// <para>
/// One assembler per stream attachment: a stream that ends or fails takes its partial batch with it (dispose the
/// assembler), and the next attachment starts at the last applied sequence number, where the source sends the batch
/// again from offset 0.
/// </para>
/// <para>
/// Refused as <see cref="WalAssemblyStatus.Invalid"/>: a fragment that is not at the offset the previous one ended at;
/// a first fragment whose offset is not 0; a fragment whose first sequence number, source id, source term, entry term
/// or total length differ from the batch in progress; an empty fragment or one that ends past the total length; an
/// entry count on a fragment that is not the last; an ordinary message while a batch is incomplete; a completed batch
/// whose header does not carry the announced first sequence number and entry count.
/// </para>
/// <para>
/// Memory: the follower holds the whole batch (this buffer, up to <see cref="MaxBatchBytes"/>) and, while applying it,
/// the engine's own copy. Fragments bound the size of a message, not the size of a batch.
/// </para>
/// Not thread-safe.
/// </summary>
public sealed class WalStreamAssembler : IDisposable
{
    /// <summary>
    /// The default <see cref="MaxBatchBytes"/>: 1 GiB. A serialized write batch lives in one array (below 2 GiB) on
    /// both nodes, and 1 GiB is the largest array <see cref="RocksArrayPool{T}"/> pools - a larger buffer would be a
    /// fresh allocation per batch. Refusing a batch is safe (the follower then takes it as part of a snapshot, as
    /// files), so the default bounds what one message of the source can make a follower allocate.
    /// </summary>
    public const int DefaultMaxBatchBytes = 1 << 30;

    private readonly FragmentBuffer _buffer = new();
    // True while the buffer holds the beginning of a batch whose end has not arrived.
    private bool _partial;
    private ulong _firstSequence;
    private string _sourceId = "";
    private long _sourceTerm;
    private long _entryTerm;

    public WalStreamAssembler(int maxBatchBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBatchBytes, WriteBatch.HeaderSize);
        MaxBatchBytes = maxBatchBytes;
    }

    /// <summary>The largest serialized batch (or unfragmented chunk) this follower takes from the stream.</summary>
    public int MaxBatchBytes { get; }

    /// <summary>Whether the beginning of a batch is held whose end has not arrived.</summary>
    public bool HasPartialBatch => _partial;

    /// <summary>Why the latest message was refused (<see cref="WalAssemblyStatus.Invalid"/> / <see cref="WalAssemblyStatus.TooLarge"/>).</summary>
    public string? LastError { get; private set; }

    /// <summary>
    /// Takes the next message of the stream (not a resync request). On <see cref="WalAssemblyStatus.Complete"/>
    /// <paramref name="chunk"/> is the serialized write batch to apply: the message's own data, or the assembled
    /// buffer - valid until the next call, <see cref="Reset"/> or disposal. Costs two flag tests and a comparison for an
    /// ordinary message.
    /// </summary>
    public WalAssemblyStatus Accept(in WalStreamMessage message, out ReadOnlyMemory<byte> chunk)
    {
        chunk = default;
        if (!message.IsFragment)
        {
            if (_partial)
            {
                return Refuse("an unfragmented chunk at sequence " + message.FirstSequence + " arrived while the batch at sequence " + _firstSequence +
                    " was incomplete (" + _buffer.ReceivedLength + " of " + _buffer.TotalLength + " bytes)");
            }
            if (_buffer.IsActive) _buffer.Release();
            if (message.Data.Length > MaxBatchBytes) return TooLarge(message.FirstSequence, message.Data.Length);
            chunk = message.Data;
            return WalAssemblyStatus.Complete;
        }
        return AcceptFragment(message, out chunk);
    }

    private WalAssemblyStatus AcceptFragment(in WalStreamMessage m, out ReadOnlyMemory<byte> chunk)
    {
        chunk = default;
        int length = m.Data.Length;
        if (!_partial)
        {
            // The previous batch (applied by now) gives its buffer back.
            _buffer.Release();
            if (m.FragmentOffset != 0)
            {
                return Refuse("a fragment at offset " + m.FragmentOffset + " of the batch at sequence " + m.FirstSequence +
                    " arrived without the beginning of that batch");
            }
            if (m.TotalLength <= WriteBatch.HeaderSize)
                return Refuse("a fragment of the batch at sequence " + m.FirstSequence + " announces a total length of " + m.TotalLength + " bytes");
            // Before anything is allocated.
            if (m.TotalLength > MaxBatchBytes) return TooLarge(m.FirstSequence, m.TotalLength);
            if (length == 0 || length > m.TotalLength)
            {
                return Refuse("the first fragment of the batch at sequence " + m.FirstSequence + " carries " + length + " bytes of " + m.TotalLength);
            }
            _buffer.Begin(m.TotalLength);
            _partial = true;
            _firstSequence = m.FirstSequence;
            _sourceId = m.SourceId;
            _sourceTerm = m.SourceTerm;
            _entryTerm = m.EntryTerm;
        }
        else if (m.FirstSequence != _firstSequence || m.SourceTerm != _sourceTerm || m.EntryTerm != _entryTerm || m.TotalLength != _buffer.TotalLength ||
                 !string.Equals(m.SourceId, _sourceId, StringComparison.Ordinal))
        {
            return Refuse("a fragment of the batch at sequence " + m.FirstSequence + " (" + m.TotalLength + " bytes, from " + m.SourceId + " in term " +
                m.SourceTerm + ", entry term " + m.EntryTerm + ") arrived while the batch at sequence " + _firstSequence + " (" + _buffer.TotalLength +
                " bytes, from " + _sourceId + " in term " + _sourceTerm + ", entry term " + _entryTerm + ") was incomplete");
        }
        if (!_buffer.TryAppend(m.FragmentOffset, m.Data.Span))
        {
            return Refuse("a fragment of the batch at sequence " + m.FirstSequence + " at offset " + m.FragmentOffset + " with " + length +
                " bytes does not continue it at offset " + _buffer.ReceivedLength + " of " + _buffer.TotalLength);
        }
        if (!_buffer.IsComplete)
        {
            if (m.EntryCount != 0)
            {
                return Refuse("a fragment of the batch at sequence " + m.FirstSequence + " carries an entry count (" + m.EntryCount +
                    ") before the end of the batch");
            }
            return WalAssemblyStatus.Incomplete;
        }
        ReadOnlyMemory<byte> all = _buffer.Memory;
        ulong headerSequence = BinaryPrimitives.ReadUInt64LittleEndian(all.Span);
        int headerCount = BinaryPrimitives.ReadInt32LittleEndian(all.Span[sizeof(ulong)..]);
        if (headerSequence != m.FirstSequence || headerCount != m.EntryCount)
        {
            return Refuse("the batch put together from fragments starts at sequence " + headerSequence + " with " + headerCount +
                " entries; its fragments announced sequence " + m.FirstSequence + " and " + m.EntryCount + " entries");
        }
        // Held until the next message: the caller applies it now.
        _partial = false;
        chunk = all;
        return WalAssemblyStatus.Complete;
    }

    private WalAssemblyStatus Refuse(string reason)
    {
        Reset();
        LastError = reason;
        return WalAssemblyStatus.Invalid;
    }

    private WalAssemblyStatus TooLarge(ulong firstSequence, int length)
    {
        Reset();
        LastError = "the batch at sequence " + firstSequence + " has " + length + " bytes, more than this node accepts from a WAL stream (MaxBatchBytes = " +
            MaxBatchBytes + ")";
        return WalAssemblyStatus.TooLarge;
    }

    /// <summary>Discards a partial batch and returns the buffer to the pool.</summary>
    public void Reset()
    {
        _partial = false;
        _buffer.Release();
    }

    public void Dispose() => Reset();
}
