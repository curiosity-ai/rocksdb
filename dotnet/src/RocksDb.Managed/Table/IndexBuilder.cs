using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Binary-search index builder. Port of ShortenedIndexBuilder (table/block_based/index_builder.{h,cc}).
/// Builds two index blocks in parallel — one keyed by internal-key separators and one by user-key separators —
/// and emits the user-key one unless two adjacent data blocks share a boundary user key (or format_version &lt;= 2),
/// in which case the separator must carry the sequence number ("index_key_is_user_key" = 0).
/// </summary>
internal sealed class ShortenedIndexBuilder : IDisposable
{
    private readonly InternalKeyComparator _icmp;
    private readonly BlockBuilder _withSeq;
    private readonly BlockBuilder _withoutSeq;
    private readonly bool _useValueDeltaEncoding;
    private readonly bool _useValueDeltaEscape;
    private readonly bool _useCommonPrefix;
    private readonly bool _includeFirstKey;
    private readonly IndexShorteningMode _shortening;
    private readonly PooledBufferWriter _scratch = new(64);
    private readonly PooledBufferWriter _encoded = new(64);
    private readonly PooledBufferWriter _deltaEncoded = new(64);
    private readonly PooledBufferWriter _currentBlockFirstKey = new(64);
    private bool _mustUseSeparatorWithSeq;
    private bool _hasLastHandle;
    private BlockHandle _lastHandle;
    private int _indexSize;
    private bool _finished;

    public ShortenedIndexBuilder(InternalKeyComparator icmp, int indexBlockRestartInterval, int formatVersion, bool useValueDeltaEncoding,
        IndexShorteningMode shortening, bool includeFirstKey, bool useCommonPrefix)
    {
        _icmp = icmp;
        _withSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefix, isUserKey: false);
        _withoutSeq = new BlockBuilder(indexBlockRestartInterval, true, useValueDeltaEncoding, DataBlockIndexType.BinarySearch, 0.75,
            useCommonPrefix, isUserKey: true);
        _useValueDeltaEscape = BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(formatVersion);
        _useCommonPrefix = useCommonPrefix;
        _useValueDeltaEncoding = useValueDeltaEncoding;
        _includeFirstKey = includeFirstKey;
        _shortening = shortening;
        _mustUseSeparatorWithSeq = formatVersion <= 2;
    }

    /// <summary>"separator_is_key_plus_seq": index keys are internal keys.</summary>
    public bool SeparatorIsKeyPlusSeq => _mustUseSeparatorWithSeq;
    public int IndexSize => _indexSize;
    public int CurrentSizeEstimate => _mustUseSeparatorWithSeq ? _withSeq.CurrentSizeEstimate : _withoutSeq.CurrentSizeEstimate;

    public void OnKeyAdded(ReadOnlySpan<byte> internalKey)
    {
        if (_includeFirstKey && _currentBlockFirstKey.WrittenCount == 0) _currentBlockFirstKey.Write(internalKey);
    }

    /// <summary>Port of ShortenedIndexBuilder::AddIndexEntry (GetSeparatorWithSeq + AddIndexEntryImpl).</summary>
    public void AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext, BlockHandle handle)
    {
        ReadOnlySpan<byte> separator;
        if (hasNext)
        {
            _scratch.Clear();
            if (_shortening != IndexShorteningMode.NoShortening
                && _icmp.FindShortestSeparator(lastKeyInCurrentBlock, firstKeyInNextBlock, _scratch))
                separator = _scratch.WrittenSpan;
            else
                separator = lastKeyInCurrentBlock;
            // ShouldUseKeyPlusSeqAsSeparator: adjacent blocks share a boundary user key.
            if (!_mustUseSeparatorWithSeq && _icmp.EqualUserKey(DbFormat.ExtractUserKey(lastKeyInCurrentBlock), DbFormat.ExtractUserKey(firstKeyInNextBlock)))
                _mustUseSeparatorWithSeq = true;
        }
        else
        {
            // Last entry: a short successor shares ~no prefix with the file's keys and would collapse the common-prefix
            // index block's prefix, so with common-prefix encoding the full last key is used (GetSeparatorWithSeq).
            _scratch.Clear();
            if (_shortening == IndexShorteningMode.ShortenSeparatorsAndSuccessor && !_useCommonPrefix
                && _icmp.FindShortSuccessor(lastKeyInCurrentBlock, _scratch))
                separator = _scratch.WrittenSpan;
            else
                separator = lastKeyInCurrentBlock;
        }

        ReadOnlySpan<byte> firstKey = _includeFirstKey ? _currentBlockFirstKey.WrittenSpan : default;
        _encoded.Clear();
        IndexValue.EncodeFull(_encoded, handle, _includeFirstKey, firstKey);
        _deltaEncoded.Clear();
        if (_useValueDeltaEncoding && _hasLastHandle)
            IndexValue.EncodeDelta(_deltaEncoded, handle, _lastHandle, _useValueDeltaEscape, _includeFirstKey, firstKey);
        _lastHandle = handle;
        _hasLastHandle = true;

        _withSeq.Add(separator, _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
        if (!_mustUseSeparatorWithSeq)
            _withoutSeq.Add(DbFormat.ExtractUserKey(separator), _encoded.WrittenSpan, _deltaEncoded.WrittenSpan, false);
        _currentBlockFirstKey.Clear();
    }

    /// <summary>Finishes and returns the index block contents (valid until disposal).</summary>
    public ReadOnlySpan<byte> Finish()
    {
        System.Diagnostics.Debug.Assert(!_finished);
        _finished = true;
        ReadOnlySpan<byte> contents = _mustUseSeparatorWithSeq ? _withSeq.Finish() : _withoutSeq.Finish();
        _indexSize = contents.Length;
        return contents;
    }

    public void Dispose()
    {
        _withSeq.Dispose();
        _withoutSeq.Dispose();
        _scratch.Dispose();
        _encoded.Dispose();
        _deltaEncoded.Dispose();
        _currentBlockFirstKey.Dispose();
    }
}
