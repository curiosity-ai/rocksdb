using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// A DB iterator with a <see cref="WriteBatchWithIndex"/>'s updates applied on top. Port of BaseDeltaIterator
/// (utilities/write_batch_with_index/write_batch_with_index_internal.cc): the base (DB) and delta (batch) iterators are
/// merged by key; a key's latest batch update wins (Put / PutEntity override, Delete / SingleDelete hide the key, merge
/// operands are applied to the base value or the batch's own Put / PutEntity).
/// <para>
/// Owns and disposes the base iterator. <see cref="Key"/>, <see cref="Value"/> and <see cref="Columns"/> are valid until
/// the next positioning call. The batch must not be modified while iterating. Errors (e.g. a merge without merge
/// operator) are thrown by the positioning call. Not thread-safe.
/// </para>
/// </summary>
public sealed class BaseDeltaIterator : IDisposable
{
    private readonly Iterator _base;
    private readonly WriteBatchWithIndexIterator _delta;
    private readonly Comparator _cmp;
    private readonly MergeOperator? _mergeOperator;
    private readonly MergeContext _mergeContext = new();
    private readonly PooledBufferWriter _mergeResult = new(128);
    private readonly PooledBufferWriter _entityScratch = new(128);
    private readonly WideColumns _columns = new();
    private bool _forward = true;
    private bool _currentAtBase = true;
    private bool _equalKeys;
    // Delta value source: plain value / entity / merge result.
    private enum DeltaValue : byte { Plain, Entity, MergedValue, MergedEntity }
    private DeltaValue _deltaValue;
    private bool _disposed;

    internal BaseDeltaIterator(Iterator baseIterator, WriteBatchWithIndexIterator delta, Comparator comparator, MergeOperator? mergeOperator)
    {
        _base = baseIterator;
        _delta = delta;
        _cmp = comparator;
        _mergeOperator = mergeOperator;
    }

    private bool BaseValid => _base.Valid;
    private bool DeltaValid => _delta.Valid;

    public bool Valid => _currentAtBase ? BaseValid : DeltaValid;

    public void SeekToFirst()
    {
        _forward = true;
        _base.SeekToFirst();
        _delta.SeekToFirst();
        UpdateCurrent();
    }

    public void SeekToLast()
    {
        _forward = false;
        _base.SeekToLast();
        _delta.SeekToLast();
        UpdateCurrent();
    }

    public void Seek(ReadOnlySpan<byte> key)
    {
        _forward = true;
        _base.Seek(key);
        _delta.Seek(key);
        UpdateCurrent();
    }

    public void SeekForPrev(ReadOnlySpan<byte> key)
    {
        _forward = false;
        _base.SeekForPrev(key);
        _delta.SeekForPrev(key);
        UpdateCurrent();
    }

    public void Next()
    {
        if (!Valid) ThrowHelper.NotSupported("Next() on invalid iterator");
        if (!_forward)
        {
            // Need to change direction: if our direction was backward and we're not equal, we have two states:
            // both iterators are valid (we're already in a good state), or only one is valid (advance that one).
            _forward = true;
            _equalKeys = false;
            if (!BaseValid) _base.SeekToFirst();
            else if (!DeltaValid) _delta.SeekToFirst();
            else if (_currentAtBase) AdvanceDelta();
            else AdvanceBase();
            if (DeltaValid && BaseValid && _cmp.CompareWithoutTimestamp(_delta.Key, false, _base.Key, false) == 0) _equalKeys = true;
        }
        Advance();
    }

    public void Prev()
    {
        if (!Valid) ThrowHelper.NotSupported("Prev() on invalid iterator");
        if (_forward)
        {
            _forward = false;
            _equalKeys = false;
            if (!BaseValid) _base.SeekToLast();
            else if (!DeltaValid) _delta.SeekToLast();
            else if (_currentAtBase) AdvanceDelta(); // Change delta from less advanced than base to more advanced
            else AdvanceBase();                      // Change base from less advanced than delta to more advanced
            if (DeltaValid && BaseValid && _cmp.CompareWithoutTimestamp(_delta.Key, false, _base.Key, false) == 0) _equalKeys = true;
        }
        Advance();
    }

    /// <summary>The current key.</summary>
    public ReadOnlySpan<byte> Key => _currentAtBase ? _base.Key : _delta.Key;

    /// <summary>
    /// BaseDeltaIterator::timestamp: the base iterator's timestamp when positioned on a DB entry, empty for an entry
    /// of the batch (its timestamp is assigned at commit).
    /// </summary>
    public ReadOnlySpan<byte> Timestamp => _currentAtBase ? _base.Timestamp : default;

    /// <summary>The current value (the default column for entities).</summary>
    public ReadOnlySpan<byte> Value
    {
        get
        {
            if (_currentAtBase) return _base.Value;
            switch (_deltaValue)
            {
                case DeltaValue.Plain:
                    return _delta.Value;
                case DeltaValue.Entity:
                    return WideColumnSerialization.GetValueOfDefaultColumn(_delta.Value);
                case DeltaValue.MergedValue:
                    return _mergeResult.WrittenSpan;
                default:
                    return WideColumnSerialization.GetValueOfDefaultColumn(_entityScratch.WrittenSpan);
            }
        }
    }

    /// <summary>True if the current entry is a wide-column entity.</summary>
    public bool IsEntity => _currentAtBase ? _base.IsEntity : _deltaValue is DeltaValue.Entity or DeltaValue.MergedEntity;

    /// <summary>The columns of the current entry (a plain value is a single default column). Reused per access.</summary>
    public WideColumns Columns
    {
        get
        {
            if (_currentAtBase) return _base.Columns;
            switch (_deltaValue)
            {
                case DeltaValue.Entity:
                    _columns.SetSerialized(_delta.Value);
                    break;
                case DeltaValue.MergedEntity:
                    _columns.SetSerialized(_entityScratch.WrittenSpan);
                    break;
                default:
                    {
                        PooledBufferWriter w = ObjectPools.RentWriter();
                        try
                        {
                            WideColumnSerialization.SerializeDefaultColumn(Value, w);
                            _columns.SetSerialized(w.WrittenSpan);
                        }
                        finally
                        {
                            ObjectPools.ReturnWriter(w);
                        }
                        break;
                    }
            }
            return _columns;
        }
    }

    private void Advance()
    {
        if (_equalKeys)
        {
            AdvanceBase();
            AdvanceDelta();
        }
        else if (_currentAtBase)
        {
            AdvanceBase();
        }
        else
        {
            AdvanceDelta();
        }
        UpdateCurrent();
    }

    private void AdvanceDelta()
    {
        if (_forward) _delta.NextKey();
        else _delta.PrevKey();
    }

    private void AdvanceBase()
    {
        if (_forward) _base.Next();
        else _base.Prev();
    }

    private MergeOperator RequireMergeOperator()
    {
        if (_mergeOperator is null) ThrowHelper.InvalidArgument("Merge operator must be set for column family");
        return _mergeOperator!;
    }

    /// <summary>SetValueAndColumnsFromDelta: the delta's value, merged with its Put/PutEntity or the base when needed.</summary>
    private void SetValueFromDelta()
    {
        WriteType type = _delta.Entry(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
        if (_mergeContext.Count == 0)
        {
            _deltaValue = type == WriteType.PutEntity ? DeltaValue.Entity : DeltaValue.Plain;
            return;
        }
        _mergeResult.Clear();
        _entityScratch.Clear();
        switch (type)
        {
            case WriteType.Delete:
            case WriteType.SingleDelete:
                MergeHelper.TimedFullMerge(RequireMergeOperator(), key, false, default, _mergeContext, _mergeResult);
                _deltaValue = DeltaValue.MergedValue;
                break;
            case WriteType.Put:
                MergeHelper.TimedFullMerge(RequireMergeOperator(), key, true, value, _mergeContext, _mergeResult);
                _deltaValue = DeltaValue.MergedValue;
                break;
            case WriteType.PutEntity:
                MergeEntity(key, value);
                break;
            case WriteType.Merge:
                if (_equalKeys)
                {
                    // Merge on top of the base value (or entity).
                    if (_base.IsEntity)
                    {
                        PooledBufferWriter w = ObjectPools.RentWriter();
                        try
                        {
                            w.Write(_base.Columns.Serialized);
                            MergeEntity(key, w.WrittenSpan);
                        }
                        finally
                        {
                            ObjectPools.ReturnWriter(w);
                        }
                    }
                    else
                    {
                        MergeHelper.TimedFullMerge(RequireMergeOperator(), key, true, _base.Value, _mergeContext, _mergeResult);
                        _deltaValue = DeltaValue.MergedValue;
                    }
                }
                else
                {
                    MergeHelper.TimedFullMerge(RequireMergeOperator(), key, false, default, _mergeContext, _mergeResult);
                    _deltaValue = DeltaValue.MergedValue;
                }
                break;
            default:
                ThrowHelper.NotSupported("Unsupported entry type for merge");
                break;
        }
    }

    private void MergeEntity(ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
    {
        MergeHelper.TimedFullMerge(RequireMergeOperator(), key, true, WideColumnSerialization.GetValueOfDefaultColumn(entity), _mergeContext, _mergeResult);
        WideColumnSerialization.ReplaceDefaultColumn(entity, _mergeResult.WrittenSpan, _entityScratch);
        _deltaValue = DeltaValue.MergedEntity;
    }

    /// <summary>BaseDeltaIterator::UpdateCurrent.</summary>
    private void UpdateCurrent()
    {
        while (true)
        {
            var deltaResult = WriteBatchWithIndex.LookupResult.NotFound;
            if (DeltaValid) deltaResult = _delta.FindLatestUpdate(_mergeContext);
            else _mergeContext.Clear();
            _equalKeys = false;
            if (!BaseValid)
            {
                if (!DeltaValid) return; // Finished, neither base nor delta is valid.
                // Base is done, delta has value.
                if (deltaResult == WriteBatchWithIndex.LookupResult.Deleted && _mergeContext.Count == 0)
                {
                    // This key is deleted and no Merge is done after Delete.
                    AdvanceDelta();
                }
                else
                {
                    _currentAtBase = false;
                    SetValueFromDelta();
                    return;
                }
            }
            else if (!DeltaValid)
            {
                // Delta is done, base has value.
                _currentAtBase = true;
                return;
            }
            else
            {
                int compare = (_forward ? 1 : -1) * _cmp.CompareWithoutTimestamp(_delta.Key, false, _base.Key, false);
                if (compare <= 0)
                {
                    // delta bigger or equal
                    if (compare == 0) _equalKeys = true;
                    if (deltaResult != WriteBatchWithIndex.LookupResult.Deleted || _mergeContext.Count > 0)
                    {
                        // delta is visible
                        _currentAtBase = false;
                        SetValueFromDelta();
                        return;
                    }
                    // Delta is less advanced and is delete.
                    AdvanceDelta();
                    if (_equalKeys) AdvanceBase();
                }
                else
                {
                    // base smaller than delta
                    _currentAtBase = true;
                    return;
                }
            }
            // delta <= base and is not visible (most recent update is delete): move on to the next key
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _base.Dispose();
        _mergeContext.Dispose();
        _mergeResult.Dispose();
        _entityScratch.Dispose();
    }
}
