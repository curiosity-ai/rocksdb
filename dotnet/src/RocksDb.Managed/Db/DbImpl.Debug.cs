using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    /// <summary>
    /// Port of GetAllKeyVersions (utilities/debug.cc): walks the internal iterator of <paramref name="cfd"/> (all
    /// memtables and table files, every version of every key) from <paramref name="beginKey"/> (null = first key) up
    /// to and including user key <paramref name="endKey"/> (null = last key), adding at most
    /// <paramref name="maxNumIkeys"/> entries to <paramref name="result"/>.
    /// </summary>
    /// <remarks>
    /// As upstream: with user-defined timestamps the bounds get the maximum timestamp appended
    /// (MaybeAddTimestampsToRange with an exclusive end), so versions of <paramref name="endKey"/> itself are excluded
    /// (unless their timestamp is the maximum); the limit check runs after an entry is added, so
    /// <paramref name="maxNumIkeys"/> = 0 still returns one entry. Keys covered by a range tombstone are skipped and
    /// tombstones are not reported, like upstream's MergingIterator with range tombstones.
    /// </remarks>
    internal void GetAllKeyVersions(ColumnFamilyData cfd, byte[]? beginKey, byte[]? endKey, int maxNumIkeys, KeyVersionCollection result)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(result);
        ArgumentOutOfRangeException.ThrowIfNegative(maxNumIkeys);
        result.Clear();
        InternalKeyComparator icmp = cfd.Icmp;
        int tsSize = icmp.UserComparator.TimestampSize;

        using var beginBuf = new PooledBufferWriter(64);
        using var endBuf = new PooledBufferWriter(64);
        if (beginKey is not null)
        {
            // Maximum timestamp means including all keys with any timestamp for start.
            if (tsSize == 0) beginBuf.Write(beginKey);
            else DbFormat.AppendKeyWithMaxTimestamp(beginBuf, beginKey, tsSize);
        }
        if (endKey is not null)
        {
            if (tsSize == 0) endBuf.Write(endKey);
            else DbFormat.AppendKeyWithMaxTimestamp(endBuf, endKey, tsSize);
        }

        DbIteratorSources sources = NewInternalIteratorSources(cfd);
        try
        {
            var rangeDel = new RangeDelAggregator(icmp, DbFormat.MaxSequenceNumber);
            using InternalIterator iter = DbIteratorFactory.NewInternalIterator(sources, rangeDel);
            if (beginKey is not null)
            {
                // InternalKey::SetMinPossibleForUserKey
                byte[] seek = new byte[beginBuf.WrittenCount + DbFormat.InternalKeyFooterSize];
                DbFormat.AppendInternalKey(seek, beginBuf.WrittenSpan, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                iter.Seek(seek);
            }
            else
            {
                iter.SeekToFirst();
            }

            int numKeys = 0;
            for (; iter.Valid; iter.Next())
            {
                ReadOnlySpan<byte> ikey = iter.Key;
                if (!DbFormat.TryParseInternalKey(ikey, out ParsedInternalKey parsed))
                {
                    ThrowHelper.Corruption("Corrupted Key: Internal Key too small or invalid type. Size: " + ikey.Length);
                }
                ReadOnlySpan<byte> userKey = ikey[..parsed.UserKeyLength];
                if (endKey is not null && icmp.UserComparator.Compare(userKey, endBuf.WrittenSpan) > 0) break;
                if (!rangeDel.IsEmpty && rangeDel.ShouldDelete(userKey, parsed.Sequence)) continue;

                result.Add(userKey, iter.Value, parsed.Sequence, parsed.Type);
                if (++numKeys >= maxNumIkeys) break;
            }
        }
        finally
        {
            sources.Cleanup?.Dispose();
        }
    }

    /// <summary>
    /// The sources of DBImpl::NewInternalIterator(read_options, arena, kMaxSequenceNumber, cfh): the referenced super
    /// version's memtables and files in total order (no prefix seek), without the read-timestamp validation.
    /// </summary>
    private DbIteratorSources NewInternalIteratorSources(ColumnFamilyData cfd)
    {
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        var mems = new MemTable[1 + sv.Imm.Length];
        mems[0] = sv.Mem;
        sv.Imm.CopyTo(mems, 1);
        VersionStorageInfo s = sv.Current.Storage;
        var levels = new IReadOnlyList<FileMetaData>[s.NumLevels];
        for (int l = 0; l < levels.Length; l++) levels[l] = s.LevelFilesArray(l);
        return new DbIteratorSources
        {
            Comparator = cfd.Icmp,
            MemTables = mems,
            Levels = levels,
            TableCache = cfd.TableCache,
            Cleanup = new SuperVersionRelease(this, sv, null),
        };
    }
}
