using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Cuckoo;

/// <summary>
/// Port of CuckooTableReader (table/cuckoo/cuckoo_table_reader.{h,cc}). Like upstream, the table can only be read
/// memory-mapped (<see cref="TableReaderOptions.AllowMmapReads"/>, else InvalidArgument "File is not mmaped"); the
/// managed port reads the whole file into memory. Point lookups probe the key's cuckoo blocks; the iterator sorts the
/// occupied buckets on first use. Only one entry per user key is stored, so lookups ignore the snapshot (as upstream).
/// </summary>
public sealed class CuckooTableReader : TableReader
{
    private readonly byte[] _data;
    private readonly TableProperties _props;
    private readonly InternalKeyComparator _icmp;
    private readonly Comparator _ucomp;
    private readonly bool _isLastLevel;
    private readonly bool _identityAsFirstHash;
    private readonly bool _useModuleHash;
    private readonly uint _numHashFunc;
    private readonly byte[] _unusedKey;
    private readonly uint _keyLength;
    private readonly uint _userKeyLength;
    private readonly uint _valueLength;
    private readonly uint _bucketLength;
    private readonly uint _cuckooBlockSize;
    private readonly ulong _tableSize;
    private uint[]? _sortedBucketIds;
    private readonly Lock _sortLock = new();

    private CuckooTableReader(byte[] data, TableProperties props, Comparator ucomp)
    {
        _data = data;
        _props = props;
        _ucomp = ucomp;
        _icmp = ucomp.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(ucomp);
        Dictionary<string, byte[]> user = props.UserCollectedProperties;
        _numHashFunc = Coding.DecodeFixed32(Get(user, CuckooTablePropertyNames.NumHashFunc, 4, "Number of hash functions not found"));
        _unusedKey = Get(user, CuckooTablePropertyNames.EmptyKey, 0, "Empty bucket value not found");
        _keyLength = (uint)props.FixedKeyLen;
        _userKeyLength = Coding.DecodeFixed32(Get(user, CuckooTablePropertyNames.UserKeyLength, 4, "User key length not found"));
        _valueLength = Coding.DecodeFixed32(Get(user, CuckooTablePropertyNames.ValueLength, 4, "Value length not found"));
        _bucketLength = _keyLength + _valueLength;
        _tableSize = Coding.DecodeFixed64(Get(user, CuckooTablePropertyNames.HashTableSize, 8, "Hash table size not found"));
        _isLastLevel = Get(user, CuckooTablePropertyNames.IsLastLevel, 1, "Is last level not found")[0] != 0;
        _identityAsFirstHash = Get(user, CuckooTablePropertyNames.IdentityAsFirstHash, 1, "identity as first hash not found")[0] != 0;
        _useModuleHash = Get(user, CuckooTablePropertyNames.UseModuleHash, 1, "hash type is not found")[0] != 0;
        _cuckooBlockSize = Coding.DecodeFixed32(Get(user, CuckooTablePropertyNames.CuckooBlockSize, 4, "Cuckoo block size not found"));
        if (_unusedKey.Length != _keyLength || (props.NumEntries > 0 && _userKeyLength + (_isLastLevel ? 0u : 8u) != _keyLength))
            ThrowHelper.Corruption("Inconsistent cuckoo table key lengths");
        ulong numBuckets = _tableSize + _cuckooBlockSize - 1;
        if (props.NumEntries > 0 && (_tableSize == 0 || numBuckets * _bucketLength > props.DataSize || props.DataSize > (ulong)data.Length))
            ThrowHelper.Corruption("Cuckoo table hash table size does not match the data size");
    }

    private static byte[] Get(Dictionary<string, byte[]> user, string name, int minLength, string missing)
    {
        if (!user.TryGetValue(name, out byte[]? v)) ThrowHelper.Corruption(missing);
        if (v.Length < minLength) ThrowHelper.Corruption("Bad cuckoo table property " + name);
        return v;
    }

    public override TableProperties Properties => _props;
    public override long FileSize => _data.Length;
    internal override InternalKeyComparator InternalComparator => _icmp;

    /// <summary>
    /// Port of CuckooTableReader's constructor (CuckooTableFactory::NewTableReader). Takes ownership of
    /// <paramref name="file"/> (read fully, then closed).
    /// </summary>
    public static CuckooTableReader Open(IRandomAccessFile file, TableReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        byte[] data;
        Footer footer;
        try
        {
            if (!options.AllowMmapReads) ThrowHelper.InvalidArgument("File is not mmaped");
            footer = TableMetaReader.ReadFooter(file, TableMagicNumbers.CuckooTable);
            // TODO (upstream): rate limit reads of whole cuckoo tables.
            data = TableMetaReader.ReadAll(file);
        }
        finally
        {
            file.Dispose();
        }
        Dictionary<string, BlockHandle> metaBlocks = TableMetaReader.ReadMetaIndex(data, footer);
        TableProperties props = TableMetaReader.ReadProperties(data, metaBlocks);
        return new CuckooTableReader(data, props, options.Comparator);
    }

    private ulong Hash(ReadOnlySpan<byte> userKey, uint hashCnt) =>
        CuckooHashing.CuckooHash(userKey, hashCnt, _useModuleHash, _tableSize, _identityAsFirstHash);

    /// <summary>
    /// Port of CuckooTableReader::Get: compares only the user key part, as only one entry per user key is stored and
    /// snapshots are not supported. Last-level files report the entry with kMaxSequenceNumber.
    /// </summary>
    public override void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        // Upstream asserts key.size() == key_length_ + (is_last_level_ ? 8 : 0); other sizes cannot be in the table.
        if ((uint)userKey.Length != _userKeyLength || _props.NumEntries == 0) return;
        ReadOnlySpan<byte> unusedUserKey = _unusedKey.AsSpan(0, userKey.Length);
        for (uint hashCnt = 0; hashCnt < _numHashFunc; ++hashCnt)
        {
            ulong offset = _bucketLength * Hash(userKey, hashCnt);
            for (uint blockIdx = 0; blockIdx < _cuckooBlockSize; ++blockIdx, offset += _bucketLength)
            {
                ReadOnlySpan<byte> bucket = _data.AsSpan((int)offset, (int)_bucketLength);
                if (_ucomp.Equal(unusedUserKey, bucket[..userKey.Length])) return;
                // Here, we compare only the user key part as we support only one entry per user key and we don't
                // support snapshot.
                if (_ucomp.Equal(userKey, bucket[..userKey.Length]))
                {
                    ReadOnlySpan<byte> value = bucket.Slice((int)_keyLength, (int)_valueLength);
                    if (_isLastLevel)
                    {
                        // Sequence number is not stored at the last level, so we will use kMaxSequenceNumber since it
                        // is unknown (GetContext::SaveValue(value, kMaxSequenceNumber) reports a value).
                        VisitLastLevel(userKey, value, ref state, visitor);
                    }
                    else
                    {
                        ReadOnlySpan<byte> fullKey = bucket[..(int)_keyLength];
                        if (!DbFormat.TryParseInternalKey(fullKey, out _)) ThrowHelper.Corruption("Corrupted key in cuckoo table");
                        visitor(ref state, fullKey, value);
                    }
                    // We don't support merge operations. So, we return here.
                    return;
                }
            }
        }
    }

    private static void VisitLastLevel<TState>(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value, ref TState state, TableEntryVisitor<TState> visitor)
        where TState : allows ref struct
    {
        int n = userKey.Length + 8;
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(n);
        try
        {
            DbFormat.AppendInternalKey(buf, userKey, DbFormat.MaxSequenceNumber, EntryType.Value);
            visitor(ref state, buf.AsSpan(0, n), value);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <summary>InitIfNeeded of the iterator: the occupied buckets sorted by user key (shared by all iterators).</summary>
    private uint[] SortedBucketIds()
    {
        uint[]? ids = Volatile.Read(ref _sortedBucketIds);
        if (ids is not null) return ids;
        lock (_sortLock)
        {
            if (_sortedBucketIds is not null) return _sortedBucketIds;
            var list = new List<uint>((int)Math.Min(_props.NumEntries, int.MaxValue));
            if (_props.NumEntries > 0)
            {
                ulong numBuckets = _tableSize + _cuckooBlockSize - 1;
                if (numBuckets >= uint.MaxValue) ThrowHelper.Corruption("too many cuckoo buckets");
                for (uint bucketId = 0; bucketId < numBuckets; ++bucketId)
                {
                    if (!_data.AsSpan((int)(bucketId * _bucketLength), (int)_keyLength).SequenceEqual(_unusedKey)) list.Add(bucketId);
                }
            }
            uint[] sorted = list.ToArray();
            Array.Sort(sorted, new BucketComparer(this));
            Volatile.Write(ref _sortedBucketIds, sorted);
            return sorted;
        }
    }

    private ReadOnlySpan<byte> BucketUserKey(uint bucketId) => _data.AsSpan((int)(bucketId * _bucketLength), (int)_userKeyLength);

    private sealed class BucketComparer(CuckooTableReader reader) : IComparer<uint>
    {
        public int Compare(uint x, uint y) => reader._ucomp.Compare(reader.BucketUserKey(x), reader.BucketUserKey(y));
    }

    public override InternalIterator NewInternalIterator(TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor) =>
        new CuckooTableIterator(this);

    public override void Dispose() { }

    /// <summary>Port of CuckooTableIterator (table/cuckoo/cuckoo_table_reader.cc).</summary>
    private sealed class CuckooTableIterator : InternalIterator
    {
        private const uint InvalidIndex = uint.MaxValue;
        private readonly CuckooTableReader _reader;
        private uint[] _sortedBucketIds = [];
        private bool _initialized;
        private uint _currKeyIdx = InvalidIndex;
        private byte[] _currKey = new byte[64];
        private int _currKeyLength;
        private int _currValueOffset;

        public CuckooTableIterator(CuckooTableReader reader) => _reader = reader;

        private void InitIfNeeded()
        {
            if (_initialized) return;
            _sortedBucketIds = _reader.SortedBucketIds();
            _currKeyIdx = InvalidIndex;
            _initialized = true;
        }

        public override bool Valid => _currKeyIdx < (uint)_sortedBucketIds.Length;
        public override ReadOnlySpan<byte> Key => _currKey.AsSpan(0, _currKeyLength);
        public override ReadOnlySpan<byte> Value => _reader._data.AsSpan(_currValueOffset, (int)_reader._valueLength);

        public override void SeekToFirst()
        {
            InitIfNeeded();
            _currKeyIdx = 0;
            PrepareKVAtCurrIdx();
        }

        public override void SeekToLast()
        {
            InitIfNeeded();
            _currKeyIdx = (uint)_sortedBucketIds.Length - 1;
            PrepareKVAtCurrIdx();
        }

        public override void Seek(ReadOnlySpan<byte> target)
        {
            InitIfNeeded();
            ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(target);
            // std::lower_bound with the bucket comparator over the user key prefix of each bucket.
            int lo = 0, hi = _sortedBucketIds.Length;
            int len = (int)_reader._userKeyLength;
            Comparator ucmp = _reader._ucomp;
            while (lo < hi)
            {
                int mid = (int)((uint)(lo + hi) >> 1);
                ReadOnlySpan<byte> k = _reader.BucketUserKey(_sortedBucketIds[mid]);
                // BucketComparator compares user_key_len_ bytes of the target too (the target is a full-length user key).
                ReadOnlySpan<byte> t = userKey.Length > len ? userKey[..len] : userKey;
                if (ucmp.Compare(k, t) < 0) lo = mid + 1;
                else hi = mid;
            }
            _currKeyIdx = (uint)lo;
            PrepareKVAtCurrIdx();
        }

        /// <summary>Upstream's SeekForPrev only asserts (not supported); the port reports NotSupported.</summary>
        public override void SeekForPrev(ReadOnlySpan<byte> target)
        {
            _currKeyIdx = InvalidIndex;
            ThrowHelper.NotSupported("SeekForPrev() is not supported in CuckooTable");
        }

        private void PrepareKVAtCurrIdx()
        {
            if (!Valid)
            {
                _currKeyLength = 0;
                return;
            }
            uint id = _sortedBucketIds[_currKeyIdx];
            int offset = (int)(id * _reader._bucketLength);
            byte[] data = _reader._data;
            if (_reader._isLastLevel)
            {
                // Always return internal key.
                int ukl = (int)_reader._userKeyLength;
                EnsureKey(ukl + 8);
                DbFormat.AppendInternalKey(_currKey, data.AsSpan(offset, ukl), 0, EntryType.Value);
                _currKeyLength = ukl + 8;
            }
            else
            {
                int kl = (int)_reader._keyLength;
                EnsureKey(kl);
                data.AsSpan(offset, kl).CopyTo(_currKey);
                _currKeyLength = kl;
            }
            _currValueOffset = offset + (int)_reader._keyLength;
        }

        private void EnsureKey(int n)
        {
            if (_currKey.Length < n) _currKey = new byte[n * 2];
        }

        public override void Next()
        {
            if (!Valid)
            {
                _currKeyLength = 0;
                return;
            }
            ++_currKeyIdx;
            PrepareKVAtCurrIdx();
        }

        public override void Prev()
        {
            if (_currKeyIdx == 0) _currKeyIdx = (uint)_sortedBucketIds.Length;
            if (!Valid)
            {
                _currKeyLength = 0;
                return;
            }
            --_currKeyIdx;
            PrepareKVAtCurrIdx();
        }
    }
}
