using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Cuckoo;

/// <summary>User-collected property names of cuckoo tables (CuckooTablePropertyNames, table/cuckoo/cuckoo_table_builder.cc).</summary>
public static class CuckooTablePropertyNames
{
    public const string EmptyKey = "rocksdb.cuckoo.bucket.empty.key";
    public const string NumHashFunc = "rocksdb.cuckoo.hash.num";
    public const string HashTableSize = "rocksdb.cuckoo.hash.size";
    public const string ValueLength = "rocksdb.cuckoo.value.length";
    public const string IsLastLevel = "rocksdb.cuckoo.file.islastlevel";
    public const string CuckooBlockSize = "rocksdb.cuckoo.hash.cuckooblocksize";
    public const string IdentityAsFirstHash = "rocksdb.cuckoo.hash.identityfirst";
    public const string UseModuleHash = "rocksdb.cuckoo.hash.usemodule";
    public const string UserKeyLength = "rocksdb.cuckoo.hash.userkeylength";
}

/// <summary>CuckooHash (table/cuckoo/cuckoo_table_factory.h): bucket of a user key for one hash function.</summary>
internal static class CuckooHashing
{
    /// <summary>kCuckooMurmurSeedMultiplier.</summary>
    public const uint MurmurSeedMultiplier = 816922183;

    public static ulong CuckooHash(ReadOnlySpan<byte> userKey, uint hashCnt, bool useModuleHash, ulong tableSize, bool identityAsFirstHash)
    {
        ulong value;
        if (hashCnt == 0 && identityAsFirstHash)
        {
            // *reinterpret_cast<const int64_t*>(user_key.data()) on little-endian hosts.
            if (userKey.Length < 8) ThrowHelper.InvalidArgument("identity_as_first_hash requires user keys of at least 8 bytes");
            value = Coding.DecodeFixed64(userKey);
        }
        else
        {
            value = Hash.MurmurHash64A(userKey, unchecked(MurmurSeedMultiplier * hashCnt));
        }
        return useModuleHash ? value % tableSize : value & (tableSize - 1);
    }
}

/// <summary>
/// Port of CuckooTableBuilder (table/cuckoo/cuckoo_table_builder.{h,cc}) with CuckooTableFactory::NewTableBuilder's
/// settings (at most 64 hash functions). All keys and all values must have the same size; only Put and Delete entries,
/// at most one entry per user key. If the first key has sequence number 0 the file is a "last level" file and stores
/// user keys only. Entries are kept in memory until <see cref="Finish"/> builds the hash table. Table property
/// collectors are not run (upstream's builder takes none).
/// </summary>
public sealed class CuckooTableBuilder : TableBuilder
{
    private const uint MaxVectorIdx = uint.MaxValue; // kMaxVectorIdx
    private const uint MaxNumHashFunc = 64;

    private struct CuckooBucket
    {
        public uint VectorIdx;
        // Accessed only by the MakeSpaceForKey function.
        public uint MakeSpaceForKeyCallId;
    }

    private readonly IWritableFile _file;
    private uint _numHashFunc = 2;
    private readonly double _maxHashTableRatio;
    private readonly uint _maxSearchDepth;
    private readonly uint _cuckooBlockSize;
    private ulong _hashTableSize;
    private bool _isLastLevelFile;
    private bool _hasSeenFirstKey;
    private bool _hasSeenFirstValue;
    private ulong _keySize;
    private ulong _valueSize;
    // A list of fixed-size key-value pairs concatenating into a string. Use GetKey(), GetUserKey(), and GetValue() to
    // retrieve a specific key / value given an index
    private readonly PooledBufferWriter _kvs = new(4096);
    private readonly PooledBufferWriter _deletedKeys = new(256);
    // Number of key-value pairs stored in kvs_ + number of deleted keys
    private ulong _numEntries;
    // Number of keys that contain value (non-deletion op)
    private ulong _numValues;
    private readonly Comparator _ucomp;
    private readonly bool _useModuleHash;
    private readonly bool _identityAsFirstHash;
    private byte[] _smallestUserKey = [];
    private byte[] _largestUserKey = [];
    private readonly TableProperties _props = new();
    private bool _closed;
    private ulong _fileSize;

    public CuckooTableBuilder(IWritableFile file, TableBuilderOptions tableBuilderOptions, CuckooTableOptions tableOptions)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(tableBuilderOptions);
        ArgumentNullException.ThrowIfNull(tableOptions);
        _file = file;
        _maxHashTableRatio = tableOptions.HashTableRatio;
        _maxSearchDepth = tableOptions.MaxSearchDepth;
        _cuckooBlockSize = Math.Max(1u, tableOptions.CuckooBlockSize);
        _useModuleHash = tableOptions.UseModuleHash;
        _identityAsFirstHash = tableOptions.IdentityAsFirstHash;
        _hashTableSize = _useModuleHash ? 0UL : 2UL;
        _ucomp = tableBuilderOptions.Comparator;
        // Data is in a huge block.
        _props.NumDataBlocks = 1;
        _props.IndexSize = 0;
        _props.FilterSize = 0;
        _props.ColumnFamilyId = tableBuilderOptions.ColumnFamilyId;
        _props.ColumnFamilyName = tableBuilderOptions.ColumnFamilyName;
        _props.DbId = tableBuilderOptions.DbId;
        _props.DbSessionId = tableBuilderOptions.DbSessionId;
        _props.OriginalFileNumber = tableBuilderOptions.FileNumber;
    }

    public override ulong NumEntries => _numEntries;
    public override TableProperties Properties => _props;

    /// <summary>Port of CuckooTableBuilder::FileSize: the final size once finished, else the expected size.</summary>
    public override ulong FileSize
    {
        get
        {
            if (_closed) return _fileSize;
            if (_numEntries == 0) return 0;
            if (_useModuleHash) return (ulong)((_keySize + _valueSize) * _numEntries / _maxHashTableRatio);
            // Account for buckets being a power of two. As elements are added, file size remains constant for a while
            // and doubles its size. Since compaction algorithm stops adding elements only after it exceeds the file
            // limit, we account for the extra element being added here.
            ulong expectedHashTableSize = _hashTableSize;
            if (expectedHashTableSize < (_numEntries + 1) / _maxHashTableRatio) expectedHashTableSize *= 2;
            return (_keySize + _valueSize) * expectedHashTableSize - 1;
        }
    }

    /// <summary>Port of CuckooTableBuilder::Add.</summary>
    public override void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        if (_numEntries >= MaxVectorIdx - 1) ThrowHelper.NotSupported("Number of keys in a file must be < 2^32-1");
        if (!DbFormat.TryParseInternalKey(key, out ParsedInternalKey ikey)) ThrowHelper.Corruption("Unable to parse key into internal key. ");
        if (ikey.Type is not (EntryType.Deletion or EntryType.Value))
            ThrowHelper.NotSupported("Unsupported key type " + (int)ikey.Type);
        ReadOnlySpan<byte> userKey = key[..ikey.UserKeyLength];

        // Determine if we can ignore the sequence number and value type from internal keys by looking at sequence
        // number from first key. We assume that if first key has a zero sequence number, then all the remaining keys
        // will have zero seq. no.
        if (!_hasSeenFirstKey)
        {
            _isLastLevelFile = ikey.Sequence == 0;
            _hasSeenFirstKey = true;
            _smallestUserKey = userKey.ToArray();
            _largestUserKey = userKey.ToArray();
            _keySize = (ulong)(_isLastLevelFile ? userKey.Length : key.Length);
        }
        if (_keySize != (ulong)(_isLastLevelFile ? userKey.Length : key.Length)) ThrowHelper.NotSupported("all keys have to be the same size");

        if (ikey.Type == EntryType.Value)
        {
            if (!_hasSeenFirstValue)
            {
                _hasSeenFirstValue = true;
                _valueSize = (ulong)value.Length;
            }
            if (_valueSize != (ulong)value.Length) ThrowHelper.NotSupported("all values have to be the same size");
            _kvs.Write(_isLastLevelFile ? userKey : key);
            _kvs.Write(value);
            ++_numValues;
        }
        else
        {
            _deletedKeys.Write(_isLastLevelFile ? userKey : key);
        }
        ++_numEntries;

        // In order to fill the empty buckets in the hash table, we identify a key which is not used so far
        // (unused_user_key). We determine this by maintaining smallest and largest keys inserted so far in bytewise
        // order and use them to find a key outside this range in Finish() operation. Note that this strategy is
        // independent of user comparator used here.
        if (userKey.SequenceCompareTo(_smallestUserKey) < 0) _smallestUserKey = userKey.ToArray();
        else if (userKey.SequenceCompareTo(_largestUserKey) > 0) _largestUserKey = userKey.ToArray();
        if (!_useModuleHash && _hashTableSize < _numEntries / _maxHashTableRatio) _hashTableSize *= 2;
    }

    private bool IsDeletedKey(ulong idx) => idx >= _numValues;

    private ReadOnlySpan<byte> GetKey(ulong idx)
    {
        if (IsDeletedKey(idx)) return _deletedKeys.WrittenSpan.Slice((int)((idx - _numValues) * _keySize), (int)_keySize);
        return _kvs.WrittenSpan.Slice((int)(idx * (_keySize + _valueSize)), (int)_keySize);
    }

    private ReadOnlySpan<byte> GetUserKey(ulong idx) => _isLastLevelFile ? GetKey(idx) : DbFormat.ExtractUserKey(GetKey(idx));

    private ReadOnlySpan<byte> GetValue(ulong idx, byte[] emptyValue)
    {
        if (IsDeletedKey(idx)) return emptyValue; // std::string(value_size_, 'a')
        return _kvs.WrittenSpan.Slice((int)(idx * (_keySize + _valueSize) + _keySize), (int)_valueSize);
    }

    private ulong Hash(ReadOnlySpan<byte> userKey, uint hashCnt) =>
        CuckooHashing.CuckooHash(userKey, hashCnt, _useModuleHash, _hashTableSize, _identityAsFirstHash);

    /// <summary>Port of CuckooTableBuilder::MakeHashTable.</summary>
    private CuckooBucket[] MakeHashTable()
    {
        ulong n = _hashTableSize + _cuckooBlockSize - 1;
        if (n > int.MaxValue) ThrowHelper.NotSupported("cuckoo hash table too large");
        var buckets = new CuckooBucket[n];
        for (int i = 0; i < buckets.Length; i++) buckets[i].VectorIdx = MaxVectorIdx;
        uint makeSpaceForKeyCallId = 0;
        var hashVals = new List<ulong>();
        for (uint vectorIdx = 0; vectorIdx < _numEntries; vectorIdx++)
        {
            ulong bucketId = 0;
            bool bucketFound = false;
            hashVals.Clear();
            ReadOnlySpan<byte> userKey = GetUserKey(vectorIdx);
            for (uint hashCnt = 0; hashCnt < _numHashFunc && !bucketFound; ++hashCnt)
            {
                ulong hashVal = Hash(userKey, hashCnt);
                // If there is a collision, check next cuckoo_block_size_ locations for empty locations. While checking,
                // if we reach end of the hash table, stop searching and proceed for next hash function.
                for (uint blockIdx = 0; blockIdx < _cuckooBlockSize; ++blockIdx, ++hashVal)
                {
                    if (buckets[hashVal].VectorIdx == MaxVectorIdx)
                    {
                        bucketId = hashVal;
                        bucketFound = true;
                        break;
                    }
                    if (_ucomp.Compare(userKey, GetUserKey(buckets[hashVal].VectorIdx)) == 0)
                        ThrowHelper.NotSupported("Same key is being inserted again.");
                    hashVals.Add(hashVal);
                }
            }
            while (!bucketFound && !MakeSpaceForKey(hashVals, ++makeSpaceForKeyCallId, buckets, ref bucketId))
            {
                // Rehash by increashing number of hash tables.
                if (_numHashFunc >= MaxNumHashFunc) ThrowHelper.NotSupported("Too many collisions. Unable to hash.");
                // We don't really need to rehash the entire table because old hashes are still valid and we only
                // increased the number of hash functions.
                ulong hashVal = Hash(userKey, _numHashFunc);
                ++_numHashFunc;
                for (uint blockIdx = 0; blockIdx < _cuckooBlockSize; ++blockIdx, ++hashVal)
                {
                    if (buckets[hashVal].VectorIdx == MaxVectorIdx)
                    {
                        bucketFound = true;
                        bucketId = hashVal;
                        break;
                    }
                    hashVals.Add(hashVal);
                }
            }
            buckets[bucketId].VectorIdx = vectorIdx;
        }
        return buckets;
    }

    private readonly record struct CuckooNode(ulong BucketId, uint Depth, uint ParentPos);

    /// <summary>
    /// Port of CuckooTableBuilder::MakeSpaceForKey: a BFS over the buckets the keys in <paramref name="hashVals"/>' buckets
    /// could move to; on finding an empty bucket, moves the keys along the path and returns the first-level bucket.
    /// </summary>
    private bool MakeSpaceForKey(List<ulong> hashVals, uint callId, CuckooBucket[] buckets, ref ulong bucketId)
    {
        // This is BFS search tree that is stored simply as a vector. Each node stores the index of parent node in the vector.
        var tree = new List<CuckooNode>();
        // We want to identify already visited buckets in the current method call so that we don't add same buckets
        // again for exploration in the tree.
        for (int hashCnt = 0; hashCnt < _numHashFunc; ++hashCnt)
        {
            // hash_vals[hash_cnt]: upstream indexes the first num_hash_func_ collected buckets.
            if (hashCnt >= hashVals.Count) ThrowHelper.NotSupported("Too many collisions. Unable to hash.");
            ulong bid = hashVals[hashCnt];
            buckets[bid].MakeSpaceForKeyCallId = callId;
            tree.Add(new CuckooNode(bid, 0, 0));
        }
        bool nullFound = false;
        int currPos = 0;
        while (!nullFound && currPos < tree.Count)
        {
            CuckooNode currNode = tree[currPos];
            uint currDepth = currNode.Depth;
            if (currDepth >= _maxSearchDepth) break;
            CuckooBucket currBucket = buckets[currNode.BucketId];
            for (uint hashCnt = 0; hashCnt < _numHashFunc && !nullFound; ++hashCnt)
            {
                ulong childBucketId = Hash(GetUserKey(currBucket.VectorIdx), hashCnt);
                // Iterate inside Cuckoo Block.
                for (uint blockIdx = 0; blockIdx < _cuckooBlockSize; ++blockIdx, ++childBucketId)
                {
                    if (buckets[childBucketId].MakeSpaceForKeyCallId == callId) continue;
                    buckets[childBucketId].MakeSpaceForKeyCallId = callId;
                    tree.Add(new CuckooNode(childBucketId, currDepth + 1, (uint)currPos));
                    if (buckets[childBucketId].VectorIdx == MaxVectorIdx)
                    {
                        nullFound = true;
                        break;
                    }
                }
            }
            ++currPos;
        }

        if (nullFound)
        {
            // There is an empty node in tree.back(). Now, traverse the path from this empty node to top of the tree and
            // at every node in the path, replace child with the parent. Stop when first level is reached in the tree
            // (happens when 0 <= bucket_to_replace_pos < num_hash_func_) and return this location in first level for
            // target key to be inserted.
            uint bucketToReplacePos = (uint)tree.Count - 1;
            while (bucketToReplacePos >= _numHashFunc)
            {
                CuckooNode node = tree[(int)bucketToReplacePos];
                buckets[node.BucketId] = buckets[tree[(int)node.ParentPos].BucketId];
                bucketToReplacePos = node.ParentPos;
            }
            bucketId = tree[(int)bucketToReplacePos].BucketId;
        }
        return nullFound;
    }

    /// <summary>Port of CuckooTableBuilder::Finish.</summary>
    public override void Finish()
    {
        if (_closed) throw new InvalidOperationException("Table builder already finished.");
        _closed = true;
        CuckooBucket[] buckets = [];
        byte[] unusedBucket = [];
        if (_numEntries > 0)
        {
            // Calculate the real hash size if module hash is enabled.
            if (_useModuleHash) _hashTableSize = (ulong)(_numEntries / _maxHashTableRatio);
            buckets = MakeHashTable();
            // Determine unused_user_key to fill empty buckets.
            byte[] unusedUserKey = (byte[])_smallestUserKey.Clone();
            int currPos = unusedUserKey.Length - 1;
            while (currPos >= 0)
            {
                --unusedUserKey[currPos];
                if (unusedUserKey.AsSpan().SequenceCompareTo(_smallestUserKey) < 0) break;
                --currPos;
            }
            if (currPos < 0)
            {
                // Try using the largest key to identify an unused key.
                unusedUserKey = (byte[])_largestUserKey.Clone();
                currPos = unusedUserKey.Length - 1;
                while (currPos >= 0)
                {
                    ++unusedUserKey[currPos];
                    if (unusedUserKey.AsSpan().SequenceCompareTo(_largestUserKey) > 0) break;
                    --currPos;
                }
            }
            if (currPos < 0) ThrowHelper.Corruption("Unable to find unused key");
            if (_isLastLevelFile)
            {
                unusedBucket = unusedUserKey;
            }
            else
            {
                unusedBucket = new byte[unusedUserKey.Length + 8];
                DbFormat.AppendInternalKey(unusedBucket, unusedUserKey, 0, EntryType.Value);
            }
        }
        _props.NumEntries = _numEntries;
        _props.NumDeletions = _numEntries - _numValues;
        _props.FixedKeyLen = _keySize;
        _props.UserCollectedProperties[CuckooTablePropertyNames.ValueLength] = Fixed64(_valueSize);

        ulong bucketSize = _keySize + _valueSize;
        // unused_bucket.resize(bucket_size, 'a')
        byte[] unusedFull = new byte[bucketSize];
        unusedFull.AsSpan().Fill((byte)'a');
        unusedBucket.AsSpan(0, (int)Math.Min((ulong)unusedBucket.Length, bucketSize)).CopyTo(unusedFull);
        byte[] emptyValue = new byte[_valueSize];
        emptyValue.AsSpan().Fill((byte)'a');
        // Write the table.
        using var output = new PooledBufferWriter(64 * 1024);
        uint numAdded = 0;
        foreach (CuckooBucket bucket in buckets)
        {
            if (bucket.VectorIdx == MaxVectorIdx)
            {
                output.Write(unusedFull);
            }
            else
            {
                ++numAdded;
                output.Write(GetKey(bucket.VectorIdx));
                if (_valueSize > 0) output.Write(GetValue(bucket.VectorIdx, emptyValue));
            }
            if (output.WrittenCount >= 256 * 1024)
            {
                _file.Append(output.WrittenSpan);
                output.Clear();
            }
        }
        _props.RawKeySize = numAdded * _props.FixedKeyLen;
        _props.RawValueSize = numAdded * _valueSize;

        ulong offset = (ulong)buckets.Length * bucketSize;
        _props.DataSize = offset;
        // unused_bucket.resize(fixed_key_len)
        _props.UserCollectedProperties[CuckooTablePropertyNames.EmptyKey] = unusedFull.AsSpan(0, (int)_props.FixedKeyLen).ToArray();
        _props.UserCollectedProperties[CuckooTablePropertyNames.NumHashFunc] = Fixed32(_numHashFunc);
        _props.UserCollectedProperties[CuckooTablePropertyNames.HashTableSize] = Fixed64(_hashTableSize);
        _props.UserCollectedProperties[CuckooTablePropertyNames.IsLastLevel] = [_isLastLevelFile ? (byte)1 : (byte)0];
        _props.UserCollectedProperties[CuckooTablePropertyNames.CuckooBlockSize] = Fixed32(_cuckooBlockSize);
        _props.UserCollectedProperties[CuckooTablePropertyNames.IdentityAsFirstHash] = [_identityAsFirstHash ? (byte)1 : (byte)0];
        _props.UserCollectedProperties[CuckooTablePropertyNames.UseModuleHash] = [_useModuleHash ? (byte)1 : (byte)0];
        _props.UserCollectedProperties[CuckooTablePropertyNames.UserKeyLength] = Fixed32((uint)_smallestUserKey.Length);

        // Write meta blocks.
        using var metaIndex = new MetaIndexBuilder();
        using (var pb = new PropertyBlockBuilder())
        {
            _props.SequenceNumberTimeMapping = [];
            _props.AddTo(pb);
            foreach (KeyValuePair<string, byte[]> kv in _props.UserCollectedProperties) pb.Add(kv.Key, kv.Value);
            ReadOnlySpan<byte> propertyBlock = pb.Finish();
            metaIndex.Add(BlockBasedTableConstants.PropertiesBlockName, new BlockHandle(offset, (ulong)propertyBlock.Length));
            output.Write(propertyBlock);
            offset += (ulong)propertyBlock.Length;
        }
        ReadOnlySpan<byte> metaIndexBlock = metaIndex.Finish();
        var metaIndexHandle = new BlockHandle(offset, (ulong)metaIndexBlock.Length);
        output.Write(metaIndexBlock);
        offset += (ulong)metaIndexBlock.Length;
        Span<byte> footer = stackalloc byte[Footer.EncodedLength];
        int len = Footer.BuildForMagic(footer, TableMagicNumbers.CuckooTable, 1, ChecksumType.NoChecksum, metaIndexHandle, BlockHandle.Null);
        output.Write(footer[..len]);
        offset += (ulong)len;
        _file.Append(output.WrittenSpan);
        _file.Flush();
        _fileSize = offset;
    }

    private static byte[] Fixed32(uint v)
    {
        byte[] b = new byte[4];
        Coding.EncodeFixed32(b, v);
        return b;
    }

    private static byte[] Fixed64(ulong v)
    {
        byte[] b = new byte[8];
        Coding.EncodeFixed64(b, v);
        return b;
    }

    public override void Abandon() => _closed = true;

    public override void Dispose()
    {
        _closed = true;
        _kvs.Dispose();
        _deletedKeys.Dispose();
    }
}
