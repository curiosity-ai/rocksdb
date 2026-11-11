using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Cache;

/// <summary>
/// File-based persistent cache tier. Port of BlockCacheTier (utilities/persistent_cache/block_cache_tier.{h,cc}),
/// BlockCacheFile / WriteableCacheFile / RandomAccessCacheFile / ThreadedWriter (block_cache_tier_file.{h,cc}),
/// CacheWriteBuffer(Allocator) (block_cache_tier_file_buffer.h) and BlockCacheTierMetadata (block_cache_tier_metadata.cc).
/// <para>
/// Layout: cache files <c>&lt;path&gt;/cache/&lt;cache id&gt;.rc</c> of up to <c>cache_file_size</c> bytes, written
/// sequentially in write buffers (the last one zero-padded to 4 KiB); each record is a 16-byte little-endian header
/// (magic 0xfefa, crc32c of header-with-zero-crc + key + value, key size, value size), the key and the value. The index
/// maps a key to (cache id, offset, size). Records still in write buffers are read from memory. The cache is not
/// persistent across restarts: <see cref="Open"/> deletes existing <c>*.rc</c> files (as upstream).
/// Eviction removes whole files, least recently used first, until 10% of the cache is free.
/// </para>
/// </summary>
public sealed class BlockCacheTier : PersistentCacheTier
{
    private const int EvictPct = 10;
    private const int MaxRetry = 3;
    internal const uint RecordMagic = 0xfefa;
    internal const int RecordHeaderSize = 16;
    internal const int FileAlignmentSize = 4 * 1024;

    private readonly PersistentCacheConfig _opt;
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.SupportsRecursion);
    // Keys are arbitrary byte strings, as upstream (BlockInfo::key_ is a std::string); the DB passes 16-byte cache keys.
    private readonly Dictionary<byte[], Lba> _blockIndex = new(Utilities.Transactions.ByteKeyComparer.Instance);
    private readonly Dictionary<uint, CacheFile> _files = new();
    private readonly LinkedList<CacheFile> _fileLru = new(); // first = most recently used
    private readonly WriteBufferAllocator _bufferAllocator;
    private BlockingCollection<InsertOp>? _insertOps;
    private Thread? _insertThread;
    private Writer? _writer;
    private CacheFile? _cacheFile;
    private uint _writerCacheId;
    private long _size;
    private long _pendingBytes;
    private long _insertDropped, _cacheHits, _cacheMisses, _cacheErrors, _bytesWritten, _bytesRead;
    private int _opened;

    internal readonly record struct Lba(uint CacheId, long Offset, int Size);

    private readonly record struct InsertOp(byte[]? Key, byte[]? Data, bool Quit);

    public BlockCacheTier(PersistentCacheConfig config)
    {
        _opt = config ?? throw new ArgumentNullException(nameof(config));
        _bufferAllocator = new WriteBufferAllocator(config.WriteBufferSize, Math.Max(1, config.WriteBufferCount));
    }

    private string CachePath => _opt.Path + "/cache";

    public override bool IsCompressed => _opt.IsCompressed;

    public override string GetPrintableOptions() => "    cache_type: block_cache_tier\n" + _opt.ToString();

    public override void Open()
    {
        if (Interlocked.Exchange(ref _opened, 1) != 0) return;
        _opt.Validate();
        Env.IFileSystem fs = _opt.FileSystem;
        fs.CreateDirectoryIfMissing(_opt.Path);
        // Create base/<cache dir> directory, or clean up an existing one.
        fs.CreateDirectoryIfMissing(CachePath);
        foreach (string f in fs.GetChildren(CachePath))
        {
            if (Path.GetExtension(f) == ".rc") fs.DeleteFile(CachePath + "/" + Path.GetFileName(f));
        }
        _writer = new Writer(this, _opt.WriterQueueDepth, _opt.WriterDispatchSize);
        _lock.EnterWriteLock();
        try
        {
            NewCacheFile();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
        if (_opt.PipelineWrites)
        {
            _insertOps = new BlockingCollection<InsertOp>();
            _insertThread = new Thread(InsertMain) { IsBackground = true, Name = "rocksdb:pcache-insert" };
            _insertThread.Start();
        }
        base.Open();
    }

    public override void Close()
    {
        if (Interlocked.Exchange(ref _opened, 0) == 0) return;
        if (_insertOps is not null)
        {
            // Queued inserts are processed first (as upstream). The insert thread waits for free write buffers, which the
            // writer threads release; if they cannot make progress for 30 s, stop waiting and drop the remaining inserts.
            _insertOps.Add(new InsertOp(null, null, true));
            if (!_insertThread!.Join(TimeSpan.FromSeconds(30)))
            {
                _bufferAllocator.Shutdown();
                _insertThread.Join();
            }
            _insertOps.Dispose();
            _insertOps = null;
        }
        _writer?.Stop();
        _lock.EnterWriteLock();
        try
        {
            foreach (CacheFile f in _files.Values) f.CloseFile();
            _files.Clear();
            _fileLru.Clear();
            _blockIndex.Clear();
            _cacheFile = null;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
        base.Close();
    }

    public override bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        if (key.IsEmpty || data.IsEmpty) ThrowHelper.InvalidArgument("empty key or data");
        if (_insertOps is { } q)
        {
            // Off load the write to the write thread; drop it when the backlog is too large.
            // BoundedQueue::Push: an op (InsertOp::Size = key + data) is discarded when size_ + t.Size() >= max_size_.
            long opSize = (long)key.Length + data.Length;
            if (Interlocked.Add(ref _pendingBytes, opSize) >= _opt.MaxWritePipelineBacklogSize)
            {
                Interlocked.Add(ref _pendingBytes, -opSize);
                Interlocked.Increment(ref _insertDropped);
                return false;
            }
            q.Add(new InsertOp(key.ToArray(), data.ToArray(), false));
            return true;
        }
        return InsertImpl(key, data);
    }

    private void InsertMain()
    {
        BlockingCollection<InsertOp> q = _insertOps!;
        while (true)
        {
            InsertOp op = q.Take();
            if (op.Quit) break;
            bool ok = false;
            for (int retry = 0; retry <= MaxRetry; retry++)
            {
                try
                {
                    if (InsertImpl(op.Key, op.Data)) { ok = true; break; }
                }
                catch (IOException)
                {
                    break;
                }
                // This can happen when the buffers are full: wait till some buffers are free.
                _bufferAllocator.WaitUntilUsable();
            }
            Interlocked.Add(ref _pendingBytes, -((long)op.Key!.Length + op.Data!.Length));
            if (!ok) Interlocked.Increment(ref _insertDropped);
        }
    }

    // InsertImpl: false = TryAgain (no buffer space).
    private bool InsertImpl(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        _lock.EnterWriteLock();
        try
        {
            // The key already exists: duplicate insert.
            if (_blockIndex.GetAlternateLookup<ReadOnlySpan<byte>>().ContainsKey(key)) return true;
            Lba lba;
            while (!_cacheFile!.Append(key, data, out lba))
            {
                if (!_cacheFile.Eof) return false;
                NewCacheFile();
            }
            byte[] k = key.ToArray();
            _blockIndex[k] = lba;
            _cacheFile.Keys.Add(k);
            Interlocked.Add(ref _bytesWritten, data.Length);
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public override bool TryLookup(ReadOnlySpan<byte> key, out byte[] data, out int length)
    {
        CacheFile? file;
        Lba lba;
        _lock.EnterReadLock();
        try
        {
            if (!_blockIndex.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(key, out lba) || !_files.TryGetValue(lba.CacheId, out file))
            {
                Interlocked.Increment(ref _cacheMisses);
                return Miss(key, out data, out length);
            }
            Interlocked.Increment(ref file.Refs);
        }
        finally
        {
            _lock.ExitReadLock();
        }
        TouchFile(file);
        byte[] scratch = RocksArrayPool<byte>.Shared.Rent(lba.Size);
        try
        {
            bool ok;
            data = [];
            length = 0;
            try
            {
                ok = file.Read(lba, scratch) && ParseRecord(scratch.AsSpan(0, lba.Size), key, out int valOffset, out int valSize)
                    && CopyOut(scratch.AsSpan(valOffset, valSize), out data, out length);
            }
            catch (IOException)
            {
                ok = false;
                data = [];
                length = 0;
            }
            if (!ok)
            {
                Interlocked.Increment(ref _cacheMisses);
                Interlocked.Increment(ref _cacheErrors);
                return Miss(key, out data, out length);
            }
            Interlocked.Add(ref _bytesRead, length);
            Interlocked.Increment(ref _cacheHits);
            return true;
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(scratch);
            Interlocked.Decrement(ref file.Refs);
        }
    }

    /// <summary>
    /// Async lookup: the index and the write buffers are consulted synchronously; a record in a closed cache file is
    /// read with async I/O (the file stays referenced, so it cannot be evicted meanwhile). Misses go to the next tier.
    /// </summary>
    public override ValueTask<PersistentCacheLookupResult> LookupAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken)
    {
        CacheFile? file;
        Lba lba;
        _lock.EnterReadLock();
        try
        {
            if (!_blockIndex.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(key.Span, out lba) || !_files.TryGetValue(lba.CacheId, out file))
            {
                Interlocked.Increment(ref _cacheMisses);
                return MissAsync(key, cancellationToken);
            }
            Interlocked.Increment(ref file.Refs);
        }
        finally
        {
            _lock.ExitReadLock();
        }
        TouchFile(file);
        byte[] scratch = RocksArrayPool<byte>.Shared.Rent(lba.Size);
        bool releaseHere = true;
        try
        {
            Env.IRandomAccessFile? reader;
            bool ok;
            try
            {
                ok = file.TryReadBuffered(lba, scratch, out reader);
            }
            catch (IOException)
            {
                ok = false;
                reader = null;
            }
            if (reader is not null)
            {
                releaseHere = false;
                return ReadRecordAsync(file, reader, lba, scratch, key, cancellationToken);
            }
            if (!ok) return ErrorMissAsync(key, cancellationToken);
            PersistentCacheLookupResult result = Found(scratch, lba, key.Span);
            return result.Found ? new ValueTask<PersistentCacheLookupResult>(result) : MissAsync(key, cancellationToken);
        }
        finally
        {
            if (releaseHere)
            {
                RocksArrayPool<byte>.Shared.Return(scratch);
                Interlocked.Decrement(ref file.Refs);
            }
        }
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<PersistentCacheLookupResult> ReadRecordAsync(CacheFile file, Env.IRandomAccessFile reader, Lba lba, byte[] scratch,
        ReadOnlyMemory<byte> key, CancellationToken cancellationToken)
    {
        bool ok;
        try
        {
            int total = 0;
            while (total < lba.Size)
            {
                int r = await reader.ReadAsync(lba.Offset + total, scratch.AsMemory(total, lba.Size - total), cancellationToken).ConfigureAwait(false);
                if (r <= 0) break;
                total += r;
            }
            ok = total == lba.Size;
        }
        catch (Exception e) when (e is IOException or ObjectDisposedException)
        {
            ok = false;
        }
        try
        {
            if (ok)
            {
                PersistentCacheLookupResult found = Found(scratch, lba, key.Span);
                if (found.Found) return found;
            }
            else
            {
                Interlocked.Increment(ref _cacheMisses);
                Interlocked.Increment(ref _cacheErrors);
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(scratch);
            Interlocked.Decrement(ref file.Refs);
        }
        return await MissAsync(key, cancellationToken).ConfigureAwait(false);
    }

    // Parses the record read into scratch; counts the hit, or the miss + error of a bad record (Found = false).
    private PersistentCacheLookupResult Found(byte[] scratch, in Lba lba, ReadOnlySpan<byte> key)
    {
        if (ParseRecord(scratch.AsSpan(0, lba.Size), key, out int valOffset, out int valSize))
        {
            CopyOut(scratch.AsSpan(valOffset, valSize), out byte[] data, out int length);
            Interlocked.Add(ref _bytesRead, length);
            Interlocked.Increment(ref _cacheHits);
            return new PersistentCacheLookupResult(true, data, length);
        }
        Interlocked.Increment(ref _cacheMisses);
        Interlocked.Increment(ref _cacheErrors);
        return default;
    }

    private ValueTask<PersistentCacheLookupResult> ErrorMissAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _cacheMisses);
        Interlocked.Increment(ref _cacheErrors);
        return MissAsync(key, cancellationToken);
    }

    private ValueTask<PersistentCacheLookupResult> MissAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken) =>
        NextTier is { } next ? next.LookupAsync(key, cancellationToken) : new ValueTask<PersistentCacheLookupResult>(new PersistentCacheLookupResult(false, [], 0));

    private bool Miss(ReadOnlySpan<byte> key, out byte[] data, out int length)
    {
        if (NextTier is { } next) return next.TryLookup(key, out data, out length);
        data = [];
        length = 0;
        return false;
    }

    private static bool CopyOut(ReadOnlySpan<byte> value, out byte[] data, out int length)
    {
        data = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, value.Length));
        value.CopyTo(data);
        length = value.Length;
        return true;
    }

    public override bool Erase(ReadOnlySpan<byte> key)
    {
        _lock.EnterWriteLock();
        try
        {
            _blockIndex.GetAlternateLookup<ReadOnlySpan<byte>>().Remove(key);
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void TouchFile(CacheFile f)
    {
        lock (_fileLru)
        {
            if (f.LruNode.List is null) return;
            _fileLru.Remove(f.LruNode);
            _fileLru.AddFirst(f.LruNode);
        }
    }

    // NewCacheFile. REQUIRES: write lock.
    private void NewCacheFile()
    {
        var f = new CacheFile(this, _writerCacheId, CachePath + "/" + _writerCacheId + ".rc", _opt.CacheFileSize);
        f.Create();
        _writerCacheId++;
        _cacheFile = f;
        _files[f.CacheId] = f;
        lock (_fileLru) _fileLru.AddFirst(f.LruNode);
    }

    /// <summary>
    /// Reserves <paramref name="size"/> bytes for a write; when full, evicts least recently used closed files until
    /// (100 - kEvictPct)% of the capacity is used. False when nothing is evictable.
    /// </summary>
    public override bool Reserve(long size)
    {
        _lock.EnterWriteLock();
        try
        {
            if (size + _size <= _opt.CacheSize)
            {
                _size += size;
                return true;
            }
            double retainFac = (100 - EvictPct) / 100.0;
            while (size + _size > _opt.CacheSize * retainFac)
            {
                CacheFile? victim = null;
                lock (_fileLru)
                {
                    for (LinkedListNode<CacheFile>? n = _fileLru.Last; n is not null; n = n.Previous)
                    {
                        CacheFile f = n.Value;
                        if (f != _cacheFile && f.IsEvictable && Volatile.Read(ref f.Refs) == 0)
                        {
                            victim = f;
                            _fileLru.Remove(n);
                            break;
                        }
                    }
                }
                if (victim is null) return false; // nothing is evictable
                _files.Remove(victim.CacheId);
                foreach (byte[] k in victim.Keys)
                {
                    if (_blockIndex.TryGetValue(k, out Lba l) && l.CacheId == victim.CacheId) _blockIndex.Remove(k);
                }
                long fileSize = victim.Delete();
                _size -= Math.Min(fileSize, _size);
            }
            _size += size;
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void ForceReserve(long size)
    {
        _lock.EnterWriteLock();
        try
        {
            _size += size;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>Waits until queued inserts and buffer writes are done (TEST_Flush).</summary>
    public override void Flush()
    {
        var sw = Stopwatch.StartNew();
        while ((Volatile.Read(ref _pendingBytes) > 0 || (_insertOps?.Count ?? 0) > 0 || (_writer?.Pending ?? 0) > 0) && sw.ElapsedMilliseconds < 30000)
            Thread.Sleep(1);
        base.Flush();
    }

    /// <summary>Number of cache files (open or closed).</summary>
    public int FileCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _files.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    public override IReadOnlyList<IReadOnlyDictionary<string, double>> Stats()
    {
        long hits = Interlocked.Read(ref _cacheHits), misses = Interlocked.Read(ref _cacheMisses), lookups = hits + misses;
        var stats = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["persistentcache.blockcachetier.bytes_written"] = Interlocked.Read(ref _bytesWritten),
            ["persistentcache.blockcachetier.bytes_read"] = Interlocked.Read(ref _bytesRead),
            ["persistentcache.blockcachetier.insert_dropped"] = Interlocked.Read(ref _insertDropped),
            ["persistentcache.blockcachetier.cache_hits"] = hits,
            ["persistentcache.blockcachetier.cache_misses"] = misses,
            ["persistentcache.blockcachetier.cache_errors"] = Interlocked.Read(ref _cacheErrors),
            ["persistentcache.blockcachetier.cache_hits_pct"] = lookups == 0 ? 0 : 100.0 * hits / lookups,
            ["persistentcache.blockcachetier.cache_misses_pct"] = lookups == 0 ? 0 : 100.0 * misses / lookups,
        };
        var outList = new List<IReadOnlyDictionary<string, double>>(base.Stats()) { stats };
        return outList;
    }

    // ---- record format (CacheRecord) -----------------------------------------------------------------

    internal static uint ComputeRecordCrc(ReadOnlySpan<byte> header, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Span<byte> tmp = stackalloc byte[RecordHeaderSize];
        header.CopyTo(tmp);
        BinaryPrimitives.WriteUInt32LittleEndian(tmp[4..], 0);
        uint crc = Crc32C.Extend(0, tmp);
        crc = Crc32C.Extend(crc, key);
        return Crc32C.Extend(crc, value);
    }

    /// <summary>CacheRecord::Deserialize: validates size, magic and crc; returns the value's position.</summary>
    internal static bool ParseRecord(ReadOnlySpan<byte> data, ReadOnlySpan<byte> expectedKey, out int valueOffset, out int valueSize)
    {
        valueOffset = valueSize = 0;
        if (data.Length < RecordHeaderSize) return false;
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data);
        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        int keySize = (int)BinaryPrimitives.ReadUInt32LittleEndian(data[8..]);
        int valSize = (int)BinaryPrimitives.ReadUInt32LittleEndian(data[12..]);
        if ((long)keySize + valSize + RecordHeaderSize != data.Length) return false;
        ReadOnlySpan<byte> key = data.Slice(RecordHeaderSize, keySize);
        ReadOnlySpan<byte> value = data.Slice(RecordHeaderSize + keySize, valSize);
        if (magic != RecordMagic || ComputeRecordCrc(data[..RecordHeaderSize], key, value) != crc) return false;
        if (!key.SequenceEqual(expectedKey)) return false;
        valueOffset = RecordHeaderSize + keySize;
        valueSize = valSize;
        return true;
    }

    // ---- cache files ---------------------------------------------------------------------------------

    /// <summary>A cache file: written through write buffers, then read from disk (WriteableCacheFile + RandomAccessCacheFile).</summary>
    private sealed class CacheFile
    {
        private readonly BlockCacheTier _tier;
        private readonly string _path;
        private readonly int _maxSize;
        private readonly Lock _rw = new();
        private readonly List<byte[]> _bufs = new();
        private readonly List<int> _bufUsed = new();
        private Env.IWritableFile? _file;
        private Env.IRandomAccessFile? _reader;
        private int _bufWoff;   // buffer being written
        private int _bufDoff;   // next buffer to dispatch
        private long _diskWoff; // logical write offset
        private int _pendingIos;
        private bool _eof;
        private bool _openFailed;
        public int Refs;
        public readonly uint CacheId;
        public readonly List<byte[]> Keys = new();
        public readonly LinkedListNode<CacheFile> LruNode;

        public CacheFile(BlockCacheTier tier, uint cacheId, string path, int maxSize)
        {
            _tier = tier;
            CacheId = cacheId;
            _path = path;
            _maxSize = maxSize;
            LruNode = new LinkedListNode<CacheFile>(this);
        }

        public bool Eof
        {
            get
            {
                lock (_rw) return _eof;
            }
        }

        /// <summary>Fully written (readable, or failed to reopen for reading): may be evicted once unreferenced.</summary>
        public bool IsEvictable
        {
            get
            {
                lock (_rw) return _reader is not null || _openFailed;
            }
        }

        public void Create() => _file = _tier._opt.FileSystem.CreateWritable(_path, truncate: true);

        public bool Append(ReadOnlySpan<byte> key, ReadOnlySpan<byte> val, out Lba lba)
        {
            lock (_rw)
            {
                lba = default;
                // We can't append since the file is full.
                if (_eof) return false;
                int recSize = RecordHeaderSize + key.Length + val.Length;
                if (!ExpandBuffer(recSize)) return false;
                lba = new Lba(CacheId, _diskWoff, recSize);
                Span<byte> hdr = stackalloc byte[RecordHeaderSize];
                BinaryPrimitives.WriteUInt32LittleEndian(hdr, RecordMagic);
                BinaryPrimitives.WriteUInt32LittleEndian(hdr[4..], 0);
                BinaryPrimitives.WriteUInt32LittleEndian(hdr[8..], (uint)key.Length);
                BinaryPrimitives.WriteUInt32LittleEndian(hdr[12..], (uint)val.Length);
                BinaryPrimitives.WriteUInt32LittleEndian(hdr[4..], ComputeRecordCrc(hdr, key, val));
                AppendBytes(hdr);
                AppendBytes(key);
                AppendBytes(val);
                _diskWoff += recSize;
                _eof = _diskWoff >= _maxSize;
                DispatchBuffer();
                return true;
            }
        }

        private bool ExpandBuffer(int size)
        {
            long free = 0;
            for (int i = _bufWoff; i < _bufs.Count; i++)
            {
                free += _bufs[i].Length - _bufUsed[i];
                if (size <= free) return true;
            }
            while (free < size)
            {
                byte[]? buf = _tier._bufferAllocator.Allocate();
                if (buf is null) return false; // unable to allocate buffers
                _bufs.Add(buf);
                _bufUsed.Add(0);
                free += buf.Length;
            }
            return true;
        }

        private void AppendBytes(ReadOnlySpan<byte> data)
        {
            while (!data.IsEmpty)
            {
                byte[] buf = _bufs[_bufWoff];
                int used = _bufUsed[_bufWoff];
                int n = Math.Min(buf.Length - used, data.Length);
                data[..n].CopyTo(buf.AsSpan(used));
                _bufUsed[_bufWoff] = used + n;
                data = data[n..];
                if (_bufUsed[_bufWoff] == buf.Length) _bufWoff++;
            }
        }

        // DispatchBuffer: one outstanding write per file; full buffers (or the last one at eof, zero padded) go to the writer.
        private void DispatchBuffer()
        {
            if (_pendingIos > 0) return;
            if (!_eof && _bufDoff == _bufWoff) return;
            if (_bufDoff >= _bufs.Count) return;
            int idx = _bufDoff;
            int used = _bufUsed[idx];
            byte[] buf = _bufs[idx];
            // Pad with zeros to the file alignment.
            int padded = Math.Min(buf.Length, (used + FileAlignmentSize - 1) / FileAlignmentSize * FileAlignmentSize);
            buf.AsSpan(used, padded - used).Clear();
            _pendingIos++;
            _bufDoff++;
            _tier._writer!.Write(this, buf, padded);
        }

        public void WriteToDisk(byte[] buf, int length, int dispatchSize)
        {
            for (int written = 0; written < length; written += dispatchSize)
                _file!.Append(buf.AsSpan(written, Math.Min(dispatchSize, length - written)));
        }

        public void BufferWriteDone()
        {
            lock (_rw)
            {
                _pendingIos--;
                if (_bufDoff < _bufs.Count) DispatchBuffer();
                if (_eof && _bufDoff >= _bufs.Count && _pendingIos == 0) CloseAndOpenForReading();
            }
        }

        // WriteableCacheFile::CloseAndOpenForReading. Runs on a writer thread: an I/O error (e.g. the cache directory was
        // removed underneath the cache) leaves the file unreadable (its lookups count as errors, as upstream's failed
        // RandomAccessCacheFile::OpenImpl) but evictable, and the write buffers are always returned to the allocator.
        private void CloseAndOpenForReading()
        {
            try
            {
                _file!.Flush();
                _file.Dispose();
                _reader = _tier._opt.FileSystem.OpenRandomAccess(_path);
            }
            catch (Exception e) when (e is IOException or ObjectDisposedException or RocksDbException or UnauthorizedAccessException)
            {
                _openFailed = true;
            }
            finally
            {
                _file = null;
                foreach (byte[] b in _bufs) _tier._bufferAllocator.Deallocate(b);
                _bufs.Clear();
                _bufUsed.Clear();
            }
        }

        /// <summary>
        /// Reads a record still in the write buffers into <paramref name="scratch"/> (true when complete); for a closed
        /// file returns false with the reader to read it from (the caller holds a reference on the file).
        /// </summary>
        public bool TryReadBuffered(in Lba lba, byte[] scratch, out Env.IRandomAccessFile? reader)
        {
            lock (_rw)
            {
                reader = _reader;
                if (reader is not null) return false;
            }
            return Read(lba, scratch);
        }

        public bool Read(in Lba lba, byte[] scratch)
        {
            lock (_rw)
            {
                if (_reader is null)
                {
                    // Still being written: read from the write buffers (WriteableCacheFile::ReadBuffer).
                    if (_bufs.Count == 0) return false;
                    int bufferSize = _bufs[0].Length;
                    int start = (int)(lba.Offset / bufferSize);
                    int off = (int)(lba.Offset % bufferSize);
                    int pending = lba.Size, dst = 0;
                    for (int i = start; pending > 0 && i < _bufs.Count; i++)
                    {
                        int n = Math.Min(pending, _bufUsed[i] - off);
                        _bufs[i].AsSpan(off, n).CopyTo(scratch.AsSpan(dst));
                        pending -= n;
                        dst += n;
                        off = 0;
                    }
                    return pending == 0;
                }
            }
            return Table.BlockFetcher.ReadFully(_reader, lba.Offset, scratch.AsSpan(0, lba.Size)) == lba.Size;
        }

        public void CloseFile()
        {
            lock (_rw)
            {
                try
                {
                    _file?.Dispose();
                }
                catch (IOException)
                {
                }
                _file = null;
                _reader?.Dispose();
                _reader = null;
                foreach (byte[] b in _bufs) _tier._bufferAllocator.Deallocate(b);
                _bufs.Clear();
                _bufUsed.Clear();
            }
        }

        /// <summary>Deletes the file; returns its size.</summary>
        public long Delete()
        {
            CloseFile();
            Env.IFileSystem fs = _tier._opt.FileSystem;
            long size = 0;
            try
            {
                size = fs.GetFileSize(_path);
                fs.DeleteFile(_path);
            }
            catch (IOException)
            {
            }
            return size;
        }
    }

    /// <summary>A bounded pool of write buffers (CacheWriteBufferAllocator).</summary>
    private sealed class WriteBufferAllocator(int bufferSize, int bufferCount)
    {
        private readonly object _lock = new();
        private readonly Stack<byte[]> _free = new();
        private int _allocated;
        private bool _shutdown;

        public byte[]? Allocate()
        {
            lock (_lock)
            {
                if (_free.Count > 0) return _free.Pop();
                if (_allocated >= bufferCount) return null;
                _allocated++;
                return new byte[bufferSize];
            }
        }

        public void Deallocate(byte[] buf)
        {
            lock (_lock)
            {
                _free.Push(buf);
                Monitor.PulseAll(_lock);
            }
        }

        /// <summary>
        /// Blocks until a buffer can be allocated (CacheWriteBufferAllocator::WaitUntilUsable waits on cond_empty_ until
        /// bufs_ is non-empty) or <see cref="Shutdown"/> is called.
        /// </summary>
        public void WaitUntilUsable()
        {
            lock (_lock)
            {
                while (!_shutdown && _free.Count == 0 && _allocated >= bufferCount) Monitor.Wait(_lock);
            }
        }

        /// <summary>Wakes up and disables <see cref="WaitUntilUsable"/> (the tier is closing).</summary>
        public void Shutdown()
        {
            lock (_lock)
            {
                _shutdown = true;
                Monitor.PulseAll(_lock);
            }
        }

    }

    /// <summary>ThreadedWriter: writer_qdepth threads writing dispatched buffers, reserving cache space first.</summary>
    private sealed class Writer
    {
        private readonly BlockCacheTier _tier;
        private readonly BlockingCollection<(CacheFile? File, byte[]? Buf, int Length)> _q = new();
        private readonly Thread[] _threads;
        private readonly int _ioSize;
        private int _pending;

        public Writer(BlockCacheTier tier, int qdepth, int ioSize)
        {
            _tier = tier;
            _ioSize = ioSize;
            _threads = new Thread[qdepth];
            for (int i = 0; i < qdepth; i++)
            {
                _threads[i] = new Thread(ThreadMain) { IsBackground = true, Name = "rocksdb:pcache-writer" };
                _threads[i].Start();
            }
        }

        public int Pending => Volatile.Read(ref _pending);

        public void Write(CacheFile file, byte[] buf, int length)
        {
            Interlocked.Increment(ref _pending);
            _q.Add((file, buf, length));
        }

        public void Stop()
        {
            for (int i = 0; i < _threads.Length; i++) _q.Add((null, null, 0));
            foreach (Thread t in _threads) t.Join();
        }

        private void ThreadMain()
        {
            while (true)
            {
                (CacheFile? file, byte[]? buf, int length) = _q.Take();
                if (file is null) break;
                // Wait for space (upstream retries every second forever); files only become evictable once fully
                // written, so this waits for other writers. After ~30 s the write is accounted anyway.
                int tries = 0;
                while (!_tier.Reserve(length))
                {
                    if (++tries > 3000)
                    {
                        _tier.ForceReserve(length);
                        break;
                    }
                    Thread.Sleep(10);
                }
                try
                {
                    file.WriteToDisk(buf!, length, _ioSize);
                }
                catch (Exception e) when (e is IOException or ObjectDisposedException or RocksDbException)
                {
                    // Error writing data to file: the records read back will fail their checksum and count as errors.
                }
                file.BufferWriteDone();
                Interlocked.Decrement(ref _pending);
            }
        }
    }
}
