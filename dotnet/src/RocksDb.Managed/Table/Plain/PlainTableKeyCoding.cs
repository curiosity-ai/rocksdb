using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Plain;

/// <summary>PlainTableEntryType (table/plain/plain_table_key_coding.cc): the two high bits of a kPrefix size byte.</summary>
internal enum PlainTableEntryType : byte
{
    FullKey = 0,
    PrefixFromPreviousKey = 1,
    KeySuffix = 2,
}

/// <summary>Constants of the plain table format (plain_table_factory.h, plain_table_key_coding.cc).</summary>
internal static class PlainTableFormat
{
    /// <summary>
    /// PlainTableFactory::kValueTypeSeqId0 = char(~0): written after the user key instead of the 8-byte internal key
    /// footer when the entry is a value with sequence number 0.
    /// </summary>
    public const byte ValueTypeSeqId0 = 0xFF;
    /// <summary>kSizeInlineLimit: sizes up to 0x3E are inlined in the flag byte; 0x3F means a varint32 follows.</summary>
    public const byte SizeInlineLimit = 0x3F;
    /// <summary>kNumInternalBytes.</summary>
    public const int NumInternalBytes = 8;

    /// <summary>EncodeSize: flag byte (type in the top two bits) plus the overflow varint.</summary>
    public static int EncodeSize(PlainTableEntryType type, uint keySize, Span<byte> dst)
    {
        dst[0] = (byte)((byte)type << 6);
        if (keySize < SizeInlineLimit)
        {
            // size inlined
            dst[0] |= (byte)keySize;
            return 1;
        }
        dst[0] |= SizeInlineLimit;
        return 1 + Coding.EncodeVarint32(dst[1..], keySize - SizeInlineLimit);
    }
}

/// <summary>
/// Port of PlainTableKeyEncoder (table/plain/plain_table_key_coding.{h,cc}): encodes internal keys in the kPlain or
/// kPrefix format (see the format notes in table/plain/plain_table_factory.h).
/// </summary>
internal sealed class PlainTableKeyEncoder
{
    private readonly uint _fixedUserKeyLen;
    private readonly SliceTransform? _prefixExtractor;
    private readonly ulong _indexSparseness;
    private ulong _keyCountForPrefix;
    private byte[] _prePrefix = new byte[16];
    private int _prePrefixLength;

    public PlainTableKeyEncoder(PlainTableEncodingType encodingType, uint userKeyLen, SliceTransform? prefixExtractor, ulong indexSparseness)
    {
        // kPrefix needs a prefix extractor; without one the keys are written in kPlain.
        EncodingType = prefixExtractor is not null ? encodingType : PlainTableEncodingType.Plain;
        _fixedUserKeyLen = userKeyLen;
        _prefixExtractor = prefixExtractor;
        _indexSparseness = indexSparseness > 1 ? indexSparseness : 1;
    }

    /// <summary>GetEncodingType: the encoding actually used.</summary>
    public PlainTableEncodingType EncodingType { get; }

    /// <summary>
    /// AppendKey: writes the encoded <paramref name="key"/> to <paramref name="output"/>. A trailing
    /// <see cref="PlainTableFormat.ValueTypeSeqId0"/> marker for a seq-0 value is appended to <paramref name="metaBytes"/>
    /// (written with the value size), like upstream's meta_bytes_buf.
    /// </summary>
    public void AppendKey(ReadOnlySpan<byte> key, IBufferWriter<byte> output, ref ulong offset, Span<byte> metaBytes, ref int metaBytesSize)
    {
        if (!DbFormat.TryParseInternalKey(key, out ParsedInternalKey parsed)) ThrowHelper.Corruption("Corrupted key: invalid internal key");
        ReadOnlySpan<byte> keyToWrite = key; // Portion of internal key to write out.
        uint userKeySize = (uint)(key.Length - 8);
        if (EncodingType == PlainTableEncodingType.Plain)
        {
            if (_fixedUserKeyLen == PlainTableOptions.VariableLength)
            {
                // Write key length
                Span<byte> sizeBuf = output.GetSpan(Coding.MaxVarint32Length);
                int len = Coding.EncodeVarint32(sizeBuf, userKeySize);
                output.Advance(len);
                offset += (ulong)len;
            }
        }
        else
        {
            Span<byte> sizeBytes = stackalloc byte[12];
            int pos = 0;
            ReadOnlySpan<byte> userKey = key[..(int)userKeySize];
            ReadOnlySpan<byte> prefix = PlainTableReader.Transform(_prefixExtractor!, userKey);
            if (_keyCountForPrefix == 0 || !prefix.SequenceEqual(_prePrefix.AsSpan(0, _prePrefixLength)) || _keyCountForPrefix % _indexSparseness == 0)
            {
                _keyCountForPrefix = 1;
                if (_prePrefix.Length < prefix.Length) _prePrefix = new byte[prefix.Length * 2];
                prefix.CopyTo(_prePrefix);
                _prePrefixLength = prefix.Length;
                pos += PlainTableFormat.EncodeSize(PlainTableEntryType.FullKey, userKeySize, sizeBytes);
            }
            else
            {
                _keyCountForPrefix++;
                if (_keyCountForPrefix == 2)
                {
                    // For second key within a prefix, need to encode prefix length
                    pos += PlainTableFormat.EncodeSize(PlainTableEntryType.PrefixFromPreviousKey, (uint)_prePrefixLength, sizeBytes[pos..]);
                }
                uint prefixLen = (uint)_prePrefixLength;
                pos += PlainTableFormat.EncodeSize(PlainTableEntryType.KeySuffix, userKeySize - prefixLen, sizeBytes[pos..]);
                keyToWrite = key[(int)prefixLen..];
            }
            output.Write(sizeBytes[..pos]);
            offset += (ulong)pos;
        }

        // Encode full key. If the row is of value type with seqId 0, flush the special flag together with the value size.
        if (parsed.Sequence == 0 && parsed.Type == EntryType.Value)
        {
            output.Write(keyToWrite[..^8]);
            offset += (ulong)(keyToWrite.Length - 8);
            metaBytes[metaBytesSize++] = PlainTableFormat.ValueTypeSeqId0;
        }
        else
        {
            output.Write(keyToWrite);
            offset += (ulong)keyToWrite.Length;
        }
    }
}

/// <summary>
/// Port of PlainTableKeyDecoder (table/plain/plain_table_key_coding.{h,cc}) in mmap mode: decodes keys from the
/// in-memory file. The decoded internal key is either a slice of the file (<see cref="InternalKey"/>) or rebuilt in the
/// decoder's buffer (seq-0 values, prefix-encoded suffixes); it is valid until the next decode. Not thread-safe: one
/// decoder per iterator / lookup.
/// </summary>
internal sealed class PlainTableKeyDecoder
{
    private byte[] _data;
    private uint _dataEnd;
    private PlainTableEncodingType _encodingType;
    private uint _fixedUserKeyLen;
    private uint _prefixLen;
    private byte[] _savedUserKey = new byte[32];
    private int _savedUserKeyLength;
    private byte[] _curKey = new byte[64];
    // The decoded internal key: _data[_keyOffset.._keyOffset+_keyLength) when _keyInFile, else _curKey[.._keyLength).
    private bool _keyInFile;
    private int _keyOffset;
    private int _keyLength;

    public PlainTableKeyDecoder(byte[] data, uint dataEnd, PlainTableEncodingType encodingType, uint userKeyLen)
    {
        _data = data;
        _dataEnd = dataEnd;
        _encodingType = encodingType;
        _fixedUserKeyLen = userKeyLen;
    }

    /// <summary>Re-points a pooled decoder at another table (keeps its buffers).</summary>
    public void Reset(byte[] data, uint dataEnd, PlainTableEncodingType encodingType, uint userKeyLen)
    {
        _data = data;
        _dataEnd = dataEnd;
        _encodingType = encodingType;
        _fixedUserKeyLen = userKeyLen;
        _prefixLen = 0;
        _savedUserKeyLength = 0;
        _keyInFile = false;
        _keyLength = 0;
    }

    /// <summary>The last decoded internal key.</summary>
    public ReadOnlySpan<byte> InternalKey => _keyInFile ? _data.AsSpan(_keyOffset, _keyLength) : _curKey.AsSpan(0, _keyLength);

    /// <summary>The user key of the last decoded internal key.</summary>
    public ReadOnlySpan<byte> UserKey => InternalKey[..^8];

    /// <summary>Offset and length of the last decoded value (NextKey).</summary>
    public uint ValueOffset { get; private set; }
    public uint ValueLength { get; private set; }

    /// <summary>The value of the last <see cref="NextKey"/>.</summary>
    public ReadOnlySpan<byte> Value => _data.AsSpan((int)ValueOffset, (int)ValueLength);

    private ReadOnlySpan<byte> Read(uint offset, ulong len)
    {
        // 64-bit length: a corrupt key size near 2^32 must not wrap around (user_key_size + 1 / + 8 in ReadInternalKey).
        if (offset + len > _dataEnd) ThrowHelper.Corruption("Unexpected EOF reading plain table entry at " + offset);
        return _data.AsSpan((int)offset, (int)len);
    }

    private uint ReadVarint32(uint offset, out uint bytesRead)
    {
        if (offset >= _dataEnd) ThrowHelper.Corruption("Unexpected EOF reading plain table varint at " + offset);
        int n = Coding.DecodeVarint32(_data.AsSpan((int)offset, (int)Math.Min(5u, _dataEnd - offset)), out uint v);
        if (n == 0) ThrowHelper.Corruption("Bad varint32 in plain table at " + offset);
        bytesRead = (uint)n;
        return v;
    }

    /// <summary>DecodeSize: entry type and size of a kPrefix flag byte (+ overflow varint).</summary>
    private PlainTableEntryType DecodeSize(uint startOffset, out uint keySize, out uint bytesRead)
    {
        byte b = Read(startOffset, 1)[0];
        var type = (PlainTableEntryType)((b & ~PlainTableFormat.SizeInlineLimit & 0xFF) >> 6);
        int inlineKeySize = b & PlainTableFormat.SizeInlineLimit;
        if (inlineKeySize < PlainTableFormat.SizeInlineLimit)
        {
            keySize = (uint)inlineKeySize;
            bytesRead = 1;
            return type;
        }
        uint extra = ReadVarint32(startOffset + 1, out uint tmp);
        keySize = PlainTableFormat.SizeInlineLimit + extra;
        bytesRead = tmp + 1;
        return type;
    }

    private Span<byte> CurKey(int length)
    {
        if (_curKey.Length < length) Array.Resize(ref _curKey, Math.Max(length, _curKey.Length * 2));
        _keyInFile = false;
        _keyLength = length;
        return _curKey.AsSpan(0, length);
    }

    /// <summary>
    /// ReadInternalKey: the user key of <paramref name="userKeySize"/> bytes at <paramref name="fileOffset"/> followed by
    /// either the seq-0 marker or the 8-byte footer. Returns the parsed footer; the key bytes stay in the file.
    /// </summary>
    private (ulong Seq, EntryType Type, bool InFile) ReadInternalKey(uint fileOffset, uint userKeySize, ref uint bytesRead)
    {
        ReadOnlySpan<byte> tmp = Read(fileOffset, (ulong)userKeySize + 1);
        if (tmp[(int)userKeySize] == PlainTableFormat.ValueTypeSeqId0)
        {
            // Special encoding for the row with seqID=0
            bytesRead += userKeySize + 1;
            return (0, EntryType.Value, false);
        }
        ReadOnlySpan<byte> ikey = Read(fileOffset, (ulong)userKeySize + 8);
        if (!DbFormat.TryParseInternalKey(ikey, out ParsedInternalKey pk))
            ThrowHelper.Corruption("Corrupted key found during next key read. Corrupted Key: invalid internal key type");
        bytesRead += userKeySize + 8;
        return (pk.Sequence, pk.Type, true);
    }

    private void NextPlainEncodingKey(uint startOffset, ref uint bytesRead)
    {
        uint userKeySize;
        if (_fixedUserKeyLen != PlainTableOptions.VariableLength)
        {
            userKeySize = _fixedUserKeyLen;
        }
        else
        {
            userKeySize = ReadVarint32(startOffset, out uint tmpRead);
            bytesRead = tmpRead;
        }
        uint keyStart = startOffset + bytesRead;
        (ulong seq, EntryType type, bool inFile) = ReadInternalKey(keyStart, userKeySize, ref bytesRead);
        if (inFile)
        {
            _keyInFile = true;
            _keyOffset = (int)keyStart;
            _keyLength = (int)userKeySize + 8;
        }
        else
        {
            // Need to copy out the internal key
            Span<byte> k = CurKey((int)userKeySize + 8);
            DbFormat.AppendInternalKey(k, _data.AsSpan((int)keyStart, (int)userKeySize), seq, type);
        }
    }

    private void NextPrefixEncodingKey(uint startOffset, ref uint bytesRead, ref bool seekable)
    {
        bool expectSuffix;
        do
        {
            PlainTableEntryType entryType = DecodeSize(startOffset + bytesRead, out uint size, out uint myBytesRead);
            if (myBytesRead == 0) ThrowHelper.Corruption("Unexpected EOF when reading size of the key");
            bytesRead += myBytesRead;
            switch (entryType)
            {
                case PlainTableEntryType.FullKey:
                {
                    expectSuffix = false;
                    uint keyStart = startOffset + bytesRead;
                    (ulong seq, EntryType type, bool inFile) = ReadInternalKey(keyStart, size, ref bytesRead);
                    ReadOnlySpan<byte> userKey = _data.AsSpan((int)keyStart, (int)size);
                    if (inFile)
                    {
                        _keyInFile = true;
                        _keyOffset = (int)keyStart;
                        _keyLength = (int)size + 8;
                    }
                    else
                    {
                        DbFormat.AppendInternalKey(CurKey((int)size + 8), userKey, seq, type);
                    }
                    if (_savedUserKey.Length < userKey.Length) _savedUserKey = new byte[userKey.Length * 2];
                    userKey.CopyTo(_savedUserKey);
                    _savedUserKeyLength = userKey.Length;
                    break;
                }
                case PlainTableEntryType.PrefixFromPreviousKey:
                    seekable = false;
                    _prefixLen = size;
                    if (_prefixLen > _savedUserKeyLength) ThrowHelper.Corruption("plain table prefix longer than the previous key");
                    // Need read another size flag for suffix
                    expectSuffix = true;
                    break;
                case PlainTableEntryType.KeySuffix:
                {
                    expectSuffix = false;
                    seekable = false;
                    uint suffixStart = startOffset + bytesRead;
                    (ulong seq, EntryType type, _) = ReadInternalKey(suffixStart, size, ref bytesRead);
                    if (_prefixLen > _savedUserKeyLength) ThrowHelper.Corruption("plain table prefix longer than the previous key");
                    int prefixLen = (int)_prefixLen;
                    Span<byte> k = CurKey(prefixLen + (int)size + 8);
                    _savedUserKey.AsSpan(0, prefixLen).CopyTo(k);
                    DbFormat.AppendInternalKey(k[prefixLen..], _data.AsSpan((int)suffixStart, (int)size), seq, type);
                    break;
                }
                default:
                    ThrowHelper.Corruption("Un-identified size flag.");
                    return;
            }
        }
        while (expectSuffix); // Another round if suffix is expected.
    }

    /// <summary>NextKeyNoValue: decodes the key at <paramref name="startOffset"/>; returns the bytes it occupies.</summary>
    public uint NextKeyNoValue(uint startOffset, out bool seekable)
    {
        uint bytesRead = 0;
        seekable = true;
        if (_encodingType == PlainTableEncodingType.Plain) NextPlainEncodingKey(startOffset, ref bytesRead);
        else NextPrefixEncodingKey(startOffset, ref bytesRead, ref seekable);
        return bytesRead;
    }

    /// <summary>NextKey: decodes the key and value at <paramref name="startOffset"/>; returns the bytes the entry occupies.</summary>
    public uint NextKey(uint startOffset, out bool seekable)
    {
        uint bytesRead = NextKeyNoValue(startOffset, out seekable);
        uint valueSize = ReadVarint32(startOffset + bytesRead, out uint valueSizeBytes);
        if (valueSizeBytes == 0) ThrowHelper.Corruption("Unexpected EOF when reading the next value's size.");
        bytesRead += valueSizeBytes;
        Read(startOffset + bytesRead, valueSize);
        ValueOffset = startOffset + bytesRead;
        ValueLength = valueSize;
        bytesRead += valueSize;
        return bytesRead;
    }
}
