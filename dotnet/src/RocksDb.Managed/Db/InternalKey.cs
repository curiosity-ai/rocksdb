using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// An owned, encoded internal key (<c>user_key | fixed64(seq &lt;&lt; 8 | type)</c>). Port of rocksdb::InternalKey
/// (db/dbformat.h). Immutable; used for file boundaries (smallest/largest) and other cold-path metadata.
/// The default value is "invalid" (empty), like a default-constructed C++ InternalKey.
/// </summary>
public readonly struct InternalKey : IEquatable<InternalKey>
{
    private readonly byte[]? _rep;

    private InternalKey(byte[] rep) => _rep = rep;

    public InternalKey(ReadOnlySpan<byte> userKey, ulong sequence, ValueType type)
    {
        _rep = new byte[userKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(_rep, userKey, sequence, type);
    }

    /// <summary>Wraps a copy of an already-encoded internal key (port of DecodeFrom).</summary>
    public static InternalKey FromEncoded(ReadOnlySpan<byte> encoded) => new(encoded.ToArray());

    /// <summary>An internal key that sorts at or after every internal key with this user key (seq 0, type 0).</summary>
    public static InternalKey MaxPossibleForUserKey(ReadOnlySpan<byte> userKey) => new(userKey, 0, ValueType.Deletion);

    /// <summary>An internal key that sorts at or before every internal key with this user key (kMaxSequenceNumber, kValueTypeForSeek).</summary>
    public static InternalKey MinPossibleForUserKey(ReadOnlySpan<byte> userKey) => new(userKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);

    public bool IsEmpty => _rep is null || _rep.Length == 0;

    /// <summary>True when the key parses (port of InternalKey::Valid).</summary>
    public bool Valid => _rep is not null && DbFormat.TryParseInternalKey(_rep, out _);

    public ReadOnlySpan<byte> Encoded => _rep;
    public ReadOnlySpan<byte> UserKey => _rep is null ? default : DbFormat.ExtractUserKey(_rep);
    public ulong Sequence => DbFormat.ExtractFooter(_rep) >> 8;
    public ValueType Type => (ValueType)(byte)DbFormat.ExtractFooter(_rep);

    public bool Equals(InternalKey other) => Encoded.SequenceEqual(other.Encoded);
    public override bool Equals(object? obj) => obj is InternalKey k && Equals(k);
    public override int GetHashCode()
    {
        var h = new HashCode();
        h.AddBytes(Encoded);
        return h.ToHashCode();
    }

    /// <summary>Debug rendering, e.g. <c>'666F6F' seq:100, type:1</c> (hex user key).</summary>
    public override string ToString() => DebugString(Encoded, hex: true);

    /// <summary>Port of InternalKey::DebugString / ParsedInternalKey::DebugString.</summary>
    public static string DebugString(ReadOnlySpan<byte> internalKey, bool hex)
    {
        if (!DbFormat.TryParseInternalKey(internalKey, out ParsedInternalKey p))
        {
            return "(bad)" + Convert.ToHexString(internalKey);
        }
        ReadOnlySpan<byte> uk = internalKey[..p.UserKeyLength];
        string user = hex ? Convert.ToHexString(uk) : Encoding.Latin1.GetString(uk);
        return "'" + user + "' seq:" + p.Sequence.ToString(System.Globalization.CultureInfo.InvariantCulture) +
               ", type:" + ((int)p.Type).ToString(System.Globalization.CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Key used for memtable/table point lookups. Port of rocksdb::LookupKey (db/lookup_key.h, db/dbformat.cc).
/// Layout: <c>varint32(internal_key_len) | user_key | [timestamp] | fixed64(seq &lt;&lt; 8 | kValueTypeForSeek)</c>.
/// </summary>
/// <remarks>
/// Allocation-free: the bytes are built in caller-provided scratch (typically
/// <c>stackalloc byte[LookupKey.StackBufferSize]</c>); if that is too small, a buffer is rented from
/// <see cref="ArrayPool{T}.Shared"/> and returned by <see cref="Dispose"/>. Usage:
/// <code>using var lkey = new LookupKey(userKey, seq, stackalloc byte[LookupKey.StackBufferSize]);</code>
/// </remarks>
public ref struct LookupKey
{
    /// <summary>Recommended stack scratch size (same as upstream's inline <c>space_[200]</c>).</summary>
    public const int StackBufferSize = 200;

    private readonly Span<byte> _buf;
    private byte[]? _rented;
    private readonly int _kstart;
    private readonly int _end;

    public LookupKey(ReadOnlySpan<byte> userKey, ulong sequence, Span<byte> scratch)
        : this(userKey, sequence, ReadOnlySpan<byte>.Empty, scratch)
    {
    }

    /// <summary>Builds a lookup key whose user key is <paramref name="userKey"/> followed by <paramref name="timestamp"/>.</summary>
    public LookupKey(ReadOnlySpan<byte> userKey, ulong sequence, ReadOnlySpan<byte> timestamp, Span<byte> scratch)
    {
        int ikeyLen = userKey.Length + timestamp.Length + DbFormat.InternalKeyFooterSize;
        int needed = ikeyLen + Coding.MaxVarint32Length;
        if (needed > scratch.Length)
        {
            _rented = ArrayPool<byte>.Shared.Rent(needed);
            scratch = _rented;
        }
        _buf = scratch;
        int p = Coding.EncodeVarint32(scratch, (uint)ikeyLen);
        _kstart = p;
        userKey.CopyTo(scratch[p..]);
        p += userKey.Length;
        timestamp.CopyTo(scratch[p..]);
        p += timestamp.Length;
        Coding.EncodeFixed64(scratch[p..], DbFormat.PackSequenceAndType(sequence, DbFormat.ValueTypeForSeek));
        _end = p + DbFormat.InternalKeyFooterSize;
    }

    /// <summary>Key suitable for lookup in a MemTable (length-prefixed internal key).</summary>
    public readonly ReadOnlySpan<byte> MemtableKey => _buf[.._end];
    /// <summary>Internal key suitable for an internal iterator.</summary>
    public readonly ReadOnlySpan<byte> InternalKey => _buf[_kstart.._end];
    /// <summary>User key (including timestamp if any).</summary>
    public readonly ReadOnlySpan<byte> UserKey => _buf[_kstart..(_end - DbFormat.InternalKeyFooterSize)];
    public readonly ulong Sequence => Coding.DecodeFixed64(_buf[(_end - DbFormat.InternalKeyFooterSize).._end]) >> 8;

    public void Dispose()
    {
        byte[]? r = _rented;
        _rented = null;
        if (r is not null) ArrayPool<byte>.Shared.Return(r);
    }
}

/// <summary>
/// Reusable growable key buffer for iterators (block iterator prefix decoding, DBIter saved keys). Port of the
/// copying subset of rocksdb::IterKey (db/dbformat.h). The buffer is rented from <see cref="ArrayPool{T}.Shared"/>
/// and returned by <see cref="Dispose"/>; it never shrinks, so steady state is allocation-free. Not thread-safe.
/// </summary>
public sealed class IterKey : IDisposable
{
    private const int InitialSize = 64;
    private byte[] _buf;
    private int _size;
    private bool _isUserKey = true;

    public IterKey() => _buf = ArrayPool<byte>.Shared.Rent(InitialSize);

    /// <summary>The key in whichever format it was set (user or internal).</summary>
    public ReadOnlySpan<byte> Key => _buf.AsSpan(0, _size);
    public int Size => _size;
    public bool IsUserKey => _isUserKey;

    /// <summary>Picked up by the next <see cref="SetKey"/> / <see cref="TrimAppend"/>.</summary>
    public void SetIsUserKey(bool isUserKey) => _isUserKey = isUserKey;

    /// <summary>The user key (strips the 8-byte footer when holding an internal key).</summary>
    public ReadOnlySpan<byte> UserKey => _isUserKey ? Key : _buf.AsSpan(0, _size - DbFormat.InternalKeyFooterSize);

    public ReadOnlySpan<byte> InternalKey
    {
        get
        {
            System.Diagnostics.Debug.Assert(!_isUserKey);
            return Key;
        }
    }

    public void Clear() => _size = 0;

    /// <summary>Copies <paramref name="key"/> (format per <see cref="SetIsUserKey"/>).</summary>
    public ReadOnlySpan<byte> SetKey(ReadOnlySpan<byte> key)
    {
        EnsureCapacity(key.Length, 0);
        key.CopyTo(_buf);
        _size = key.Length;
        return Key;
    }

    public ReadOnlySpan<byte> SetUserKey(ReadOnlySpan<byte> key)
    {
        _isUserKey = true;
        return SetKey(key);
    }

    public ReadOnlySpan<byte> SetInternalKey(ReadOnlySpan<byte> internalKey)
    {
        _isUserKey = false;
        return SetKey(internalKey);
    }

    /// <summary>Encodes <c>userKey | footer(seq, type)</c> into the buffer.</summary>
    public ReadOnlySpan<byte> SetInternalKey(ReadOnlySpan<byte> userKey, ulong sequence, ValueType type)
    {
        _isUserKey = false;
        EnsureCapacity(userKey.Length + DbFormat.InternalKeyFooterSize, 0);
        _size = DbFormat.AppendInternalKey(_buf, userKey, sequence, type);
        return Key;
    }

    /// <summary>Rewrites the footer of the held internal key in place.</summary>
    public void UpdateInternalKey(ulong sequence, ValueType type)
    {
        System.Diagnostics.Debug.Assert(!_isUserKey && _size >= DbFormat.InternalKeyFooterSize);
        Coding.EncodeFixed64(_buf.AsSpan(_size - DbFormat.InternalKeyFooterSize), DbFormat.PackSequenceAndType(sequence, type));
    }

    /// <summary>Keeps the first <paramref name="sharedLength"/> bytes and appends <paramref name="nonShared"/> (block prefix decoding).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrimAppend(int sharedLength, ReadOnlySpan<byte> nonShared)
    {
        System.Diagnostics.Debug.Assert((uint)sharedLength <= (uint)_size);
        int total = sharedLength + nonShared.Length;
        if (total > _buf.Length) EnsureCapacity(total, sharedLength);
        nonShared.CopyTo(_buf.AsSpan(sharedLength));
        _size = total;
    }

    /// <summary>Appends bytes to the key.</summary>
    public void Append(ReadOnlySpan<byte> bytes) => TrimAppend(_size, bytes);

    private void EnsureCapacity(int size, int preserve)
    {
        if (size <= _buf.Length) return;
        byte[] n = ArrayPool<byte>.Shared.Rent(Math.Max(size, _buf.Length * 2));
        _buf.AsSpan(0, preserve).CopyTo(n);
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = n;
    }

    public void Dispose()
    {
        byte[] b = _buf;
        _buf = [];
        _size = 0;
        if (b.Length > 0) ArrayPool<byte>.Shared.Return(b);
    }
}
