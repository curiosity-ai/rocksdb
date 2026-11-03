using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Per key-value protection of a WriteBatch. Port of WriteBatch::ProtectionInfo, WriteBatchInternal::UpdateProtectionInfo
/// (ProtectionInfoUpdater), WriteBatch::VerifyChecksum and the prot_info_ bookkeeping of the record writers, save points
/// and Append (db/write_batch.cc). Each key-value record has a ProtectionInfoKVOC64 (<see cref="KvChecksum"/>) over its
/// key, value, operation type (without the column family variant) and column family id, computed when the record is
/// added and verified before the batch is written to the WAL and when its entries are inserted into memtables.
/// </summary>
/// <remarks>
/// The protection is computed from the record just written to the rep (the ReadRecordFromWriteBatch view of it, which
/// VerifyChecksum also uses) rather than from each Put's arguments; the result is identical. Batches without
/// protection pay one predictable branch per record.
/// </remarks>
public sealed partial class WriteBatch
{
    /// <summary>prot_info_ entries (ProtectionInfoKVOC64), one per counted record; null when unprotected.</summary>
    private ulong[]? _protInfo;
    private int _protCount;
    private byte _protectionBytesPerKey;

    /// <summary>
    /// Creates an empty batch with per key-value protection. Port of
    /// <c>WriteBatch(reserved_bytes, max_bytes, protection_bytes_per_key, default_cf_ts_sz)</c>.
    /// </summary>
    /// <param name="reservedBytes">Initial capacity of the rep.</param>
    /// <param name="maxBytes">Maximum rep size; 0 for unlimited.</param>
    /// <param name="protectionBytesPerKey">0 (no protection) or 8: each record's protection info is computed as it is
    /// added, so corruption of the batch before it reaches the WAL / memtables is detected (Corruption).</param>
    public WriteBatch(int reservedBytes, int maxBytes, int protectionBytesPerKey) : this(reservedBytes, maxBytes)
    {
        if (protectionBytesPerKey is not (0 or 8)) ThrowHelper.InvalidArgument("WriteBatch protection_bytes_per_key must be zero or eight");
        if (protectionBytesPerKey != 0)
        {
            _protectionBytesPerKey = (byte)protectionBytesPerKey;
            _protInfo = new ulong[8];
        }
    }

    /// <summary>GetProtectionBytesPerKey: 0 (unprotected) or 8.</summary>
    public int ProtectionBytesPerKey => _protectionBytesPerKey;

    /// <summary>The protection infos (ProtectionInfoKVOC64) of the counted records, in order; empty when unprotected.</summary>
    internal ReadOnlySpan<ulong> ProtectionInfo => _protInfo is null ? default : _protInfo.AsSpan(0, _protCount);

    /// <summary>Called by the record writers after a counted record was appended at <paramref name="recordStart"/>.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ProtectRecord(int recordStart)
    {
        ReadOnlySpan<byte> record = _rep.WrittenSpan[recordStart..];
        var reader = new WriteBatchReader(record, wholeBatch: false, count: 0);
        if (!reader.TryReadNext(out WriteBatchEntry e)) return;
        AddProtection(Compute(e));
    }

    private void AddProtection(ulong info)
    {
        ulong[] p = _protInfo!;
        if (_protCount == p.Length) Array.Resize(ref _protInfo, p.Length * 2);
        _protInfo![_protCount++] = info;
    }

    /// <summary>ProtectionInfo64().ProtectKVO(key, value, op_type).ProtectC(cf) of a record (op type without the CF variant).</summary>
    private static ulong Compute(in WriteBatchEntry e) =>
        KvChecksum.ProtectC(KvChecksum.ProtectKvo(e.Key, ProtectedValue(e), e.Type), e.ColumnFamilyId);

    /// <summary>The value as protected: TimedPut covers the packed value (value | fixed64 write time) stored after it.</summary>
    private static ReadOnlySpan<byte> ProtectedValue(in WriteBatchEntry e) =>
        e.Type == EntryType.ValuePreferredSeqno
            ? MemoryMarshal.CreateReadOnlySpan(ref MemoryMarshal.GetReference(e.Value), e.Value.Length + sizeof(ulong))
            : e.Value;

    /// <summary>
    /// Port of WriteBatchInternal::UpdateProtectionInfo: 0 drops the protection; 8 computes it for every record unless
    /// the batch is already protected with 8 bytes per key.
    /// </summary>
    internal void UpdateProtectionInfo(int bytesPerKey)
    {
        if (bytesPerKey == 0)
        {
            _protInfo = null;
            _protCount = 0;
            _protectionBytesPerKey = 0;
            return;
        }
        if (bytesPerKey != 8) ThrowHelper.NotSupported("WriteBatch protection info must be zero or eight bytes/key");
        if (_protInfo is not null) return;
        _protectionBytesPerKey = 8;
        _protInfo = new ulong[Math.Max(8, Count)];
        _protCount = 0;
        foreach (WriteBatchEntry e in new WriteBatchReader(_rep.WrittenSpan))
        {
            if (e.IsCounted) AddProtection(Compute(e));
        }
    }

    /// <summary>
    /// Port of WriteBatch::VerifyChecksum: every record's protection info is stripped of its fields as read from the rep;
    /// throws Corruption ("ProtectionInfo mismatch") when one does not cancel out, or when the count is wrong. No-op for
    /// unprotected batches.
    /// </summary>
    public void VerifyChecksum()
    {
        if (_protInfo is null) return;
        ReadOnlySpan<ulong> prot = ProtectionInfo;
        var reader = new WriteBatchReader(_rep.WrittenSpan[HeaderSize..], wholeBatch: false, count: 0);
        int idx = 0;
        while (idx < prot.Length && reader.TryReadNext(out WriteBatchEntry e))
        {
            if (!e.IsCounted) continue;
            ulong info = KvChecksum.StripKvo(KvChecksum.StripC(prot[idx++], e.ColumnFamilyId), e.Key, ProtectedValue(e), e.Type);
            KvChecksum.ThrowIfMismatch(info);
        }
        if (idx != Count) ThrowHelper.Corruption("WriteBatch has wrong count");
    }

    /// <summary>Clear / SetContents: protection entries are dropped (the configured size is kept for new records).</summary>
    private void ResetProtection()
    {
        _protCount = 0;
    }

    /// <summary>SetContents / return to the pool: the batch becomes unprotected.</summary>
    private void DropProtection()
    {
        _protInfo = null;
        _protCount = 0;
        _protectionBytesPerKey = 0;
    }

    /// <summary>RollbackToSavePoint: prot_info_->entries_.resize(savepoint.count).</summary>
    private void TruncateProtection(uint count)
    {
        if (_protInfo is not null && (uint)_protCount > count) _protCount = (int)count;
    }

    /// <summary>
    /// The protection part of WriteBatchInternal::Append: <paramref name="srcCount"/> entries of <paramref name="src"/>
    /// are appended; appending an unprotected batch drops the destination's (empty) protection.
    /// </summary>
    private void AppendProtection(WriteBatch src, int srcCount)
    {
        if ((src._protInfo is not null && src._protCount != src.Count) || (_protInfo is not null && _protCount != Count - srcCount))
            ThrowHelper.Corruption("Write batch has inconsistent count and number of checksums");
        if (src._protInfo is not null)
        {
            // A group of batches written together may mix protected and unprotected ones (upstream asserts they do
            // not): records already in an unprotected destination keep it unprotected.
            if (_protInfo is null && Count != srcCount) return;
            if (_protInfo is null)
            {
                _protInfo = new ulong[Math.Max(8, srcCount)];
                _protCount = 0;
                _protectionBytesPerKey = src._protectionBytesPerKey;
            }
            for (int i = 0; i < srcCount; i++) AddProtection(src._protInfo[i]);
        }
        else if (_protInfo is not null)
        {
            // In this special case, we allow write batch without prot_info to be appended to write batch with empty
            // prot_info
            _protInfo = null;
            _protCount = 0;
            _protectionBytesPerKey = 0;
        }
    }
}
