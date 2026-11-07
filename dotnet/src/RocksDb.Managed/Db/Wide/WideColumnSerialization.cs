using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Wide;

/// <summary>
/// Position of one column inside a serialized entity (offsets into the entity bytes). <see cref="IsBlobIndex"/>: the
/// column's value is a serialized blob index (version 2 column type kTypeBlobIndex) instead of the value itself.
/// </summary>
internal readonly record struct WideColumnSlot(int NameOffset, int NameLength, int ValueOffset, int ValueLength, bool IsBlobIndex);

/// <summary>
/// Reads the blob a blob-backed entity column references (rocksdb::BlobFetcher as used by
/// WideColumnSerialization::ResolveEntityBlobColumns). Implemented by structs so resolution does not allocate.
/// </summary>
internal interface IEntityBlobFetcher
{
    /// <summary>Appends the blob referenced by the serialized, non-inlined blob index <paramref name="blobIndex"/> to <paramref name="output"/>.</summary>
    void FetchBlob(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndex, IBufferWriter<byte> output);
}

/// <summary>
/// Wide-column entity encoding. Port of WideColumnSerialization (db/wide/wide_column_serialization.{h,cc}).
/// <para>
/// Version 1 (entities without blob columns):
/// <c>varint32 version=1 | varint32 N | N x (varint32 name size, name, varint32 value size) | N values</c>.
/// Version 2 (entities with blob-backed columns, written by flush/compaction value separation and blob direct write):
/// <c>varint32 version=2 | varint32 N | varint32 name_sizes_bytes | varint32 value_sizes_bytes | varint32 names_bytes |
/// N column types (kTypeValue 0x01 / kTypeBlobIndex 0x11) | N varint32 name sizes | N varint32 value sizes | names |
/// values</c>; the value of a kTypeBlobIndex column is a serialized blob index. Like upstream, version 2 is written only
/// when at least one column is blob-backed. Column names are strictly increasing in bytewise order; the default column
/// has the empty name and sorts first.
/// </para>
/// <para>
/// Readers that consume column values (<see cref="Deserialize"/>, <see cref="GetValueOfDefaultColumn"/>) need a fully
/// resolved entity and report a blob reference as Corruption, like upstream's Deserialize(..., blob_columns = nullptr);
/// the read paths resolve blob columns first with <see cref="ResolveEntityBlobColumns{TFetcher}"/>.
/// </para>
/// </summary>
internal static class WideColumnSerialization
{
    public const uint Version1 = 1;
    public const uint Version2 = 2;

    /// <summary>Exact size of the version 1 encoding (SerializedSizeV1).</summary>
    public static int SerializedSizeV1(ReadOnlySpan<WideColumn> sortedColumns)
    {
        long size = Coding.VarintLength(Version1) + Coding.VarintLength((ulong)sortedColumns.Length);
        foreach (WideColumn c in sortedColumns)
        {
            size += Coding.VarintLength((ulong)c.Name.Length) + c.Name.Length + Coding.VarintLength((ulong)c.Value.Length) + c.Value.Length;
        }
        if (size > int.MaxValue) ThrowHelper.InvalidArgument("Wide column entity too large");
        return (int)size;
    }

    /// <summary>
    /// Port of WideColumnSerialization::Serialize (version 1). The columns must be sorted by name
    /// (<see cref="SortColumns"/>); duplicate or out-of-order names fail with Corruption ("Wide columns out of order")
    /// like upstream.
    /// </summary>
    public static void Serialize(ReadOnlySpan<WideColumn> sortedColumns, IBufferWriter<byte> output)
    {
        int total = SerializedSizeV1(sortedColumns);
        Span<byte> dst = output.GetSpan(total);
        int p = Coding.EncodeVarint32(dst, Version1);
        p += Coding.EncodeVarint32(dst[p..], (uint)sortedColumns.Length);
        for (int i = 0; i < sortedColumns.Length; i++)
        {
            ReadOnlySpan<byte> name = sortedColumns[i].Name.Span;
            if (i > 0 && sortedColumns[i - 1].Name.Span.SequenceCompareTo(name) >= 0) ThrowHelper.Corruption("Wide columns out of order");
            p += Coding.EncodeVarint32(dst[p..], (uint)name.Length);
            name.CopyTo(dst[p..]);
            p += name.Length;
            p += Coding.EncodeVarint32(dst[p..], (uint)sortedColumns[i].Value.Length);
        }
        foreach (WideColumn c in sortedColumns)
        {
            c.Value.Span.CopyTo(dst[p..]);
            p += c.Value.Length;
        }
        output.Advance(p);
    }

    /// <summary>WideColumnsHelper::SortColumns: stable order by column name (bytewise).</summary>
    public static void SortColumns(Span<WideColumn> columns)
    {
        if (columns.Length < 2) return;
        // Insertion sort for the common small case; Sort otherwise.
        if (columns.Length <= 16)
        {
            for (int i = 1; i < columns.Length; i++)
            {
                WideColumn c = columns[i];
                int j = i - 1;
                while (j >= 0 && columns[j].Name.Span.SequenceCompareTo(c.Name.Span) > 0)
                {
                    columns[j + 1] = columns[j];
                    j--;
                }
                columns[j + 1] = c;
            }
            return;
        }
        columns.Sort(static (a, b) => a.Name.Span.SequenceCompareTo(b.Name.Span));
    }

    /// <summary>Serializes an entity holding only the default column (a plain value as an entity).</summary>
    public static void SerializeDefaultColumn(ReadOnlySpan<byte> value, IBufferWriter<byte> output)
    {
        int total = Coding.VarintLength(Version1) + Coding.VarintLength(1) + Coding.VarintLength(0) + Coding.VarintLength((ulong)value.Length) + value.Length;
        Span<byte> dst = output.GetSpan(total);
        int p = Coding.EncodeVarint32(dst, Version1);
        p += Coding.EncodeVarint32(dst[p..], 1);
        p += Coding.EncodeVarint32(dst[p..], 0);
        p += Coding.EncodeVarint32(dst[p..], (uint)value.Length);
        value.CopyTo(dst[p..]);
        output.Advance(p + value.Length);
    }

    /// <summary>
    /// Port of WideColumnSerialization::Deserialize(input, columns, blob_columns = nullptr): decodes the column positions
    /// of a fully resolved <paramref name="entity"/> into <paramref name="slots"/> (cleared first). Corruption on
    /// malformed input and for blob-backed columns.
    /// </summary>
    public static void Deserialize(ReadOnlySpan<byte> entity, List<WideColumnSlot> slots)
    {
        string? error = TryDeserialize(entity, slots, out bool hasBlob);
        if (error is not null) ThrowHelper.Corruption(error);
        if (hasBlob) ThrowHelper.Corruption("Wide column blob reference in a context where it is not expected.");
    }

    /// <summary>
    /// Port of WideColumnSerialization::Deserialize(input, columns, blob_columns): like <see cref="Deserialize"/> but
    /// blob-backed columns are allowed (<see cref="WideColumnSlot.IsBlobIndex"/>; their blob indexes are validated).
    /// Returns whether any column is blob-backed.
    /// </summary>
    public static bool DeserializeWithBlobColumns(ReadOnlySpan<byte> entity, List<WideColumnSlot> slots)
    {
        string? error = TryDeserialize(entity, slots, out bool hasBlob);
        if (error is not null) ThrowHelper.Corruption(error);
        if (!hasBlob) return false;
        foreach (WideColumnSlot s in slots)
        {
            if (!s.IsBlobIndex) continue;
            if (s.ValueLength == 0) ThrowHelper.Corruption("Empty blob index in wide column");
            if (Blob.BlobIndex.TryDecode(entity.Slice(s.ValueOffset, s.ValueLength), out _) is not null)
            {
                ThrowHelper.Corruption("Error decoding blob index in wide column");
            }
        }
        return true;
    }

    /// <summary>
    /// Port of WideColumnSerialization::HasBlobColumns: true for a version 2 entity with at least one kTypeBlobIndex
    /// column. Only the header and the column types are read. Corruption on decode errors.
    /// </summary>
    public static bool HasBlobColumns(ReadOnlySpan<byte> entity)
    {
        int pos = 0;
        if (!TryGetVarint32(entity, ref pos, out uint version)) ThrowHelper.Corruption("Error decoding wide column version");
        if (version < Version2) return false;
        if (version > Version2) ThrowHelper.Corruption("Corrupt or unsupported wide column version");
        if (!TryGetVarint32(entity, ref pos, out uint numColumns)) ThrowHelper.Corruption("Error decoding number of wide columns");
        if (numColumns == 0) return false;
        if (!TryGetVarint32(entity, ref pos, out _) || !TryGetVarint32(entity, ref pos, out _) || !TryGetVarint32(entity, ref pos, out _))
        {
            ThrowHelper.Corruption("Error decoding wide column skip info");
        }
        if ((uint)(entity.Length - pos) < numColumns) ThrowHelper.Corruption("Error decoding wide column types");
        bool any = false;
        for (int i = 0; i < numColumns; i++)
        {
            var t = (EntryType)entity[pos + i];
            if (t == EntryType.BlobIndex) any = true;
            else if (t != EntryType.Value) ThrowHelper.Corruption("Unsupported wide column ValueType");
        }
        return any;
    }

    /// <summary>
    /// Port of WideColumnSerialization::ForEachBlobFileNumber: the blob indexes of the blob-backed columns of
    /// <paramref name="entity"/> (none for version 1). <paramref name="slots"/> is scratch. Corruption on decode errors.
    /// </summary>
    public static void GetBlobColumnIndexes(ReadOnlySpan<byte> entity, List<WideColumnSlot> slots)
    {
        if (!HasBlobColumns(entity))
        {
            slots.Clear();
            return;
        }
        DeserializeWithBlobColumns(entity, slots);
        // Keep only the blob columns.
        int w = 0;
        for (int i = 0; i < slots.Count; i++)
        {
            if (slots[i].IsBlobIndex) slots[w++] = slots[i];
        }
        slots.RemoveRange(w, slots.Count - w);
    }

    /// <summary>
    /// Port of WideColumnSerialization::ResolveEntityBlobColumns: fetches the blobs of the blob-backed columns of
    /// <paramref name="entity"/> (inlined blob indexes yield their value) and writes the fully resolved entity as
    /// version 1 to <paramref name="output"/>. Returns false (and writes nothing) when the entity has no blob columns.
    /// <paramref name="slots"/> and <paramref name="scratch"/> are reusable scratch buffers.
    /// </summary>
    public static bool ResolveEntityBlobColumns<TFetcher>(ReadOnlySpan<byte> entity, ReadOnlySpan<byte> userKey, ref TFetcher fetcher,
        List<WideColumnSlot> slots, PooledBufferWriter scratch, IBufferWriter<byte> output)
        where TFetcher : struct, IEntityBlobFetcher
    {
        if (!HasBlobColumns(entity)) return false;
        DeserializeWithBlobColumns(entity, slots);
        int n = slots.Count;
        // Fetched values go to scratch; remember their (offset, length) in place of the slot's value position.
        scratch.Clear();
        Span<long> resolved = n <= 64 ? stackalloc long[n] : new long[n];
        for (int i = 0; i < n; i++)
        {
            WideColumnSlot s = slots[i];
            if (!s.IsBlobIndex) continue;
            ReadOnlySpan<byte> idx = entity.Slice(s.ValueOffset, s.ValueLength);
            Blob.BlobIndex bi = Blob.BlobIndex.Decode(idx);
            int start = scratch.WrittenCount;
            if (bi.IsInlined) scratch.Write(bi.InlinedValue);
            else fetcher.FetchBlob(userKey, idx, scratch);
            resolved[i] = ((long)start << 32) | (uint)(scratch.WrittenCount - start);
        }
        ReadOnlySpan<byte> fetched = scratch.WrittenSpan;
        long total = Coding.VarintLength(Version1) + Coding.VarintLength((ulong)n);
        for (int i = 0; i < n; i++)
        {
            int vlen = slots[i].IsBlobIndex ? (int)(uint)resolved[i] : slots[i].ValueLength;
            total += Coding.VarintLength((ulong)slots[i].NameLength) + slots[i].NameLength + Coding.VarintLength((ulong)vlen) + vlen;
        }
        if (total > int.MaxValue) ThrowHelper.InvalidArgument("Wide column entity too large");
        Span<byte> dst = output.GetSpan((int)total);
        int p = Coding.EncodeVarint32(dst, Version1);
        p += Coding.EncodeVarint32(dst[p..], (uint)n);
        for (int i = 0; i < n; i++)
        {
            WideColumnSlot s = slots[i];
            p += Coding.EncodeVarint32(dst[p..], (uint)s.NameLength);
            entity.Slice(s.NameOffset, s.NameLength).CopyTo(dst[p..]);
            p += s.NameLength;
            p += Coding.EncodeVarint32(dst[p..], (uint)(s.IsBlobIndex ? (int)(uint)resolved[i] : s.ValueLength));
        }
        for (int i = 0; i < n; i++)
        {
            WideColumnSlot s = slots[i];
            ReadOnlySpan<byte> v = s.IsBlobIndex ? fetched.Slice((int)(resolved[i] >> 32), (int)(uint)resolved[i]) : entity.Slice(s.ValueOffset, s.ValueLength);
            v.CopyTo(dst[p..]);
            p += v.Length;
        }
        output.Advance(p);
        return true;
    }

    /// <summary>
    /// Port of WideColumnSerialization::SerializeV2: writes the columns of <paramref name="columns"/> (sorted, unique
    /// names) as a version 2 entity. Columns with <see cref="EntityColumnList.IsBlobIndex"/> get type kTypeBlobIndex
    /// and their value (a serialized blob index) as payload. Corruption "Wide columns out of order" like upstream.
    /// </summary>
    public static void SerializeV2(EntityColumnList columns, IBufferWriter<byte> output)
    {
        int n = columns.Count;
        long nameSizesBytes = 0, valueSizesBytes = 0, namesBytes = 0, valuesBytes = 0;
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<byte> name = columns.GetName(i);
            if (i > 0 && columns.GetName(i - 1).SequenceCompareTo(name) >= 0) ThrowHelper.Corruption("Wide columns out of order");
            int vlen = columns.GetValue(i).Length;
            nameSizesBytes += Coding.VarintLength((ulong)name.Length);
            namesBytes += name.Length;
            valueSizesBytes += Coding.VarintLength((ulong)vlen);
            valuesBytes += vlen;
        }
        if (nameSizesBytes > uint.MaxValue || valueSizesBytes > uint.MaxValue) ThrowHelper.InvalidArgument("Wide column metadata too large");
        if (namesBytes > uint.MaxValue) ThrowHelper.InvalidArgument("Wide column names too large");
        if (valuesBytes > uint.MaxValue) ThrowHelper.InvalidArgument("Wide column values too large");
        long total = Coding.VarintLength(Version2) + Coding.VarintLength((ulong)n) + n + Coding.VarintLength((ulong)nameSizesBytes)
                     + Coding.VarintLength((ulong)valueSizesBytes) + Coding.VarintLength((ulong)namesBytes) + nameSizesBytes + valueSizesBytes
                     + namesBytes + valuesBytes;
        if (total > int.MaxValue) ThrowHelper.InvalidArgument("Wide column entity too large");
        Span<byte> dst = output.GetSpan((int)total);
        int p = Coding.EncodeVarint32(dst, Version2);
        p += Coding.EncodeVarint32(dst[p..], (uint)n);
        p += Coding.EncodeVarint32(dst[p..], (uint)nameSizesBytes);
        p += Coding.EncodeVarint32(dst[p..], (uint)valueSizesBytes);
        p += Coding.EncodeVarint32(dst[p..], (uint)namesBytes);
        for (int i = 0; i < n; i++) dst[p++] = (byte)(columns.IsBlobIndex(i) ? EntryType.BlobIndex : EntryType.Value);
        int s4 = p, s5 = s4 + (int)nameSizesBytes, s6 = s5 + (int)valueSizesBytes, s7 = s6 + (int)namesBytes;
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<byte> name = columns.GetName(i);
            ReadOnlySpan<byte> value = columns.GetValue(i);
            s4 += Coding.EncodeVarint32(dst[s4..], (uint)name.Length);
            s5 += Coding.EncodeVarint32(dst[s5..], (uint)value.Length);
            name.CopyTo(dst[s6..]);
            s6 += name.Length;
            value.CopyTo(dst[s7..]);
            s7 += value.Length;
        }
        output.Advance(s7);
    }

    /// <summary>
    /// Writes <paramref name="columns"/> as version 2 when a column is blob-backed and as version 1 otherwise (the
    /// choice upstream's flush/compaction extraction, GC and blob direct write make).
    /// </summary>
    public static void Serialize(EntityColumnList columns, IBufferWriter<byte> output)
    {
        if (columns.HasBlobColumns)
        {
            SerializeV2(columns, output);
            return;
        }
        int n = columns.Count;
        long total = Coding.VarintLength(Version1) + Coding.VarintLength((ulong)n);
        for (int i = 0; i < n; i++)
        {
            int nl = columns.GetName(i).Length, vl = columns.GetValue(i).Length;
            total += Coding.VarintLength((ulong)nl) + nl + Coding.VarintLength((ulong)vl) + vl;
        }
        if (total > int.MaxValue) ThrowHelper.InvalidArgument("Wide column entity too large");
        Span<byte> dst = output.GetSpan((int)total);
        int p = Coding.EncodeVarint32(dst, Version1);
        p += Coding.EncodeVarint32(dst[p..], (uint)n);
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<byte> name = columns.GetName(i);
            if (i > 0 && columns.GetName(i - 1).SequenceCompareTo(name) >= 0) ThrowHelper.Corruption("Wide columns out of order");
            p += Coding.EncodeVarint32(dst[p..], (uint)name.Length);
            name.CopyTo(dst[p..]);
            p += name.Length;
            p += Coding.EncodeVarint32(dst[p..], (uint)columns.GetValue(i).Length);
        }
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<byte> v = columns.GetValue(i);
            v.CopyTo(dst[p..]);
            p += v.Length;
        }
        output.Advance(p);
    }

    /// <summary>Non-throwing <see cref="Deserialize"/>: returns the Corruption message, or null.</summary>
    public static string? TryDeserialize(ReadOnlySpan<byte> entity, List<WideColumnSlot> slots, out bool hasBlobColumns)
    {
        slots.Clear();
        hasBlobColumns = false;
        int pos = 0;
        if (!TryGetVarint32(entity, ref pos, out uint version)) return "Error decoding wide column version";
        if (version > Version2) return "Corrupt or unsupported wide column version";
        if (!TryGetVarint32(entity, ref pos, out uint numColumns)) return "Error decoding number of wide columns";
        return version < Version2
            ? DeserializeV1(entity, pos, numColumns, slots)
            : DeserializeV2(entity, pos, numColumns, slots, out hasBlobColumns);
    }

    private static bool TryGetVarint32(ReadOnlySpan<byte> input, ref int pos, out uint value)
    {
        if (pos >= input.Length)
        {
            value = 0;
            return false;
        }
        int n = Coding.DecodeVarint32(input[pos..], out value);
        if (n <= 0) return false;
        pos += n;
        return true;
    }

    private static string? DeserializeV1(ReadOnlySpan<byte> input, int pos, uint numColumns, List<WideColumnSlot> slots)
    {
        if (numColumns > (uint)(input.Length - pos)) return "Error decoding wide column name"; // each column needs >= 2 bytes
        Span<uint> valueSizes = numColumns <= 64 ? stackalloc uint[(int)numColumns] : new uint[numColumns];
        Span<int> nameOffsets = numColumns <= 64 ? stackalloc int[(int)numColumns] : new int[numColumns];
        Span<int> nameLengths = numColumns <= 64 ? stackalloc int[(int)numColumns] : new int[numColumns];
        for (int i = 0; i < numColumns; i++)
        {
            if (!TryGetVarint32(input, ref pos, out uint nameLen) || nameLen > (uint)(input.Length - pos)) return "Error decoding wide column name";
            nameOffsets[i] = pos;
            nameLengths[i] = (int)nameLen;
            pos += (int)nameLen;
            if (i > 0 && input.Slice(nameOffsets[i - 1], nameLengths[i - 1]).SequenceCompareTo(input.Slice(nameOffsets[i], nameLengths[i])) >= 0)
            {
                return "Wide columns out of order";
            }
            if (!TryGetVarint32(input, ref pos, out valueSizes[i])) return "Error decoding wide column value size";
        }
        long dataPos = pos;
        // Size a fresh result list once (numColumns is bounded by the input: every column was decoded above).
        slots.EnsureCapacity(slots.Count + (int)numColumns);
        for (int i = 0; i < numColumns; i++)
        {
            if (dataPos + valueSizes[i] > input.Length) return "Error decoding wide column value payload";
            slots.Add(new WideColumnSlot(nameOffsets[i], nameLengths[i], (int)dataPos, (int)valueSizes[i], IsBlobIndex: false));
            dataPos += valueSizes[i];
        }
        if (dataPos != input.Length) return "Unexpected trailing data after wide column entity";
        return null;
    }

    private static string? DeserializeV2(ReadOnlySpan<byte> input, int pos, uint numColumns, List<WideColumnSlot> slots, out bool hasBlobColumns)
    {
        hasBlobColumns = false;
        if (!TryGetVarint32(input, ref pos, out uint nameSizesBytes)) return "Error decoding wide column name sizes bytes";
        if (!TryGetVarint32(input, ref pos, out uint valueSizesBytes)) return "Error decoding wide column value sizes bytes";
        if (!TryGetVarint32(input, ref pos, out uint namesBytes)) return "Error decoding wide column names bytes";
        if ((uint)(input.Length - pos) < numColumns) return "Error decoding wide column types";
        for (int i = 0; i < numColumns; i++)
        {
            var t = (EntryType)input[pos + i];
            // IsValidColumnValueType: kTypeValue or kTypeBlobIndex.
            if (t == EntryType.BlobIndex) hasBlobColumns = true;
            else if (t != EntryType.Value) return "Unsupported wide column ValueType";
        }
        int types = pos;
        pos += (int)numColumns;
        long metadataSize = (long)nameSizesBytes + valueSizesBytes + namesBytes;
        if (input.Length - pos < metadataSize) return "Error decoding wide column sections";
        int s4 = pos, s4Limit = s4 + (int)nameSizesBytes;
        int s5 = s4Limit, s5Limit = s5 + (int)valueSizesBytes;
        int s6 = s5Limit;
        int s7 = s6 + (int)namesBytes;
        long namePos = 0, valuePos = 0;
        // Size a fresh result list once (numColumns is bounded by the input: one type byte per column, checked above).
        slots.EnsureCapacity(slots.Count + (int)numColumns);
        for (int i = 0; i < numColumns; i++)
        {
            if (!TryGetVarint32(input[..s4Limit], ref s4, out uint ns)) return "Error decoding wide column name size";
            if (!TryGetVarint32(input[..s5Limit], ref s5, out uint vs)) return "Error decoding wide column value size";
            if (namePos + ns > namesBytes) return "Error decoding wide column name";
            if (i > 0)
            {
                WideColumnSlot prev = slots[^1];
                if (input.Slice(prev.NameOffset, prev.NameLength).SequenceCompareTo(input.Slice(s6 + (int)namePos, (int)ns)) >= 0)
                {
                    return "Wide columns out of order";
                }
            }
            if (s7 + valuePos + vs > input.Length) return "Error decoding wide column value payload";
            slots.Add(new WideColumnSlot(s6 + (int)namePos, (int)ns, s7 + (int)valuePos, (int)vs, input[types + i] == (byte)EntryType.BlobIndex));
            namePos += ns;
            valuePos += vs;
        }
        long totalValuesBytes = input.Length - s7;
        if (s4 != s4Limit || s5 != s5Limit || namePos != namesBytes || valuePos != totalValuesBytes) return "Wide column section size mismatch";
        return null;
    }

    /// <summary>
    /// Port of WideColumnSerialization::GetValueOfDefaultColumn: the value of the column with the empty name, or an
    /// empty span when the entity has none.
    /// </summary>
    public static ReadOnlySpan<byte> GetValueOfDefaultColumn(ReadOnlySpan<byte> entity)
    {
        List<WideColumnSlot> slots = t_slots ??= new List<WideColumnSlot>(8);
        Deserialize(entity, slots);
        if (slots.Count == 0 || slots[0].NameLength != 0) return default;
        WideColumnSlot s = slots[0];
        return entity.Slice(s.ValueOffset, s.ValueLength);
    }

    [ThreadStatic] private static List<WideColumnSlot>? t_slots;

    /// <summary>
    /// Writes <paramref name="entity"/> with its default column replaced by <paramref name="newDefault"/> (added when
    /// missing). Used for merges on top of an entity (MergeHelper::TimedFullMergeWithEntity).
    /// </summary>
    public static void ReplaceDefaultColumn(ReadOnlySpan<byte> entity, ReadOnlySpan<byte> newDefault, IBufferWriter<byte> output)
    {
        var slots = new List<WideColumnSlot>(8);
        Deserialize(entity, slots);
        bool hasDefault = slots.Count > 0 && slots[0].NameLength == 0;
        int n = slots.Count + (hasDefault ? 0 : 1);
        long total = Coding.VarintLength(Version1) + Coding.VarintLength((ulong)n);
        total += Coding.VarintLength(0) + Coding.VarintLength((ulong)newDefault.Length) + newDefault.Length;
        for (int i = hasDefault ? 1 : 0; i < slots.Count; i++)
        {
            total += Coding.VarintLength((ulong)slots[i].NameLength) + slots[i].NameLength + Coding.VarintLength((ulong)slots[i].ValueLength) + slots[i].ValueLength;
        }
        Span<byte> dst = output.GetSpan((int)total);
        int p = Coding.EncodeVarint32(dst, Version1);
        p += Coding.EncodeVarint32(dst[p..], (uint)n);
        p += Coding.EncodeVarint32(dst[p..], 0);
        p += Coding.EncodeVarint32(dst[p..], (uint)newDefault.Length);
        for (int i = hasDefault ? 1 : 0; i < slots.Count; i++)
        {
            p += Coding.EncodeVarint32(dst[p..], (uint)slots[i].NameLength);
            entity.Slice(slots[i].NameOffset, slots[i].NameLength).CopyTo(dst[p..]);
            p += slots[i].NameLength;
            p += Coding.EncodeVarint32(dst[p..], (uint)slots[i].ValueLength);
        }
        newDefault.CopyTo(dst[p..]);
        p += newDefault.Length;
        for (int i = hasDefault ? 1 : 0; i < slots.Count; i++)
        {
            entity.Slice(slots[i].ValueOffset, slots[i].ValueLength).CopyTo(dst[p..]);
            p += slots[i].ValueLength;
        }
        output.Advance(p);
    }
}
