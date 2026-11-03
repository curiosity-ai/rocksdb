using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tools;

/// <summary>
/// Text helpers shared by the tools. Output is byte-oriented like the native tools: raw keys and values are mapped
/// one byte per char (Latin-1) and the console writer encodes with Latin-1, so non-hex output matches native byte for byte.
/// </summary>
internal static class ToolText
{
    public static readonly Encoding Bytes = Encoding.Latin1;

    /// <summary>Slice::ToString(false): the raw bytes.</summary>
    public static string Raw(ReadOnlySpan<byte> s) => Bytes.GetString(s);

    /// <summary>Slice::ToString(hex): raw bytes or upper-case hex without prefix.</summary>
    public static string ToString(ReadOnlySpan<byte> s, bool hex) => hex ? Convert.ToHexString(s) : Raw(s);

    /// <summary>LDBCommand::StringToHex: "0x" + upper-case hex.</summary>
    public static string StringToHex(ReadOnlySpan<byte> s) => "0x" + Convert.ToHexString(s);

    /// <summary>A command-line argument as bytes (Latin-1, so that any byte can be passed).</summary>
    public static byte[] ArgBytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>LDBCommand::HexToString: requires the "0x" prefix.</summary>
    public static byte[] HexToBytes(string s)
    {
        if (s.Length < 2 || s[0] != '0' || s[1] != 'x') throw new ToolException("Invalid hex input " + s + ".  Must start with 0x");
        try
        {
            return Convert.FromHexString(s.AsSpan(2));
        }
        catch (FormatException)
        {
            throw new ToolException("Invalid hex input " + s);
        }
    }

    /// <summary>ParsedInternalKey::DebugString(log_err_key = true, hex): "'key' seq:N, type:T".</summary>
    public static string InternalKeyDebugString(ReadOnlySpan<byte> internalKey, bool hex)
    {
        if (!DbFormat.TryParseInternalKey(internalKey, out ParsedInternalKey p)) return "(bad)" + EscapeString(internalKey);
        return "'" + ToString(internalKey[..p.UserKeyLength], hex) + "' seq:" + p.Sequence.ToString(CultureInfo.InvariantCulture) + ", type:"
            + ((int)p.Type).ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>EscapeString (util/string_util.cc): printable ASCII kept, others as \xNN.</summary>
    public static string EscapeString(ReadOnlySpan<byte> s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (byte b in s)
        {
            if (b >= ' ' && b <= '~') sb.Append((char)b);
            else sb.Append("\\x").Append(b.ToString("X2", CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    private static string N(ulong v) => v.ToString(CultureInfo.InvariantCulture);

    /// <summary>std::to_string(double): fixed with 6 decimals.</summary>
    public static string Dbl(double v) => v.ToString("F6", CultureInfo.InvariantCulture);

    /// <summary>TableProperties::ToString(prop_delim, kv_delim) (table/table_properties.cc), same keys and order.</summary>
    public static string TablePropertiesToString(TableProperties p, string propDelim, string kvDelim)
    {
        var r = new StringBuilder(1024);
        void Add(string k, string v) => r.Append(k).Append(kvDelim).Append(v).Append(propDelim);
        Add("# data blocks", N(p.NumDataBlocks));
        Add("# data blocks compression rejected", N(p.NumDataBlocksCompressionRejected));
        Add("# data blocks compression bypassed", N(p.NumDataBlocksCompressionBypassed));
        Add("# uniform blocks", N(p.NumUniformBlocks));
        Add("# entries", N(p.NumEntries));
        Add("# deletions", N(p.NumDeletions));
        Add("# merge operands", N(p.NumMergeOperands));
        Add("# range deletions", N(p.NumRangeDeletions));
        Add("raw key size", N(p.RawKeySize));
        Add("raw average key size", Dbl(p.NumEntries != 0 ? 1.0 * p.RawKeySize / p.NumEntries : 0.0));
        Add("raw value size", N(p.RawValueSize));
        Add("raw average value size", Dbl(p.NumEntries != 0 ? 1.0 * p.RawValueSize / p.NumEntries : 0.0));
        Add("data block size", N(p.DataSize));
        Add("data uncompressed size", N(p.UncompressedDataSize));
        Add("index block size (user-key? " + N(p.IndexKeyIsUserKey) + ", delta-value? " + N(p.IndexValueIsDeltaEncoded) + ")", N(p.IndexSize));
        if (p.IndexPartitions != 0)
        {
            Add("# index partitions", N(p.IndexPartitions));
            Add("top-level index size", N(p.TopLevelIndexSize));
        }
        Add("filter block size", N(p.FilterSize));
        Add("# entries for filter", N(p.NumFilterEntries));
        Add("(estimated) table size", N(p.DataSize + p.IndexSize + p.FilterSize));
        Add("standard index is stub", N(p.StandardIndexIsStub));
        Add("filter policy name", Na(p.FilterPolicyName));
        Add("prefix extractor name", Na(p.PrefixExtractorName));
        Add("column family ID", p.ColumnFamilyId == TableProperties.UnknownColumnFamily ? "N/A" : N(p.ColumnFamilyId));
        Add("column family name", Na(p.ColumnFamilyName));
        Add("comparator name", Na(p.ComparatorName));
        Add("user defined timestamps persisted", p.UserDefinedTimestampsPersisted != 0 ? "true" : "false");
        Add("largest sequence number in file", N(p.KeyLargestSeqno));
        Add("smallest sequence number in file", N(p.KeySmallestSeqno));
        Add("merge operator name", Na(p.MergeOperatorName));
        Add("property collectors names", Na(p.PropertyCollectorsNames));
        Add("SST file compression algo", Na(p.CompressionName));
        Add("SST file compression options", Na(p.CompressionOptions));
        Add("creation time", N(p.CreationTime));
        Add("time stamp of earliest key", N(p.OldestKeyTime));
        Add("time stamp of newest key", N(p.NewestKeyTime));
        Add("file creation time", N(p.FileCreationTime));
        // LsmInfoAtCreation::DecodeFrom: bits 0..7 level (0xff unknown), bits 8..9 state.
        ulong lsm = p.LsmInfoAtCreation;
        ulong state = (lsm >> 8) & 3;
        string level, bottom;
        switch (state)
        {
            case 1:
            case 2:
                level = (lsm & 0xff) == 0xff ? "unknown" : N(lsm & 0xff);
                bottom = state == 2 ? "true" : "false";
                break;
            case 3:
                level = bottom = "N/A";
                break;
            default:
                level = bottom = "unknown";
                break;
        }
        Add("level at creation", level);
        Add("bottommost at creation", bottom);
        Add("slow compression estimated data size", N(p.SlowCompressionEstimatedDataSize));
        Add("fast compression estimated data size", N(p.FastCompressionEstimatedDataSize));
        Add("DB identity", p.DbId);
        Add("DB session identity", p.DbSessionId);
        Add("DB host id", p.DbHostId);
        Add("original file number", N(p.OriginalFileNumber));
        Add("unique ID", UniqueIdHumanString(p));
        var mapping = new SeqnoToTimeMapping();
        Add("Sequence number to time mapping", mapping.DecodeFrom(p.SequenceNumberTimeMapping) is null ? mapping.ToHumanString() : "N/A");
        return r.ToString();
    }

    private static string Na(string s) => s.Length == 0 ? "N/A" : s;

    /// <summary>GetUniqueIdFromTableProperties + UniqueIdToHumanString (table/unique_id.cc): "N/A" without db/session ids.</summary>
    public static string UniqueIdHumanString(TableProperties p)
    {
        if (p.DbId.Length == 0 || p.DbSessionId.Length == 0) return "N/A";
        Cache.OffsetableCacheKey.GetSstInternalUniqueId(p.DbId, p.DbSessionId, p.OriginalFileNumber, out ulong id0, out ulong id1);
        // InternalUniqueIdToExternal.
        const ulong HiOffsetForZero = 17391078804906429400UL;
        const ulong LoOffsetForZero = 6417269962128484497UL;
        Hash.BijectiveHash2x64(unchecked(id1 + HiOffsetForZero), unchecked(id0 + LoOffsetForZero), out ulong hi, out ulong lo);
        Span<byte> bytes = stackalloc byte[16];
        Coding.EncodeFixed64(bytes, lo);
        Coding.EncodeFixed64(bytes[8..], hi);
        string hex = Convert.ToHexString(bytes);
        var sb = new StringBuilder(hex.Length + 2);
        for (int i = 0; i < hex.Length; i++)
        {
            if (i > 0 && i % 16 == 0) sb.Append('-');
            sb.Append(hex[i]);
        }
        return sb.ToString();
    }
}

/// <summary>A command failure (LDBCommandExecuteResult::Failed): printed as "Failed: message".</summary>
internal sealed class ToolException(string message) : Exception(message);
