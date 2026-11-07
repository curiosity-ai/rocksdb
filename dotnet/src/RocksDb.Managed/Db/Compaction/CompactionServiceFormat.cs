using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Options;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>
/// The remote compaction job description sent by the primary (rocksdb::CompactionServiceInput,
/// db/compaction/compaction_job.h). Serialized by <see cref="CompactionServiceFormat"/>.
/// </summary>
internal sealed class CompactionServiceInput
{
    public string CfName = "";
    /// <summary>Snapshot sequence numbers, strictly increasing.</summary>
    public List<ulong> Snapshots = new();
    /// <summary>Table file names ("000012.sst") of all inputs of all levels.</summary>
    public List<string> InputFiles = new();
    public List<string> InputBlobFiles = new();
    public bool HasBlobGcCutoffFileNumber;
    public ulong BlobGcCutoffFileNumber;
    public ulong BlobFileNumberStart;
    public ulong BlobFileNumberLimit;
    public int OutputLevel;
    public string DbId = "";
    public bool HasBegin;
    public byte[] Begin = [];
    public bool HasEnd;
    public byte[] End = [];
    public ulong OptionsFileNumber;
    public ulong MinManifestFileNumber;
    public ulong MinManifestFileSize;
    public ulong PreserveInternalTimeSeconds;
    public ulong PrecludeLastLevelDataSeconds;
    public ulong PreserveTimeMinSeqnoFloor;
    public int ProximalLevel;
    public int ProximalOutputRangeType;
    public ulong KeepInLastLevelThroughSeqno;
}

/// <summary>One output table of a remote compaction (rocksdb::CompactionServiceOutputFile).</summary>
internal sealed class CompactionServiceOutputFile
{
    public string FileName = "";
    public ulong FileSize;
    public ulong SmallestSeqno;
    public ulong LargestSeqno;
    public byte[] SmallestInternalKey = [];
    public byte[] LargestInternalKey = [];
    public ulong OldestBlobFileNumber;
    public ulong OldestAncestorTime;
    public ulong FileCreationTime;
    public ulong EpochNumber;
    public byte[] FileChecksum = [];
    public string FileChecksumFuncName = FileMetaData.UnknownFileChecksumFuncName;
    public ulong ParanoidHash;
    public bool MarkedForCompaction;
    public UniqueId64x2 UniqueId;
    public TableProperties TableProperties = new();
    public bool IsProximalLevelOutput;
    public Temperature FileTemperature;
}

/// <summary>CompactionJobStats fields carried in the result (include/rocksdb/compaction_job_stats.h).</summary>
internal sealed class RemoteCompactionJobStats
{
    public ulong ElapsedMicros, CpuMicros, NumInputRecords, NumBlobsRead, NumOutputRecords, TotalInputBytes, TotalBlobBytesRead, TotalOutputBytes,
        TotalOutputBytesBlob, NumRecordsReplaced, TotalInputRawKeyBytes, TotalInputRawValueBytes, NumInputDeletionRecords, NumExpiredDeletionRecords,
        NumCorruptKeys, FileWriteNanos, FileRangeSyncNanos, FileFsyncNanos, FilePrepareWriteNanos, NumSingleDelFallthru, NumSingleDelMismatch;
    public ulong NumInputFiles, NumInputFilesAtOutputLevel, NumOutputFiles, NumOutputFilesBlob;
    public bool HasAccurateNumInputRecords, IsFullCompaction, IsManualCompaction, IsRemoteCompaction;
    public byte[] SmallestOutputKeyPrefix = [], LargestOutputKeyPrefix = [];
}

/// <summary>InternalStats::CompactionStats fields carried in the result (db/internal_stats.h).</summary>
internal sealed class RemoteCompactionLevelStats
{
    public ulong Micros, CpuMicros, BytesReadNonOutputLevels, BytesReadOutputLevel, BytesSkippedNonOutputLevels, BytesSkippedOutputLevel, BytesReadBlob,
        BytesWritten, BytesWrittenBlob, BytesMoved, NumInputRecords, NumDroppedRecords, NumOutputRecords, Count;
    public int NumInputFilesInNonOutputLevels, NumInputFilesInOutputLevel, NumFilteredInputFilesInNonOutputLevels, NumFilteredInputFilesInOutputLevel,
        NumOutputFiles, NumOutputFilesBlob;
    /// <summary>Per CompactionReason counts (kNumOfReasons entries).</summary>
    public int[] Counts = new int[CompactionServiceFormat.NumCompactionReasons];
}

/// <summary>The worker's answer (rocksdb::CompactionServiceResult). Serialized by <see cref="CompactionServiceFormat"/>.</summary>
internal sealed class CompactionServiceResult
{
    public StatusCode StatusCode;
    public StatusSubCode StatusSubCode;
    public byte StatusSeverity;
    public string StatusMessage = "";
    public List<CompactionServiceOutputFile> OutputFiles = new();
    /// <summary>Hex-free encodings (VersionEdit BlobFileAddition / BlobFileGarbage EncodeTo) of the blob metadata.</summary>
    public List<byte[]> BlobFileAdditions = new();
    public List<byte[]> BlobFileGarbages = new();
    public ulong BlobCompactionVersion;
    public ulong BlobGcCutoffVersion;
    public int OutputLevel;
    public string OutputPath = "";
    public ulong BytesRead;
    public ulong BytesWritten;
    public RemoteCompactionJobStats Stats = new();
    public RemoteCompactionLevelStats OutputLevelStats = new();
    public bool HasProximalLevelOutput;
    public RemoteCompactionLevelStats ProximalLevelStats = new();

    public bool Ok => StatusCode == StatusCode.Ok;

    public RocksDbException ToException() =>
        new(StatusCode, StatusMessage.Length > 0 ? StatusMessage : StatusCode.ToString(), StatusSubCode) { Severity = (ErrorSeverity)StatusSeverity };
}

/// <summary>
/// The wire format of remote compaction (db/compaction/compaction_service_job.cc, CompactionServiceInput/Result::Write
/// and Read): a fixed32 format version (kOptionsString = 1) followed by an options string produced by
/// OptionTypeInfo::SerializeType over the type maps cs_input_type_info / cs_result_type_info
/// (options/options_helper.cc, include/rocksdb/utilities/options_type.h):
/// <list type="bullet">
/// <item>"name=value;" for every field; strings (kEncodedString) are upper-case hex; booleans "true"/"false";</item>
/// <item>vectors join elements with ':' (an element containing ':' is wrapped in braces) and are wrapped in braces when
/// the result contains '=' or starts with a brace and has several elements (SerializeVector); fixed arrays alike
/// (unique_id, counts);</item>
/// <item>structs are "{name=value;...}"; string maps "{hexkey=hexvalue;...}".</item>
/// </list>
/// Native writes the fields in unordered_map order; field order is irrelevant to the parser, so the managed writer uses
/// the declaration order. Parsing ignores unknown fields (ignore_unknown_options), accepts values with or without the
/// outer braces (StringToMap keeps them since 11.13, older releases strip them) and resets absent fields to their
/// defaults, so 11.12 native workers / primaries and this port exchange jobs in both directions.
/// </summary>
internal static class CompactionServiceFormat
{
    /// <summary>BinaryFormatVersion::kOptionsString.</summary>
    public const uint FormatVersion = 1;

    /// <summary>CompactionReason::kNumOfReasons (size of the per-reason counts array).</summary>
    public const int NumCompactionReasons = 21;

    // ---- writer --------------------------------------------------------------------------------------

    private sealed class Writer
    {
        private readonly StringBuilder _sb = new();

        public Writer Str(string name, string value) => Raw(name, Hex(Encoding.UTF8.GetBytes(value)));
        public Writer Bytes(string name, ReadOnlySpan<byte> value) => Raw(name, Hex(value));
        public Writer U64(string name, ulong v) => Raw(name, v.ToString(CultureInfo.InvariantCulture));
        public Writer I32(string name, int v) => Raw(name, v.ToString(CultureInfo.InvariantCulture));
        public Writer Bool(string name, bool v) => Raw(name, v ? "true" : "false");

        public Writer Raw(string name, string value)
        {
            _sb.Append(name).Append('=').Append(value).Append(';');
            return this;
        }

        public override string ToString() => _sb.ToString();
    }

    private static string Hex(ReadOnlySpan<byte> b) => Convert.ToHexString(b);

    /// <summary>SerializeVector / SerializeArray with separator ':'.</summary>
    private static string Vector(IReadOnlyList<string> elems)
    {
        var sb = new StringBuilder();
        int printed = 0;
        foreach (string e in elems)
        {
            if (e.Length == 0) continue;
            if (printed++ > 0) sb.Append(':');
            if (e.Contains(':')) sb.Append('{').Append(e).Append('}');
            else sb.Append(e);
        }
        string result = sb.ToString();
        if (result.Contains('=')) return "{" + result + "}";
        if (printed > 1 && result[0] == '{') return "{" + result + "}";
        return result;
    }

    private static string Struct(Writer w) => "{" + w + "}";

    public static byte[] Write(CompactionServiceInput i)
    {
        var w = new Writer()
            .Str("cf_name", i.CfName)
            .Raw("snapshots", Vector(ToStrings(i.Snapshots)))
            .Raw("input_files", Vector(HexStrings(i.InputFiles)))
            .Raw("input_blob_files", Vector(HexStrings(i.InputBlobFiles)))
            .Bool("has_blob_gc_cutoff_file_number", i.HasBlobGcCutoffFileNumber)
            .U64("blob_gc_cutoff_file_number", i.BlobGcCutoffFileNumber)
            .U64("blob_file_number_start", i.BlobFileNumberStart)
            .U64("blob_file_number_limit", i.BlobFileNumberLimit)
            .I32("output_level", i.OutputLevel)
            .Str("db_id", i.DbId)
            .Bool("has_begin", i.HasBegin)
            .Bytes("begin", i.Begin)
            .Bool("has_end", i.HasEnd)
            .Bytes("end", i.End)
            .U64("options_file_number", i.OptionsFileNumber)
            .U64("min_manifest_file_number", i.MinManifestFileNumber)
            .U64("min_manifest_file_size", i.MinManifestFileSize)
            .U64("preserve_internal_time_seconds", i.PreserveInternalTimeSeconds)
            .U64("preclude_last_level_data_seconds", i.PrecludeLastLevelDataSeconds)
            .U64("preserve_time_min_seqno_floor", i.PreserveTimeMinSeqnoFloor)
            .I32("proximal_level", i.ProximalLevel)
            .I32("proximal_output_range_type", i.ProximalOutputRangeType)
            .U64("keep_in_last_level_through_seqno", i.KeepInLastLevelThroughSeqno);
        return WithVersion(w.ToString());
    }

    public static byte[] Write(CompactionServiceResult r)
    {
        var status = new Writer().I32("code", (int)r.StatusCode).I32("subcode", (int)r.StatusSubCode).I32("severity", r.StatusSeverity).Str("message", r.StatusMessage);
        var files = new List<string>(r.OutputFiles.Count);
        foreach (CompactionServiceOutputFile f in r.OutputFiles) files.Add(Struct(OutputFile(f)));
        var additions = new List<string>();
        foreach (byte[] a in r.BlobFileAdditions) additions.Add(Hex(a));
        var garbages = new List<string>();
        foreach (byte[] g in r.BlobFileGarbages) garbages.Add(Hex(g));
        var internalStats = new Writer()
            .Raw("output_level_stats", Struct(LevelStats(r.OutputLevelStats)))
            .Bool("has_proximal_level_output", r.HasProximalLevelOutput)
            .Raw("proximal_level_stats", Struct(LevelStats(r.ProximalLevelStats)));
        var w = new Writer()
            .Raw("status", Struct(status))
            .Raw("output_files", Vector(files))
            .Raw("blob_file_additions", Vector(additions))
            .Raw("blob_file_garbages", Vector(garbages))
            .U64("blob_compaction_version", r.BlobCompactionVersion)
            .U64("blob_gc_cutoff_version", r.BlobGcCutoffVersion)
            .I32("output_level", r.OutputLevel)
            .Str("output_path", r.OutputPath)
            .U64("bytes_read", r.BytesRead)
            .U64("bytes_written", r.BytesWritten)
            .Raw("stats", Struct(JobStats(r.Stats)))
            .Raw("internal_stats", Struct(internalStats));
        return WithVersion(w.ToString());
    }

    private static Writer OutputFile(CompactionServiceOutputFile f) => new Writer()
        .Str("file_name", f.FileName)
        .U64("file_size", f.FileSize)
        .U64("smallest_seqno", f.SmallestSeqno)
        .U64("largest_seqno", f.LargestSeqno)
        .Bytes("smallest_internal_key", f.SmallestInternalKey)
        .Bytes("largest_internal_key", f.LargestInternalKey)
        .U64("oldest_blob_file_number", f.OldestBlobFileNumber)
        // Misspelled key kept for compatibility with other versions.
        .U64("oldest_ancester_time", f.OldestAncestorTime)
        .U64("file_creation_time", f.FileCreationTime)
        .U64("epoch_number", f.EpochNumber)
        .Bytes("file_checksum", f.FileChecksum)
        .Str("file_checksum_func_name", f.FileChecksumFuncName)
        .U64("paranoid_hash", f.ParanoidHash)
        .Bool("marked_for_compaction", f.MarkedForCompaction)
        .Raw("unique_id", Vector([f.UniqueId.Lo.ToString(CultureInfo.InvariantCulture), f.UniqueId.Hi.ToString(CultureInfo.InvariantCulture)]))
        .Raw("table_properties", Struct(TablePropertiesFields(f.TableProperties)))
        .Bool("is_proximal_level_output", f.IsProximalLevelOutput)
        .Raw("file_temperature", OptionEnumMaps.Temperature.ToName(f.FileTemperature));

    private static Writer TablePropertiesFields(TableProperties p)
    {
        var w = new Writer()
            .U64("orig_file_number", p.OriginalFileNumber).U64("data_size", p.DataSize).U64("uncompressed_data_size", p.UncompressedDataSize)
            .U64("index_size", p.IndexSize).U64("index_partitions", p.IndexPartitions).U64("top_level_index_size", p.TopLevelIndexSize)
            .U64("index_key_is_user_key", p.IndexKeyIsUserKey).U64("index_value_is_delta_encoded", p.IndexValueIsDeltaEncoded)
            .U64("udi_is_primary_index", p.UdiIsPrimaryIndex).U64("standard_index_is_stub", p.StandardIndexIsStub)
            .U64("filter_size", p.FilterSize).U64("raw_key_size", p.RawKeySize).U64("raw_value_size", p.RawValueSize)
            .U64("num_data_blocks", p.NumDataBlocks).U64("num_data_blocks_compression_rejected", p.NumDataBlocksCompressionRejected)
            .U64("num_data_blocks_compression_bypassed", p.NumDataBlocksCompressionBypassed).U64("num_uniform_blocks", p.NumUniformBlocks)
            .U64("num_entries", p.NumEntries).U64("num_filter_entries", p.NumFilterEntries).U64("num_deletions", p.NumDeletions)
            .U64("num_merge_operands", p.NumMergeOperands).U64("num_range_deletions", p.NumRangeDeletions).U64("format_version", p.FormatVersion)
            .U64("fixed_key_len", p.FixedKeyLen).U64("column_family_id", p.ColumnFamilyId)
            // Key retains the old field name for serialization compatibility (oldest_ancestor_time).
            .U64("creation_time", p.CreationTime).U64("oldest_key_time", p.OldestKeyTime).U64("newest_key_time", p.NewestKeyTime)
            .U64("file_creation_time", p.FileCreationTime).U64("slow_compression_estimated_data_size", p.SlowCompressionEstimatedDataSize)
            .U64("fast_compression_estimated_data_size", p.FastCompressionEstimatedDataSize).U64("tail_start_offset", p.TailStartOffset)
            .U64("user_defined_timestamps_persisted", p.UserDefinedTimestampsPersisted).U64("key_largest_seqno", p.KeyLargestSeqno)
            .U64("key_smallest_seqno", p.KeySmallestSeqno).U64("data_block_restart_interval", p.DataBlockRestartInterval)
            .U64("index_block_restart_interval", p.IndexBlockRestartInterval).U64("separate_key_value_in_data_block", p.SeparateKeyValueInDataBlock)
            .U64("lsm_info_at_creation", p.LsmInfoAtCreation)
            .Str("db_id", p.DbId).Str("db_session_id", p.DbSessionId).Str("db_host_id", p.DbHostId).Str("column_family_name", p.ColumnFamilyName)
            .Str("filter_policy_name", p.FilterPolicyName).Str("comparator_name", p.ComparatorName).Str("merge_operator_name", p.MergeOperatorName)
            .Str("prefix_extractor_name", p.PrefixExtractorName).Str("property_collectors_names", p.PropertyCollectorsNames)
            .Str("compression_name", p.CompressionName).Str("compression_options", p.CompressionOptions)
            .Bytes("seqno_to_time_mapping", p.SequenceNumberTimeMapping);
        // OptionTypeInfo::StringMap: "{hexkey=hexvalue;...}" (keys sorted: std::map).
        var keys = new List<string>(p.UserCollectedProperties.Keys);
        keys.Sort(StringComparer.Ordinal);
        var map = new StringBuilder("{");
        foreach (string k in keys) map.Append(Hex(Encoding.UTF8.GetBytes(k))).Append('=').Append(Hex(p.UserCollectedProperties[k])).Append(';');
        map.Append('}');
        w.Raw("user_collected_properties", map.ToString());
        w.Raw("readable_properties", "{}");
        return w;
    }

    private static Writer JobStats(RemoteCompactionJobStats s) => new Writer()
        .U64("elapsed_micros", s.ElapsedMicros).U64("cpu_micros", s.CpuMicros).Bool("has_accurate_num_input_records", s.HasAccurateNumInputRecords)
        .U64("num_input_records", s.NumInputRecords).U64("num_blobs_read", s.NumBlobsRead).U64("num_input_files", s.NumInputFiles)
        .U64("num_input_files_at_output_level", s.NumInputFilesAtOutputLevel).U64("num_output_records", s.NumOutputRecords)
        .U64("num_output_files", s.NumOutputFiles).U64("num_output_files_blob", s.NumOutputFilesBlob).Bool("is_full_compaction", s.IsFullCompaction)
        .Bool("is_manual_compaction", s.IsManualCompaction).Bool("is_remote_compaction", s.IsRemoteCompaction)
        .U64("total_input_bytes", s.TotalInputBytes).U64("total_blob_bytes_read", s.TotalBlobBytesRead).U64("total_output_bytes", s.TotalOutputBytes)
        .U64("total_output_bytes_blob", s.TotalOutputBytesBlob).U64("num_records_replaced", s.NumRecordsReplaced)
        .U64("total_input_raw_key_bytes", s.TotalInputRawKeyBytes).U64("total_input_raw_value_bytes", s.TotalInputRawValueBytes)
        .U64("num_input_deletion_records", s.NumInputDeletionRecords).U64("num_expired_deletion_records", s.NumExpiredDeletionRecords)
        .U64("num_corrupt_keys", s.NumCorruptKeys).U64("file_write_nanos", s.FileWriteNanos).U64("file_range_sync_nanos", s.FileRangeSyncNanos)
        .U64("file_fsync_nanos", s.FileFsyncNanos).U64("file_prepare_write_nanos", s.FilePrepareWriteNanos)
        .Bytes("smallest_output_key_prefix", s.SmallestOutputKeyPrefix).Bytes("largest_output_key_prefix", s.LargestOutputKeyPrefix)
        .U64("num_single_del_fallthru", s.NumSingleDelFallthru).U64("num_single_del_mismatch", s.NumSingleDelMismatch);

    private static Writer LevelStats(RemoteCompactionLevelStats s)
    {
        var counts = new string[s.Counts.Length];
        for (int i = 0; i < counts.Length; i++) counts[i] = s.Counts[i].ToString(CultureInfo.InvariantCulture);
        return new Writer()
            .U64("micros", s.Micros).U64("cpu_micros", s.CpuMicros).U64("bytes_read_non_output_levels", s.BytesReadNonOutputLevels)
            .U64("bytes_read_output_level", s.BytesReadOutputLevel).U64("bytes_skipped_non_output_levels", s.BytesSkippedNonOutputLevels)
            .U64("bytes_skipped_output_level", s.BytesSkippedOutputLevel).U64("bytes_read_blob", s.BytesReadBlob).U64("bytes_written", s.BytesWritten)
            .U64("bytes_written_blob", s.BytesWrittenBlob).U64("bytes_moved", s.BytesMoved)
            .I32("num_input_files_in_non_output_levels", s.NumInputFilesInNonOutputLevels).I32("num_input_files_in_output_level", s.NumInputFilesInOutputLevel)
            .I32("num_filtered_input_files_in_non_output_levels", s.NumFilteredInputFilesInNonOutputLevels)
            .I32("num_filtered_input_files_in_output_level", s.NumFilteredInputFilesInOutputLevel)
            .I32("num_output_files", s.NumOutputFiles).I32("num_output_files_blob", s.NumOutputFilesBlob)
            .U64("num_input_records", s.NumInputRecords).U64("num_dropped_records", s.NumDroppedRecords).U64("num_output_records", s.NumOutputRecords)
            .U64("count", s.Count).Raw("counts", Vector(counts));
    }

    private static List<string> ToStrings(List<ulong> v)
    {
        var r = new List<string>(v.Count);
        foreach (ulong x in v) r.Add(x.ToString(CultureInfo.InvariantCulture));
        return r;
    }

    private static List<string> HexStrings(List<string> v)
    {
        var r = new List<string>(v.Count);
        foreach (string x in v) r.Add(Hex(Encoding.UTF8.GetBytes(x)));
        return r;
    }

    private static byte[] WithVersion(string s)
    {
        int n = Encoding.UTF8.GetByteCount(s);
        byte[] b = new byte[4 + n];
        BinaryPrimitives.WriteUInt32LittleEndian(b, FormatVersion);
        Encoding.UTF8.GetBytes(s, b.AsSpan(4));
        return b;
    }

    // ---- reader --------------------------------------------------------------------------------------

    private static string Body(ReadOnlySpan<byte> data, string what)
    {
        if (data.Length <= 4) ThrowHelper.InvalidArgument("Invalid " + what + " string");
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(data);
        if (version != FormatVersion)
        {
            ThrowHelper.NotSupported("Compaction Service " + (what == "CompactionServiceInput" ? "Input" : "Result") + " data version not supported: "
                + version.ToString(CultureInfo.InvariantCulture));
        }
        return Encoding.UTF8.GetString(data[4..]);
    }

    /// <summary>CompactionServiceInput::Read (unknown fields ignored, absent ones default).</summary>
    public static CompactionServiceInput ReadInput(ReadOnlySpan<byte> data)
    {
        Dictionary<string, string> m = ToMap(Body(data, "CompactionServiceInput"));
        var i = new CompactionServiceInput
        {
            CfName = GetStr(m, "cf_name"),
            Snapshots = GetVector(m, "snapshots", static s => ParseU64("snapshots", s)),
            InputFiles = GetVector(m, "input_files", static s => Encoding.UTF8.GetString(DecodeHex("input_files", s))),
            InputBlobFiles = GetVector(m, "input_blob_files", static s => Encoding.UTF8.GetString(DecodeHex("input_blob_files", s))),
            HasBlobGcCutoffFileNumber = GetBool(m, "has_blob_gc_cutoff_file_number"),
            BlobGcCutoffFileNumber = GetU64(m, "blob_gc_cutoff_file_number"),
            BlobFileNumberStart = GetU64(m, "blob_file_number_start"),
            BlobFileNumberLimit = GetU64(m, "blob_file_number_limit"),
            OutputLevel = GetI32(m, "output_level"),
            DbId = GetStr(m, "db_id"),
            HasBegin = GetBool(m, "has_begin"),
            Begin = GetBytes(m, "begin"),
            HasEnd = GetBool(m, "has_end"),
            End = GetBytes(m, "end"),
            OptionsFileNumber = GetU64(m, "options_file_number"),
            MinManifestFileNumber = GetU64(m, "min_manifest_file_number"),
            MinManifestFileSize = GetU64(m, "min_manifest_file_size"),
            PreserveInternalTimeSeconds = GetU64(m, "preserve_internal_time_seconds"),
            PrecludeLastLevelDataSeconds = GetU64(m, "preclude_last_level_data_seconds"),
            PreserveTimeMinSeqnoFloor = GetU64(m, "preserve_time_min_seqno_floor"),
            ProximalLevel = GetI32(m, "proximal_level"),
            ProximalOutputRangeType = GetI32(m, "proximal_output_range_type"),
            KeepInLastLevelThroughSeqno = GetU64(m, "keep_in_last_level_through_seqno"),
        };
        if ((i.BlobFileNumberStart == 0) != (i.BlobFileNumberLimit == 0) || (i.BlobFileNumberStart != 0 && i.BlobFileNumberStart >= i.BlobFileNumberLimit))
        {
            ThrowHelper.InvalidArgument("CompactionServiceInput has an invalid blob file number range");
        }
        for (int k = 1; k < i.Snapshots.Count; k++)
        {
            if (i.Snapshots[k - 1] >= i.Snapshots[k]) ThrowHelper.InvalidArgument("CompactionServiceInput snapshots must be strictly increasing");
        }
        return i;
    }

    /// <summary>CompactionServiceResult::Read.</summary>
    public static CompactionServiceResult ReadResult(ReadOnlySpan<byte> data)
    {
        Dictionary<string, string> m = ToMap(Body(data, "CompactionServiceResult"));
        var r = new CompactionServiceResult();
        if (m.TryGetValue("status", out string? st))
        {
            Dictionary<string, string> sm = ToMap(st);
            r.StatusCode = (StatusCode)ParseU64("code", sm.GetValueOrDefault("code", "0"));
            r.StatusSubCode = (StatusSubCode)ParseU64("subcode", sm.GetValueOrDefault("subcode", "0"));
            r.StatusSeverity = (byte)ParseU64("severity", sm.GetValueOrDefault("severity", "0"));
            r.StatusMessage = GetStr(sm, "message");
        }
        r.OutputFiles = GetVector(m, "output_files", ParseOutputFile);
        r.BlobFileAdditions = GetVector(m, "blob_file_additions", static s => DecodeHex("blob_file_additions", s));
        r.BlobFileGarbages = GetVector(m, "blob_file_garbages", static s => DecodeHex("blob_file_garbages", s));
        r.BlobCompactionVersion = GetU64(m, "blob_compaction_version");
        r.BlobGcCutoffVersion = GetU64(m, "blob_gc_cutoff_version");
        r.OutputLevel = GetI32(m, "output_level");
        r.OutputPath = GetStr(m, "output_path");
        r.BytesRead = GetU64(m, "bytes_read");
        r.BytesWritten = GetU64(m, "bytes_written");
        if (m.TryGetValue("stats", out string? stats)) r.Stats = ParseJobStats(ToMap(stats));
        if (m.TryGetValue("internal_stats", out string? istats))
        {
            Dictionary<string, string> im = ToMap(istats);
            if (im.TryGetValue("output_level_stats", out string? o)) r.OutputLevelStats = ParseLevelStats(ToMap(o));
            r.HasProximalLevelOutput = GetBool(im, "has_proximal_level_output");
            if (im.TryGetValue("proximal_level_stats", out string? p)) r.ProximalLevelStats = ParseLevelStats(ToMap(p));
        }
        return r;
    }

    private static CompactionServiceOutputFile ParseOutputFile(string s)
    {
        Dictionary<string, string> m = ToMap(s);
        var f = new CompactionServiceOutputFile
        {
            FileName = GetStr(m, "file_name"),
            FileSize = GetU64(m, "file_size"),
            SmallestSeqno = GetU64(m, "smallest_seqno"),
            LargestSeqno = GetU64(m, "largest_seqno"),
            SmallestInternalKey = GetBytes(m, "smallest_internal_key"),
            LargestInternalKey = GetBytes(m, "largest_internal_key"),
            OldestBlobFileNumber = GetU64(m, "oldest_blob_file_number"),
            OldestAncestorTime = GetU64(m, "oldest_ancester_time"),
            FileCreationTime = GetU64(m, "file_creation_time"),
            EpochNumber = GetU64(m, "epoch_number"),
            FileChecksum = GetBytes(m, "file_checksum"),
            FileChecksumFuncName = m.ContainsKey("file_checksum_func_name") ? GetStr(m, "file_checksum_func_name") : FileMetaData.UnknownFileChecksumFuncName,
            ParanoidHash = GetU64(m, "paranoid_hash"),
            MarkedForCompaction = GetBool(m, "marked_for_compaction"),
            IsProximalLevelOutput = GetBool(m, "is_proximal_level_output"),
        };
        List<ulong> id = GetVector(m, "unique_id", static v => ParseU64("unique_id", v));
        if (id.Count == 2) f.UniqueId = new UniqueId64x2(id[0], id[1]);
        else if (id.Count != 0) ThrowHelper.InvalidArgument("Serialized value has a wrong number of elements for unique_id");
        if (m.TryGetValue("file_temperature", out string? t)) f.FileTemperature = OptionEnumMaps.Temperature.Parse("file_temperature", StripOuterBraces(t));
        if (m.TryGetValue("table_properties", out string? tp)) f.TableProperties = ParseTableProperties(ToMap(tp));
        return f;
    }

    private static TableProperties ParseTableProperties(Dictionary<string, string> m)
    {
        var p = new TableProperties
        {
            OriginalFileNumber = GetU64(m, "orig_file_number"), DataSize = GetU64(m, "data_size"), UncompressedDataSize = GetU64(m, "uncompressed_data_size"),
            IndexSize = GetU64(m, "index_size"), IndexPartitions = GetU64(m, "index_partitions"), TopLevelIndexSize = GetU64(m, "top_level_index_size"),
            IndexKeyIsUserKey = GetU64(m, "index_key_is_user_key"), IndexValueIsDeltaEncoded = GetU64(m, "index_value_is_delta_encoded"),
            UdiIsPrimaryIndex = GetU64(m, "udi_is_primary_index"), StandardIndexIsStub = GetU64(m, "standard_index_is_stub"),
            FilterSize = GetU64(m, "filter_size"), RawKeySize = GetU64(m, "raw_key_size"), RawValueSize = GetU64(m, "raw_value_size"),
            NumDataBlocks = GetU64(m, "num_data_blocks"), NumDataBlocksCompressionRejected = GetU64(m, "num_data_blocks_compression_rejected"),
            NumDataBlocksCompressionBypassed = GetU64(m, "num_data_blocks_compression_bypassed"), NumUniformBlocks = GetU64(m, "num_uniform_blocks"),
            NumEntries = GetU64(m, "num_entries"), NumFilterEntries = GetU64(m, "num_filter_entries"), NumDeletions = GetU64(m, "num_deletions"),
            NumMergeOperands = GetU64(m, "num_merge_operands"), NumRangeDeletions = GetU64(m, "num_range_deletions"),
            FormatVersion = GetU64(m, "format_version"), FixedKeyLen = GetU64(m, "fixed_key_len"),
            ColumnFamilyId = m.ContainsKey("column_family_id") ? GetU64(m, "column_family_id") : TableProperties.UnknownColumnFamily,
            CreationTime = GetU64(m, "creation_time"), OldestKeyTime = GetU64(m, "oldest_key_time"), NewestKeyTime = GetU64(m, "newest_key_time"),
            FileCreationTime = GetU64(m, "file_creation_time"), SlowCompressionEstimatedDataSize = GetU64(m, "slow_compression_estimated_data_size"),
            FastCompressionEstimatedDataSize = GetU64(m, "fast_compression_estimated_data_size"), TailStartOffset = GetU64(m, "tail_start_offset"),
            UserDefinedTimestampsPersisted = m.ContainsKey("user_defined_timestamps_persisted") ? GetU64(m, "user_defined_timestamps_persisted") : 1,
            KeyLargestSeqno = m.ContainsKey("key_largest_seqno") ? GetU64(m, "key_largest_seqno") : TableProperties.Unknown,
            KeySmallestSeqno = m.ContainsKey("key_smallest_seqno") ? GetU64(m, "key_smallest_seqno") : TableProperties.Unknown,
            DataBlockRestartInterval = GetU64(m, "data_block_restart_interval"), IndexBlockRestartInterval = GetU64(m, "index_block_restart_interval"),
            SeparateKeyValueInDataBlock = GetU64(m, "separate_key_value_in_data_block"), LsmInfoAtCreation = GetU64(m, "lsm_info_at_creation"),
            DbId = GetStr(m, "db_id"), DbSessionId = GetStr(m, "db_session_id"), DbHostId = GetStr(m, "db_host_id"),
            ColumnFamilyName = GetStr(m, "column_family_name"), FilterPolicyName = GetStr(m, "filter_policy_name"),
            ComparatorName = GetStr(m, "comparator_name"), MergeOperatorName = GetStr(m, "merge_operator_name"),
            PrefixExtractorName = GetStr(m, "prefix_extractor_name"), PropertyCollectorsNames = GetStr(m, "property_collectors_names"),
            CompressionName = GetStr(m, "compression_name"), CompressionOptions = GetStr(m, "compression_options"),
            SequenceNumberTimeMapping = GetBytes(m, "seqno_to_time_mapping"),
        };
        if (m.TryGetValue("user_collected_properties", out string? ucp))
        {
            foreach (string token in SplitTokens(StripOuterBraces(ucp), ';'))
            {
                if (token.Length == 0) continue;
                int eq = token.IndexOf('=');
                if (eq < 0) ThrowHelper.InvalidArgument("Invalid string map entry: " + token);
                p.UserCollectedProperties[Encoding.UTF8.GetString(DecodeHex("user_collected_properties", token[..eq]))] =
                    DecodeHex("user_collected_properties", token[(eq + 1)..]);
            }
        }
        return p;
    }

    private static RemoteCompactionJobStats ParseJobStats(Dictionary<string, string> m) => new()
    {
        ElapsedMicros = GetU64(m, "elapsed_micros"), CpuMicros = GetU64(m, "cpu_micros"), HasAccurateNumInputRecords = GetBool(m, "has_accurate_num_input_records"),
        NumInputRecords = GetU64(m, "num_input_records"), NumBlobsRead = GetU64(m, "num_blobs_read"), NumInputFiles = GetU64(m, "num_input_files"),
        NumInputFilesAtOutputLevel = GetU64(m, "num_input_files_at_output_level"), NumOutputRecords = GetU64(m, "num_output_records"),
        NumOutputFiles = GetU64(m, "num_output_files"), NumOutputFilesBlob = GetU64(m, "num_output_files_blob"), IsFullCompaction = GetBool(m, "is_full_compaction"),
        IsManualCompaction = GetBool(m, "is_manual_compaction"), IsRemoteCompaction = GetBool(m, "is_remote_compaction"),
        TotalInputBytes = GetU64(m, "total_input_bytes"), TotalBlobBytesRead = GetU64(m, "total_blob_bytes_read"), TotalOutputBytes = GetU64(m, "total_output_bytes"),
        TotalOutputBytesBlob = GetU64(m, "total_output_bytes_blob"), NumRecordsReplaced = GetU64(m, "num_records_replaced"),
        TotalInputRawKeyBytes = GetU64(m, "total_input_raw_key_bytes"), TotalInputRawValueBytes = GetU64(m, "total_input_raw_value_bytes"),
        NumInputDeletionRecords = GetU64(m, "num_input_deletion_records"), NumExpiredDeletionRecords = GetU64(m, "num_expired_deletion_records"),
        NumCorruptKeys = GetU64(m, "num_corrupt_keys"), FileWriteNanos = GetU64(m, "file_write_nanos"), FileRangeSyncNanos = GetU64(m, "file_range_sync_nanos"),
        FileFsyncNanos = GetU64(m, "file_fsync_nanos"), FilePrepareWriteNanos = GetU64(m, "file_prepare_write_nanos"),
        SmallestOutputKeyPrefix = GetBytes(m, "smallest_output_key_prefix"), LargestOutputKeyPrefix = GetBytes(m, "largest_output_key_prefix"),
        NumSingleDelFallthru = GetU64(m, "num_single_del_fallthru"), NumSingleDelMismatch = GetU64(m, "num_single_del_mismatch"),
    };

    private static RemoteCompactionLevelStats ParseLevelStats(Dictionary<string, string> m)
    {
        var s = new RemoteCompactionLevelStats
        {
            Micros = GetU64(m, "micros"), CpuMicros = GetU64(m, "cpu_micros"), BytesReadNonOutputLevels = GetU64(m, "bytes_read_non_output_levels"),
            BytesReadOutputLevel = GetU64(m, "bytes_read_output_level"), BytesSkippedNonOutputLevels = GetU64(m, "bytes_skipped_non_output_levels"),
            BytesSkippedOutputLevel = GetU64(m, "bytes_skipped_output_level"), BytesReadBlob = GetU64(m, "bytes_read_blob"), BytesWritten = GetU64(m, "bytes_written"),
            BytesWrittenBlob = GetU64(m, "bytes_written_blob"), BytesMoved = GetU64(m, "bytes_moved"),
            NumInputFilesInNonOutputLevels = GetI32(m, "num_input_files_in_non_output_levels"), NumInputFilesInOutputLevel = GetI32(m, "num_input_files_in_output_level"),
            NumFilteredInputFilesInNonOutputLevels = GetI32(m, "num_filtered_input_files_in_non_output_levels"),
            NumFilteredInputFilesInOutputLevel = GetI32(m, "num_filtered_input_files_in_output_level"),
            NumOutputFiles = GetI32(m, "num_output_files"), NumOutputFilesBlob = GetI32(m, "num_output_files_blob"),
            NumInputRecords = GetU64(m, "num_input_records"), NumDroppedRecords = GetU64(m, "num_dropped_records"), NumOutputRecords = GetU64(m, "num_output_records"),
            Count = GetU64(m, "count"),
        };
        // SizeTolerantDeserializeArray: extra elements are ignored, missing ones zero-filled.
        List<int> counts = GetVector(m, "counts", static v => checked((int)ParseI64("counts", v)));
        for (int i = 0; i < s.Counts.Length && i < counts.Count; i++) s.Counts[i] = counts[i];
        return s;
    }

    // ---- options-string primitives (StringToMap / NextToken / StripOuterBraces) ------------------------

    /// <summary>
    /// StringToMap (options/options_helper.cc): "k1=v1;k2={nested};..." with the outer braces of the whole string
    /// stripped; braced values keep their braces. Later duplicates win.
    /// </summary>
    internal static Dictionary<string, string> ToMap(string optsStr)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        string opts = optsStr.Trim();
        while (opts.Length > 2 && opts[0] == '{' && opts[^1] == '}') opts = opts[1..^1].Trim();
        int pos = 0;
        while (pos < opts.Length)
        {
            int eq = opts.IndexOfAny(['=', '{', '}', ';'], pos);
            if (eq < 0) ThrowHelper.InvalidArgument("Mismatched key value pair, '=' expected");
            if (opts[eq] != '=') ThrowHelper.InvalidArgument("Unexpected char in key");
            string key = opts[pos..eq].Trim();
            if (key.Length == 0) ThrowHelper.InvalidArgument("Empty key found");
            int valueStart = eq + 1;
            while (valueStart < opts.Length && char.IsWhiteSpace(opts[valueStart])) valueStart++;
            string value;
            if (valueStart < opts.Length && opts[valueStart] == '{')
            {
                int count = 1;
                int bracePos = valueStart + 1;
                while (bracePos < opts.Length && count > 0)
                {
                    if (opts[bracePos] == '{') count++;
                    else if (opts[bracePos] == '}') count--;
                    if (count > 0) bracePos++;
                }
                if (count != 0) ThrowHelper.InvalidArgument("Mismatched curly braces for nested options");
                value = opts[valueStart..(bracePos + 1)];
                pos = bracePos + 1;
                while (pos < opts.Length && char.IsWhiteSpace(opts[pos])) pos++;
                if (pos < opts.Length && opts[pos] != ';') ThrowHelper.InvalidArgument("Unexpected chars after nested options");
            }
            else
            {
                int end = opts.IndexOf(';', valueStart);
                if (end < 0)
                {
                    value = opts[valueStart..].Trim();
                    pos = opts.Length;
                }
                else
                {
                    value = opts[valueStart..end].Trim();
                    pos = end;
                }
            }
            map[key] = value;
            pos++;
        }
        return map;
    }

    /// <summary>OptionTypeInfo::StripOuterBraces: removes one pair of outer braces if the leading one closes at the end.</summary>
    internal static string StripOuterBraces(string value)
    {
        if (value.Length < 2 || value[0] != '{' || value[^1] != '}') return value;
        int depth = 0;
        for (int i = 0; i + 1 < value.Length; ++i)
        {
            if (value[i] == '{') ++depth;
            else if (value[i] == '}' && --depth == 0) return value; // outer braces are independent
        }
        return value[1..^1];
    }

    /// <summary>ParseVector's tokenization (OptionTypeInfo::NextToken): braced tokens lose their braces.</summary>
    private static List<string> SplitTokens(string s, char separator)
    {
        var tokens = new List<string>();
        int pos = 0;
        while (pos < s.Length)
        {
            while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
            if (pos >= s.Length) break;
            if (s[pos] == '{')
            {
                int count = 1;
                int bracePos = pos + 1;
                while (bracePos < s.Length)
                {
                    if (s[bracePos] == '{') count++;
                    else if (s[bracePos] == '}' && --count == 0) break;
                    bracePos++;
                }
                if (count != 0) ThrowHelper.InvalidArgument("Mismatched curly braces for nested options");
                tokens.Add(s[(pos + 1)..bracePos].Trim());
                pos = bracePos + 1;
                while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
                if (pos < s.Length && s[pos] != separator) ThrowHelper.InvalidArgument("Unexpected chars after nested options");
                pos++;
            }
            else
            {
                int end = s.IndexOf(separator, pos);
                if (end < 0)
                {
                    tokens.Add(s[pos..].Trim());
                    break;
                }
                tokens.Add(s[pos..end].Trim());
                pos = end + 1;
            }
        }
        return tokens;
    }

    private static List<T> GetVector<T>(Dictionary<string, string> m, string name, Func<string, T> parse)
    {
        var r = new List<T>();
        if (!m.TryGetValue(name, out string? v)) return r;
        foreach (string token in SplitTokens(StripOuterBraces(v), ':')) r.Add(parse(token));
        return r;
    }

    private static string GetStr(Dictionary<string, string> m, string name) =>
        m.TryGetValue(name, out string? v) ? Encoding.UTF8.GetString(DecodeHex(name, StripOuterBraces(v))) : "";

    private static byte[] GetBytes(Dictionary<string, string> m, string name) =>
        m.TryGetValue(name, out string? v) ? DecodeHex(name, StripOuterBraces(v)) : [];

    private static ulong GetU64(Dictionary<string, string> m, string name) => m.TryGetValue(name, out string? v) ? ParseU64(name, StripOuterBraces(v)) : 0;

    private static int GetI32(Dictionary<string, string> m, string name) =>
        m.TryGetValue(name, out string? v) ? checked((int)ParseI64(name, StripOuterBraces(v))) : 0;

    private static bool GetBool(Dictionary<string, string> m, string name) =>
        m.TryGetValue(name, out string? v) && OptionStrings.ParseBool(name, StripOuterBraces(v));

    private static ulong ParseU64(string name, string v) => OptionStrings.ParseUInt64(name, v);

    private static long ParseI64(string name, string v) => OptionStrings.ParseInt64(name, v);

    /// <summary>Slice::DecodeHex (upper or lower case).</summary>
    private static byte[] DecodeHex(string name, string v)
    {
        if (v.Length == 0) return [];
        if ((v.Length & 1) != 0) ThrowHelper.InvalidArgument("Invalid hex encoding for " + name);
        try
        {
            return Convert.FromHexString(v);
        }
        catch (FormatException)
        {
            ThrowHelper.InvalidArgument("Invalid hex encoding for " + name);
            return [];
        }
    }
}
