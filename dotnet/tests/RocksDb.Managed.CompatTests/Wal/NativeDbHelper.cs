using System.Text;
using RocksDb.Managed.Db;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>Helpers to open native (rocksdb-sharp) databases for the WAL / WriteBatch compat tests.</summary>
internal static class NativeDbHelper
{
    public static readonly string[] ExtraColumnFamilies = ["one", "two"];

    /// <summary>Merge operator that joins operands with ',' (like the C++ StringAppendOperator).</summary>
    public static RocksDbSharp.MergeOperator CreateConcatMergeOperator() => RocksDbSharp.MergeOperators.Create(
        "StringAppendOperator",
        static (ReadOnlySpan<byte> key, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(default, false, operands);
        },
        static (ReadOnlySpan<byte> key, bool hasExisting, ReadOnlySpan<byte> existing, RocksDbSharp.MergeOperators.OperandsEnumerator operands, out bool success) =>
        {
            success = true;
            return Join(existing, hasExisting, operands);
        });

    private static byte[] Join(ReadOnlySpan<byte> existing, bool hasExisting, RocksDbSharp.MergeOperators.OperandsEnumerator operands)
    {
        var result = new List<byte>();
        if (hasExisting) result.AddRange(existing.ToArray());
        for (int i = 0; i < operands.Count; i++)
        {
            if (result.Count > 0) result.Add((byte)',');
            result.AddRange(operands.Get(i).ToArray());
        }
        return result.ToArray();
    }

    // rocksdb-sharp resolves merge callbacks through the managed options objects, which must therefore outlive every
    // native DB that uses them (including background flushes). Test-only: keep them alive for the process lifetime.
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    public static RocksDbSharp.ColumnFamilyOptions CfOptions(RocksDbSharp.MergeOperator mergeOperator)
    {
        var o = new RocksDbSharp.ColumnFamilyOptions().SetMergeOperator(mergeOperator);
        s_keepAlive.Add(o);
        s_keepAlive.Add(mergeOperator);
        return o;
    }

    /// <summary>Opens (creating if needed) a native DB with the default CF plus <see cref="ExtraColumnFamilies"/>.</summary>
    public static RocksDbSharp.RocksDb Open(string path, RocksDbSharp.DbOptions options, RocksDbSharp.MergeOperator mergeOperator)
    {
        options.SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        s_keepAlive.Add(options);
        var cfs = new RocksDbSharp.ColumnFamilies(CfOptions(mergeOperator));
        foreach (string name in ExtraColumnFamilies) cfs.Add(name, CfOptions(mergeOperator));
        return RocksDbSharp.RocksDb.Open(options, path, cfs);
    }

    public static uint CfId(RocksDbSharp.ColumnFamilyHandle cf) => RocksDbSharp.Native.Instance.rocksdb_column_family_handle_get_id(cf.Handle);

    /// <summary>Formats a batch rep as "Put(cf,key,value)@seq ..." using the managed reader.</summary>
    public static string Describe(ReadOnlySpan<byte> rep)
    {
        var sb = new StringBuilder();
        var reader = new WriteBatchReader(rep);
        ulong seq = reader.Sequence;
        foreach (WriteBatchEntry e in reader)
        {
            string k = Encoding.UTF8.GetString(e.Key), v = Encoding.UTF8.GetString(e.Value);
            switch (e.Type)
            {
                case EntryType.Value: sb.Append($"Put({e.ColumnFamilyId},{k},{v})@{seq++} "); break;
                case EntryType.Deletion: sb.Append($"Delete({e.ColumnFamilyId},{k})@{seq++} "); break;
                case EntryType.SingleDeletion: sb.Append($"SingleDelete({e.ColumnFamilyId},{k})@{seq++} "); break;
                case EntryType.RangeDeletion: sb.Append($"DeleteRange({e.ColumnFamilyId},{k},{v})@{seq++} "); break;
                case EntryType.Merge: sb.Append($"Merge({e.ColumnFamilyId},{k},{v})@{seq++} "); break;
                case EntryType.LogData: sb.Append($"LogData({v}) "); break;
                default: sb.Append($"{e.Type} "); break;
            }
        }
        return sb.ToString().TrimEnd();
    }

    public static string? FindWal(string dir)
    {
        string[] logs = Directory.GetFiles(dir, "*.log");
        Array.Sort(logs, StringComparer.Ordinal);
        return logs.Length == 0 ? null : logs[^1];
    }

    public static ulong LogNumberOf(string walPath) =>
        ulong.Parse(Path.GetFileNameWithoutExtension(walPath), System.Globalization.CultureInfo.InvariantCulture);
}
