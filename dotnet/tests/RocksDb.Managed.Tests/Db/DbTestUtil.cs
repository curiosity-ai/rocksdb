using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Shared helpers for DB-level tests.</summary>
internal static class DbTestUtil
{
    public const string DbPath = "/db";

    public static MemoryFileSystem NewFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(DbPath);
        fs.SyncDirectory("/");
        return fs;
    }

    public static DbOptions NewOptions(IFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs };

    public static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    public static string? S(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    public static string? GetString(RocksDatabase db, string key) => S(db.Get(B(key)));

    public static string? GetString(ColumnFamily cf, string key) => S(cf.Get(B(key)));

    public static string? GetString(RocksDatabase db, string key, Snapshot snapshot) => S(db.Get(new ReadOptions { Snapshot = snapshot }, B(key)));

    public static byte[] Key(int i) => B("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    public static byte[] Value(int i, int round) => B("value-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" + round.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', i % 50));
}

/// <summary>Joins operands with ',' (like the C++ StringAppendOperator with ',' delimiter).</summary>
public sealed class StringAppendMergeOperator : MergeOperator
{
    public override string Name => "StringAppendOperator";

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands, IBufferWriter<byte> result)
    {
        bool first = true;
        if (hasExistingValue)
        {
            result.Write(existingValue);
            first = false;
        }
        for (int i = 0; i < operands.Count; i++)
        {
            if (!first) result.Write(","u8);
            result.Write(operands[i]);
            first = false;
        }
        return true;
    }
}

/// <summary>Little-endian uint64 addition (like the C++ UInt64AddOperator).</summary>
public sealed class UInt64AddMergeOperator : MergeOperator
{
    public override string Name => "UInt64AddOperator";

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands, IBufferWriter<byte> result)
    {
        ulong sum = hasExistingValue && existingValue.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(existingValue) : 0;
        for (int i = 0; i < operands.Count; i++)
        {
            if (operands[i].Length == 8) sum += BinaryPrimitives.ReadUInt64LittleEndian(operands[i]);
        }
        BinaryPrimitives.WriteUInt64LittleEndian(result.GetSpan(8), sum);
        result.Advance(8);
        return true;
    }
}
