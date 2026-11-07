using System.Globalization;
using System.Text;

namespace RocksDb.Managed.ClusterSoak;

/// <summary>Jepsen op types: :invoke, then :ok, :fail (definitely not applied) or :info (indeterminate).</summary>
internal enum OpType
{
    Invoke,
    Ok,
    Fail,
    Info,
}

/// <summary>Operation functions.</summary>
internal enum OpF
{
    /// <summary>Register write: Keys[0] = register index, Vals[0] = version.</summary>
    Write,
    /// <summary>Register read: Keys = register indexes, Vals = versions read (-1 absent); Note = "leader"/"follower".</summary>
    Read,
    /// <summary>Bank transfer (one batch of two UInt64Add merges): Keys = [from, to], Vals = [amount].</summary>
    Transfer,
    /// <summary>Bank read under one snapshot: Vals = balances (signed); Note = "leader"/"follower".</summary>
    BankRead,
    /// <summary>Counter add (UInt64Add merge): Keys[0] = counter, Vals[0] = delta.</summary>
    Add,
    /// <summary>Nemesis action (process -1): Note = description.</summary>
    Nemesis,
}

/// <summary>One history event.</summary>
internal sealed class Op
{
    public long Index;
    public long TimeUs;
    public OpType Type;
    public int Process;
    public OpF F;
    public string Node = "-";
    public int[] Keys = [];
    public long[] Vals = [];
    public string Note = "";

    public string ToLine()
    {
        var sb = new StringBuilder(64);
        sb.Append(Index.ToString(CultureInfo.InvariantCulture)).Append('\t')
          .Append(TimeUs.ToString(CultureInfo.InvariantCulture)).Append('\t')
          .Append(Type.ToString().ToLowerInvariant()).Append('\t')
          .Append(Process.ToString(CultureInfo.InvariantCulture)).Append('\t')
          .Append(F.ToString().ToLowerInvariant()).Append('\t')
          .Append(Node).Append('\t');
        for (int i = 0; i < Keys.Length; i++) sb.Append(i == 0 ? "" : ",").Append(Keys[i].ToString(CultureInfo.InvariantCulture));
        sb.Append('\t');
        for (int i = 0; i < Vals.Length; i++) sb.Append(i == 0 ? "" : ",").Append(Vals[i].ToString(CultureInfo.InvariantCulture));
        sb.Append('\t').Append(Note.Replace('\t', ' ').Replace('\n', ' '));
        return sb.ToString();
    }

    public static Op Parse(string line)
    {
        string[] p = line.Split('\t');
        var op = new Op
        {
            Index = long.Parse(p[0], CultureInfo.InvariantCulture),
            TimeUs = long.Parse(p[1], CultureInfo.InvariantCulture),
            Type = Enum.Parse<OpType>(p[2], ignoreCase: true),
            Process = int.Parse(p[3], CultureInfo.InvariantCulture),
            F = Enum.Parse<OpF>(p[4], ignoreCase: true),
            Node = p[5],
            Note = p.Length > 8 ? p[8] : "",
        };
        op.Keys = p[6].Length == 0 ? [] : Array.ConvertAll(p[6].Split(','), static s => int.Parse(s, CultureInfo.InvariantCulture));
        op.Vals = p[7].Length == 0 ? [] : Array.ConvertAll(p[7].Split(','), static s => long.Parse(s, CultureInfo.InvariantCulture));
        return op;
    }
}

/// <summary>
/// The history of one epoch (RUN phase up to its VERIFY): kept in memory for the checker and appended to
/// history/epoch-N.tsv (one op per line, in a total order; the index is the order).
/// </summary>
internal sealed class History : IDisposable
{
    private readonly Lock _gate = new();
    private readonly StreamWriter _file;
    private readonly long _t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
    private long _next;

    public History(string path)
    {
        Path = path;
        _file = new StreamWriter(new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false), 1 << 16);
    }

    public string Path { get; }
    public List<Op> Ops { get; } = [];
    public long NowUs => System.Diagnostics.Stopwatch.GetTimestamp() / (System.Diagnostics.Stopwatch.Frequency / 1_000_000);

    public Op Add(Op op)
    {
        lock (_gate)
        {
            op.Index = _next++;
            op.TimeUs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000 - _t0;
            Ops.Add(op);
            _file.WriteLine(op.ToLine());
        }
        return op;
    }

    public void Flush()
    {
        lock (_gate) _file.Flush();
    }

    public void Dispose()
    {
        lock (_gate) _file.Dispose();
    }

    public static List<Op> Read(string path)
    {
        var list = new List<Op>();
        foreach (string line in File.ReadLines(path))
            if (line.Length > 0) list.Add(Op.Parse(line));
        return list;
    }
}
