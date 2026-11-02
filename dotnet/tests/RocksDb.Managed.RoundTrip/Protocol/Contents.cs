using System.Text;

namespace RocksDb.Managed.RoundTrip.Protocol;

/// <summary>Unsigned lexicographic byte-string order (the bytewise comparator).</summary>
public sealed class ByteComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
{
    public static readonly ByteComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);

    public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

    public int GetHashCode(byte[] obj)
    {
        var h = new HashCode();
        h.AddBytes(obj);
        return h.ToHashCode();
    }
}

/// <summary>Contents of one column family: live key/values plus keys that must be absent.</summary>
public sealed class CfContents
{
    public SortedDictionary<byte[], byte[]> Live { get; } = new(ByteComparer.Instance);
    public HashSet<byte[]> Absent { get; } = new(ByteComparer.Instance);

    public CfContents Clone()
    {
        var c = new CfContents();
        foreach (KeyValuePair<byte[], byte[]> kv in Live) c.Live.Add(kv.Key, kv.Value);
        foreach (byte[] k in Absent) c.Absent.Add(k);
        return c;
    }
}

/// <summary>
/// Canonical DB contents: per column family, the live entries in key order and (for expected files) keys that must
/// not exist. File format, one item per line: <c>cf\tNAME</c>, <c>+\tHEXKEY\tHEXVALUE</c>, <c>-\tHEXKEY</c>.
/// Dumps written by the child use the same format without "-" lines, so they can be diffed directly.
/// </summary>
public sealed class Contents
{
    public SortedDictionary<string, CfContents> ColumnFamilies { get; } = new(StringComparer.Ordinal);

    public CfContents this[string cf] => ColumnFamilies[cf];

    public Contents Clone()
    {
        var c = new Contents();
        foreach (KeyValuePair<string, CfContents> kv in ColumnFamilies) c.ColumnFamilies.Add(kv.Key, kv.Value.Clone());
        return c;
    }

    public long LiveCount
    {
        get
        {
            long n = 0;
            foreach (CfContents cf in ColumnFamilies.Values) n += cf.Live.Count;
            return n;
        }
    }

    public void Write(string path)
    {
        using var w = new StreamWriter(path, false, new UTF8Encoding(false), 1 << 16);
        foreach (KeyValuePair<string, CfContents> cf in ColumnFamilies)
        {
            w.Write("cf\t"); w.WriteLine(cf.Key);
            foreach (KeyValuePair<byte[], byte[]> kv in cf.Value.Live)
            {
                w.Write("+\t"); w.Write(Hex.Encode(kv.Key)); w.Write('\t'); w.WriteLine(Hex.Encode(kv.Value));
            }
            foreach (byte[] k in cf.Value.Absent)
            {
                w.Write("-\t"); w.WriteLine(Hex.Encode(k));
            }
        }
    }

    public static Contents Read(string path)
    {
        var c = new Contents();
        CfContents? cur = null;
        foreach (string line in File.ReadLines(path))
        {
            if (line.Length == 0) continue;
            string[] f = line.Split('\t');
            switch (f[0])
            {
                case "cf":
                    cur = new CfContents();
                    c.ColumnFamilies.Add(f[1], cur);
                    break;
                case "+":
                    (cur ?? throw new InvalidDataException("entry before cf line")).Live.Add(Hex.Decode(f[1]), Hex.Decode(f[2]));
                    break;
                case "-":
                    (cur ?? throw new InvalidDataException("entry before cf line")).Absent.Add(Hex.Decode(f[1]));
                    break;
                default:
                    throw new InvalidDataException("bad contents line: " + line);
            }
        }
        return c;
    }

    /// <summary>
    /// Differences between <paramref name="expected"/> and <paramref name="actual"/> live data (and expected-absent
    /// keys present in actual), at most <paramref name="max"/> lines. Empty = equal.
    /// </summary>
    public static List<string> Diff(Contents expected, Contents actual, int max)
    {
        var diffs = new List<string>();
        var cfNames = new SortedSet<string>(expected.ColumnFamilies.Keys, StringComparer.Ordinal);
        cfNames.UnionWith(actual.ColumnFamilies.Keys);
        foreach (string cf in cfNames)
        {
            if (!expected.ColumnFamilies.TryGetValue(cf, out CfContents? e)) { diffs.Add($"unexpected column family '{cf}'"); continue; }
            if (!actual.ColumnFamilies.TryGetValue(cf, out CfContents? a)) { diffs.Add($"missing column family '{cf}'"); continue; }
            using SortedDictionary<byte[], byte[]>.Enumerator ei = e.Live.GetEnumerator();
            using SortedDictionary<byte[], byte[]>.Enumerator ai = a.Live.GetEnumerator();
            bool eh = ei.MoveNext(), ah = ai.MoveNext();
            while ((eh || ah) && diffs.Count < max)
            {
                int c = !eh ? 1 : !ah ? -1 : ByteComparer.Instance.Compare(ei.Current.Key, ai.Current.Key);
                if (c < 0)
                {
                    diffs.Add($"[{cf}] missing key {Hex.Show(ei.Current.Key)} (expected value {Hex.Show(ei.Current.Value)})");
                    eh = ei.MoveNext();
                }
                else if (c > 0)
                {
                    diffs.Add($"[{cf}] unexpected key {Hex.Show(ai.Current.Key)} = {Hex.Show(ai.Current.Value)}");
                    ah = ai.MoveNext();
                }
                else
                {
                    if (!ei.Current.Value.AsSpan().SequenceEqual(ai.Current.Value))
                        diffs.Add($"[{cf}] key {Hex.Show(ei.Current.Key)}: expected {Hex.Show(ei.Current.Value)}, actual {Hex.Show(ai.Current.Value)}");
                    eh = ei.MoveNext();
                    ah = ai.MoveNext();
                }
            }
            foreach (byte[] k in e.Absent)
            {
                if (diffs.Count >= max) break;
                if (a.Live.ContainsKey(k)) diffs.Add($"[{cf}] deleted key {Hex.Show(k)} is present");
            }
        }
        if (diffs.Count >= max) diffs.Add("... (more differences omitted)");
        return diffs;
    }
}
