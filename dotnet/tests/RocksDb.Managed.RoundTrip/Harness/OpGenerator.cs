using System.Text;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>Knobs for <see cref="OpGenerator"/>.</summary>
public sealed class GeneratorOptions
{
    public int Ops { get; set; } = 1500;
    public int KeySpace { get; set; } = 3000;
    public bool AllowFlush { get; set; } = true;
    public bool AllowCompact { get; set; }
    public bool AllowSnapshots { get; set; } = true;
    public bool AllowColumnFamilyChanges { get; set; } = true;
    public bool AllowSingleDelete { get; set; } = true;
    public bool AllowRangeDelete { get; set; } = true;
    public bool AllowMerge { get; set; } = true;
    /// <summary>Probability of a large value (&gt; 64 KiB) per put.</summary>
    public double LargeValueRate { get; set; } = 0.002;
}

/// <summary>
/// Seeded random op streams that respect the RocksDB API contracts, so the model's expected contents are well defined:
/// <list type="bullet">
/// <item>SingleDelete only on "sd/" keys, which only ever see Put (when absent) and SingleDelete (when present);
/// range deletes, deletes and merges never touch them.</item>
/// <item>Merges only on column families with a merge operator; uint64add CFs only get 8-byte values.</item>
/// </list>
/// Keys include ASCII "k" keys, binary keys with 0x00 / 0xff bytes and the empty key; values include empty and
/// large (&gt; block size, &gt; 64 KiB) values.
/// </summary>
public static class OpGenerator
{

    public static void Generate(ScriptBuilder b, GeneratorOptions o)
    {
        Random r = b.Random;
        ReferenceModel m = b.Model;
        long nextSnapshot = 1000 + r.Next(1000) * 1000;
        long openSnapshot = -1;
        int opsSinceSnapshot = 0;
        for (int i = 0; i < o.Ops; i++)
        {
            string cf = PickCf(r, m);
            int roll = r.Next(1000);
            if (roll < 420)
            {
                b.Add(Op.Put(cf, MainKey(r, o), Value(r, m.MergeOf(cf), o)));
            }
            else if (roll < 560)
            {
                if (o.AllowMerge && m.MergeOf(cf) != MergeKind.None) b.Add(Op.Merge(cf, MainKey(r, o), Operand(r, m.MergeOf(cf))));
                else b.Add(Op.Put(cf, MainKey(r, o), Value(r, m.MergeOf(cf), o)));
            }
            else if (roll < 660)
            {
                b.Add(Op.Delete(cf, ExistingOrRandomKey(r, m, cf, o)));
            }
            else if (roll < 740)
            {
                if (o.AllowSingleDelete) SingleDeleteStep(b, r, cf);
            }
            else if (roll < 760)
            {
                if (o.AllowRangeDelete) b.Add(RangeDelete(r, cf, o));
            }
            else if (roll < 900)
            {
                var ops = new List<Op>();
                int n = 1 + r.Next(20);
                for (int j = 0; j < n; j++)
                {
                    string c = PickCf(r, m);
                    int kind = r.Next(10);
                    if (kind < 5) ops.Add(Op.Put(c, MainKey(r, o), Value(r, m.MergeOf(c), o)));
                    else if (kind < 7 && o.AllowMerge && m.MergeOf(c) != MergeKind.None) ops.Add(Op.Merge(c, MainKey(r, o), Operand(r, m.MergeOf(c))));
                    else if (kind < 9) ops.Add(Op.Delete(c, ExistingOrRandomKey(r, m, c, o)));
                    else if (o.AllowRangeDelete) ops.Add(RangeDelete(r, c, o));
                }
                b.Batch(r.Next(4) == 0, ops);
            }
            else if (roll < 915)
            {
                b.Add(Op.Sync(r.Next(3) == 0));
            }
            else if (roll < 930)
            {
                if (o.AllowFlush) b.Add(Op.Flush(cf));
            }
            else if (roll < 945)
            {
                if (o.AllowSnapshots)
                {
                    if (openSnapshot < 0)
                    {
                        openSnapshot = nextSnapshot++;
                        opsSinceSnapshot = 0;
                        b.Add(Op.Snap(openSnapshot));
                    }
                    else if (opsSinceSnapshot > 30)
                    {
                        b.SnapVerify(openSnapshot);
                        b.Add(Op.SnapRelease(openSnapshot));
                        openSnapshot = -1;
                    }
                }
            }
            else if (roll < 950)
            {
                if (o.AllowCompact) b.Add(Op.Compact(cf));
            }
            else if (roll < 953)
            {
                if (o.AllowColumnFamilyChanges && b.Spec.ColumnFamilies.Exists(c => c.Name == "late"))
                {
                    if (m.HasCf("late")) b.Add(Op.DropCf("late"));
                    else b.Add(Op.CreateCf("late"));
                }
            }
            else
            {
                // Reads/writes of edge-case keys: empty key, keys made of 0x00 / 0xff bytes.
                byte[] k = EdgeKey(r);
                if (r.Next(3) == 0) b.Add(Op.Delete(cf, k));
                else b.Add(Op.Put(cf, k, Value(r, m.MergeOf(cf), o)));
            }
            opsSinceSnapshot++;
        }
        if (openSnapshot >= 0)
        {
            b.SnapVerify(openSnapshot);
            b.Add(Op.SnapRelease(openSnapshot));
        }
    }

    private static string PickCf(Random r, ReferenceModel m)
    {
        var names = new List<string>(m.ColumnFamilyNames);
        // default gets half of the traffic.
        return r.Next(2) == 0 ? "default" : names[r.Next(names.Count)];
    }

    public static byte[] MainKey(Random r, GeneratorOptions o) =>
        Encoding.ASCII.GetBytes("k" + r.Next(o.KeySpace).ToString("D6", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] ExistingOrRandomKey(Random r, ReferenceModel m, string cf, GeneratorOptions o)
    {
        CfContents c = m.Current[cf];
        if (c.Live.Count > 0 && r.Next(2) == 0)
        {
            // Pick a near-random existing non-"sd/" key by seeking from a random main key.
            byte[] probe = MainKey(r, o);
            foreach (byte[] k in c.Live.Keys)
            {
                if (ByteComparer.Instance.Compare(k, probe) >= 0 && !IsSdKey(k)) return k;
            }
        }
        return MainKey(r, o);
    }

    private static bool IsSdKey(byte[] k) => k.Length >= 3 && k[0] == (byte)'s' && k[1] == (byte)'d' && k[2] == (byte)'/';

    private static void SingleDeleteStep(ScriptBuilder b, Random r, string cf)
    {
        byte[] k = Encoding.ASCII.GetBytes("sd/" + r.Next(200).ToString("D4", System.Globalization.CultureInfo.InvariantCulture));
        if (b.Model.Current[cf].Live.ContainsKey(k)) b.Add(Op.SingleDelete(cf, k));
        else b.Add(Op.Put(cf, k, b.Model.MergeOf(cf) == MergeKind.UInt64Add ? ReferenceModel.U64((ulong)r.NextInt64()) : RandomBytes(r, r.Next(1, 40))));
    }

    private static Op RangeDelete(Random r, string cf, GeneratorOptions o)
    {
        if (r.Next(5) == 0)
        {
            // Binary key space below "k": [0x00.., 0x00 0x80).
            return Op.DeleteRange(cf, [0x00], [0x00, 0x80]);
        }
        int a = r.Next(o.KeySpace);
        int width = 1 + r.Next(40);
        return Op.DeleteRange(cf,
            Encoding.ASCII.GetBytes("k" + a.ToString("D6", System.Globalization.CultureInfo.InvariantCulture)),
            Encoding.ASCII.GetBytes("k" + (a + width).ToString("D6", System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static byte[] EdgeKey(Random r) => r.Next(6) switch
    {
        0 => [],
        1 => [0x00],
        2 => [0x00, 0x00, (byte)r.Next(256)],
        3 => [0xff],
        4 => [0xff, 0xff, (byte)r.Next(256), 0x00],
        _ => [0x00, (byte)r.Next(0x80), 0xff],
    };

    public static byte[] RandomBytes(Random r, int n)
    {
        byte[] b = new byte[n];
        r.NextBytes(b);
        return b;
    }

    private static byte[] Value(Random r, MergeKind merge, GeneratorOptions o)
    {
        if (merge == MergeKind.UInt64Add) return ReferenceModel.U64((ulong)r.Next(1_000_000));
        double p = r.NextDouble();
        if (p < o.LargeValueRate) return RandomBytes(r, 65 * 1024 + r.Next(150 * 1024));
        if (p < 0.05) return [];
        if (p < 0.08) return RandomBytes(r, 4096 + r.Next(12 * 1024)); // larger than the block size
        int n = r.Next(1, 100);
        byte[] v = new byte[n];
        for (int i = 0; i < n; i++) v[i] = (byte)('a' + r.Next(26));
        return v;
    }

    private static byte[] Operand(Random r, MergeKind merge) => merge == MergeKind.UInt64Add
        ? ReferenceModel.U64((ulong)r.Next(1000))
        : Encoding.ASCII.GetBytes("m" + r.Next(1000).ToString(System.Globalization.CultureInfo.InvariantCulture));
}
