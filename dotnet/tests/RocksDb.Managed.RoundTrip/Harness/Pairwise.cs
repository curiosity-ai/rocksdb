namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>
/// Strength-2 covering arrays over mixed-level factors (AETG-style greedy, as in the unit suite's WAL matrix): each new
/// row is the best of 300 seeded random candidates, the first of which is seeded with an uncovered pair, so the array
/// is deterministic for a seed. Every pair of levels of every two factors that <paramref name="excluded"/> allows occurs
/// in some row, and every row satisfies <paramref name="valid"/> (rows are repaired with <paramref name="repair"/>).
/// </summary>
public static class Pairwise
{
    public static List<int[]> Generate(int[] levels, int seed, Func<int, int, int, int, bool> excluded, Action<int[]> repair)
    {
        var rnd = new Random(seed);
        var uncovered = new HashSet<(int, int, int, int)>();
        for (int a = 0; a < levels.Length; a++)
        {
            for (int b = a + 1; b < levels.Length; b++)
            {
                for (int x = 0; x < levels[a]; x++)
                {
                    for (int y = 0; y < levels[b]; y++)
                    {
                        if (!excluded(a, x, b, y)) uncovered.Add((a, b, x, y));
                    }
                }
            }
        }
        var rows = new List<int[]>();
        int stuck = 0;
        while (uncovered.Count > 0)
        {
            int[]? best = null;
            int bestCount = -1;
            for (int c = 0; c < 300; c++)
            {
                var row = new int[levels.Length];
                for (int i = 0; i < levels.Length; i++) row[i] = rnd.Next(levels[i]);
                if (c == 0)
                {
                    foreach ((int a, int b, int x, int y) in uncovered)
                    {
                        row[a] = x;
                        row[b] = y;
                        break;
                    }
                }
                repair(row);
                int count = 0;
                foreach ((int a, int b, int x, int y) in uncovered)
                {
                    if (row[a] == x && row[b] == y) count++;
                }
                if (count > bestCount)
                {
                    best = row;
                    bestCount = count;
                }
            }
            if (bestCount == 0)
            {
                // A pair the repair function always breaks: it cannot be covered; drop it.
                if (++stuck > 3)
                {
                    foreach ((int a, int b, int x, int y) p in uncovered)
                    {
                        uncovered.Remove(p);
                        break;
                    }
                    stuck = 0;
                }
                continue;
            }
            stuck = 0;
            rows.Add(best!);
            uncovered.RemoveWhere(p => best![p.Item1] == p.Item3 && best![p.Item2] == p.Item4);
        }
        return rows;
    }

    /// <summary>The allowed pairs no row of <paramref name="rows"/> covers (for the coverage self-test).</summary>
    public static List<string> Uncovered(int[] levels, List<int[]> rows, Func<int, int, int, int, bool> excluded)
    {
        var missing = new List<string>();
        for (int a = 0; a < levels.Length; a++)
        {
            for (int b = a + 1; b < levels.Length; b++)
            {
                for (int x = 0; x < levels[a]; x++)
                {
                    for (int y = 0; y < levels[b]; y++)
                    {
                        if (excluded(a, x, b, y)) continue;
                        if (!rows.Exists(r => r[a] == x && r[b] == y)) missing.Add($"factor {a}={x} x factor {b}={y}");
                    }
                }
            }
        }
        return missing;
    }
}
