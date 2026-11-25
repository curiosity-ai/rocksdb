using System.Text.RegularExpressions;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

/// <summary>
/// <see cref="AtomicBits"/>, the compare-exchange loops that replace <c>Interlocked.Or</c> / <c>And</c> whose previous
/// value is used: .NET 11.0.0's Tier0 expansion of those can give the target address and the new value one register,
/// so a Release build failed every first insert into a HyperClock cache with a NullReferenceException (audit #15).
/// The test suites run Debug IL, which does not hit it, so a source scan keeps the pattern out.
/// </summary>
[TestClass]
public sealed class AtomicBitsTests
{
    [TestMethod]
    public void OrAndAndReturnThePreviousValue()
    {
        long l = 0b1010;
        Assert.AreEqual(0b1010L, AtomicBits.Or(ref l, 0b0101));
        Assert.AreEqual(0b1111L, l);
        Assert.AreEqual(0b1111L, AtomicBits.And(ref l, 0b0110));
        Assert.AreEqual(0b0110L, l);
        Assert.AreEqual(0b0110L, AtomicBits.Or(ref l, long.MinValue));
        Assert.AreEqual(long.MinValue | 0b0110, l);

        int i = 1;
        Assert.AreEqual(1, AtomicBits.Or(ref i, 1 << 31));
        Assert.AreEqual(1 | (1 << 31), i);
    }

    [TestMethod]
    public void ConcurrentOrsSetEveryBitOnceEach()
    {
        long word = 0;
        int firstSetters = 0;
        Parallel.For(0, 64 * 50, n =>
        {
            long bit = 1L << (n % 64);
            if ((AtomicBits.Or(ref word, bit) & bit) == 0) Interlocked.Increment(ref firstSetters);
        });
        Assert.AreEqual(-1L, word);
        Assert.AreEqual(64, firstSetters);
    }

    [TestMethod]
    public void SourcesDoNotUseTheResultOfInterlockedOrOrAnd()
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "RocksDb.Managed.slnx"))) dir = Path.GetDirectoryName(dir);
        if (dir is null)
        {
            Assert.Inconclusive("source tree not found next to the test binaries");
            return;
        }
        // A call whose result is discarded is a statement: nothing but an if / while condition (ending in ')') or a block
        // boundary may come before it on its line, and its closing parenthesis is followed by ';'.
        var call = new Regex(@"Interlocked\s*\.\s*(Or|And)\s*\(", RegexOptions.CultureInvariant);
        static bool EndsStatement(string code, int openParen)
        {
            int depth = 0;
            for (int j = openParen; j < code.Length; j++)
            {
                if (code[j] == '(') depth++;
                else if (code[j] == ')' && --depth == 0) return code[(j + 1)..].TrimStart().StartsWith(';');
            }
            return false; // the call continues on the next line: treated as used
        }
        var offenders = new List<string>();
        foreach (string project in new[] { "src", "tools", "bench" })
        {
            foreach (string file in Directory.EnumerateFiles(Path.Combine(dir, project), "*.cs", SearchOption.AllDirectories))
            {
                if (file.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
                    file.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                    continue;
                string[] lines = File.ReadAllLines(file);
                for (int i = 0; i < lines.Length; i++)
                {
                    string code = lines[i];
                    int comment = code.IndexOf("//", StringComparison.Ordinal);
                    if (comment >= 0) code = code[..comment];
                    foreach (Match m in call.Matches(code))
                    {
                        string before = code[..m.Index].TrimEnd();
                        bool atStatementStart = before.Length == 0 || before.EndsWith(')') || before.EndsWith('{') || before.EndsWith('}') ||
                            before.EndsWith(';') || before.EndsWith("else", StringComparison.Ordinal);
                        bool statement = atStatementStart && EndsStatement(code, m.Index + m.Length - 1);
                        if (!statement) offenders.Add($"{Path.GetRelativePath(dir, file)}:{i + 1}: {lines[i].Trim()}");
                    }
                }
            }
        }
        Assert.AreEqual(0, offenders.Count, "use Util.AtomicBits for an Interlocked.Or / And whose result is used:\n" + string.Join("\n", offenders));
    }
}
