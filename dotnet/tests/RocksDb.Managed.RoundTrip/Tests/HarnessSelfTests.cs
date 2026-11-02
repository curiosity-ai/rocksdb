using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>The child-process harness itself: crashes, hangs and verification failures are captured and decoded.</summary>
[TestClass]
public sealed class HarnessSelfTests : RoundTripTestBase
{
    private static DbSpec SimpleSpec()
    {
        var spec = new DbSpec();
        spec.ColumnFamilies.Add(new CfSpec { Name = "default" });
        return spec;
    }

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task CrashInChild_IsReportedWithSignal(EngineKind engine)
    {
        RoundTripHarness h = CreateHarness(SimpleSpec(), engine.ToString());
        ScriptBuilder b = h.NewScript("crash").Add(Op.Put("default", "a"u8.ToArray(), "1"u8.ToArray())).Add(Op.Crash());
        ChildResult r = await h.RunChildAsync(engine, b, TimeSpan.FromSeconds(60), null);
        TestContext.WriteLine(r.Describe());
        Assert.IsFalse(r.Succeeded);
        Assert.IsFalse(r.TimedOut);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            Assert.AreEqual(128 + 6, r.ExitCode, r.Describe());
            StringAssert.Contains(r.ExitDescription, "SIGABRT");
        }
        StringAssert.Contains(string.Join("\n", r.Stderr), "crash op");
    }

    [TestMethod]
    public void DebugAssertInProcess_BecomesTestFailureNotFailFast()
    {
        ThrowingTraceListener.Install();
        DebugAssertException ex = Assert.ThrowsExactly<DebugAssertException>(static () => System.Diagnostics.Debug.Fail("self-test"));
        StringAssert.Contains(ex.Message, "self-test");
    }

    [TestMethod]
    public async Task HangInChild_IsKilledAtTimeout()
    {
        RoundTripHarness h = CreateHarness(SimpleSpec(), "");
        ScriptBuilder b = h.NewScript("hang").Add(Op.Hang());
        ChildResult r = await h.RunChildAsync(EngineKind.Native, b, TimeSpan.FromSeconds(5), null);
        TestContext.WriteLine(r.Describe());
        Assert.IsTrue(r.TimedOut);
        Assert.IsFalse(r.Succeeded);
        CollectionAssert.Contains(r.Stdout.ToList(), "HANGING");
    }

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task VerificationFailureInChild_ReportsDifferences(EngineKind engine)
    {
        RoundTripHarness h = CreateHarness(SimpleSpec(), engine.ToString());
        ScriptBuilder b = h.NewScript("verify").Add(Op.Put("default", "present"u8.ToArray(), "1"u8.ToArray()));
        var wrong = new Contents();
        wrong.ColumnFamilies.Add("default", new CfContents());
        wrong["default"].Live.Add("missing"u8.ToArray(), "x"u8.ToArray());
        string path = b.NewFile("wrong.txt");
        wrong.Write(path);
        b.Add(Op.Verify(path));
        ChildResult r = await h.RunChildAsync(engine, b, TimeSpan.FromSeconds(60), null);
        TestContext.WriteLine(r.Describe());
        Assert.AreEqual(ChildHost.ExitVerificationFailed, r.ExitCode, r.Describe());
        string err = string.Join("\n", r.Stderr);
        StringAssert.Contains(err, "missing key \"missing\"");
        StringAssert.Contains(err, "unexpected key \"present\"");
    }

    [TestMethod]
    public async Task KillAfterAck_StopsChildMidStream()
    {
        RoundTripHarness h = CreateHarness(CrashWorkload.Spec(1 << 20), "");
        ScriptBuilder b = h.NewScript("writer").Add(Op.CrashWriter(1_000_000, 10));
        long acked = 0;
        ChildResult r = await h.RunChildAsync(EngineKind.Native, b, TimeSpan.FromSeconds(60), line =>
        {
            if (!line.StartsWith("ACK ", StringComparison.Ordinal)) return false;
            acked = long.Parse(line.AsSpan(4), System.Globalization.CultureInfo.InvariantCulture);
            return acked >= 50;
        });
        TestContext.WriteLine(r.Describe());
        Assert.IsTrue(r.KilledByParent, r.Describe());
        Assert.IsFalse(r.TimedOut);
        Assert.IsTrue(acked >= 50);
        if (OperatingSystem.IsLinux()) StringAssert.Contains(r.ExitDescription, "SIGKILL");
        Assert.IsFalse(r.Stdout.Contains("DONE"), "the writer should have been killed mid-stream");
    }
}
