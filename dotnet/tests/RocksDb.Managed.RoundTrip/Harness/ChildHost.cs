using System.Globalization;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>
/// Child-process mode of the test executable:
/// <c>roundtrip-child --engine native|managed --db DIR --spec SPEC.json --script SCRIPT.txt</c>.
/// Opens the DB with the engine, runs the script and closes the DB. Exit codes: 0 success,
/// <see cref="ExitVerificationFailed"/> when a verify command found different contents,
/// <see cref="ExitError"/> on any other exception, <see cref="ExitUsage"/> on bad arguments. A crash or kill shows up
/// as a signal / NTSTATUS exit code instead (decoded by <see cref="ChildResult"/>).
/// </summary>
internal static class ChildHost
{
    public const string ChildArgument = "roundtrip-child";
    public const int ExitUsage = 2;
    public const int ExitVerificationFailed = 3;
    public const int ExitError = 4;

    /// <summary>The --engine spelling of an engine (the child protocol's CLI boundary).</summary>
    public static string EngineArgument(EngineKind engine) => engine == EngineKind.Native ? "native" : "managed";

    private static EngineKind ParseEngine(string name) => name switch
    {
        "native" => EngineKind.Native,
        "managed" => EngineKind.Managed,
        _ => throw new ArgumentException("unknown engine " + name),
    };

    public static int Run(string[] args)
    {
        string? engineName = null, db = null, specPath = null, scriptPath = null;
        for (int i = 1; i < args.Length - 1; i += 2)
        {
            switch (args[i])
            {
                case "--engine": engineName = args[i + 1]; break;
                case "--db": db = args[i + 1]; break;
                case "--spec": specPath = args[i + 1]; break;
                case "--script": scriptPath = args[i + 1]; break;
                default:
                    Console.Error.WriteLine("unknown argument " + args[i]);
                    return ExitUsage;
            }
        }
        if (engineName is null || db is null || specPath is null || scriptPath is null)
        {
            Console.Error.WriteLine("usage: roundtrip-child --engine native|managed --db DIR --spec SPEC.json --script SCRIPT.txt");
            return ExitUsage;
        }

        TextWriter stdout = Console.Out;
        stdout.WriteLine("CHILD pid=" + Environment.ProcessId.ToString(CultureInfo.InvariantCulture) + " engine=" + engineName);
        stdout.Flush();
        IEngine engine = ParseEngine(engineName) switch
        {
            EngineKind.Native => new NativeEngine(),
            _ => new ManagedEngine(),
        };
        try
        {
            DbSpec spec = DbSpec.FromJson(File.ReadAllText(specPath));
            engine.Open(db, spec);
            var runner = new ScriptRunner(engine, db, spec, stdout);
            runner.Run(Script.Read(scriptPath));
            engine.Close();
            stdout.WriteLine("OK");
            stdout.Flush();
            return 0;
        }
        catch (VerificationException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return ExitVerificationFailed;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return ExitError;
        }
    }
}
