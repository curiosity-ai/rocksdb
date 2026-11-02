using BenchmarkDotNet.Running;
using RocksDb.Managed.Benchmarks.DbBench;

// Usage (see bench/README.md and bench/run-all.sh):
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*'
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench --engine rocksdb-sharp \
//       --benchmarks fillseq,fillrandom,readrandom --num 1000000 --value_size 100
//   ... -- compare --engines c-native,rocksdb-sharp,managed,managed-async --csv out.csv -- --num 100000 --threads 4
//   ... -- report --csv out.csv --out out.md
if (args.Length > 0)
{
    try
    {
        switch (args[0])
        {
            case "dbbench": return DbBenchRunner.Run(args);
            case "compare": return CompareRunner.Compare(args);
            case "report": return CompareRunner.Report(args);
            case "machine-info":
                foreach (string line in CompareRunner.MachineInfo()) Console.WriteLine(line);
                return 0;
        }
    }
    catch (ArgumentException e)
    {
        Console.Error.WriteLine(e.Message);
        return 1;
    }
}
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;
