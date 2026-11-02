using BenchmarkDotNet.Running;
using RocksDb.Managed.Benchmarks.DbBench;

// Usage:
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*'
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench --engine native \
//       --benchmarks fillseq,fillrandom,readrandom --num 1000000 --value_size 100
if (args.Length > 0 && args[0] == "dbbench")
{
    return DbBenchRunner.Run(args);
}
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;
