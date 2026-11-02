using BenchmarkDotNet.Running;

// Usage: dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*'
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
