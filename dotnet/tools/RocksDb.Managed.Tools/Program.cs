using RocksDb.Managed.Env;
using RocksDb.Managed.Tools;

// rocksdb-tools: "ldb ..." or "sst_dump ..." (the first argument selects the tool).
using var stdout = new StreamWriter(Console.OpenStandardOutput(), ToolText.Bytes) { NewLine = "\n", AutoFlush = false };
using var stderr = new StreamWriter(Console.OpenStandardError(), ToolText.Bytes) { NewLine = "\n", AutoFlush = true };
return ToolMain.Run(args, LocalFileSystem.Instance, stdout, stderr);

namespace RocksDb.Managed.Tools
{
    /// <summary>Entry point shared by the console program and the tests.</summary>
    public static class ToolMain
    {
        public static int Run(IReadOnlyList<string> args, IFileSystem fileSystem, TextWriter stdout, TextWriter stderr)
        {
            ArgumentNullException.ThrowIfNull(args);
            if (args.Count == 0 || args[0] is not ("ldb" or "sst_dump"))
            {
                stderr.Write("usage: RocksDb.Managed.Tools ldb --db=<path> <command> [args] | sst_dump --file=<path> [--command=...]\n");
                return 1;
            }
            var rest = new List<string>(args.Count - 1);
            for (int i = 1; i < args.Count; i++) rest.Add(args[i]);
            int rc = args[0] == "ldb" ? LdbTool.Run(rest, fileSystem, stdout, stderr) : SstDumpTool.Run(rest, fileSystem, stdout, stderr);
            stdout.Flush();
            return rc;
        }
    }
}
