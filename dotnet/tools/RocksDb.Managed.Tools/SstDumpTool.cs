using System.Globalization;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Tools;

/// <summary>
/// Port of SSTDumpTool::Run (tools/sst_dump_tool.cc): <c>--file=&lt;file or dir&gt; [--command=scan|check|raw|verify|recompress|identify]
/// [--output_hex] [--input_key_hex] [--from=&lt;key&gt;] [--to=&lt;key&gt;] [--prefix=&lt;key&gt;] [--read_num=N] [--verify_checksum]
/// [--show_properties] [--show_summary] [--list_meta_blocks]</c>. Block-based tables only.
/// </summary>
internal static class SstDumpTool
{
    private enum PathKind { Unknown, File, Dir }

    public static int Run(IReadOnlyList<string> args, IFileSystem fs, TextWriter stdout, TextWriter stderr)
    {
        var paths = new List<(string Path, PathKind Kind)>();
        string command = "";
        bool outputHex = false, inputKeyHex = false, verifyChecksum = false, showProperties = false, showSummary = false, listMetaBlocks = false;
        bool hasFrom = false, hasTo = false, useFromAsPrefix = false;
        string fromKey = "", toKey = "";
        ulong readNum = ulong.MaxValue;
        for (int i = 0; i < args.Count; i++)
        {
            string a = args[i];
            if (a.StartsWith("--file=", StringComparison.Ordinal)) paths.Add((a[7..], PathKind.Unknown));
            else if (a == "--output_hex") outputHex = true;
            else if (a == "--input_key_hex") inputKeyHex = true;
            else if (a == "--verify_checksum") verifyChecksum = true;
            else if (a.StartsWith("--command=", StringComparison.Ordinal)) command = a[10..];
            else if (a.StartsWith("--from=", StringComparison.Ordinal)) { fromKey = a[7..]; hasFrom = true; }
            else if (a.StartsWith("--to=", StringComparison.Ordinal)) { toKey = a[5..]; hasTo = true; }
            else if (a.StartsWith("--prefix=", StringComparison.Ordinal)) { fromKey = a[9..]; useFromAsPrefix = true; }
            else if (a == "--show_properties") showProperties = true;
            else if (a == "--show_summary") showSummary = true;
            else if (a == "--list_meta_blocks") listMetaBlocks = true;
            else if (a.StartsWith("--read_num=", StringComparison.Ordinal) && ulong.TryParse(a.AsSpan(11), NumberStyles.None, CultureInfo.InvariantCulture, out ulong n)) readNum = n;
            else if (a is "--decode_blob_index" or "--show_sequence_number_type" or "--json") { /* accepted, no effect */ }
            else if (a.StartsWith("--compression_types=", StringComparison.Ordinal) || a.StartsWith("--block_size=", StringComparison.Ordinal)
                || a.StartsWith("--compression_level", StringComparison.Ordinal) || a.StartsWith("--readahead_size=", StringComparison.Ordinal))
            {
                /* recompress / readahead tuning: accepted */
            }
            else if (a == "--help")
            {
                PrintHelp(stdout);
                return 0;
            }
            else if (a == "--")
            {
                for (++i; i < args.Count; i++) paths.Add((args[i], PathKind.Unknown));
            }
            else if (a.StartsWith('-'))
            {
                stderr.Write("Unrecognized argument '" + a + "'\n\n");
                PrintHelp(stderr);
                return 1;
            }
            else
            {
                paths.Add((a, PathKind.Unknown));
            }
        }
        if (useFromAsPrefix && hasFrom)
        {
            stderr.Write("Cannot specify --prefix and --from\n\n");
            return 1;
        }
        byte[] from, to;
        try
        {
            from = inputKeyHex && (hasFrom || useFromAsPrefix) ? ToolText.HexToBytes(fromKey) : ToolText.ArgBytes(fromKey);
            to = inputKeyHex && hasTo ? ToolText.HexToBytes(toKey) : ToolText.ArgBytes(toKey);
        }
        catch (ToolException e)
        {
            stderr.Write(e.Message + "\n");
            return 1;
        }
        if (paths.Count == 0)
        {
            stderr.Write("file or directory must be specified.\n\n");
            PrintHelp(stderr);
            return 1;
        }
        stdout.Write("options.env is 0x0\n");

        var filenames = new List<string>();
        for (int i = 0; i < paths.Count; i++)
        {
            string p = paths[i].Path;
            List<string> children = [];
            bool isDir = SafeDirectoryExists(fs, p);
            if (isDir)
            {
                try { children = [.. fs.GetChildren(p)]; }
                catch (Exception e) when (e is IOException or RocksDbException) { }
            }
            if (!isDir || children.Count == 0)
            {
                if (!fs.FileExists(p) && !isDir)
                {
                    stderr.Write("NotFound: " + p + ": No such file or directory\n");
                    return 1;
                }
                filenames.Add(p);
                paths[i] = (p, PathKind.File);
            }
            else
            {
                children.Sort(StringComparer.Ordinal);
                foreach (string c in children) filenames.Add(p + "/" + c);
                paths[i] = (p, PathKind.Dir);
            }
        }

        ulong totalRead = 0, totalFiles = 0, totalDataBlocks = 0, totalDataSize = 0, totalIndexSize = 0, totalFilterSize = 0;
        var valid = new List<string>();
        foreach (string filename in filenames)
        {
            if (filename.Length <= 4 || !filename.EndsWith(".sst", StringComparison.Ordinal)) continue;
            if (command == "verify") verifyChecksum = true;
            using var dumper = new SstFileDumper(fs, filename, verifyChecksum, outputHex, silent: false, stdout);
            if (dumper.InitError is { } initError)
            {
                stderr.Write(filename + ": " + initError.Message + "\n");
                continue;
            }
            valid.Add(filename);
            if (valid.Count == 1 && command is "check" or "scan" or "")
            {
                stdout.Write("from [" + Convert.ToHexString(from) + "] to [" + Convert.ToHexString(to) + "]\n");
            }
            if (command == "recompress")
            {
                // Deviation: the compression benchmark of recompress is not ported (it measures native codecs);
                // the input file's properties are printed when requested.
                stdout.Write("Input file " + filename + "   Compression: " + dumper.Properties!.CompressionName + "\n");
                if (showProperties) PrintProperties(stdout, "Input file properties:", dumper.Properties!);
                continue;
            }
            if (command == "raw")
            {
                string outName = filename[..^4] + "_dump.txt";
                var sw = new StringWriter { NewLine = "\n" };
                dumper.DumpTable(sw);
                using (IWritableFile w = fs.CreateWritable(outName))
                {
                    w.Append(ToolText.Bytes.GetBytes(sw.ToString()));
                    w.Sync();
                }
                stdout.Write("raw dump written to file " + outName + "\n");
                continue;
            }
            if (command is "" or "scan" or "check")
            {
                string? err = dumper.ReadSequential(command == "scan", readNum > 0 ? readNum - totalRead : readNum, hasFrom || useFromAsPrefix, from, hasTo, to,
                    useFromAsPrefix);
                if (err is not null) stderr.Write(filename + ": " + err + "\n");
                totalRead += dumper.ReadNumber;
                if (readNum > 0 && totalRead > readNum) break;
            }
            if (command == "verify")
            {
                string? err = dumper.VerifyChecksum(fs, filename);
                if (err is not null) stderr.Write(filename + " is corrupted: " + err + "\n");
                else stdout.Write("The file is ok\n");
                continue;
            }
            TableProperties props = dumper.Properties!;
            if (showProperties || showSummary)
            {
                if (showProperties)
                {
                    stdout.Write("Table Properties:\n------------------------------\n  " + ToolText.TablePropertiesToString(props, "\n  ", ": "));
                }
                totalFiles++;
                totalDataBlocks += props.NumDataBlocks;
                totalDataSize += props.DataSize;
                totalIndexSize += props.IndexSize;
                totalFilterSize += props.FilterSize;
                if (showProperties)
                {
                    stdout.Write("Raw user collected properties\n------------------------------\n");
                    var names = new List<string>(props.UserCollectedProperties.Keys);
                    names.Sort(StringComparer.Ordinal);
                    foreach (string name in names) stdout.Write("  # " + name + ": 0x" + Convert.ToHexString(props.UserCollectedProperties[name]) + "\n");
                }
            }
            if (listMetaBlocks)
            {
                stdout.Write("Meta Blocks:\n------------------------------\n");
                using IRandomAccessFile file = fs.OpenRandomAccess(filename);
                var names = new List<string>(MetaBlockNames(dumper));
                names.Sort(StringComparer.Ordinal);
                foreach (string name in names)
                {
                    BlockHandle h = MetaHandle(dumper, name);
                    stdout.Write("  " + name + ": " + h.Offset.ToString(CultureInfo.InvariantCulture) + " " + h.Size.ToString(CultureInfo.InvariantCulture) + "\n");
                }
            }
        }

        if (showSummary)
        {
            stdout.Write("total number of files: " + totalFiles.ToString(CultureInfo.InvariantCulture) + "\n");
            stdout.Write("total number of data blocks: " + totalDataBlocks.ToString(CultureInfo.InvariantCulture) + "\n");
            stdout.Write("total data block size: " + totalDataSize.ToString(CultureInfo.InvariantCulture) + "\n");
            stdout.Write("total index block size: " + totalIndexSize.ToString(CultureInfo.InvariantCulture) + "\n");
            stdout.Write("total filter block size: " + totalFilterSize.ToString(CultureInfo.InvariantCulture) + "\n");
        }
        if (valid.Count == 0)
        {
            foreach ((string p, PathKind kind) in paths)
            {
                if (kind == PathKind.Dir)
                {
                    stdout.Write("------------------------------\n");
                    stderr.Write("No valid SST files found in " + p + "\n");
                }
                else
                {
                    stderr.Write(p + " is not a valid SST file\n");
                }
            }
            return 1;
        }
        if (command == "identify")
        {
            if (paths.Count > 1 || paths[0].Kind == PathKind.Dir)
            {
                stdout.Write("------------------------------\n");
                string msg = paths.Count == 1 ? " found in " + paths[0].Path : "";
                stdout.Write("List of valid SST files" + msg + ":\n");
                foreach (string f in valid) stdout.Write(f + "\n");
                stdout.Write("Number of valid SST files: " + valid.Count.ToString(CultureInfo.InvariantCulture) + "\n");
            }
            else
            {
                stdout.Write(paths[0].Path + " is a valid SST file\n");
            }
        }
        return 0;
    }

    private static IEnumerable<string> MetaBlockNames(SstFileDumper d) => d.MetaBlocks.Keys;

    private static BlockHandle MetaHandle(SstFileDumper d, string name) => d.MetaBlocks[name];

    private static bool SafeDirectoryExists(IFileSystem fs, string p)
    {
        try
        {
            return fs.DirectoryExists(p);
        }
        catch (RocksDbException)
        {
            return Directory.Exists(p);
        }
    }

    private static void PrintProperties(TextWriter o, string title, TableProperties p) =>
        o.Write(title + "\n------------------------------\n  " + ToolText.TablePropertiesToString(p, "\n  ", ": ") + "\n");

    private static void PrintHelp(TextWriter o) => o.Write(
        "sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw|recompress|identify|verify]\n" +
        "    --file=<data_dir_OR_sst_file>\n" +
        "      Path to SST file or directory containing SST files\n\n" +
        "    --command=check|scan|raw|verify|identify\n" +
        "        check: Iterate over entries in files but don't print anything except if an error is encountered (default command)\n" +
        "        scan: Iterate over entries in files and print them to screen\n" +
        "        raw: Dump all the table contents to <file_name>_dump.txt\n" +
        "        verify: Iterate all the blocks in files verifying checksum to detect possible corruption but don't print anything except if a corruption is encountered\n" +
        "        recompress: (not ported) prints the input file's compression\n" +
        "        identify: Reports a file is a valid SST file or lists all valid SST files under a directory\n\n" +
        "    --output_hex\n      Can be combined with scan command to print the keys and values in Hex\n\n" +
        "    --from=<user_key>\n    --to=<user_key>\n    --prefix=<user_key>\n    --input_key_hex\n    --read_num=<num>\n" +
        "    --show_properties\n    --show_summary\n    --list_meta_blocks\n    --verify_checksum\n");
}
