namespace RocksDb.Managed.Soak;

/// <summary>State of one register-like key (register or range-delete key space).</summary>
internal struct Cell
{
    /// <summary>Version of the last write applied (-1: never written).</summary>
    public long Version;
    /// <summary>The key exists (the last applied write was a put).</summary>
    public bool Present;
    public uint Crc;
    public int Length;

    public static Cell Absent => new() { Version = -1 };

    public readonly bool SameAs(in Cell o) =>
        Present == o.Present && (!Present || (Version == o.Version && Crc == o.Crc && Length == o.Length));

    public override readonly string ToString() => Present ? $"v{Version} (len {Length}, crc {Crc:x8})" : "absent";
}

/// <summary>
/// The model: the resolved state of every key after the last successful verification, plus the next version each
/// writer uses. Bounded by the key spaces (fixed size), so a multi-day run does not grow it. Each successful
/// verification replaces it by what the verifier observed (Jepsen-style resolution of the indeterminate operations).
/// </summary>
internal sealed class Model
{
    private const int Magic = 0x4C444F4D; // "MODL"

    public int Cycle;
    public bool BankInitialized;
    public Cell[] Registers = [];
    public long[] NextVersion = [];
    public ulong[] Balances = [];
    public ulong[] Counters = [];
    public Cell[] Rdel = [];
    public long RdelNextVersion;

    public static Model Empty(Layout l)
    {
        var m = new Model
        {
            Registers = new Cell[l.Writers * l.KeysPerWriter],
            NextVersion = new long[l.Writers],
            Balances = new ulong[l.Accounts],
            Counters = new ulong[l.Counters],
            Rdel = new Cell[l.RdelKeys],
        };
        Array.Fill(m.Registers, Cell.Absent);
        Array.Fill(m.Rdel, Cell.Absent);
        Array.Fill(m.NextVersion, 1);
        m.RdelNextVersion = 1;
        return m;
    }

    public Model Clone()
    {
        var m = (Model)MemberwiseClone();
        m.Registers = (Cell[])Registers.Clone();
        m.NextVersion = (long[])NextVersion.Clone();
        m.Balances = (ulong[])Balances.Clone();
        m.Counters = (ulong[])Counters.Clone();
        m.Rdel = (Cell[])Rdel.Clone();
        return m;
    }

    public void Save(string path)
    {
        string tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write))
        using (var w = new BinaryWriter(fs))
        {
            w.Write(Magic);
            w.Write(Cycle);
            w.Write(BankInitialized);
            WriteCells(w, Registers);
            w.Write(NextVersion.Length);
            foreach (long v in NextVersion) w.Write(v);
            w.Write(Balances.Length);
            foreach (ulong v in Balances) w.Write(v);
            w.Write(Counters.Length);
            foreach (ulong v in Counters) w.Write(v);
            WriteCells(w, Rdel);
            w.Write(RdelNextVersion);
            fs.Flush(flushToDisk: true);
        }
        File.Move(tmp, path, overwrite: true);
    }

    public static Model Load(string path)
    {
        using var r = new BinaryReader(File.OpenRead(path));
        if (r.ReadInt32() != Magic) throw new InvalidDataException("not a model file: " + path);
        var m = new Model { Cycle = r.ReadInt32(), BankInitialized = r.ReadBoolean(), Registers = ReadCells(r) };
        m.NextVersion = new long[r.ReadInt32()];
        for (int i = 0; i < m.NextVersion.Length; i++) m.NextVersion[i] = r.ReadInt64();
        m.Balances = new ulong[r.ReadInt32()];
        for (int i = 0; i < m.Balances.Length; i++) m.Balances[i] = r.ReadUInt64();
        m.Counters = new ulong[r.ReadInt32()];
        for (int i = 0; i < m.Counters.Length; i++) m.Counters[i] = r.ReadUInt64();
        m.Rdel = ReadCells(r);
        m.RdelNextVersion = r.ReadInt64();
        return m;
    }

    private static void WriteCells(BinaryWriter w, Cell[] cells)
    {
        w.Write(cells.Length);
        foreach (Cell c in cells)
        {
            w.Write(c.Version);
            w.Write(c.Present);
            w.Write(c.Crc);
            w.Write(c.Length);
        }
    }

    private static Cell[] ReadCells(BinaryReader r)
    {
        var cells = new Cell[r.ReadInt32()];
        for (int i = 0; i < cells.Length; i++)
            cells[i] = new Cell { Version = r.ReadInt64(), Present = r.ReadBoolean(), Crc = r.ReadUInt32(), Length = r.ReadInt32() };
        return cells;
    }
}
