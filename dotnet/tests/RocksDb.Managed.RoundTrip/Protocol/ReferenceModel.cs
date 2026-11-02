using System.Buffers.Binary;

namespace RocksDb.Managed.RoundTrip.Protocol;

/// <summary>
/// The parent's reference model of the database: expected contents per column family under the upstream semantics
/// of every data op (including merge operators, range deletes and SST ingestion). Applying a script to the model
/// before the child runs it gives the expected-contents files the child verifies against.
/// </summary>
public sealed class ReferenceModel
{
    private readonly Dictionary<string, MergeKind> _merge = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<Op>> _sstFiles = new(StringComparer.Ordinal);
    private readonly Dictionary<long, Contents> _snapshots = new();
    private List<Op>? _openSst;
    private string? _openSstPath;

    public DbSpec Spec { get; }
    public Contents Current { get; } = new();
    public Dictionary<string, Contents> Checkpoints { get; } = new(StringComparer.Ordinal);

    /// <summary>Contents of the latest backup added to each backup directory.</summary>
    public Dictionary<string, Contents> Backups { get; } = new(StringComparer.Ordinal);

    /// <summary>Upper bound on remembered deleted keys per CF (they become "must be absent" checks).</summary>
    public int MaxAbsentPerCf { get; set; } = 4096;

    public ReferenceModel(DbSpec spec)
    {
        Spec = spec;
        foreach (CfSpec cf in spec.ColumnFamilies)
        {
            if (cf.Initial) AddCf(cf.Name);
        }
    }

    /// <summary>A model whose current contents are a copy of <paramref name="contents"/> (e.g. a checkpoint's).</summary>
    public static ReferenceModel FromContents(DbSpec spec, Contents contents)
    {
        var m = new ReferenceModel(new DbSpec { ColumnFamilies = [] });
        foreach (KeyValuePair<string, CfContents> cf in contents.ColumnFamilies)
        {
            m.Current.ColumnFamilies.Add(cf.Key, cf.Value.Clone());
            m._merge[cf.Key] = spec.Cf(cf.Key).Merge;
        }
        return m.WithSpec(spec);
    }

    private ReferenceModel(ReferenceModel other, DbSpec spec)
    {
        Spec = spec;
        Current = other.Current;
        _merge = other._merge;
    }

    private ReferenceModel WithSpec(DbSpec spec) => new(this, spec);

    public IEnumerable<string> ColumnFamilyNames => Current.ColumnFamilies.Keys;

    public bool HasCf(string cf) => Current.ColumnFamilies.ContainsKey(cf);

    public MergeKind MergeOf(string cf) => _merge[cf];

    public Contents SnapshotContents(long id) => _snapshots[id];

    private void AddCf(string name)
    {
        Current.ColumnFamilies.Add(name, new CfContents());
        _merge[name] = Spec.Cf(name).Merge;
    }

    public void ApplyAll(IEnumerable<Op> ops)
    {
        foreach (Op op in ops) Apply(op);
    }

    public void Apply(Op op)
    {
        if (_openSst is not null && op.Kind is OpKind.SstPut or OpKind.SstMerge or OpKind.SstDelete or OpKind.SstDeleteRange)
        {
            _openSst.Add(op);
            return;
        }
        switch (op.Kind)
        {
            case OpKind.Put: Put(op.Cf!, op.Key!, op.Value!); break;
            case OpKind.Merge: Merge(op.Cf!, op.Key!, op.Value!); break;
            case OpKind.Delete:
            case OpKind.SingleDelete: Remove(op.Cf!, op.Key!); break;
            case OpKind.DeleteRange: DeleteRange(op.Cf!, op.Key!, op.Value!); break;
            case OpKind.CreateCf: AddCf(op.Cf!); break;
            case OpKind.DropCf: Current.ColumnFamilies.Remove(op.Cf!); break;
            case OpKind.SstBegin:
                _openSst = [];
                _openSstPath = op.Arg!;
                break;
            case OpKind.SstEnd:
                _sstFiles[_openSstPath!] = _openSst!;
                _openSst = null;
                break;
            case OpKind.Ingest:
                foreach (string file in op.Arg!.Split('|')) Ingest(op.Cf!, _sstFiles[file]);
                break;
            case OpKind.Checkpoint: Checkpoints[op.Arg!] = Current.Clone(); break;
            case OpKind.Backup: Backups[op.Arg!] = Current.Clone(); break;
            case OpKind.Snap: _snapshots[op.N] = Current.Clone(); break;
            case OpKind.SnapRelease: _snapshots.Remove(op.N); break;
            default: break; // control / verification ops don't change contents
        }
    }

    /// <summary>
    /// An ingested file gets one global sequence number: its range tombstones delete older data but not the file's
    /// own point entries (same seqno), then its point entries apply on top of the existing data.
    /// </summary>
    private void Ingest(string cf, List<Op> entries)
    {
        foreach (Op e in entries)
        {
            if (e.Kind == OpKind.SstDeleteRange) DeleteRange(cf, e.Key!, e.Value!);
        }
        foreach (Op e in entries)
        {
            switch (e.Kind)
            {
                case OpKind.SstPut: Put(cf, e.Key!, e.Value!); break;
                case OpKind.SstMerge: Merge(cf, e.Key!, e.Value!); break;
                case OpKind.SstDelete: Remove(cf, e.Key!); break;
            }
        }
    }

    public void Put(string cf, byte[] k, byte[] v)
    {
        CfContents c = Current[cf];
        c.Live[k] = v;
        c.Absent.Remove(k);
    }

    public void Remove(string cf, byte[] k)
    {
        CfContents c = Current[cf];
        c.Live.Remove(k);
        if (c.Absent.Count < MaxAbsentPerCf) c.Absent.Add(k);
    }

    public void DeleteRange(string cf, byte[] begin, byte[] end)
    {
        CfContents c = Current[cf];
        var doomed = new List<byte[]>();
        foreach (byte[] k in c.Live.Keys)
        {
            if (ByteComparer.Instance.Compare(k, begin) >= 0 && ByteComparer.Instance.Compare(k, end) < 0) doomed.Add(k);
        }
        foreach (byte[] k in doomed) Remove(cf, k);
    }

    public void Merge(string cf, byte[] k, byte[] operand)
    {
        CfContents c = Current[cf];
        bool has = c.Live.TryGetValue(k, out byte[]? existing);
        c.Live[k] = FullMerge(_merge[cf], has ? existing : null, operand);
        c.Absent.Remove(k);
    }

    /// <summary>Upstream merge semantics (utilities/merge_operators/uint64add.cc, string_append/stringappend.cc).</summary>
    public static byte[] FullMerge(MergeKind kind, byte[]? existing, byte[] operand)
    {
        switch (kind)
        {
            case MergeKind.UInt64Add:
                ulong sum = (existing is { Length: 8 } ? BinaryPrimitives.ReadUInt64LittleEndian(existing) : 0)
                    + (operand.Length == 8 ? BinaryPrimitives.ReadUInt64LittleEndian(operand) : 0);
                byte[] r = new byte[8];
                BinaryPrimitives.WriteUInt64LittleEndian(r, sum);
                return r;
            case MergeKind.StringAppend:
                return existing is null ? operand : [.. existing, (byte)',', .. operand];
            default:
                throw new InvalidOperationException("merge on a column family without merge operator");
        }
    }

    public static byte[] U64(ulong v)
    {
        byte[] r = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(r, v);
        return r;
    }
}
