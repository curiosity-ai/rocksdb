namespace RocksDb.Managed;

public sealed partial class ColumnFamily
{
    /// <summary>
    /// Changes mutable options of this column family (DB::SetOptions), given as OPTIONS-file name/value pairs, e.g.
    /// <c>("write_buffer_size", "131072")</c>, <c>("disable_auto_compactions", "true")</c>, <c>("compression",
    /// "kZSTD")</c>. All pairs are validated first; InvalidArgument for empty input, unknown names, immutable options
    /// ("Option not changeable") or bad values, in which case nothing changes. On success the new values take effect
    /// for subsequent memtables, flushes and compactions, and a new OPTIONS file is written.
    /// </summary>
    public void SetOptions(IEnumerable<KeyValuePair<string, string>> options) =>
        _db.SetOptionsAsync(_cfd, options, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc cref="SetOptions(IEnumerable{KeyValuePair{string, string}})"/>
    public ValueTask SetOptionsAsync(IEnumerable<KeyValuePair<string, string>> options, CancellationToken cancellationToken = default) =>
        _db.SetOptionsAsync(_cfd, options, synchronous: false, cancellationToken);

    /// <summary>
    /// Typed <see cref="SetOptions(IEnumerable{KeyValuePair{string, string}})"/>: <paramref name="update"/> edits a copy
    /// of the current options (e.g. <c>o =&gt; o.WriteBufferSize = 128 &lt;&lt; 20</c>); every mutable option it changed
    /// is applied. Changing an immutable option fails with InvalidArgument.
    /// </summary>
    public void SetOptions(Action<ColumnFamilyOptions> update)
    {
        ArgumentNullException.ThrowIfNull(update);
        ColumnFamilyOptions copy = _cfd.Options.Clone();
        update(copy);
        _db.SetOptionsAsync(_cfd, copy, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc cref="SetOptions(Action{ColumnFamilyOptions})"/>
    public ValueTask SetOptionsAsync(Action<ColumnFamilyOptions> update, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(update);
        ColumnFamilyOptions copy = _cfd.Options.Clone();
        update(copy);
        return _db.SetOptionsAsync(_cfd, copy, synchronous: false, cancellationToken);
    }
}

public sealed partial class RocksDatabase
{
    /// <summary><see cref="ColumnFamily.SetOptions(IEnumerable{KeyValuePair{string, string}})"/> for <paramref name="columnFamily"/>.</summary>
    public void SetOptions(ColumnFamily columnFamily, IEnumerable<KeyValuePair<string, string>> options)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        columnFamily.SetOptions(options);
    }

    /// <summary><see cref="ColumnFamily.SetOptions(IEnumerable{KeyValuePair{string, string}})"/> for the default column family.</summary>
    public void SetOptions(IEnumerable<KeyValuePair<string, string>> options) => _default.SetOptions(options);

    public ValueTask SetOptionsAsync(ColumnFamily columnFamily, IEnumerable<KeyValuePair<string, string>> options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return columnFamily.SetOptionsAsync(options, cancellationToken);
    }

    /// <summary>
    /// Changes mutable DB options (DB::SetDBOptions), given as OPTIONS-file name/value pairs, e.g.
    /// <c>("max_background_jobs", "4")</c>, <c>("delayed_write_rate", "1048576")</c>, <c>("stats_dump_period_sec",
    /// "60")</c>. InvalidArgument for empty input, unknown or immutable options and bad values (nothing changes then).
    /// A new OPTIONS file is written on success.
    /// </summary>
    public void SetDBOptions(IEnumerable<KeyValuePair<string, string>> options) =>
        _db.SetDbOptionsAsync(options, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc cref="SetDBOptions(IEnumerable{KeyValuePair{string, string}})"/>
    public ValueTask SetDBOptionsAsync(IEnumerable<KeyValuePair<string, string>> options, CancellationToken cancellationToken = default) =>
        _db.SetDbOptionsAsync(options, synchronous: false, cancellationToken);

    /// <summary>
    /// Typed <see cref="SetDBOptions(IEnumerable{KeyValuePair{string, string}})"/>: <paramref name="update"/> edits a
    /// copy of the current DB options; every mutable option it changed is applied (InvalidArgument for immutable ones).
    /// </summary>
    public void SetDBOptions(Action<DbOptions> update)
    {
        ArgumentNullException.ThrowIfNull(update);
        DbOptions copy = _db.Options.Clone();
        update(copy);
        _db.SetDbOptionsAsync(copy, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>A copy of the DB options currently in effect (DB::GetDBOptions).</summary>
    public DbOptions GetDBOptions() => _db.Options.Clone();
}
