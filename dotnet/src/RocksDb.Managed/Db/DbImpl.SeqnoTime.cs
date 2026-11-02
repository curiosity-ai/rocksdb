namespace RocksDb.Managed.Db;

/// <summary>
/// The DB-wide seqno-to-time mapping used by preserve_internal_time_seconds / preclude_last_level_data_seconds (and
/// TimedPut). Port of DBImpl::GetSeqnoToTimeSample, EnsureSeqnoToTimeMapping, PrepopulateSeqnoToTimeMapping,
/// RegisterRecordSeqnoTimeWorker, RecordSeqnoToTimeMapping and InstallSuperVersionForConfigChange
/// (db/db_impl/db_impl.cc, db_impl_open.cc). Upstream hands each super version an immutable copy; here flushes and
/// compactions take a copy under the DB mutex (<see cref="CopySeqnoToTimeMapping"/>).
/// </summary>
internal sealed partial class DbImpl
{
    private readonly SeqnoToTimeMapping _seqnoToTimeMapping = new(); // guarded by _mutex
    private Timer? _recordSeqnoTimeTimer;                             // guarded by _mutex
    private ulong _recordSeqnoTimeCadence;                            // guarded by _mutex

    /// <summary>
    /// GetSeqnoToTimeSample: the latest sequence number (at least 1) and the current unix time, sampled in this order
    /// (a one-sided bound). REQUIRES: mutex.
    /// </summary>
    private (ulong Seqno, ulong Time) GetSeqnoToTimeSample()
    {
        ulong seqno = Math.Max(Versions.LastSequence, 1UL);
        return (seqno, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    }

    /// <summary>
    /// The mapping a flush or compaction of <paramref name="cfd"/> uses (SuperVersion::GetSeqnoToTimeMapping): a copy of
    /// the DB's mapping when the column family has preserve / preclude enabled, else null. REQUIRES: mutex.
    /// </summary>
    internal SeqnoToTimeMapping? CopySeqnoToTimeMapping(ColumnFamilyData cfd)
    {
        if (!new MinAndMaxPreserveSeconds(cfd.Options).IsEnabled) return null;
        var copy = new SeqnoToTimeMapping();
        copy.CopyFrom(_seqnoToTimeMapping);
        return copy;
    }

    /// <summary>Test hook: a copy of the DB-wide mapping.</summary>
    internal SeqnoToTimeMapping TestGetSeqnoToTimeMapping()
    {
        lock (_mutex)
        {
            var copy = new SeqnoToTimeMapping();
            copy.CopyFrom(_seqnoToTimeMapping);
            return copy;
        }
    }

    /// <summary>
    /// Port of DBImpl::EnsureSeqnoToTimeMapping (with InstallSuperVersionForConfigChange): data written after
    /// preserve / preclude is set on a column family must get a reasonable time estimate, so the mapping gets an entry
    /// unless it has a recent one. REQUIRES: mutex.
    /// </summary>
    private void EnsureSeqnoToTimeMapping(ColumnFamilyOptions options)
    {
        var preserveInfo = new MinAndMaxPreserveSeconds(options);
        if (!preserveInfo.IsEnabled) return;
        (ulong seqno, ulong now) = GetSeqnoToTimeSample();
        // Ensure at least one sample that is sufficiently recent
        ulong lastSample = 0;
        if (_seqnoToTimeMapping.Empty)
        {
            // The exact best settings will be found and applied in RegisterRecordSeqnoTimeWorker()
            _seqnoToTimeMapping.SetCapacity(SeqnoToTimeMapping.MaxSeqnoToTimeEntries);
        }
        else
        {
            lastSample = _seqnoToTimeMapping.GetProximalTimeBeforeSeqno(DbFormat.MaxSequenceNumber);
        }
        ulong cadence = preserveInfo.GetRecodingCadence();
        // Extend cadence so as to avoid stepping on toes of recorder job, which could lag a bit.
        cadence += 3 + cadence / 100;
        if (now >= cadence && lastSample <= now - cadence) _seqnoToTimeMapping.Append(seqno, now);
    }

    /// <summary>
    /// Port of DBImpl::PrepopulateSeqnoToTimeMapping: a DB created with preserve / preclude set gets 100 pre-allocated
    /// sequence numbers (recorded in the MANIFEST) mapped linearly back over the preserve window, so that data imported
    /// later can keep a rough write time. Only while opening a new DB, before any write.
    /// </summary>
    private async ValueTask PrepopulateSeqnoToTimeMappingAsync(MinAndMaxPreserveSeconds preserveInfo, bool synchronous, CancellationToken ct)
    {
        if (!preserveInfo.IsEnabled || Versions.LastSequence != 0) return;
        const ulong Max = SeqnoToTimeMapping.MaxSeqnoTimePairsPerSst;
        Versions.SetLastSequence(Max);
        // And record in manifest, to avoid going backwards in seqno on re-open (potentially with different options).
        var edit = new VersionEdit { ColumnFamily = 0, LastSequence = Max };
        await Versions.LogAndApplyAsync(Versions.GetColumnFamily(0)!, edit, synchronous, ct).ConfigureAwait(false);
        lock (_mutex)
        {
            (ulong seqno, ulong now) = GetSeqnoToTimeSample();
            ulong populateHistoricalSeconds = preserveInfo.MaxPreserveSeconds;
            if (seqno > 1 && now > populateHistoricalSeconds)
            {
                // seqno=0 is reserved
                _seqnoToTimeMapping.PrePopulate(1, seqno, now - populateHistoricalSeconds, now);
            }
        }
    }

    /// <summary>
    /// Port of DBImpl::RegisterRecordSeqnoTimeWorker: sizes the mapping for the column families' preserve settings and
    /// (re)starts the recording task with the cadence min_preserve_seconds / 100 (run immediately), or clears the
    /// mapping and stops the task when no column family uses the feature.
    /// </summary>
    private void RegisterRecordSeqnoTimeWorker()
    {
        if (_readOnly) return;
        lock (_mutex)
        {
            var preserveInfo = new MinAndMaxPreserveSeconds();
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                if (!cfd.Dropped) preserveInfo.Combine(cfd.Options);
            }
            ulong cadence = preserveInfo.GetRecodingCadence();
            if (cadence == 0)
            {
                // To return as much as possible to the feature being disabled, clear the existing mapping
                _seqnoToTimeMapping.SetCapacity(0);
                _seqnoToTimeMapping.SetMaxTimeSpan(ulong.MaxValue);
            }
            else
            {
                ulong cap = Math.Min(SeqnoToTimeMapping.MaxSeqnoToTimeEntries,
                    preserveInfo.MaxPreserveSeconds * SeqnoToTimeMapping.MaxSeqnoTimePairsPerCf / preserveInfo.MinPreserveSeconds);
                _seqnoToTimeMapping.SetCapacity(cap);
                _seqnoToTimeMapping.SetMaxTimeSpan(preserveInfo.MaxPreserveSeconds);
            }
            if (cadence == _recordSeqnoTimeCadence && (cadence == 0) == (_recordSeqnoTimeTimer is null)) return;
            _recordSeqnoTimeCadence = cadence;
            _recordSeqnoTimeTimer?.Dispose();
            _recordSeqnoTimeTimer = null;
            if (cadence != 0 && !_shuttingDown)
            {
                TimeSpan period = TimeSpan.FromMilliseconds(Math.Min((double)cadence * 1000, 0xfffffffe));
                _recordSeqnoTimeTimer = new Timer(static s => ((DbImpl)s!).RecordSeqnoToTimeMapping(), this, TimeSpan.Zero, period);
            }
        }
    }

    /// <summary>Port of DBImpl::RecordSeqnoToTimeMapping: records the next sample.</summary>
    internal void RecordSeqnoToTimeMapping()
    {
        if (IsDisposed) return;
        lock (_mutex)
        {
            if (_shuttingDown) return;
            (ulong seqno, ulong now) = GetSeqnoToTimeSample();
            _seqnoToTimeMapping.Append(seqno, now);
        }
    }

    private void StopRecordSeqnoTimeWorker()
    {
        Timer? t;
        lock (_mutex)
        {
            t = _recordSeqnoTimeTimer;
            _recordSeqnoTimeTimer = null;
            _recordSeqnoTimeCadence = 0;
        }
        t?.Dispose();
    }
}
