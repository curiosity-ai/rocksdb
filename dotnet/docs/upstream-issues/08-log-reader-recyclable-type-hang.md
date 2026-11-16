# log::Reader loops forever on a recyclable record type in a non-recycled log

**Suggested title:** `log::Reader: a recyclable record type after the first record of a legacy log hangs ReadRecord (DB::Open never returns)`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package). The code is unchanged on
`main` (11.14).

### Expected behavior

A single damaged byte in a WAL (or MANIFEST) record is reported as corruption and handled according to
`wal_recovery_mode`: `kPointInTimeRecovery` / `kSkipAnyCorruptedRecords` keep the records before it,
`kTolerateCorruptedTailRecords` / `kAbsoluteConsistency` fail the open with Corruption.

### Actual behavior

If the type byte of any record after the first one in a log that did not start with a recyclable record is changed to a
recyclable type (for example `kFullType` 1 becomes `kRecyclableFullType` 5, a single bit flip), `DB::Open` never returns:
it spins at 100% CPU in `log::Reader::ReadRecord`.

Cause, in `db/log_reader.cc` `Reader::ReadPhysicalRecord`:

```cpp
    if (is_recyclable_type) {
      header_size = kRecyclableHeaderSize;
      if (first_record_read_ && !recycled_) {
        // A recycled log should have started with a recycled record
        return kBadRecord;
      }
```

The function returns `kBadRecord` without consuming anything from `buffer_`. `ReadRecord`'s `case kBadRecord:` only breaks
out of the switch, the loop calls `ReadPhysicalRecord` again, which parses the same header and returns `kBadRecord`
again, forever. `FragmentBufferedReader::TryReadFragment` has the same pattern.

### Steps to reproduce

1. Open a DB (default options, `recycle_log_file_num = 0`), write three keys, close without flushing.
2. In the WAL, change byte 6 of the third record's header (the type) from 1 to 5.
3. Open the DB with `wal_recovery_mode = kPointInTimeRecovery`: the call does not return.

### Suggested fix

Treat the record like a checksum mismatch, which it is (the CRC covers the type byte): drop the buffer and return
`kBadRecordChecksum`, as the CRC mismatch path does:

```cpp
      if (first_record_read_ && !recycled_) {
        // A recycled log should have started with a recycled record
        *drop_size = buffer_.size();
        buffer_.clear();
        return kBadRecordChecksum;
      }
```

The managed port (RocksDb.Managed) does this; regression tests `RecyclableTypeInLegacyWalDoesNotHangOpen` (all four
recovery modes).
