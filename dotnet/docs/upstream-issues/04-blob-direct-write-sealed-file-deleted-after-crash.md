# Blob direct write: a crash before the flush commits loses the values in sealed blob files

**Suggested title:** `enable_blob_direct_write: blob files sealed before a crash are purged on the next open; their keys fail`

**Affected versions:** `main` only (blob direct write is not in a release yet). Found by reading the code at
`02815ee01c2548afc52b82df26af460e28d1ee22` (2026-10-31) while porting the feature. **Not reproduced against native
RocksDB**: the released 11.12 library this project tests against has no blob direct write, and the reproduction below
was not compiled. The managed port, which follows the same design, loses the keys in the scenario described.

### Expected behavior

With `enable_blob_direct_write = true`, a write that `DB::Put` acknowledged (and, with `WriteOptions::sync = true`,
synced) is readable after the process crashes and the DB is opened again, as it is without blob direct write: its blob
index is in the WAL and the value is in a blob file that was synced.

### Actual behavior

After such a crash, `Get` of the keys whose values went into a **sealed** direct-write blob file fails (blob file not
found), because the next `DB::Open` deletes that file as obsolete.

The window is not limited to a flush in progress. A blob file is sealed (footer written) in two places, and in both
cases it is registered in the MANIFEST only when the flush of the memtable that references it commits:

- `BlobFilePartitionManager::WriteBlob` seals the active file of a partition when the next record would take it past
  `blob_file_size`, and keeps it in `current_generation_sealed_files_` (`db/blob/blob_file_partition_manager.cc`,
  `seal_current_file`).
- `BlobFilePartitionManager::PrepareFlushAdditions`, called by `DBImpl::FlushMemTableToOutputFile` before the flush
  job runs, seals the deferred files of the picked memtables (`SealDeferredFile`).

The file reaches a `Version` through the flush's `VersionEdit` only. If the process dies first, the next open:

1. replays the WAL; the blob indexes of the lost file are recovered into the memtable (or into a table file by the
   recovery flush);
2. runs the full-scan obsolete file purge. `DBImpl::ShouldKeepBlobFileDuringPurge` keeps a blob file only if a
   partition manager tracks its number (`IsTrackedBlobFileNumber`, empty in a new process) or if it has no footer
   (`ShouldKeepFooterlessBlobFile`). A sealed file has a footer and is in no version, so it is deleted;
3. reads of those keys go through `ResolveBlobDirectWriteIndex`, whose fallback for files absent from the version
   finds no file.

Footer-less files (the active file of each partition at the time of the crash) are kept and read correctly; only
sealed files are lost. So with the default `blob_file_size` (256 MiB) the loss needs more than 256 MiB of blob values
in one memtable or a crash during a flush, but with a small `blob_file_size` any crash after the first rotation of a
memtable's lifetime loses data.

The comment in `DBImpl::CancelAllBackgroundWork` says recovery of unregistered files is deliberately out of scope:

```cpp
// Blob direct-write relies on clean shutdown flushes to register live blob
// files in MANIFEST before close. WAL recovery of unregistered files is out
// of scope in v1.
```

That makes the loss expected after a crash, but it is not stated in the option's documentation
(`AdvancedColumnFamilyOptions::enable_blob_direct_write`), and it includes writes made with `sync = true`, which the
application was told are durable. If the limitation stays, it should at least be documented there; better, recovery
should keep (and register, or protect until the recovered memtables are flushed) every blob file that a replayed WAL
record references, for example by collecting the file numbers of the blob indexes replayed in `RecoverLogFiles` and
treating them like `IsTrackedBlobFileNumber` during the purge at open.

### Steps to reproduce the behavior

Not run. A test along these lines in `db/blob/db_blob_direct_write_test.cc` should show it:

```cpp
TEST_F(DBBlobDirectWriteTest, SealedFileSurvivesCrashBeforeFlush) {
  Options options = GetBlobDirectWriteCompatibleOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.min_blob_size = 0;
  options.blob_file_size = 4096;           // rotate after a few values
  options.write_buffer_size = 64 << 20;    // no automatic flush
  options.avoid_flush_during_shutdown = true;
  options.env = fault_env_.get();          // FaultInjectionTestEnv over the test env
  DestroyAndReopen(options);

  WriteOptions sync;
  sync.sync = true;
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(db_->Put(sync, Key(i), std::string(512, 'a' + i % 26)));
  }
  // Several blob files were sealed by size; none is in the MANIFEST yet.

  // Crash: drop the process state without the close flush.
  fault_env_->SetFilesystemActive(false);
  Close();
  fault_env_->SetFilesystemActive(true);
  fault_env_->DropUnsyncedFileData();   // everything above was synced

  Reopen(options);
  for (int i = 0; i < 64; ++i) {
    ASSERT_EQ(Get(Key(i)), std::string(512, 'a' + i % 26));  // fails for keys in sealed files
  }
}
```

If `Close()` with the file system inactive still runs a registration flush in this setup, killing the process after
the writes (a death test, or copying the DB directory while a sync point blocks the flush thread at
`FlushJob::Run:PostBuildTable`) gives the same state.
