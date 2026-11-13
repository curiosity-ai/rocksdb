# Methods compiled during measurement, before and after the warm-up write rounds (2026-11-12)

Managed engine, net10.0 Release, 4-vCPU VM, one run each. "jit" is the `jit` column of the RESULT row: methods the JIT
compiled or recompiled (all threads, incl. the tiering background compiler) while the benchmark was timed.

| configuration | benchmark | before | after |
|---|---|---:|---:|
| 2,000,000 x 100 B, 1 thread, 64 MiB write buffer | fillrandom | 229 | 6 |
| | readrandomwriterandom (100% writes) | 135 | 12 |
| | flush | 23 | 4 |
| 2,000,000 x 100 B, 4 threads | fillrandom | 210 | 8 |
| | readrandomwriterandom (100% writes) | 91 | 1 |
| | flush | 3 | 2 |
| compaction-heavy (1,000,000 x 100 B, 4 MiB memtables / files, 16 MiB L1) | fillrandom | 552 | 7 |
| | overwrite | 30 | 1 |
| | waitforcompaction | 25 | 0 |
| | readrandom | 2 | 6 |

Before: of the 552 methods in the compaction-heavy fill, 12 were compiled for the first time (e.g.
`LevelCompactionPicker+Builder::TryExtendNonL0TrivialMove`, `FileMetaData::Clone`, `Crc32C::BuildShiftTable`) and the
rest were tier promotions, mostly `Compaction`, `CompactionJob`, `CompactionPicker`, `VersionStorageInfo` and `DbImpl`
(DBBENCH_JIT_METHODS). The warm-up wrote 200,000 keys into a 64 MiB write buffer, so it never switched a memtable
automatically, and its LSM tree never grew past a couple of levels.

Warm-up time: 5.3 / 6.0 / 5.6 s before; after, 76.6 s (1 thread, 6 rounds), 101.9 s (4 threads) and 10.7 s
(compaction-heavy, 3 rounds). `--warmup_write_rounds 0` restores the previous warm-up.
