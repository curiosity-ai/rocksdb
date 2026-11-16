# A UserWriteCallback that throws leaves its writer linked in the write queue; every later write hangs

**Suggested title:** `UserWriteCallback::OnWriteEnqueued / OnWalWriteFinish are not exception safe: a throw unwinds WriteImpl with the stack Writer still linked in newest_writer_`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

Either the API states that the callbacks must not throw (`include/rocksdb/user_write_callback.h` says nothing about
exceptions, and unlike `WriteCallback::Callback` they have no way to report a failure), or an exception from a callback
fails that write and leaves the DB usable (or stopped with a background error).

### Actual behavior

An exception thrown by `OnWriteEnqueued` propagates out of `DB::WriteWithCallback`, and the next write on the DB never
returns. `WriteThread::JoinBatchGroup` (`db/write_thread.cc`) calls the callback right after linking the writer:

```cpp
  bool linked_as_leader = LinkOne(w, &newest_writer_);

  w->CheckWriteEnqueuedCallback();

  if (linked_as_leader) {
    SetState(w, STATE_GROUP_LEADER);
  }
```

The `Writer` lives on the stack of `DBImpl::WriteImpl`, so the exception unwinds it while it is still the newest writer
(and possibly the group leader that nobody else will replace): the next writer links behind a destroyed object and
waits for a state change that never comes (or reads freed stack memory). `OnWalWriteFinish` is worse:
`DBImpl::WriteGroupToWAL` / `ConcurrentWriteGroupToWAL` (`db/db_impl/db_impl_write.cc`) call `CheckPostWalWriteCallback`
for every writer of the group on the leader's thread, so one follower's throwing callback unwinds the leader's write,
with the rest of the group still waiting on it (found by reading; the reproduction below covers `OnWriteEnqueued`).

### Steps to reproduce

The C++ program:

```cpp
// A UserWriteCallback::OnWriteEnqueued that throws leaves its writer linked in the write queue: the next write hangs.
#include <rocksdb/db.h>
#include <rocksdb/user_write_callback.h>
#include <rocksdb/write_batch.h>

#include <chrono>
#include <cstdio>
#include <future>
#include <stdexcept>

using namespace ROCKSDB_NAMESPACE;

struct ThrowingCallback : UserWriteCallback {
  void OnWriteEnqueued() override { throw std::runtime_error("callback failed"); }
  void OnWalWriteFinish() override {}
};

int main() {
  Options options;
  options.create_if_missing = true;
  Status s = DestroyDB("/tmp/up14", options);
  std::unique_ptr<DB> db;
  s = DB::Open(options, "/tmp/up14", &db);
  if (!s.ok()) { printf("open: %s\n", s.ToString().c_str()); return 1; }

  WriteBatch batch;
  s = batch.Put("a", "1");
  ThrowingCallback cb;
  try {
    s = db->WriteWithCallback(WriteOptions(), &batch, &cb);
    printf("WriteWithCallback: %s\n", s.ToString().c_str());
  } catch (const std::exception& e) {
    printf("WriteWithCallback threw: %s\n", e.what());
  }

  auto put = std::async(std::launch::async, [&] { return db->Put(WriteOptions(), "b", "2"); });
  if (put.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) {
    printf("Put(b): still waiting after 10 s\n");
    fflush(stdout);
    _Exit(1);  // the write queue is stranded; Close would hang too
  }
  printf("Put(b): %s\n", put.get().ToString().c_str());
  return 0;
}
```

The prebuilt 11.12 library was built from other headers than the ones at hand, so this program was not run as is
(linked against that library it crashes before the first write, as the class layouts differ). The program that was run
against `librocksdb.so` 11.12 does the same through the C API, calling `DBImpl::WriteWithCallback` through its exported
symbol, and runs the second write in a child process with a 10 s alarm:

<details>
<summary>ABI-independent variant (run)</summary>

```cpp
// ABI-independent variant for running against a prebuilt library built from other headers: the DB, WriteOptions and
// WriteBatch come from the C API (their layouts are the library's own); only DBImpl::WriteWithCallback is called
// directly, through its exported symbol.
#include <rocksdb/c.h>

#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>

namespace rocksdb {
class WriteOptions;
class WriteBatch;
class UserWriteCallback {  // as include/rocksdb/user_write_callback.h
 public:
  virtual ~UserWriteCallback() = default;
  virtual void OnWriteEnqueued() = 0;
  virtual void OnWalWriteFinish() = 0;
};
struct Status {  // returned in memory (non-trivial destructor); large enough for rocksdb::Status
  alignas(8) char bytes[64];
  ~Status() {}
};
class DBImpl {
 public:
  Status WriteWithCallback(const WriteOptions&, WriteBatch*, UserWriteCallback*);
};
}  // namespace rocksdb

// The C API wrappers are { DB* rep; }, { WriteOptions rep; }, { WriteBatch rep; } (db/c.cc).
struct Wrap { void* rep; };

struct ThrowingCallback : rocksdb::UserWriteCallback {
  void OnWriteEnqueued() override { throw std::runtime_error("callback failed"); }
  void OnWalWriteFinish() override {}
};

int main() {
  setvbuf(stdout, nullptr, _IONBF, 0);
  char* err = nullptr;
  system("rm -rf /tmp/up14");
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_t* db = rocksdb_open(o, "/tmp/up14", &err);
  if (err) { printf("open: %s\n", err); return 1; }
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  rocksdb_writebatch_t* b = rocksdb_writebatch_create();
  rocksdb_writebatch_put(b, "a", 1, "1", 1);

  auto* impl = static_cast<rocksdb::DBImpl*>(reinterpret_cast<Wrap*>(db)->rep);
  ThrowingCallback cb;
  try {
    impl->WriteWithCallback(*reinterpret_cast<rocksdb::WriteOptions*>(wo), reinterpret_cast<rocksdb::WriteBatch*>(b),
                            &cb);
    printf("WriteWithCallback returned\n");
  } catch (const std::exception& e) {
    printf("WriteWithCallback threw: %s\n", e.what());
  }

  printf("Put(b) in a child process with a 10 s limit ...\n");
  pid_t pid = fork();
  if (pid == 0) {
    alarm(10);
    rocksdb_put(db, wo, "b", 1, "2", 1, &err);
    printf("Put(b): %s\n", err ? err : "OK");
    _exit(0);
  }
  int st = 0;
  waitpid(pid, &st, 0);
  if (WIFSIGNALED(st) && WTERMSIG(st) == SIGALRM) printf("Put(b): still waiting after 10 s\n");
  else if (WIFSIGNALED(st)) printf("Put(b): child killed by signal %d\n", WTERMSIG(st));
  return 0;
}
```

</details>

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
g++ -std=c++20 repro.cc -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
WriteWithCallback threw: callback failed
Put(b) in a child process with a 10 s limit ...
Put(b): still waiting after 10 s
```

With a callback that does not throw, the same program prints `Put(b): OK`.

### Suggested fix

Either of:

- (a) Declare the callbacks `noexcept` (an escaping exception then terminates at the throw instead of corrupting the
  write queue), or at least document that they must not throw.
- (b) Catch in `Writer::CheckWriteEnqueuedCallback` / `CheckPostWalWriteCallback` and convert the exception into a
  status on that writer: the writer completes its write group normally, the other writers get their own outcome, and
  the throwing write returns an error (with the DB stopped by a background error if the write's outcome cannot be known
  any more). RocksDB is built with exceptions enabled, so this is possible.

The managed port (RocksDb.Managed) does (b): an exception from either callback is caught and recorded on the writer, the
write group completes, the DB sets a hard background error (Aborted, reason WriteCallback, no automatic recovery) and the
original exception is rethrown to that caller; later writes fail until `Resume()`, and the throwing write's outcome is
documented as indeterminate (regression test `ThrowingUserWriteCallbackFailsSafely`, both callbacks, leader and
follower, five write modes).
