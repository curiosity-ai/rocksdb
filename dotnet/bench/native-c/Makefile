#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
#
# Builds rocksdb_bench against the librocksdb.so shipped in the RocksDB NuGet
# package (rocksdb-sharp), i.e. the very same binary the C# benchmarks load.
# The package version is read from dotnet/Directory.Build.props
# (NativeRocksDbPackageVersion); run `dotnet restore` first so the package is
# in the NuGet cache. Override with `make ROCKSDB_LIB_DIR=/path/to/dir`.
#
#   make            # builds ./rocksdb_bench (rpath $ORIGIN/lib -> NuGet librocksdb.so)
#   make clean

CC ?= cc
PROPS := ../../Directory.Build.props
PKG_VERSION := $(shell sed -n 's:.*<NativeRocksDbPackageVersion>\(.*\)</NativeRocksDbPackageVersion>.*:\1:p' $(PROPS))
NUGET_PACKAGES ?= $(HOME)/.nuget/packages
ROCKSDB_LIB_DIR ?= $(NUGET_PACKAGES)/rocksdb/$(PKG_VERSION)/runtimes/linux-x64/native

# include/rocksdb/c.h is the header of the matching upstream release (v11.12),
# not the newer one in the parent tree.
CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Werror -Iinclude -pthread
# The NuGet file is named librocksdb.so but its SONAME is librocksdb.so.<major>.<minor>,
# so the loader looks for the SONAME. lib/ holds a symlink with that name pointing at the
# NuGet file, and the binary's rpath is $ORIGIN/lib: no LD_LIBRARY_PATH needed and the
# exact same library file as rocksdb-sharp is loaded.
SONAME := $(shell readelf -d $(ROCKSDB_LIB_DIR)/librocksdb.so 2>/dev/null | sed -n 's/.*Library soname: \[\(.*\)\]/\1/p')
LDFLAGS += -Llib -Wl,-rpath,'$$ORIGIN/lib' -pthread
LDLIBS += -l:$(SONAME)

all: rocksdb_bench

rocksdb_bench: rocksdb_bench.c include/rocksdb/c.h $(PROPS)
	@test -f $(ROCKSDB_LIB_DIR)/librocksdb.so || { echo "librocksdb.so not found in $(ROCKSDB_LIB_DIR); run 'dotnet restore' in dotnet/ first" >&2; exit 1; }
	mkdir -p lib
	ln -sfn $(ROCKSDB_LIB_DIR)/librocksdb.so lib/$(SONAME)
	$(CC) $(CFLAGS) -o $@ rocksdb_bench.c $(LDFLAGS) $(LDLIBS)

print-libdir:
	@echo $(ROCKSDB_LIB_DIR)

clean:
	rm -rf rocksdb_bench lib

.PHONY: all clean print-libdir
