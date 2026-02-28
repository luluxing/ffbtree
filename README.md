# FFBtree

This repository benchmarks deterministic B-tree variants under different split strategies, storage modes, and workloads.

This README is written for three branches:
- `main`
- `synchrn`
- `expInsertBuf`

## Prerequisites

- Linux (benchmark scripts use `bash` and optionally `numactl`)
- `cmake >= 3.3`
- C++ compiler with C++11 support (e.g., `g++`)
- `make`
- Python 3 (for plotting scripts in `benchmark/`)


Notes:
- Tests use GoogleTest fetched by CMake (`FetchContent`).
- Data files are expected under `data/`.
- Some branch scripts generate or consume binary `.bin` datasets.

## Quick Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Run tests:

```bash
ctest --test-dir build --output-on-failure
```

## Code Structure

- `include/`: index implementations and storage helpers
- `src/indexes.hpp`: benchmark-facing index wrappers (varies by branch)
- `benchmark/`: benchmark drivers, scripts, and plotting tools
- `tests/`: unit tests for tree implementations
- `data/`: input datasets (`seq_*`, `rand_*`, `zipf_*`)

## Branch Guide

### `main`

Focus:
- Disk-based one-dimensional index benchmarking
- Simulation benchmark
- LRU-related benchmark executable

Main executables from CMake:
- `bm_1d` (`benchmark/one_dim.cpp`)
- `bm_lru` (`benchmark/benchmark_lru.cpp`)
- `simulate_btree` (`benchmark/simulate_btree_insert.cpp`)

`bm_1d` CLI:

```text
bm_1d <data_size> <batch_size> <file_name> <distribution> <index>
```

- `distribution`: `0=random`, `1=sequential`
- `index`:
  - `0=NormalBtreeDiskIndex`
  - `1=CLRSBtreeDiskIndex`
  - `2=BlinkTreeDiskIndex`
  - `3=FFBtreeDiskIndex`

Example:

```bash
./build/bm_1d 1000000 1000 ./data/seq_1M.bin 1 1
```

### `synchrn`

Focus:
- Adds concurrent in-memory benchmarks (`bm_cc`, `bm_cc_rw`)
- Adds sync/memory-related tests
- Adds Zipf input support in benchmark utilities

Additional executables from CMake:
- `bm_cc` (`benchmark/benchmark_cc.cpp`)
- `bm_cc_rw` (`benchmark/benchmark_cc_rw.cpp`)

`bm_1d` in this branch keeps the same 5-argument interface as `main`.

`bm_cc` CLI:

```text
bm_cc <data_size> <file_name> <distribution> <thread_num> <index> <read_cost(us)> <write_cost(us)> <output_file>
```

- `distribution`: `0=random`, `1=sequential`, `2=zipf`
- `index`: `0=BtreeOLCIndex`, `1=FFBtreeOLCIndex`
- Output format (`.bin`): `(latency_ns, thread_id, restart_count)`

Example:

```bash
./build/bm_cc 1000000 ./data/zipf_1M.bin 2 16 1 100 200 ./build/cc_1M_t16.bin
```

`bm_cc_rw` CLI:

```text
bm_cc_rw <data_size> <bulk_load_size> <file_name> <distribution> <thread_num> <index> <read_cost(us)> <write_cost(us)> <write_ratio> <output_file>
```

- `write_ratio` is in `[0,1]`

Example:

```bash
./build/bm_cc_rw 1000000 500000 ./data/seq_1M.bin 1 16 0 100 200 0.8 ./build/ccrw_1M_t16.bin
```

### `expInsertBuf`

Focus:
- Experimental insertion-buffer behavior and latency recording
- `bm_1d` outputs per-operation latency records to a binary file
- Includes plotting helpers such as `benchmark/plot_test_bin.py`

Executables from CMake:
- `bm_1d`
- `simulate_btree`

`bm_1d` CLI in this branch:

```text
bm_1d <data_size> <file_name> <distribution> <index> <read_latency> <write_latency> <output_file>
```

- `distribution`: `0=random`, `1=sequential`, `2=zipf`
- Output record layout: `(latency_ns, io_num, height)` in binary

Example:

```bash
./build/bm_1d 1000000 ./data/seq_1M.bin 1 1 100 200 ./build/test.bin
python3 benchmark/plot_test_bin.py
```

Adjust paths/executable names if your local build output differs.
