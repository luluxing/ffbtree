# Oct 28, 2025
# Objective: Run the simulation experiment with different node sizes and data sizes
# and record the results.

ROOT_DIR="/scratch1/xingl/deterministic_perf"
SRC_FILE="$ROOT_DIR/include/utils.hpp"
BUILD_DIR="$ROOT_DIR/build"
EXECUTABLE="$BUILD_DIR/bm_1d"
LOG_DIR="$ROOT_DIR/benchmark/"
DATA_DIR="$ROOT_DIR/data"

mkdir -p "$LOG_DIR"

PAGE_SIZES=(1024 4096)
INDEXES=(0 1 3)

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
  sed -Ei "s/static const uint64_t pageSize=[0-9]+;/static const uint64_t pageSize=${PAGE_SIZE};/" "$SRC_FILE"

  # Build in Release
  cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$BUILD_DIR" -j >/dev/null

  # Run experiments
  for INDEX in "${INDEXES[@]}"; do
    "$EXECUTABLE" 100000000 1 "$DATA_DIR/seq_100M.txt" 1 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_seq_100M_index-${INDEX}_$(date +%F_%H_%M).out"

    "$EXECUTABLE" 100000000 1 "$DATA_DIR/rand_100M.txt" 0 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_rand_100M_index-${INDEX}_$(date +%F_%H_%M).out"

    "$EXECUTABLE" 10000000 1 "$DATA_DIR/seq_10M.txt" 1 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_seq_10M_index-${INDEX}_$(date +%F_%H_%M).out"

    "$EXECUTABLE" 10000000 1 "$DATA_DIR/rand_10M.txt" 0 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_rand_10M_index-${INDEX}_$(date +%F_%H_%M).out"

    "$EXECUTABLE" 1000000 1 "$DATA_DIR/rand_1M.txt" 0 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_rand_1M_index-${INDEX}_$(date +%F_%H_%M).out"

    "$EXECUTABLE" 1000000 1 "$DATA_DIR/seq_1M.txt" 1 "$INDEX" > "${LOG_DIR}/bm_1d_page-${PAGE_SIZE}_seq_1M_index-${INDEX}_$(date +%F_%H_%M).out"
  done
done