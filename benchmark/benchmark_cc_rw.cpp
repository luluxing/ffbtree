#include <chrono>
#include <vector>
#include <thread>
#include <mutex>
#include <utility>
#include <cstdio>
#include <random>
#include <unordered_set>
#include "utility.cpp"

// Structure for binary output: latency and thread_id
struct LatencyRecord {
  long long latency_ns;
  int thread_id;
  int restart_count;
};

void gen_lookups(int data_size, int lookup_size, std::vector<int>& inserted, int* lookups) {
  srand(9);
  for (int i = 0; i < lookup_size; i++) {
    lookups[i] = inserted[rand() % inserted.size()];
  }
}

class LatencyRecorder {
 public:
  // Each thread has its own latency vector
  static thread_local std::vector<long long> local_latencies;
  static thread_local std::vector<int> local_restart_counts;
  static thread_local std::vector<int> local_thread_ids;
  static thread_local int current_thread_id;

  void set_thread_id(int thread_id) {
      current_thread_id = thread_id;
  }

  void record(long long latency_ns, int restart_count) {
      local_latencies.push_back(latency_ns);
      local_restart_counts.push_back(restart_count);
      local_thread_ids.push_back(current_thread_id);
  }

  void merge() {
      std::lock_guard<std::mutex> guard(mu_);
      for (auto &lat : local_latencies)
          all_latencies_.push_back(lat);
      local_latencies.clear();
      for (auto &restart_count : local_restart_counts)
          all_restart_counts_.push_back(restart_count);
      local_restart_counts.clear();
      for (auto &thread_id : local_thread_ids)
          all_thread_ids_.push_back(thread_id);
      local_thread_ids.clear();
  }

  std::vector<long long> get_all_latencies() const {
      return all_latencies_;
  }

  std::vector<int> get_all_restart_counts() const {
      return all_restart_counts_;
  }

  std::vector<int> get_all_thread_ids() const {
      return all_thread_ids_;
  }

 private:
  std::mutex mu_;
  std::vector<long long> all_latencies_;
  std::vector<int> all_restart_counts_;
  std::vector<int> all_thread_ids_;
};

// define static thread_local variable
thread_local std::vector<long long> LatencyRecorder::local_latencies;
thread_local std::vector<int> LatencyRecorder::local_restart_counts;
thread_local std::vector<int> LatencyRecorder::local_thread_ids;
thread_local int LatencyRecorder::current_thread_id;

LatencyRecorder recorder;

void preload_worker(OneDimMemIndex<int, int>* index, int start_idx, int end_idx, int* data) {
  for (int j = start_idx; j < end_idx; j++) {
    index->Insert(data[j], data[j]);
  }
}

void thread_worker(OneDimMemIndex<int, int>* index, int start_idx, int end_idx,
                  int* data, int data_size, int bulk_load_size, double write_ratio,
                  int thread_id) {
  recorder.set_thread_id(thread_id);
  std::mt19937 rng(thread_id);
  std::uniform_real_distribution<double> op_dist(0.0, 1.0);
  std::vector<int> thread_inserted_keys; // For efficient random selection
  // Pre-build vector of preloaded keys for efficient lookup selection
  std::vector<int> preloaded_keys(data, data + bulk_load_size);
  int inserted_count = start_idx;
  
  while (inserted_count < end_idx) {
    bool do_write = op_dist(rng) <= write_ratio;
    int key;
    long long latency = 0;
    int restart_count = -1;
    if (do_write) {
      key = data[inserted_count];
      inserted_count++;
      auto op_start = std::chrono::high_resolution_clock::now();
      restart_count = index->Insert(key, key);
      auto op_end = std::chrono::high_resolution_clock::now();
      latency = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
      recorder.record(latency, restart_count);
      thread_inserted_keys.push_back(key);
    } else {
      // For lookups, choose from preloaded keys OR keys inserted by this thread
      int total_available = bulk_load_size + thread_inserted_keys.size();
      if (total_available == 0) {
        // Fallback: should not happen if bulk_load_size > 0
        fprintf(stderr, "Error: total_available is 0\n");
        key = data[0];
      } else {
        int choice = rng() % total_available;
        if (choice < bulk_load_size) {
          // Choose from preloaded keys
          key = preloaded_keys[choice];
        } else {
          // Choose from thread-inserted keys
          key = thread_inserted_keys[choice - bulk_load_size];
        }
      }
      int value = 0;
      auto op_start = std::chrono::high_resolution_clock::now();
      index->Lookup(key, value);
      auto op_end = std::chrono::high_resolution_clock::now();
      latency = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
      recorder.record(latency, -1);
    }
  }
  recorder.merge(); // merge thread-local data at the end
}
  
int main(int argc, char** argv) {
  if (argc != 11) {
    fprintf(stderr, 
      "Usage: %s <data_size> <bulk_load_size> <file_name> <distribution> <thread_num> <index> <read_cost(us)> <write_cost(us)> <write_ratio(0-100)> <output_file>\n"
      "\n"
      "Arguments:\n"
      "  data_size      : Number of data items to insert (integer)\n"
      "  bulk_load_size : Number of items to preload in bulk (integer)\n"
      "  file_name      : Path to file containing input data (will be created if not exists)\n"
      "  distribution   : Data distribution type:\n"
      "                     0 - RANDOM (uniform random integers)\n"
      "                     1 - SEQUENTIAL (0, 1, 2, ...)\n"
      "                     2 - ZIPF (Zipfian distribution)\n"
      "  thread_num     : Number of threads to use (integer)\n"
      "  index          : Index type:\n"
      "                     0 - BtreeOLCIndex\n"
      "                     1 - CCBtreeOLCIndex\n"
      "  read_cost(us)   : Read cost in microseconds (double)\n"
      "  write_cost(us)  : Write cost in microseconds (double)\n"
      "  write_ratio    : Percentage of operations that are writes (double)\n"
      "  output_file    : Path to output file (string)\n"
      "\n"
      "Example:\n"
      "  %s 1000000 500000 ../data/seq_1M.txt 1 64 2 100 200 0.8 output.bin\n",
      argv[0], argv[0]);
    return 1;
  }
  int data_size = atoi(argv[1]);
  int bulk_load_size = atoi(argv[2]);
  if (bulk_load_size < 0 || bulk_load_size > data_size) {
    fprintf(stderr, "Error: bulk_load_size must be in [0, data_size], got %d (data_size=%d)\n", bulk_load_size, data_size);
    return 1;
  }
  char* file_name = argv[3];
  DataDistribution dist = static_cast<DataDistribution>(atoi(argv[4]));
  int thread_num = atoi(argv[5]);
  OneDimMemIndex<int, int>* index = get_mem_index(atoi(argv[6]));
  
  double write_ratio = atof(argv[9]);
  if (write_ratio < 0.0 || write_ratio > 1.0) {
    fprintf(stderr, "Error: write_ratio must be in [0,1], got %f\n", write_ratio);
    delete index;
    return 1;
  }
  char* output_file = argv[10];

  int* data = new int[data_size];
  get_input(file_name, data_size, data, dist);

  auto calc_range = [&](int idx, int total_items) -> std::pair<int, int> {
    int64_t start = (static_cast<int64_t>(idx) * total_items) / thread_num;
    int64_t end = (static_cast<int64_t>(idx + 1) * total_items) / thread_num;
    return {static_cast<int>(start), static_cast<int>(end)};
  };

  // Preload the index using 8 threads
  fprintf(stdout, "Preloading index using %d threads\n", 8);
  std::vector<std::thread> preload_threads;
  preload_threads.reserve(8);
  for (int i = 0; i < 8; i++) {
    auto range = calc_range(i, bulk_load_size);
    int start_idx = range.first;
    int end_idx = range.second;
    preload_threads.emplace_back(preload_worker, index, start_idx, end_idx, data);
  }
  for (auto& thread : preload_threads) thread.join();
  preload_threads.clear();
  fprintf(stdout, "Preloading index done\n");

  index->EnableManualCost(atof(argv[7]), atof(argv[8])); // microseconds
  std::vector<std::thread> threads;
  threads.reserve(thread_num);

  // Mixed read/write workload according to write_ratio.
  for (int i = 0; i < thread_num; i++) {
    auto range = calc_range(i, data_size);
    int start_idx = range.first;
    int end_idx = range.second;
    threads.emplace_back(thread_worker, index, start_idx, end_idx, data, data_size, bulk_load_size, write_ratio, i);
  }
  for (auto& thread : threads) thread.join(); 
  threads.clear();
  threads.reserve(thread_num);

  auto latencies = recorder.get_all_latencies();
  auto thread_ids = recorder.get_all_thread_ids();
  auto restart_counts = recorder.get_all_restart_counts();
  if (latencies.size() != thread_ids.size() || restart_counts.size() != latencies.size()) {
    fprintf(stderr, "Error: Mismatch in sizes of latencies, thread_ids, and restart_counts\n");
    delete[] data;
    delete index;
    return 1;
  }
  // Write latency and thread_id pairs to binary file
  FILE* out_fp = fopen(output_file, "wb");
  if (out_fp == nullptr) {
    fprintf(stderr, "Error: Failed to open output file %s\n", output_file);
    delete[] data;
    delete index;
    return 1;
  }
  
  fprintf(stdout, "Writing to output file %s %d records\n", output_file, latencies.size());
  fprintf(stdout, "Header: latency_ns, thread_id, restart_count (-1 for lookups)\n");
  for (size_t i = 0; i < latencies.size(); i++) {
    LatencyRecord record;
    record.latency_ns = latencies[i];
    record.thread_id = thread_ids[i];
    record.restart_count = restart_counts[i];
    fwrite(&record, sizeof(LatencyRecord), 1, out_fp);
  }
  fclose(out_fp);
  
  // fprintf(stdout, "Verifying index\n");
  // index->EnableManualCost(0.0, 0.0); // microseconds
  // verify_index(index, data_size, data);
  delete[] data;
  delete index;
}
