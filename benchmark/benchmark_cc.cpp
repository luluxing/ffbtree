#include <chrono>
#include <vector>
#include <thread>
#include <mutex>
#include <utility>
#include <cstdio>
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

void thread_worker(OneDimMemIndex<int, int>* index, int start_idx, int end_idx, int* data, int thread_id) {
  recorder.set_thread_id(thread_id);
  for (int j = start_idx; j < end_idx; j++) {
    auto insert_start = std::chrono::high_resolution_clock::now();
    int restart_count = index->Insert(data[j], data[j]);
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end - insert_start).count();
    recorder.record(latency, restart_count);
  }
  recorder.merge(); // merge thread-local data at the end
}
  
int main(int argc, char** argv) {
  if (argc != 9) {
    fprintf(stderr, 
      "Usage: %s <data_size> <file_name> <distribution> <thread_num> <index> <read_cost(us)> <write_cost(us)> <output_file>\n"
      "\n"
      "Arguments:\n"
      "  data_size      : Number of data items to insert (integer)\n"
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
      "  output_file    : Path to output file (string)\n"
      "\n"
      "Example:\n"
      "  %s 1000000 data/seq_1M.txt 0 1 2 0.1 0.3 output.bin\n",
      argv[0], argv[0]);
    return 1;
  }
  int data_size = atoi(argv[1]);
  char* file_name = argv[2];
  DataDistribution dist = static_cast<DataDistribution>(atoi(argv[3]));
  int thread_num = atoi(argv[4]);
  OneDimMemIndex<int, int>* index = get_mem_index(atoi(argv[5]));
  index->EnableManualCost(atof(argv[6]), atof(argv[7])); // microseconds
  char* output_file = argv[8];

  int* data = new int[data_size];
  get_input(file_name, data_size, data, dist);

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

  auto calc_range = [&](int idx) -> std::pair<int, int> {
    int64_t start = (static_cast<int64_t>(idx) * data_size) / thread_num;
    int64_t end = (static_cast<int64_t>(idx + 1) * data_size) / thread_num;
    return {static_cast<int>(start), static_cast<int>(end)};
  };

  for (int i = 0; i < thread_num; i++) {
    auto range = calc_range(i);
    int start_idx = range.first;
    int end_idx = range.second;
    threads.emplace_back(thread_worker, index, start_idx, end_idx, data, i);
  }
  for (auto& thread : threads) thread.join();
  
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
  
  fprintf(stdout, "Writing to output file %s\n", output_file);
  fprintf(stdout, "Header: latency_ns, thread_id, restart_count\n");
  for (size_t i = 0; i < latencies.size(); i++) {
    LatencyRecord record;
    record.latency_ns = latencies[i];
    record.thread_id = thread_ids[i];
    record.restart_count = restart_counts[i];
    fwrite(&record, sizeof(LatencyRecord), 1, out_fp);
  }
  fclose(out_fp);
  
  // verify_index(index, data_size, data);
  delete[] data;
  delete index;
}
