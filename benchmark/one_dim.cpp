#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>
#include <vector>
#include <unordered_set>

#include "indexes.hpp"
#include "zipf_generator.hpp"

using duration_type = std::chrono::nanoseconds; 
const char* time_unit = "ns";

struct LatencyRecord {
  long long latency_ns;
  int io_num;
  int height;
};

enum class DataDistribution {
  RANDOM = 0,
  SEQUENTIAL = 1,
  ZIPFIAN = 2
};

OneDimIndex<int, int>* get_index(int index_type, std::string params,
                          int read_latency, int write_latency) {
  switch (index_type) {
    case 0:
      return new NormalBtreeDiskIndex<int, int>(params, read_latency, write_latency);
    case 1:
      return new FsplitNBtreeDiskIndex<int, int>(params, read_latency, write_latency);
    case 2:
      return new BLinktreeDiskIndex<int, int>();
    case 3:
      return new CCBtreeDiskIndex<int, int>(params, read_latency, write_latency);
    default:
      fprintf(stderr, "Invalid index type: %d\n", index_type);
      return nullptr;
  }
}

void gen_data(int data_size, int* data,
               DataDistribution dist=DataDistribution::RANDOM) {
  srand(9);
  if (dist == DataDistribution::SEQUENTIAL) {
    for (int i = 0; i < data_size; i++) {
      data[i] = i;
    }
  } else if (dist == DataDistribution::ZIPFIAN) {
    // Generate Zipfian distribution data
    // Zipfian distribution: probability of rank i is proportional to 1/(i+1)^s
    const double zipf_skew = 0.8;
    const int domain_size = data_size * 2; // Use a larger domain to ensure uniqueness
    
    ZipfianGenerator zipf(0, domain_size);
    for (int i = 0; i < data_size; i++) {
      data[i] = zipf.Next();
    }
  } else {
    std::unordered_set<int> used;
    int min_val = std::numeric_limits<int>::min();
    int max_val = std::numeric_limits<int>::max();
    for (int i = 0; i < data_size; ) {
      int num = rand() % max_val;
      if (used.insert(num).second) {
        data[i++] = num;
        // fprintf(stderr, "%d %d", i, num);
      }
      // fprintf(stderr, "%d %d", i, num);
    }
  }
}

void write_data(const char* file_name, int data_size, int* data) {
  FILE* fp = fopen(file_name, "w");
  if (fp == NULL) {
    fprintf(stderr, "Error opening file for writing: %s\n", file_name);
    return;
  }
  for (int i = 0; i < data_size; i++) {
    fprintf(fp, "%d\n", data[i]);
  }
  fclose(fp);
}

void write_data_binary(const char* file_name, int data_size, int* data) {
  FILE* fp = fopen(file_name, "wb");
  if (fp == NULL) {
    fprintf(stderr, "Error opening file for writing: %s\n", file_name);
    return;
  }
  fwrite(data, sizeof(int), data_size, fp);
  fclose(fp);
}

int count_lines(const char* file_name) {
    FILE* fp = fopen(file_name, "r");
    if (!fp) return -1;
    int count = 0;
    char buf[4096];
    while (fgets(buf, sizeof(buf), fp)) {
        count++;
    }
    fclose(fp);
    return count;
}

void get_input(char* file_name, int data_size, int* data,
               DataDistribution dist=DataDistribution::RANDOM) {
  FILE* fp = fopen(file_name, "rb");

  if (fp == NULL) {
    gen_data(data_size, data, dist);
    write_data_binary(file_name, data_size, data);
    return;
  }
  // Check file size for binary files
  fseek(fp, 0, SEEK_END);
  long file_size = ftell(fp);
  long expected_size = data_size * sizeof(int);
  fseek(fp, 0, SEEK_SET);
  
  if (file_size < expected_size) {
    // File doesn't have enough data, regenerate
    fclose(fp);
    gen_data(data_size, data, dist);
    write_data_binary(file_name, data_size, data);
  } else {
    // Read binary data
    size_t items_read = fread(data, sizeof(int), data_size, fp);
    if (items_read != static_cast<size_t>(data_size)) {
      fprintf(stderr, "Warning: Expected %d items, but read %zu items\n", 
              data_size, items_read);
    }
    fclose(fp);
  }
}


int main(int argc, char** argv) {
  if (argc != 8) {
    fprintf(stderr,
        "Usage: %s <data_size> <file_name> <distribution> <index> <read_latency> <write_latency> <output_file>\n"
        "\n"
        "Arguments:\n"
        "  data_size      : Number of data items to insert (integer)\n"
        "  file_name      : Path to file containing input data (will be created if not exists)\n"
        "  distribution   : Data distribution type:\n"
        "                     0 - RANDOM (uniform random integers)\n"
        "                     1 - SEQUENTIAL (0, 1, 2, ...)\n"
        "                     2 - ZIPFIAN (Zipfian distribution = 0.8)\n"
        "  index          : Index type:\n"
        "                     0 - NormalBtreeDiskIndex\n"
        "                     1 - FsplitNBtreeDiskIndex\n"
        "                     2 - BlinkTreeDiskIndex\n"
        "                     3 - CCBtreeDiskIndex\n"
        "  read_latency   : Read latency in microseconds (int)\n"
        "  write_latency  : Write latency in microseconds (int)\n"
        "  output_file    : Path to output file for latency records (binary)\n"
        "\n"
        "Example:\n"
        "  %s 100000 input.txt 0 2 100 200 output.bin\n"
        "    -> Inserts 100k random integers using 'input.txt' to BlinkTree.\n",
        argv[0], argv[0]);
    return 1;
  }
  // int init_size = 1000000;
  // int insert_size = 1000;
  int data_size = atoi(argv[1]);
  char* file_name = argv[2];
  DataDistribution dist = static_cast<DataDistribution>(atoi(argv[3]));
  int index_type = atoi(argv[4]);
  int read_latency = atoi(argv[5]);
  int write_latency = atoi(argv[6]);
  char* output_file = argv[7];
  // Concatenate the parameters
  std::string params = std::string(argv[1]) + "_" + std::string(argv[3]) + "_" + std::string(argv[4]) + "_" + std::string(output_file) + ".db";
  OneDimIndex<int, int>* index = get_index(index_type, params, read_latency, write_latency);

  int* data = new int[data_size];
  get_input(file_name, data_size, data, dist);

  // Insert data into the index
  int i = 0;
  std::vector<LatencyRecord> records;
  while (i < data_size) {
    struct LatencyRecord record;
    auto start = std::chrono::high_resolution_clock::now();
    record.io_num = index->Insert(data[i], data[i]);
    auto end = std::chrono::high_resolution_clock::now();
    i++;
    auto elapsed = std::chrono::duration_cast<duration_type>(end - start).count();
    record.height = index->GetHeight();
    record.latency_ns = elapsed;
    records.push_back(record);
  }
  // Write latency and thread_id pairs to binary file
  FILE* out_fp = fopen(output_file, "wb");
  if (out_fp == nullptr) {
    fprintf(stderr, "Error: Failed to open output file %s\n", output_file);
    delete[] data;
    delete index;
    return 1;
  }
  fprintf(stdout, "Writing to output file %s %d records\n", output_file, records.size());
  fprintf(stdout, "Header: latency_ns, io_num, height\n");
  for (size_t i = 0; i < records.size(); i++) {
    fwrite(&records[i], sizeof(LatencyRecord), 1, out_fp);
  }
  fclose(out_fp);
  fprintf(stdout, "Leaf utilization: %.2f\n", index->GetLeafUtil());
  fprintf(stdout, "Inner utilization: %.2f\n", index->GetInnerUtil());


  // Verify the correctness of the index
  // int result = 0;
  // for (int i = 0; i < data_size; i++) {
  //   if (!index->Lookup(data[i], result)) {
  //     fprintf(stderr, "Lookup failed for key %d\n", data[i]);
  //     delete[] data;
  //     delete index;
  //     return 1;
  //   }
  //   if (result != data[i]) {
  //     fprintf(stderr, "Lookup returned incorrect value for key %d: expected %d, got %d\n",
  //             data[i], data[i], result);
  //     delete[] data;
  //     delete index;
  //     return 1;
  //   }
  // }
  // fprintf(stdout, "All lookups succeeded.\n");
  delete[] data;
  delete index;
  
  return 0;
}