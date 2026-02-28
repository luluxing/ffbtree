#include <chrono>
#include <cstdio>
#include <unordered_set>
#include <limits>

#include "indexes.hpp"
#include "zipf_generator.hpp"

using duration_type = std::chrono::nanoseconds; //std::chrono::microseconds;
const char* time_unit = "ns"; //"us";

enum class DataDistribution {
  RANDOM = 0,
  SEQUENTIAL = 1,
  ZIPFIAN = 2
};

OneDimDiskIndex<int, int>* get_index(int index_type, int cache_size) {
  switch (index_type) {
    case 0:
      return new NormalBtreeDiskIndex<int, int>(cache_size);
    case 1:
      return new FsplitNBtreeDiskIndex<int, int>(cache_size);
    case 2:
      return new BLinktreeDiskIndex<int, int>();
    case 3:
      return new CCBtreeDiskIndex<int, int>(cache_size);
    default:
      fprintf(stderr, "Invalid index type: %d\n", index_type);
      return nullptr;
  }
}

OneDimMemIndex<int, int>* get_mem_index(int index_type) {
  switch (index_type) {
    case 0:
      return new BtreeOLCIndex<int, int>();
    case 1:
      return new CCBtreeOLCIndex<int, int>();
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
  FILE* fp = fopen(file_name, "r");

  if (fp == NULL) {
    gen_data(data_size, data, dist);
    write_data_binary(file_name, data_size, data);
    return;
  }
  // int line_count = count_lines(file_name);
  fseek(fp, 0, SEEK_END);
  long file_size = ftell(fp);
  long expected_size = data_size * sizeof(int);
  fseek(fp, 0, SEEK_SET);
  if (file_size < expected_size) {
    // Erase the lines in the file and generate new data
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

template <typename IndexType>
void verify_index(IndexType* index, int data_size, int* data) {
  int result = 0;
  for (int i = 0; i < data_size; i++) {
    if (!index->Lookup(data[i], result)) {
      fprintf(stderr, "Lookup failed for key %d\n", data[i]);
      return;
    }
    if (result != data[i]) {
      fprintf(stderr, "Lookup returned incorrect value for key %d: expected %d, got %d\n",
              data[i], data[i], result);
      return;
    }
  }
  fprintf(stdout, "All lookups succeeded.\n");
}
