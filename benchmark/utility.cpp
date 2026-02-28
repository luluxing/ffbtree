#include <chrono>
#include <cstdio>
#include <unordered_set>
#include <limits>

#include "indexes.hpp"

using duration_type = std::chrono::nanoseconds; //std::chrono::microseconds;
const char* time_unit = "ns"; //"us";

enum class DataDistribution {
  RANDOM = 0,
  SEQUENTIAL = 1
};

OneDimIndex<int, int>* get_index(int index_type, int cache_size) {
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

void gen_data(int data_size, int* data,
               DataDistribution dist=DataDistribution::RANDOM) {
  srand(9);
  if (dist == DataDistribution::SEQUENTIAL) {
    for (int i = 0; i < data_size; i++) {
      data[i] = i;
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
    write_data(file_name, data_size, data);
    return;
  }
  int line_count = count_lines(file_name);
  if (line_count < data_size) {
    // Erase the lines in the file and generate new data
    fclose(fp);
    gen_data(data_size, data, dist);
    write_data(file_name, data_size, data);
  } else {
    for (int i = 0; i < data_size; i++) {
      fscanf(fp, "%d", &data[i]);
    }
    fclose(fp);
  }
}

void verify_index(OneDimIndex<int, int>* index, int data_size, int* data) {
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
