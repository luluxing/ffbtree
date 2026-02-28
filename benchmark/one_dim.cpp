#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>
#include <unordered_set>

#include "utility.cpp"

int main(int argc, char** argv) {
  if (argc != 6) {
    fprintf(stderr,
        "Usage: %s <data_size> <batch_size> <file_name> <distribution> <index>\n"
        "\n"
        "Arguments:\n"
        "  data_size      : Number of data items to insert (integer)\n"
        "  batch_size     : Number of insertions per batch (integer)\n"
        "  file_name      : Path to file containing input data (will be created if not exists)\n"
        "  distribution   : Data distribution type:\n"
        "                     0 - RANDOM (uniform random integers)\n"
        "                     1 - SEQUENTIAL (0, 1, 2, ...)\n"
        "  index          : Index type:\n"
        "                     0 - NormalBtreeDiskIndex\n"
        "                     1 - FsplitNBtreeDiskIndex\n"
        "                     2 - BlinkTreeDiskIndex\n"
        "                     3 - CCBtreeDiskIndex\n"
        "\n"
        "Example:\n"
        "  %s 100000 1000 input.txt 0 2\n"
        "    -> Inserts 100k random integers in batches of 1k using 'input.txt' to BlinkTree.\n",
        argv[0], argv[0]);
    return 1;
  }
  // int init_size = 1000000;
  // int insert_size = 1000;
  int batch_size = atoi(argv[2]);
  int data_size = atoi(argv[1]);
  char* file_name = argv[3];
  DataDistribution dist = static_cast<DataDistribution>(atoi(argv[4]));
  OneDimIndex<int, int>* index = get_index(atoi(argv[5]), 1);

  // BtreeDiskIndex<int, int> index;
  // BLinktreeDiskIndex<int, int> index;
  int* data = new int[data_size];
  get_input(file_name, data_size, data, dist);

  // Insert data into the index
  int i = 0;
  while (i < data_size) {
    int io_num = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < batch_size; j++) {
      io_num += index->Insert(data[i], data[i]);
      i++;
      if (i >= data_size) {
        break;
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<duration_type>(end - start).count();
    fprintf(stdout, "%ld %s %d %d\n", elapsed, time_unit, io_num, index->GetHeight());
  }
  fprintf(stdout, "Leaf utilization: %.2f\n", index->GetLeafUtil());
  fprintf(stdout, "Inner utilization: %.4f\n", index->GetInnerUtil());

  verify_index(index, data_size, data);
  delete[] data;
  delete index;
  
  return 0;
}