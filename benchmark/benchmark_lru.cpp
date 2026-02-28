#include <chrono>
#include <vector>
#include "utility.cpp"

void gen_lookups(int data_size, int lookup_size, std::vector<int>& inserted, int* lookups) {
  srand(9);
  for (int i = 0; i < lookup_size; i++) {
    lookups[i] = inserted[rand() % inserted.size()];
  }
}

int main(int argc, char** argv) {
  if (argc != 7) {
    fprintf(stderr, 
      "Usage: %s <data_size> <lookup_size> <file_name> <distribution> <index> <cache_size>\n"
      "\n"
      "Arguments:\n"
      "  data_size      : Number of data items to insert (integer)\n"
      "  lookup_size    : Number of lookups to perform (integer)\n"
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
      "  %s 1000000 1000 data/seq_1M.txt 0 3 100\n",
      argv[0], argv[0]);
    return 1;
  }
  int data_size = atoi(argv[1]);
  int lookup_size = atoi(argv[2]);
  char* file_name = argv[3];
  DataDistribution dist = static_cast<DataDistribution>(atoi(argv[4]));
  OneDimDiskIndex<int, int>* index = get_index(atoi(argv[5]), atoi(argv[6]));

  int* data = new int[data_size];
  get_input(file_name, data_size, data, dist);
  std::vector<int> inserted;
  // Insert data into the index
  int i = 0;
  while (i < data_size) {
    int io_num = 0;
    auto start = std::chrono::high_resolution_clock::now();
    io_num += index->Insert(data[i], data[i]);
    inserted.push_back(data[i]);
    i++;
    if (i >= data_size) {
      break;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto insert_elapsed = std::chrono::duration_cast<duration_type>(end - start).count();
    int* lookups = new int[lookup_size];
    gen_lookups(data_size, lookup_size, inserted, lookups);
    int result = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < lookup_size; j++) {
      index->Lookup(lookups[j], result);
    }
    end = std::chrono::high_resolution_clock::now();
    auto lookup_elapsed = std::chrono::duration_cast<duration_type>(end - start).count();
    fprintf(stdout, "Insert-time=%ld, lookup-time=%ld, insert-io=%d, height=%d\n",
            insert_elapsed, lookup_elapsed, io_num, index->GetHeight());
  }
  
  verify_index(index, data_size, data);
  delete[] data;
  delete index;
}