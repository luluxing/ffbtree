#include <iostream>
#include <vector>
#include <queue>
#include <stack>
#include "simulation/sim_btree.hpp"
#include "simulation/dataset_builder.hpp"

const int NODE_SIZE = 8;

void gen_normal_illcase(int insert_num, std::vector<Data>& dataset) {
  DatasetBuilder dataset_builder(NODE_SIZE);
  dataset_builder.GenDataset(insert_num, dataset, DatasetType::ASCENDING);
}

void gen_fsplit1_illcase(int insert_num, std::vector<Data>& dataset) {
  DatasetBuilder dataset_builder(NODE_SIZE);
  dataset_builder.GenDataset(insert_num, dataset, DatasetType::ILLPROACTIVE);
}

void gen_random(int insert_num, std::vector<Data>& dataset) {
  DatasetBuilder dataset_builder(NODE_SIZE);
  dataset_builder.GenDataset(insert_num, dataset, DatasetType::RANDOM);
}

void run_indexes(std::vector<Data>& dataset) {
  SimBTree sim_btree(NODE_SIZE);
  fprintf(stdout, "=====Normal B+-tree\n");
  sim_btree.EnablePrintIOStat();
  sim_btree.InsertDataset(dataset);

  SimBTree sim_btree2(NODE_SIZE, 1, SplitType::FSplitN);
  fprintf(stdout, "=====FSplitN\n");
  sim_btree2.EnablePrintIOStat();
  sim_btree2.InsertDataset(dataset);

  SimBTree sim_btree4(NODE_SIZE, 1, SplitType::BFSplit);
  fprintf(stdout, "=====BFSplit\n");
  sim_btree4.EnablePrintIOStat();
  sim_btree4.InsertDataset(dataset);
}

int main(int argc, char** argv) {
  if (argc != 3) {
    fprintf(stderr,
      "Usage: %s <insert_num> <case:sequential/illproactive/random>\n",
      argv[0]);
    return 1;
  }
  
  std::vector<Data> dataset;
  fprintf(stdout,
    "=====Benchmarking %s case. Insert number: %d.\n",
    argv[2], std::stoi(argv[1]));
  if (std::string(argv[2]) == "sequential") {
    gen_normal_illcase(std::stoi(argv[1]), dataset);
  } else if (std::string(argv[2]) == "illproactive") {
    gen_fsplit1_illcase(std::stoi(argv[1]), dataset);
  } else if (std::string(argv[2]) == "random") {
    gen_random(std::stoi(argv[1]), dataset);
  } else {
    fprintf(stderr, "Invalid ill case type\n");
    return 1;
  }
  run_indexes(dataset);

  return 0;
}
