#pragma once

#include <vector>
#include <cstdlib>
#include "simulation/sim_nodes.hpp"
#include "simulation/split_strategy.hpp"
#include "simulation/sim_btree.hpp"

enum DatasetType {
  ASCENDING,
  DESCENDING,
  RANDOM,
  ILLPROACTIVE
};

int SumInsertion(std::vector<Data>& dataset) {
  int sum = 0;
  for (int i = 0; i < dataset.size(); i++) {
    sum += dataset[i].count;
  }
  return sum;
}

class SimBTree;

class DatasetBuilder {
 public:
  DatasetBuilder(int node_capacity) : node_cap_(node_capacity) {}

  void InsertDataset(const std::vector<Data>& dataset) {
    // This method would typically insert the dataset into a tree structure
    // For now, it's a placeholder that does nothing
    // The actual implementation would depend on the specific use case
  }
  

  void GenDataset(int num, std::vector<Data>& dataset, DatasetType dataset_type) {
    switch (dataset_type) {
      case ASCENDING:
        GenAscendingCase(num, dataset);
        break;
      case DESCENDING:
        GenDescendingCase(num, dataset);
        break;
      case RANDOM:
        GenRandomCase(num, dataset);
        break;
      case ILLPROACTIVE:
        GenIllProactiveCase(num, dataset);
        break;
      default:
        fprintf(stderr, "Invalid dataset type: %d\n", dataset_type);
        std::abort();
    }
  }

 private:
  int node_cap_; // Node capacity for the tree

  void GenRandomCase(int num, std::vector<Data>& dataset) {
    // Set a random seed
    srand(1);
    int inserted = 0;
    do {
      // Pick a random number between 0 and inserted
      int random_num = inserted == 0 ? 0 : rand() % inserted;
      dataset.push_back({random_num, 1});
      inserted++;
    } while (inserted < num);
  }

  void GenAscendingCase(int num, std::vector<Data>& dataset) {
    for (int i = 0; i < num; i++) {
      dataset.push_back({i, 1});
    }
  }

  void GenDescendingCase(int num, std::vector<Data>& dataset) {
    dataset.push_back({0, num});
  }

  void GenIllProactiveCase(int num, std::vector<Data>& dataset) {
    SimBTree sim_btree(node_cap_, 1, SplitType::FSplitN);
    int inserted = 0;
    std::vector<Data> temp_dataset;
    do {
      Data operation;
      inserted += sim_btree.GenFullNode(num - inserted, operation);
      sim_btree.InsertDataItem(operation);
      temp_dataset.push_back(operation);
    } while (inserted < num);
    CombineDataItems(temp_dataset, dataset);
  }

  void CombineDataItems(const std::vector<Data>& src, std::vector<Data>& dst) {
    // Iterate over src and combine the neighboring data items
    // if their leaf locations are the same
    for (int i = 0; i < src.size(); i++) {
      if (i == 0) {
        dst.push_back(src[i]);
        continue;
      }
      if (src[i].rank == src[i-1].rank) {
        dst[dst.size()-1].count += src[i].count;
      } else {
        dst.push_back(src[i]);
      }
    }
  }

  
};