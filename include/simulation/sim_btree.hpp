#pragma once

#include <cassert>
#include <vector>
#include <queue>
#include <stack>
#include <map>

#include "simulation/sim_nodes.hpp"
#include "simulation/split_strategy.hpp"
// #include "simulation/dataset_builder.hpp"

/*
  SimBTree is a simulation of btree data structure. It does not store any data,
  but to maintain a valid B-tree structure. It is used to record the node utilization
  of the B-tree, especially during node splits, and to simulate the B-tree operations.
*/
class SimBTree {
 public:
  int pro_split_num;
  int inner_split_num;
  int leaf_split_num;
  int zero_split_num;
  // int insert_io_num;

  std::map<int, int> split_map_num;

  SimBTree(int capacity, int height=1, SplitType split_type=SplitType::Normal)
  : node_cap_(capacity),
    split_type_(split_type) {
    pro_split_num = 0;
    inner_split_num = 0;
    leaf_split_num = 0;
    zero_split_num = 0;
    split_map_num.clear();
    split_map_num[1] = 0;
    last_io_stat_ = {0, 0};
    switch (split_type) {
      case SplitType::Normal:
        split_strategy_ = new NormalSplit();
        break;
      case SplitType::FSplitN:
        split_strategy_ = new FSplitN();
        break;
      case SplitType::BFSplit:
        split_strategy_ = new BFSplit();
        break;
      // case SplitType::BlinkSplit:
      //   split_strategy_ = new BlinkSplit();
      //   break;
      default:
        fprintf(stderr, "Invalid split type\n");
        std::exit(1);
    }
    if (height > 1) {
      fprintf(stdout, "Caution! The tree is not empty initially\n");
      std::abort();
    } else {
      root_ = new SimLeafNode(node_cap_);
      first_leaf_ = static_cast<SimLeafNode*>(root_);
    }
  }

  ~SimBTree() {
    std::queue<SimNode*> queue;
    queue.push(root_);
    while (!queue.empty()) {
      SimNode* node = queue.front();
      queue.pop();
      if (node->type_ == NodeType::Inner) {
        SimInternal* inner = static_cast<SimInternal*>(node);
        for (int i = 0; i < inner->children_.size(); i++) {
          queue.push(inner->children_[i]);
        }
      }
      delete node;
    }
  }

  void EnablePrintIOStat() {
    enable_print_io_stat_ = true;
  }

  void InsertDataItem(Data& data) {
    int rank = data.rank;
    int num = data.count;
    for (int i = 0; i < num; i++) {
      bool restart = false;
      do {
        SimLeafNode* leaf = FindLeafNode(rank);
        Path path = TraverseUp(leaf); // Path starts from the leaf node to the root
        // restart = false;
        int hinc = split_strategy_->traverseAndInsert(path, restart);
        if (hinc > 0) {
          root_ = path.nodes[path.nodes.size() - 1];
        }
      } while (restart);
      if (enable_print_io_stat_) {
        int total_io = split_strategy_->getTotalRWset();
        split_strategy_->resetIOStat();
        if (total_io == last_io_stat_.first) {
          last_io_stat_.second++;
        } else {
          fprintf(stdout, "%d %d %d\n", last_io_stat_.first, last_io_stat_.second, root_->level_);
          last_io_stat_ = {total_io, 1};
        }
      }
    }
  }

  // The ith element of the dataset indicates the number of data items
  // that need to be inserted to the ith rank of the dataset
  void InsertDataset(std::vector<Data>& dataset) {
    for (int i = 0; i < dataset.size(); i++) {
      InsertDataItem(dataset[i]);
    }
    if (enable_print_io_stat_) {
      fprintf(stdout, "%d %d %d\n", last_io_stat_.first, last_io_stat_.second, root_->level_);
    }
  }

  // Generate a rightmost path that contains filled nodes
  int GenFullNode(int num, Data& operation) {
    // Traverse the rightmost path and find the last non-full node
    SimNode* last_non_full = findLastNonFull();
    if (last_non_full == nullptr) {
      // Find the rightmost leaf and insert one data item
      SimLeafNode* leaf = findRightmostLeaf();
      int leaf_item_rank = findLeafItemRank(leaf);
      operation = {leaf_item_rank, 1};
      return 1;
    }
    // If the node is leaf, fill the node
    if (last_non_full->type_ == NodeType::Leaf) {
      SimLeafNode* leaf = static_cast<SimLeafNode*>(last_non_full);
      int fill_num = (leaf->capacity_ - leaf->count_) > num ?
                        num : (leaf->capacity_ - leaf->count_);
      int leaf_item_rank = findLeafItemRank(leaf);
      operation = {leaf_item_rank, fill_num};
      return fill_num;
    }
    // If the node is nonleaf, find its leftmost leaf child and fill it
    SimInternal* inner = static_cast<SimInternal*>(last_non_full);
    SimLeafNode* leftmost_leaf = findLeftmostLeaf(inner);
    int fill_num = 1;
    int leaf_item_rank = findLeafItemRank(leftmost_leaf);
    operation = {leaf_item_rank, fill_num};
    return fill_num;
  }

  int GetHeight() {
    return root_->level_;
  }

  int GetLeafNodeCount(int leaf_rank) {
    SimLeafNode* leaf = first_leaf_;
    int rank = 0;
    while (leaf != nullptr) {
      if (rank == leaf_rank) {
        return leaf->count_;
      }
      rank++;
      leaf = static_cast<SimLeafNode*>(leaf->right_);
    }
    return -1;
  }

  int GetInnerNodeCount(int inner_rank, int inner_level) {
    if (root_->level_ < inner_level) {
      return -1;
    }
    SimNode* node = root_;
    int level = node->level_;
    while (level > inner_level) {
      node = static_cast<SimInternal*>(node)->children_[0];
      level = node->level_;
    }
    int rank = 0;
    while (node != nullptr) {
      if (rank == inner_rank) {
        return static_cast<SimInternal*>(node)->children_.size();
      }
      rank++;
      node = static_cast<SimInternal*>(node)->right_;
    }
    return -1;
  }

 private:
  bool enable_print_io_stat_ = false;
  int node_cap_;
  SimNode* root_;
  SimLeafNode* first_leaf_;
  SplitType split_type_;
  SplitStrategy* split_strategy_;
  std::pair<int, int> last_io_stat_;

  SimLeafNode* FindLeafNode(int rank) {
    SimLeafNode* leaf = first_leaf_;
    while (leaf != nullptr) {
      if (leaf->count_ >= rank) {
        return leaf;
      }
      rank -= leaf->count_;
      leaf = static_cast<SimLeafNode*>(leaf->right_);
    }
    fprintf(stderr, "Error: Leaf node not found\n");
    return nullptr;
  }

  Path TraverseUp(SimLeafNode* leaf) {
    Path path;
    path.AddNode(leaf);
    SimNode* node = leaf;
    SimInternal* inner;
    while (node->parent_ != nullptr) {
      inner = static_cast<SimInternal*>(node->parent_);
      path.AddNode(inner);
      node = inner;
    }
    return path;
  }

  SimNode* findLastNonFull() {
    SimNode* node = root_;
    SimNode* last_non_full = nullptr;
    while (true) {
      if (node->type_ == NodeType::Leaf) {
        SimLeafNode* leaf = static_cast<SimLeafNode*>(node);
        if (!leaf->isFull()) {
          last_non_full = node;
        }
        break;
      } else {
        SimInternal* inner = static_cast<SimInternal*>(node);
        if (!inner->isFull()) {
          last_non_full = node;
        }
        node = inner->children_[inner->children_.size() - 1];
      }
      
    }
    return last_non_full;
  }

  int findLeafItemRank(SimLeafNode* leaf) {
    int item_rank = 0;
    SimLeafNode* node = first_leaf_;
    while (node != nullptr && node != leaf) {
      item_rank += node->count_;
      node = static_cast<SimLeafNode*>(node->right_);
    }
    if (node == nullptr) {
      fprintf(stderr, "Error: Found null leaf node\n");
      std::abort();
    }
    return item_rank == 0 ? 0 : item_rank + 1;
  }

  SimLeafNode* findLeftmostLeaf(SimInternal* inner) {
    SimNode* node = inner->children_[0];
    while (node->type_ == NodeType::Inner) {
      node = static_cast<SimInternal*>(node)->children_[0];
    }
    SimLeafNode* leftmost_leaf = static_cast<SimLeafNode*>(node);
    return leftmost_leaf;
  }

  SimLeafNode* findRightmostLeaf() {
    SimLeafNode* leaf = first_leaf_;
    while (leaf->right_ != nullptr) {
      leaf = static_cast<SimLeafNode*>(leaf->right_);
    }
    return leaf;
  }

};