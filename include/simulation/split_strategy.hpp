#pragma once

#include <vector>
#include <queue>
#include <set>
#include <cassert>

#include "simulation/sim_nodes.hpp"

// All the proactive splits are allowed only once per descending path
enum class SplitType : uint8_t {
  Normal=1, // Normal B+-tree split strategy
  // FSplit1=2, // Split top down when the encountered node is full. Only once per path
  FSplitN=3, // Split top down when the encountered node is full. Allow multiple splits
  BFSplit=4, // Split top down when the encountered node is critical. Only once per path
  BlinkSplit=5 // Split like Blink-tree
};

class SplitStrategy {
 public:
  // Single-insert API: default to shared logic; children may override
  virtual int traverseAndInsert(Path& path, bool& restart) {
    return doTraverseAndInsert(path);
  }

 
   virtual ~SplitStrategy() = default;

  int getTotalRWset() {
    return iostat_.read_set_.size() + iostat_.write_set_.size();
  }

  void resetIOStat() {
    iostat_.read_set_.clear();
    iostat_.write_set_.clear();
  }

 protected:
  int doTraverseAndInsert(Path& path) {
    int path_idx = 0;
    SimNode* node = path.nodes[path_idx];
    SimLeafNode* leaf = static_cast<SimLeafNode*>(node);
    if (leaf->isFull()) {
      SimNode* split_node = leaf->SplitWithNewData(1);
      SimNode* prev_node = leaf;
      addWriteSet(split_node);
      addWriteSet(leaf);
      path_idx++;
      while (path_idx < path.nodes.size()) {
        node = path.nodes[path_idx];
        SimInternal* inner = static_cast<SimInternal*>(node);
        if (inner->isFull()) {
          SimNode* new_inner = inner->Split(prev_node, split_node);
          addWriteSet(new_inner);
          addWriteSet(inner);
          path_idx++;
          split_node = new_inner;
          prev_node = node;
        } else {
          inner->AddChild(prev_node, split_node);
          addWriteSet(inner);
          split_node = nullptr;
          break;
        }
      }
      if (split_node != nullptr) {
        // Need to create a new root
        SimInternal* new_root = new SimInternal(node->capacity_);
        new_root->children_.push_back(node);
        new_root->children_.push_back(split_node);
        new_root->level_ = node->level_ + 1;
        node->SetParent(new_root);
        split_node->SetParent(new_root);
        addWriteSet(new_root);
        path.AddNode(new_root);
        return 1;
      }
    } else {
      leaf->count_++;
      addWriteSet(leaf);
    }
    return 0;
  }

  struct IOStat {
    std::set<SimNode*> read_set_;
    std::set<SimNode*> write_set_;
  } iostat_;

  void addReadSet(SimNode* node) {
    iostat_.read_set_.insert(node);
  }

  void addWriteSet(SimNode* node) {
    iostat_.write_set_.insert(node);
  }

};
 
 // Strategy 1: Classic B+-tree Splitting
class NormalSplit : public SplitStrategy {
 public:

  int traverseAndInsert(Path& path, bool& restart) override {
    beforeInsert(path);
    return SplitStrategy::doTraverseAndInsert(path);
  }

 private:
  void beforeInsert(Path& path) {
    for (int i = 0; i < path.size(); i++) {
      addReadSet(path.nodes[i]);
    }
    return;
  }
 
 };
 
class FSplitN : public SplitStrategy {
 public:
  int traverseAndInsert(Path& path, bool& restart) override {
    int height_inc = 0;
    // This restart is not required in CLRS
    if (restart) {
      // If already restarted, do nothing
      restart = false;
    } else {
      height_inc = beforeInsert(path, restart);
    }
    if (restart) return height_inc;
    return SplitStrategy::doTraverseAndInsert(path);
  }

 private:
  // In CLRS, the insertion is done in one pass
  int beforeInsert(Path& path, bool& restart) {
    // Scan from the rightmost node to the leftmost node and split when necessary
    int path_idx = path.size() - 1;
    int height_inc = 0;
    SimNode* new_root = nullptr;
    bool split_happened = false;
    while (path_idx >= 0) {
      addReadSet(path.nodes[path_idx]);
      SimNode* node = path.nodes[path_idx];
      if (node->type_ == NodeType::Leaf) {
        SimLeafNode* leaf = static_cast<SimLeafNode*>(node);
        if (leaf->isFull()) {
          leaf->SplitWithNewData(0);
          addWriteSet(leaf);
          addWriteSet(leaf->right_);
          split_happened = true;
          if (path_idx >= path.size() - 1) {
            new_root = makeNewRoot(leaf, leaf->right_);
            height_inc++;
          } else {
            SimInternal* parent = static_cast<SimInternal*>(leaf->parent_);
            parent->AddChild(leaf, leaf->right_);
            addWriteSet(parent);
          }
          // break;
        }
      } else {
        SimInternal* inner = static_cast<SimInternal*>(node);
        if (inner->isFull()) {
          inner->ProactiveSplit();
          addWriteSet(inner);
          addWriteSet(inner->right_);
          split_happened = true;
          if (path_idx >= path.size() - 1) {
            // Make a new root
            new_root = makeNewRoot(inner, inner->right_);
            height_inc++;
          } else {
            // Add the new inner node to the parent
            SimInternal* parent = static_cast<SimInternal*>(inner->parent_);
            parent->AddChild(inner, inner->right_);
            addWriteSet(parent);
          }
          // break;
        }
      }
      path_idx--;
    }
    if (new_root != nullptr) {
      path.AddNode(new_root);
    }
    restart = split_happened;
    return height_inc;
  }
  
  SimNode* makeNewRoot(SimNode* left, SimNode* right) {
    SimInternal* new_root = new SimInternal(left->capacity_);
    new_root->children_.push_back(left);
    new_root->children_.push_back(right);
    new_root->level_ = left->level_ + 1;
    left->SetParent(new_root);
    right->SetParent(new_root);
    addWriteSet(new_root);
    return new_root;
  }

 };
 
 
class BFSplit : public SplitStrategy {
 public:

  int traverseAndInsert(Path& path, bool& restart) override {
    int height_inc = 0;
    if (restart) {
      restart = false;
    } else {
      height_inc = beforeInsert(path, restart);
    }
    if (restart) return height_inc;
    return SplitStrategy::doTraverseAndInsert(path);
  }

 private:
  void examine_node(SimNode* node, SimNode* parent,
                   SimNode*& critical, SimNode*& tobe_critical) {

    if (node->is_critical_) {
      return;
    }
    if (node->type_ == NodeType::Leaf) {
      SimLeafNode* leaf = static_cast<SimLeafNode*>(node);
      if (leaf->isFull(1)) {
        tobe_critical = node;
      }
    } else {
      // if (node->maybe_critical_) {
      // Count the critical children and check if the node is critical or not
      int num_cc = count_critical_children(node);
      SimInternal* inner = static_cast<SimInternal*>(node);
      if (inner->children_.size() >= inner->capacity_ - num_cc) {
        critical = node;
        tobe_critical = node;
      }
      // }
    }
  }

  // In the real implementation, this is done using a bitmap
  int count_critical_children(SimNode* node) {
    if (node->type_ == NodeType::Leaf) {
      return 0;
    }
    SimInternal* inner = static_cast<SimInternal*>(node);
    int num_cc = 0;
    for (int i = 0; i < inner->children_.size(); i++) {
      if (inner->children_[i]->is_critical_) {
        num_cc++;
      }
    }
    return num_cc;
  }

  void set_critical(SimNode* tobe_critical) {
    if (tobe_critical == nullptr) {
      return;
    }
    SimNode* node = tobe_critical;
    node->is_critical_ = true;
    addWriteSet(tobe_critical);
    // node->maybe_critical_ = false;
    
    SimNode* parent = tobe_critical->parent_;
    if (parent == nullptr) {
      return;
    }
    // Set parent bitmap to reflect child criticality
    // if (tobe_critical->type_ == NodeType::Leaf) {
    //   addPreInsIO(1);
    // } else {
    //   addPreInsIO(2);
    // }
    addWriteSet(parent);
  }
 
  int beforeInsert(Path& path, bool& restart) {
    // Parent of current node
    SimNode* parent_node = nullptr;
    SimNode* node = nullptr;
    SimNode* last_critical = nullptr;
    SimNode* last_tobe_critical = nullptr;
    int path_idx = path.size() - 1;
    while (path_idx >= 0) {
      addReadSet(path.nodes[path_idx]);
      node = path.nodes[path_idx];
      if (node->is_critical_) {
        last_critical = node;
      }
      examine_node(node, parent_node, last_critical, last_tobe_critical);
      parent_node = node;
      path_idx--;
    }
    set_critical(last_tobe_critical);
    
    SimNode* rt = proactive_split(last_critical, restart);
    if (rt != nullptr) {
      path.AddNode(rt);
      return 1;
    }
    return 0;
  }

  SimNode* proactive_split(SimNode* critical, bool& restart) {
    if (critical == nullptr) {
      restart = false;
      return nullptr;
    }
    SimNode* new_root = nullptr;
    SimNode* node = critical;
    if (node->type_ == NodeType::Leaf) {
      SimLeafNode* leaf = static_cast<SimLeafNode*>(node);
      leaf->SplitWithNewData(0);
      leaf->is_critical_ = false;
      addWriteSet(leaf);
      addWriteSet(leaf->right_);
    } else {
      SimInternal* inner = static_cast<SimInternal*>(node);
      inner->ProactiveSplit();
      inner->is_critical_ = false;
      addWriteSet(inner);
      addWriteSet(inner->right_);
    }
    if (node->parent_ == nullptr) {
      // Create a new root
      new_root = makeNewRoot(node, node->right_);
    } else {
      // Insert the new node into the parent
      SimInternal* parent = static_cast<SimInternal*>(node->parent_);
      parent->AddChild(node, node->right_);
      addWriteSet(parent);
    }
    restart = true;
    return new_root;
  }

  SimNode* makeNewRoot(SimNode* left, SimNode* right) {
    SimInternal* new_root = new SimInternal(left->capacity_);
    new_root->children_.push_back(left);
    new_root->children_.push_back(right);
    new_root->level_ = left->level_ + 1;
    left->SetParent(new_root);
    right->SetParent(new_root);
    addWriteSet(new_root);
    return new_root;
  }
 
  };
 
//  // Strategy 5: Blink-tree Splitting
//  class BlinkSplit : public SplitStrategy {
//   public:
//    int traverseAndInsert(Path& path, int num, int& isplit, int& lsplit, int& io_num) {
//      int height_inc = 0;
//      int num_inserted = 0;
//      while (num_inserted < num) {
//        bool have_splits = false;
//        // Insert to the leaf node
//        SimNode* leaf_node = path.nodes[path.nodes.size() - 1];
//        num_inserted++;
//        if (leaf_node->type_ != NodeType::Leaf) {
//          fprintf(stderr, "Impossible path\n");
//          std::abort();
//        }
//        SimLeafNode* leaf = static_cast<SimLeafNode*>(leaf_node);
//        if (leaf->isFull()) {
//          SimLeafNode* new_leaf = leaf->SplitWithNewData(1, true);
//          lsplit++;
//          if (path.nodes.size() > 1) {
//            SimNode* parent = path.nodes[path.nodes.size() - 2];
//            if (parent->type_ != NodeType::Inner) {
//              fprintf(stderr, "Impossible path\n");
//              std::abort();
//            }
//            SimInternal* inner = static_cast<SimInternal*>(parent);
//            split_map_[inner] = std::make_pair(leaf, new_leaf);
//            io_num += 2; // Write both leaf nodes
//          } else {
//            SimInternal* new_root = new SimInternal(leaf->capacity_);
//            leaf->right_ = nullptr;
//            new_leaf->right_ = nullptr;
//            new_root->children_.push_back(leaf);
//            new_root->children_.push_back(new_leaf);
//            path.PrependNode(new_root);
//            height_inc++;
//            io_num += 3; // Write the new root and two leaf nodes
//          }
//          have_splits = true;
//        } else {
//          leaf->count_++;
//        }
//        if (have_splits) {
//          continue;
//        }
 
//        for (int i = path.nodes.size() - 2; i >= 0; i--) {
//          SimNode* cur = path.nodes[i];
//          if (cur->type_ == NodeType::Leaf) {
//            fprintf(stderr, "Cannot have another leaf node\n");
//            std::abort();
//          }
//          SimInternal* inner = static_cast<SimInternal*>(cur);
//          if (split_map_.find(inner) != split_map_.end()) {
//            if (inner->isFull()) {
//              SimNode* new_inner = inner->Split(split_map_[inner].first,
//                                                split_map_[inner].second, true);
//              isplit++;
//              if (i > 0) {
//                SimNode* parent = path.nodes[i - 1];
//                if (parent->type_ != NodeType::Inner) {
//                  fprintf(stderr, "Impossible path\n");
//                  std::abort();
//                }
//                SimInternal* p = static_cast<SimInternal*>(parent);
//                split_map_[p] = std::make_pair(inner, new_inner);
//                io_num += 2; // Write both inner nodes
//              } else {
//                SimInternal* new_root = new SimInternal(inner->capacity_);
//                inner->right_ = nullptr;
//                new_inner->right_ = nullptr;
//                new_root->children_.push_back(inner);
//                new_root->children_.push_back(new_inner);
//                path.PrependNode(new_root);
//                height_inc++;
//                io_num += 3; // Write the new root and two leaf nodes
//              }
//            } else {
//              inner->AddChild(split_map_[inner].first, split_map_[inner].second, true);
//              io_num++;
//            }
//            split_map_.erase(inner);
//            break;
//          }
//        }
//      }
//      return height_inc;
//    }
 
//    int examNode(Path& path, int loc, int* num_splits) override {
//      return 0;
//    }
 
//    void GenIllCase(SimNode* root, int num, std::vector<Data>& dataset) override {
//      return;
//    }
 
//   private:
//    // Mapping from parent node to the split node and its sibling and the pivot
//    std::map<SimInternal*, std::pair<SimNode*, SimNode*>> split_map_;
//  };
 