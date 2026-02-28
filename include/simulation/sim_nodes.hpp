#pragma once

#include <vector>
#include <cassert>

enum class NodeType : uint8_t { Inner=1, Leaf=2 };
enum class NodeLoc : uint8_t { Left=1, Right=2, Middle=3 };

using Rank = int;

struct Data {
  Rank rank;
  int count;
};

struct SimNode {
  int capacity_;
  NodeType type_;
  SimNode* right_;
  SimNode* parent_;
  int level_; // Leaf node at level=0; root at level=height_
  bool is_critical_ = false;
  // bool maybe_critical_ = false;

  void SetParent(SimNode* parent) {
    parent_ = parent;
  }
};

class SimLeafNode : public SimNode {
 public:
  SimLeafNode(int capacity) {
    capacity_ = capacity;
    type_ = NodeType::Leaf;
    count_ = 0;
    right_ = nullptr;
    parent_ = nullptr;
    level_ = 1;
  }

  SimLeafNode* SplitWithNewData(int num) {
    int old_size = (count_ + num) / 2;
    int new_size = count_ + num - old_size;
    SimLeafNode* new_leaf = new SimLeafNode(capacity_);
    count_ = old_size;
    new_leaf->SetParent(parent_);
    new_leaf->level_ = level_;
    new_leaf->count_ = new_size;
    new_leaf->right_ = right_;
    right_ = new_leaf;
    return new_leaf;
  }

  // SimLeafNode* UnevenSplit(int num, int allowed_space) {
  //   int new_size = capacity_ - allowed_space;
  //   int old_size = count_ + num - new_size;
  //   if (old_size < 0) {
  //     fprintf(stderr, "Leaf node entries are negative\n");
  //     std::abort();
  //   }
  //   SimLeafNode* new_leaf = new SimLeafNode(capacity_);
  //   count_ = old_size;
  //   return new_leaf;
  // }

  bool isFull(int allowed_space=0) {
    return count_ >= capacity_ - allowed_space;
  }


  ~SimLeafNode() {}

  int count_;
};

class SimInternal : public SimNode {
 public:
  SimInternal(int capacity) {
    capacity_ = capacity;
    type_ = NodeType::Inner;
    right_ = nullptr;
    parent_ = nullptr;
  }

  SimNode* Split(SimNode* child, SimNode* new_sibling) {
    SimInternal* new_inner = new SimInternal(capacity_);
    new_inner->level_ = level_;
    int old_size = (children_.size() + 1) / 2;
    int new_size = children_.size() + 1 - old_size;
    for (int i = 0; i < children_.size(); i++) {
      if (children_[i] == child) {
        children_.insert(children_.begin() + i + 1, new_sibling);
        break;
      }
    }
    for (int i = old_size; i < children_.size(); i++) {
      new_inner->children_.push_back(children_[i]);
      children_[i]->SetParent(new_inner);
    }
    children_.resize(old_size);
    new_inner->SetParent(parent_);
    new_inner->right_ = right_;
    right_ = new_inner;
    // if (reset_ptr) {
    //   new_inner->right_ = right_;
    //   right_ = new_inner;
    //   child->right_ = nullptr;
    //   new_sibling->right_ = nullptr;
    // }
    return new_inner;
  }

  void AddChild(SimNode* child, SimNode* new_sibling) {
    for (int i = 0; i < children_.size(); i++) {
      if (children_[i] == child) {
        children_.insert(children_.begin() + i + 1, new_sibling);
        return;
      }
    }
    fprintf(stderr, "Error: Child not found\n");
    std::abort();
  }

  bool isFull() {
    return children_.size() >= capacity_;
  }

  SimNode* ProactiveSplit() {
    SimInternal* new_inner = new SimInternal(capacity_);
    new_inner->level_ = level_;
    new_inner->SetParent(parent_);
    int old_size = children_.size() / 2;
    int new_size = children_.size() - old_size;
    for (int i = old_size; i < children_.size(); i++) {
      new_inner->children_.push_back(children_[i]);
      children_[i]->SetParent(new_inner);
    }
    children_.resize(old_size);
    new_inner->right_ = right_;
    right_ = new_inner;
    return new_inner;
  }

  bool NeedReactiveSplit() {
    return children_.size() == capacity_;
  }

  ~SimInternal() {}

  std::vector<SimNode*> children_;
};

struct Path {
  std::vector<SimNode*> nodes;
  
  void AddNode(SimNode* node) {
    nodes.push_back(node);
  }

  void PrependNode(SimNode* node) {
    nodes.insert(nodes.begin(), node);
  }

  int size() {
    return nodes.size();
  }

  void Clear() {
    nodes.clear();
  }

  Path() {}

  // Copy constructor: copy the nodes from the other path
  Path(const Path& other) {
    nodes = other.nodes;
  }
};
