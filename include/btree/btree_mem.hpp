#pragma once

#include <set>
#include <thread>
#include <chrono>
#include <stack>
#include "../utils.hpp"
#include "../storage_manage.hpp"

namespace btreemem {

enum class InsertStrategy : uint8_t {
  Normal,
  FsplitN
};

template <typename KeyType, typename ValueType>
class BTreeMem;

struct BTreeMemNode {
  PageType node_type;
  int item_count;
};

// Base class for insert algorithms
template <typename KeyType, typename ValueType>
class InsertAlgorithm {
public:
  virtual bool insert(BTreeMem<KeyType, ValueType>& btree, 
                      KeyType& key, ValueType& val,
                      std::stack<BTreeMemNode*>& path) = 0;
  virtual ~InsertAlgorithm() = default;
};

// Normal insert algorithm
template <typename KeyType, typename ValueType>
class NormalInsert : public InsertAlgorithm<KeyType, ValueType> {
public:
  bool insert(BTreeMem<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<BTreeMemNode*>& path) override {
    return btree.normal_insert(key, val, path);
  }
};

// FsplitN insert algorithm
template <typename KeyType, typename ValueType>
class FsplitNInsert : public InsertAlgorithm<KeyType, ValueType> {
public:
  bool insert(BTreeMem<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<BTreeMemNode*>& path) override {
    return btree.fsplitN_insert(key, val, path);
  }
};

template<typename KeyType, typename ValueType>
struct BTreeMemLeafNode : public BTreeMemNode {
  
  static const int max_entries = (pageSize - sizeof(BTreeMemNode)) / (sizeof(KeyType) + sizeof(ValueType));

  KeyType keys[max_entries];
  ValueType values[max_entries];

  BTreeMemLeafNode() {
    item_count = 0;
    node_type = PageType::BTreeLeaf;
  }

  bool isFull() { return item_count == max_entries; }
  
  
  int lowerBound(KeyType k) {
    int low = 0;
    int high = item_count;
    do {
      int mid = low + (high - low) / 2;
      if (k < keys[mid]) {
        high = mid;
      } else if (k > keys[mid]) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }
  
  void insert(KeyType k, ValueType v) {
    if (item_count == max_entries) {
      fprintf(stdout, "Leaf node is full\n");
      std::exit(1);
    }
    if (item_count == 0) {
      keys[0] = k;
      values[0] = v;
    } else {
      int pos = lowerBound(k);
      if ((pos < item_count) && (keys[pos] == k)) {
        values[pos] = v;
        return;
      }
      memmove(keys + pos + 1, keys + pos, (item_count - pos) * sizeof(KeyType));
      memmove(values + pos + 1, values + pos, (item_count - pos) * sizeof(ValueType));
      keys[pos] = k;
      values[pos] = v;
    }
    item_count++;
  }

  BTreeMemLeafNode* split(KeyType& sep, void* k, void* v) {
    BTreeMemLeafNode* new_leaf = new BTreeMemLeafNode();
    new_leaf->item_count = item_count - (item_count / 2);
    item_count = item_count - new_leaf->item_count;
    memcpy(new_leaf->keys, keys + item_count, new_leaf->item_count * sizeof(KeyType));
    memcpy(new_leaf->values, values + item_count, new_leaf->item_count * sizeof(ValueType));
    sep = keys[item_count - 1];

    if (k) {
      KeyType* key = reinterpret_cast<KeyType*>(k);
      ValueType* value = reinterpret_cast<ValueType*>(v);
      if (*key < sep) {
        insert(*key, *value);
      } else {
        new_leaf->insert(*key, *value);
      }
    }
    return new_leaf;
  }
};

template<typename KeyType>
struct BTreeMemInnerNode : public BTreeMemNode {
  static const int max_entries = (pageSize - sizeof(BTreeMemNode)) / (sizeof(KeyType) + sizeof(BTreeMemNode*));
  BTreeMemNode* children[max_entries];
  KeyType keys[max_entries];

  BTreeMemInnerNode() {
    item_count = 0;
    node_type = PageType::BTreeInner;
  }

  bool isFull() { return item_count == max_entries - 1; }

  unsigned lowerBound(KeyType k) {
    unsigned low = 0;
    unsigned high = item_count;
    do {
      unsigned mid = low + (high - low) / 2;
      if (k < keys[mid]) {
        high = mid;
      } else if (k > keys[mid]) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }

  BTreeMemInnerNode* split(KeyType& sep, void* k, void* v) {
    BTreeMemInnerNode* new_inner = new BTreeMemInnerNode();
    new_inner->item_count = item_count - (item_count / 2);
    item_count = item_count - new_inner->item_count - 1;
    sep = keys[item_count];
    memcpy(new_inner->keys, keys + item_count + 1, (new_inner->item_count + 1) * sizeof(KeyType));
    memcpy(new_inner->children, children + item_count + 1, (new_inner->item_count + 1) * sizeof(BTreeMemNode*));
    if (k) {
      KeyType* key = reinterpret_cast<KeyType*>(k);
      BTreeMemNode* node = reinterpret_cast<BTreeMemNode*>(v);
      if (*key < sep) {
        insert(*key, node);
      } else {
        new_inner->insert(*key, node);
      }
    }
    return new_inner;
  }
  
  void insert(KeyType k, BTreeMemNode* child) {
    if (item_count == max_entries - 1) {
      fprintf(stdout, "Inner node is full\n");
      std::exit(1);
    }
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, (item_count - pos + 1) * sizeof(KeyType));
    memmove(children + pos + 1, children + pos, (item_count - pos + 1) * sizeof(BTreeMemNode*));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    item_count++;
  }
  
};

template <typename KeyType, typename ValueType>
class BTreeMem {

 public:
  BTreeMem(InsertStrategy is = InsertStrategy::FsplitN,
            bool bulk = false) {
    insert_strategy_ = is;
    // Initialize the root node
    root_ = new BTreeMemLeafNode<KeyType, ValueType>();
    height_ = 1;
    set_insert_algorithm();
  }

  ~BTreeMem() {
    std::stack<BTreeMemNode*> nodes;
    nodes.push(root_);
    while (!nodes.empty()) {
      BTreeMemNode* node = nodes.top();
      nodes.pop();
      if (node->node_type == PageType::BTreeInner) {
        auto inner = static_cast<BTreeMemInnerNode<KeyType>*>(node);
        for (int i = 0; i < inner->item_count; i++) {
          nodes.push(inner->children[i]);
        }
        delete inner;
      } else {
        auto leaf = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(node);
        delete leaf;
      }
    }
    delete insert_algorithm_;
  }

  void insert(KeyType key, ValueType val) {
    insert_cost_.clear();
    std::stack<BTreeMemNode*> path;
    bool full_leaf = insert_algorithm_->insert(*this, key, val, path);

    // Now we are at a leaf node
    BTreeMemNode* cur_node = path.top();
    path.pop();
    if (full_leaf) {
      // Only normal B+-tree can reach here
      KeyType sep;
      auto nl = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(cur_node);
      BTreeMemNode* new_node = nl->split(sep, &key, &val);
      insert_cost_.write_set.insert(new_node);
      insert_cost_.read_set.insert(nl);
      while (!path.empty()) {
        auto parent_node = static_cast<BTreeMemInnerNode<KeyType>*>(path.top());
        path.pop();
        if (parent_node->isFull()) {
          KeyType new_sep;
          BTreeMemNode* new_split_node = parent_node->split(new_sep, &sep, new_node);
          insert_cost_.write_set.insert(new_split_node);
          insert_cost_.read_set.insert(parent_node);
          cur_node = parent_node;
          sep = new_sep;
          new_node = new_split_node;
        } else {
          parent_node->insert(sep, new_node);
          insert_cost_.write_set.insert(parent_node);
          new_node = nullptr;
          break;
        }
      }
      if (new_node != nullptr) {
        make_root(sep, cur_node, new_node);
        insert_cost_.write_set.insert(root_);
      }
    } else {
      // FsplitN and normal B+-tree can reach here
      auto leaf = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(cur_node);
      leaf->insert(key, val);
      insert_cost_.write_set.insert(leaf);
    }
    insert_cost_.add_read_write_cost();
  }

  bool lookup(KeyType k, ValueType& result) {
    bool is_inner = (root_->node_type == PageType::BTreeInner);
    BTreeMemNode* cur_node = root_;

    while (is_inner) {
      auto inner_node = static_cast<BTreeMemInnerNode<KeyType>*>(cur_node);
      int pos = inner_node->lowerBound(k);

      cur_node = inner_node->children[pos];
      is_inner = (cur_node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    auto leaf = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(cur_node);
    int pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->item_count) && (leaf->keys[pos] == k)) {
      success = true;
      result = leaf->values[pos];
    }
    
    return success;
  }

  void enable_manual_cost(double read_cost, double write_cost) {
    insert_cost_.read_cost_ = read_cost;
    insert_cost_.write_cost_ = write_cost;
  }

  int get_height() {
    return height_;
  }

  double GetLeafUtil() {
    return ((double)leaf_entry_num_) / ((double)(leaf_num_ * max_leaf_item));
  }

  double GetInnerUtil() {
    return ((double)inner_entry_num_) / ((double)(inner_num_ * (max_inner_item - 1)));
  }

 private:
  BTreeMemNode* root_;
  int height_;
  static const int max_leaf_item = BTreeMemLeafNode<KeyType, ValueType>::max_entries;
  static const int max_inner_item = BTreeMemInnerNode<KeyType>::max_entries;
  InsertStrategy insert_strategy_;
  InsertAlgorithm<KeyType, ValueType>* insert_algorithm_;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;

  struct InsertCost {
    double read_cost_;
    double write_cost_;
    std::set<BTreeMemNode*> read_set;
    std::set<BTreeMemNode*> write_set;
    void clear() {
      read_set.clear();
      write_set.clear();
    }
    void add_read_write_cost() {
      std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long long>(read_cost_ * read_set.size())));
      std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long long>(write_cost_ * write_set.size())));
    }
  };
  InsertCost insert_cost_;
  

  void set_insert_algorithm() {
    switch (insert_strategy_) {
      case InsertStrategy::Normal:
        insert_algorithm_ = new NormalInsert<KeyType, ValueType>();
        break;
      case InsertStrategy::FsplitN:
        insert_algorithm_ = new FsplitNInsert<KeyType, ValueType>();
        break;
    }
  }

  void make_root(KeyType sep, BTreeMemNode* left_node, BTreeMemNode* right_node) {
    auto new_root = new BTreeMemInnerNode<KeyType>();
    new_root->item_count = 1;
    new_root->keys[0] = sep;
    new_root->children[0] = left_node;
    new_root->children[1] = right_node;
    root_ = new_root;
    height_++;

    inner_num_++;
    inner_entry_num_++;
  }

 public:
  // Normal insert implementation
  bool normal_insert(KeyType key, ValueType val, std::stack<BTreeMemNode*>& path) {
    // Implement the normal insert logic here
    BTreeMemNode* cur_node = root_;
    bool is_inner = (cur_node->node_type == PageType::BTreeInner);
    // Parent of current node
    BTreeMemNode* parent_node = nullptr;
    while (is_inner) {
      path.push(cur_node);
      insert_cost_.read_set.insert(cur_node);
      auto inner_node = static_cast<BTreeMemInnerNode<KeyType>*>(cur_node);
      int pos = inner_node->lowerBound(key);
      parent_node = cur_node;
      cur_node = inner_node->children[pos];
      is_inner = (cur_node->node_type == PageType::BTreeInner);
    }
    path.push(cur_node);
    insert_cost_.read_set.insert(cur_node);
    auto leaf = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(cur_node);
    return leaf->isFull();
  }

  // FsplitN insert implementation
  bool fsplitN_insert(KeyType key, ValueType val, std::stack<BTreeMemNode*>& path) {
// restart:
    BTreeMemNode* cur_node = root_;
    // Parent of current node
    BTreeMemInnerNode<KeyType>* parent_node = nullptr;

    insert_cost_.read_set.insert(cur_node);
    while (cur_node->node_type == PageType::BTreeInner) {
      auto inner_node = static_cast<BTreeMemInnerNode<KeyType>*>(cur_node);
      if (inner_node->isFull()) {
        // Split eagerly if full
        KeyType sep;
        BTreeMemNode* new_node = inner_node->split(sep, nullptr, nullptr);
        insert_cost_.write_set.insert(new_node);
        insert_cost_.read_set.insert(cur_node);
        if (parent_node == nullptr) {
          // Create a new root
          make_root(sep, cur_node, new_node);
          path.push(root_);
          insert_cost_.write_set.insert(root_);
        } else {
          // Insert the new node into the parent
          parent_node->insert(sep, new_node);
          insert_cost_.write_set.insert(parent_node);
        }
        // goto restart;
        if (key <= sep) {
          // Do nothing
        } else {
          cur_node = new_node;
          inner_node = static_cast<BTreeMemInnerNode<KeyType>*>(cur_node);
        }
      }

      path.push(cur_node);
      parent_node = inner_node;
      unsigned pos = inner_node->lowerBound(key);
      cur_node = inner_node->children[pos];
      insert_cost_.read_set.insert(cur_node);
    }

    // Now we are at a leaf node
    auto leaf = static_cast<BTreeMemLeafNode<KeyType, ValueType>*>(cur_node);
    if (leaf->isFull()) {
      KeyType sep;
      BTreeMemNode* new_node = leaf->split(sep, nullptr, nullptr);
      insert_cost_.write_set.insert(new_node);
      insert_cost_.read_set.insert(leaf);
      if (parent_node == nullptr) {
        // Create a new root
        make_root(sep, cur_node, new_node);
        insert_cost_.write_set.insert(root_);
      } else {
        // Insert the new node into the parent
        parent_node->insert(sep, new_node);
        insert_cost_.write_set.insert(parent_node);
      }
      // goto restart;
      if (key <= sep) {
        path.push(cur_node);
      } else {
        path.push(new_node);
      }
    } else {
      path.push(cur_node);
    }
    if (leaf->isFull()) {
      fprintf(stdout, "Error:Leaf node is still full\n");
      std::exit(1);
    }
    return false;
  }
  
};

} // namespace btreemem