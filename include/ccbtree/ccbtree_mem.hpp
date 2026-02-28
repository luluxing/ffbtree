// The only difference is that tree nodes are checked for critical conditions
// when descending during the insertion process.

#include <stack>
#include <thread>
#include <chrono>
#include "storage_manage.hpp"
#include "utils.hpp"

namespace ccbtreemem {

struct CCBtreeMemNode {
  PageType node_type;
  int item_count;
  bool is_critical;
  bool maybe_critical;
  uint8_t critical[bitmapSize];
};

template<typename KeyType, typename ValueType>
struct CCBtreeMemLeafNode : public CCBtreeMemNode {
  static const int max_entries = (pageSize - sizeof(CCBtreeMemNode)) / (sizeof(KeyType) + sizeof(ValueType));
  KeyType keys[max_entries];
  ValueType values[max_entries];

  CCBtreeMemLeafNode() {
    node_type = PageType::BTreeLeaf;
    item_count = 0;
    is_critical = false;
    maybe_critical = false;
    memset(critical, 0, sizeof(uint8_t) * bitmapSize);
  }

  bool isFull(int leaf_empty) { return item_count == max_entries - leaf_empty + 1; }

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

  void insert(KeyType k, ValueType v, int leaf_empty) {
    if (isFull(leaf_empty)) {
      fprintf(stdout, "Leaf node is full\n");
      std::abort();
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

  CCBtreeMemLeafNode* split(KeyType& sep) {
    CCBtreeMemLeafNode* new_leaf = new CCBtreeMemLeafNode();
    is_critical = false;
    maybe_critical = false;
    new_leaf->item_count = item_count - item_count / 2;
    item_count = item_count - new_leaf->item_count;
    sep = keys[item_count - 1];
    memcpy(new_leaf->keys, keys + item_count, new_leaf->item_count * sizeof(KeyType));
    memcpy(new_leaf->values, values + item_count, new_leaf->item_count * sizeof(ValueType));
    return new_leaf;
  }
};

template<typename KeyType>
struct CCBtreeMemInnerNode : public CCBtreeMemNode {
  static const int max_entries = (pageSize - sizeof(CCBtreeMemNode)) / (sizeof(KeyType) + sizeof(CCBtreeMemNode*));
  KeyType keys[max_entries];
  CCBtreeMemNode* children[max_entries];

  CCBtreeMemInnerNode() {
    node_type = PageType::BTreeInner;
    item_count = 0;
    is_critical = false;
    maybe_critical = false;
    memset(critical, 0, sizeof(uint8_t) * bitmapSize);
  }

  bool isFull(int inner_empty) {
    return item_count == max_entries - 1 - inner_empty; 
  }

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
  
  CCBtreeMemInnerNode* split(KeyType& sep) {
    CCBtreeMemInnerNode* new_inner = new CCBtreeMemInnerNode();
    is_critical = false;
    maybe_critical = false;
    new_inner->item_count = item_count - item_count / 2;
    item_count = item_count - new_inner->item_count - 1;
    sep = keys[item_count];
    memcpy(new_inner->keys, keys + item_count + 1, (new_inner->item_count + 1) * sizeof(KeyType));
    memcpy(new_inner->children, children + item_count + 1, (new_inner->item_count + 1) * sizeof(CCBtreeMemNode*));
    split_bitmap(this, new_inner);
    return new_inner;
  }

  void insert(KeyType k, CCBtreeMemNode* child, int inner_empty) {
    if (isFull(inner_empty)) {
      fprintf(stdout, "Inner node is full\n");
      std::abort();
    }
    int pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, (item_count - pos + 1) * sizeof(KeyType));
    memmove(children + pos + 1, children + pos, (item_count - pos + 1) * sizeof(CCBtreeMemNode*));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    item_count++;
  }
};

template <typename KeyType, typename ValueType>
class CCBtreeMem {
 public:
  CCBtreeMem(int leaf_empty = 1, int inner_empty = 0) {
    // Initialize the root node
    root_ = new CCBtreeMemLeafNode<KeyType, ValueType>();
    height_ = 1;
    leaf_empty_ = leaf_empty;
    inner_empty_ = inner_empty;
    leaf_entry_num_ = 0;
    leaf_num_ = 0;
    inner_entry_num_ = 0;
    inner_num_ = 0;
  }

  ~CCBtreeMem() {
    std::stack<CCBtreeMemNode*> nodes;
    nodes.push(root_);
    while (!nodes.empty()) {
      CCBtreeMemNode* node = nodes.top();
      nodes.pop();
      if (node->node_type == PageType::BTreeLeaf) {
        auto l = static_cast<CCBtreeMemLeafNode<KeyType, ValueType>*>(node);
        delete l;
      } else {
        auto inner = static_cast<CCBtreeMemInnerNode<KeyType>*>(node);
        for (int i = 0; i < inner->item_count; i++) {
          nodes.push(inner->children[i]);
        }
        delete inner;
      }
    }
  }

  int insert(KeyType key, ValueType val) {
    insert_cost_.clear();
    CCBtreeMemNode* cur_node = root_;
    insert_cost_.read_set.insert(cur_node);

    std::stack<CCBtreeMemNode*> path;
    bool is_inner = (cur_node->node_type == PageType::BTreeInner);
    // Parent of current node
    CCBtreeMemInnerNode<KeyType>* parent_node = nullptr;
    std::pair<CCBtreeMemNode*, CCBtreeMemNode*> last_critical_pair = {nullptr, nullptr};
    std::pair<CCBtreeMemNode*, CCBtreeMemNode*> last_tobe_critical_pair = {nullptr, nullptr};
    while (true) {
      path.push(cur_node);
      if (cur_node->is_critical) {
        last_critical_pair = {cur_node, parent_node};
      }
      examine_node(cur_node, parent_node, last_critical_pair, last_tobe_critical_pair);
      if (cur_node->node_type == PageType::BTreeLeaf) {
        break;
      }
      auto inner_node = static_cast<CCBtreeMemInnerNode<KeyType>*>(cur_node);
      int pos = inner_node->lowerBound(key);
      parent_node = inner_node;
      cur_node = inner_node->children[pos];
      insert_cost_.read_set.insert(cur_node);
      is_inner = (cur_node->node_type == PageType::BTreeInner);
    }
    set_critical(last_tobe_critical_pair);
    if (last_tobe_critical_pair.first != nullptr) {
      insert_cost_.write_set.insert(last_tobe_critical_pair.first);
    }
    if (last_tobe_critical_pair.second != nullptr) {
      insert_cost_.write_set.insert(last_tobe_critical_pair.second);
    }
    KeyType sep;
    CCBtreeMemNode* split_node = proactive_split(last_critical_pair, sep);
    if (last_critical_pair.first != nullptr) {
      insert_cost_.write_set.insert(last_critical_pair.first);
    }
    if (last_critical_pair.second != nullptr) {
      insert_cost_.write_set.insert(last_critical_pair.second);
    }
    // if the cur_block is proactively split, we need to check where to insert
    // cur_block = sm_->get_block(cur_node_id, &io_num);
    if (cur_node == last_critical_pair.first && split_node != nullptr) {
      if (key >= sep) {
        cur_node = split_node;
      }
    }
    auto leaf_node = static_cast<CCBtreeMemLeafNode<KeyType, ValueType>*>(cur_node);
    leaf_node->insert(key, val, leaf_empty_);
    insert_cost_.write_set.insert(leaf_node);
    insert_cost_.add_read_write_cost();
    return 0;
  }

  bool lookup(KeyType k, ValueType& result) {
    CCBtreeMemNode* cur_node = root_;
    
    while (cur_node->node_type == PageType::BTreeInner) {
      auto inner_node = static_cast<CCBtreeMemInnerNode<KeyType>*>(cur_node);
      int pos = inner_node->lowerBound(k);
      cur_node = inner_node->children[pos];
    }

    auto leaf_node = static_cast<CCBtreeMemLeafNode<KeyType, ValueType>*>(cur_node);
    int pos = leaf_node->lowerBound(k);
    bool success = false;
    if ((pos < leaf_node->item_count) && (leaf_node->keys[pos] == k)) {
      success = true;
      result = leaf_node->values[pos];
    }
    
    return success;
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

  // Debugging purpose
  bool critical_node(CCBtreeMemNode* node) {
    return node->is_critical || node->maybe_critical;
  }

 private:
  CCBtreeMemNode* root_;
  int height_;
  static const int max_leaf_item = CCBtreeMemLeafNode<KeyType, ValueType>::max_entries;
  static const int max_inner_item = CCBtreeMemInnerNode<KeyType>::max_entries;
  int leaf_empty_;
  int inner_empty_;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;

  struct InsertCost {
    double read_cost_;
    double write_cost_;
    std::set<CCBtreeMemNode*> read_set;
    std::set<CCBtreeMemNode*> write_set;
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
  void enable_manual_cost(double read_cost, double write_cost) {
    insert_cost_.read_cost_ = read_cost;
    insert_cost_.write_cost_ = write_cost;
  }

  void make_root(KeyType sep, CCBtreeMemNode* left_node, CCBtreeMemNode* right_node) {
    auto new_root = new CCBtreeMemInnerNode<KeyType>();
    new_root->item_count = 1;
    new_root->keys[0] = sep;
    new_root->children[0] = left_node;
    new_root->children[1] = right_node;
    height_++;
    root_ = new_root;

    inner_num_++;
    inner_entry_num_++;
  }

  CCBtreeMemNode* proactive_split(std::pair<CCBtreeMemNode*, CCBtreeMemNode*>& critical,
                                  KeyType& sep) {
    if (critical.first == nullptr) {
      return nullptr;
    }
    CCBtreeMemNode* cur_node = critical.first;
    CCBtreeMemNode* split_node = nullptr;
    if (cur_node->node_type == PageType::BTreeInner) {
      auto inner_node = static_cast<CCBtreeMemInnerNode<KeyType>*>(cur_node);
      split_node = inner_node->split(sep);
    } else {
      auto leaf_node = static_cast<CCBtreeMemLeafNode<KeyType, ValueType>*>(cur_node);
      split_node = leaf_node->split(sep);
    }
    if (critical.second == nullptr) {
      // Create a new root
      make_root(sep, critical.first, split_node);
    } else {
      // Insert the new node into the parent
      auto parent_node = static_cast<CCBtreeMemInnerNode<KeyType>*>(critical.second);
      parent_node->insert(sep, split_node, inner_empty_);
    }
    return split_node;
  }

  void set_critical(std::pair<CCBtreeMemNode*, CCBtreeMemNode*>& tobe_critical) {
    if (tobe_critical.first == nullptr) {
      return;
    }
    CCBtreeMemNode* cur_node = tobe_critical.first;
    cur_node->is_critical = true;
    cur_node->maybe_critical = false;

    if (tobe_critical.second == nullptr) {
      return;
    }
    auto parent_node = static_cast<CCBtreeMemInnerNode<KeyType>*>(tobe_critical.second);
    for (int i = 0; i < parent_node->item_count; i++) {
      if (parent_node->children[i] == cur_node) {
        BITMAP_SET(parent_node->critical, i);
        break;
      }
    }
    parent_node->maybe_critical = true;
  }

  void examine_node(CCBtreeMemNode* node, CCBtreeMemNode* parent_node,
                   std::pair<CCBtreeMemNode*, CCBtreeMemNode*>& critical,
                   std::pair<CCBtreeMemNode*, CCBtreeMemNode*>&  tobe_critical) {
    if (node->is_critical) {
      return;
    }
    if (node->node_type == PageType::BTreeLeaf) {
      if (node->item_count >= max_leaf_item - leaf_empty_) {
        tobe_critical.first = node;
        tobe_critical.second = parent_node;
      }
    } else {
      if (node->maybe_critical) {
        // Count the critical children and check if the node is critical or not
        int num_cc = CountBitmapOnes(node->critical);
        if (node->item_count >= (max_inner_item - 1) - inner_empty_ - (num_cc)) {
          critical.first = node;
          critical.second = parent_node;
          tobe_critical.first = node;
          tobe_critical.second = parent_node;
        }
      }
    }
  }

};


} // namespace ccbtreemem