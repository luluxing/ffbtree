// The only difference is that tree nodes are checked for critical conditions
// when descending during the insertion process.

#include <algorithm>
#include <vector>
#include <stack>
#include <thread>
#include <chrono>
#include "storage_manage.hpp"
#include "utils.hpp"

namespace ccbtreeolc {

// This is the same as the OptLock in BTreeOLC.hpp
struct OptLock {
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};

  bool isLocked(uint64_t version) {
    return ((version & 0b10) == 0b10);
  }

  uint64_t readLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = typeVersionLockObsolete.load();
    if (isLocked(version) || isObsolete(version)) {
      _mm_pause();
      needRestart = true;
    }
    return version;
  }

  void writeLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = readLockOrRestart(needRestart);
    if (needRestart) return;

    upgradeToWriteLockOrRestart(version, needRestart);
    if (needRestart) return;
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
      version = version + 0b10;
    } else {
      _mm_pause();
      needRestart = true;
    }
  }

  void writeUnlock() {
    typeVersionLockObsolete.fetch_add(0b10);
  }

  bool isObsolete(uint64_t version) {
    return (version & 1) == 1;
  }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart = (startRead != typeVersionLockObsolete.load());
  }

  void writeUnlockObsolete() {
    typeVersionLockObsolete.fetch_add(0b11);
  }
};

struct CCBtreeOLCNode : public OptLock {
  PageType node_type;
  int item_count;
  bool is_critical;
  bool maybe_critical;
  uint8_t critical[bitmapSize];
};

template<typename KeyType, typename ValueType>
struct CCBtreeOLCLeafNode : public CCBtreeOLCNode {
  static const int max_entries = (pageSize - sizeof(CCBtreeOLCNode)) / (sizeof(KeyType) + sizeof(ValueType));
  KeyType keys[max_entries];
  ValueType values[max_entries];
  CCBtreeOLCLeafNode* next;

  CCBtreeOLCLeafNode() {
    node_type = PageType::BTreeLeaf;
    item_count = 0;
    is_critical = false;
    maybe_critical = false;
    memset(critical, 0, sizeof(uint8_t) * bitmapSize);
    next = nullptr;
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

  CCBtreeOLCLeafNode* split(KeyType& sep) {
    CCBtreeOLCLeafNode* new_leaf = new CCBtreeOLCLeafNode();
    is_critical = false;
    maybe_critical = false;
    new_leaf->item_count = item_count - item_count / 2;
    item_count = item_count - new_leaf->item_count;
    sep = keys[item_count - 1];
    memcpy(new_leaf->keys, keys + item_count, new_leaf->item_count * sizeof(KeyType));
    memcpy(new_leaf->values, values + item_count, new_leaf->item_count * sizeof(ValueType));
    new_leaf->next = next;
    next = new_leaf;
    return new_leaf;
  }
};

template<typename KeyType>
struct CCBtreeOLCInnerNode : public CCBtreeOLCNode {
  static const int max_entries = (pageSize - sizeof(CCBtreeOLCNode)) / (sizeof(KeyType) + sizeof(CCBtreeOLCNode*));
  KeyType keys[max_entries];
  CCBtreeOLCNode* children[max_entries];

  CCBtreeOLCInnerNode() {
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
  
  CCBtreeOLCInnerNode* split(KeyType& sep) {
    CCBtreeOLCInnerNode* new_inner = new CCBtreeOLCInnerNode();
    is_critical = false;
    maybe_critical = false;
    new_inner->item_count = item_count - item_count / 2;
    item_count = item_count - new_inner->item_count - 1;
    sep = keys[item_count];
    memcpy(new_inner->keys, keys + item_count + 1, (new_inner->item_count + 1) * sizeof(KeyType));
    memcpy(new_inner->children, children + item_count + 1, (new_inner->item_count + 1) * sizeof(CCBtreeOLCNode*));
    split_bitmap(this, new_inner);
    return new_inner;
  }

  void insert(KeyType k, CCBtreeOLCNode* child, int inner_empty) {
    if (isFull(inner_empty)) {
      fprintf(stdout, "Inner node is full\n");
      std::abort();
    }
    int pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, (item_count - pos + 1) * sizeof(KeyType));
    memmove(children + pos + 1, children + pos, (item_count - pos + 1) * sizeof(CCBtreeOLCNode*));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    item_count++;
  }
};

template <typename KeyType, typename ValueType>
class CCBtreeOLC {
 private:
  struct InsertCost {
    std::vector<CCBtreeOLCNode*> read_set;
    std::vector<CCBtreeOLCNode*> write_set;
    double read_cost_;
    double write_cost_;

    void add_read(CCBtreeOLCNode* node) {
      if (std::find(read_set.begin(), read_set.end(), node) == read_set.end()) {
        burn_cpu(read_cost_);
        read_set.push_back(node);
      }
    }
    void add_dirty(CCBtreeOLCNode* node) {
      write_set.push_back(node);
    }
    void flush() {
      add_write_cost();
    }

    void add_write_cost() {
      burn_cpu(write_cost_ * write_set.size());
    }
    inline void burn_cpu(double microseconds) {
      auto start = std::chrono::high_resolution_clock::now();
      while (std::chrono::duration<double, std::micro>(
                std::chrono::high_resolution_clock::now() - start)
                .count() < microseconds) {
          asm volatile("" ::: "memory");
      }
    }
  };

  struct InsertContext {
    using PathEntry = std::pair<CCBtreeOLCNode*, uint64_t>;

    void ensureCapacity() {
      if (path_.capacity() < 32) path_.reserve(32);
      if (write_candidates_.capacity() < 8) write_candidates_.reserve(8);
      if (locked_.capacity() < 8) locked_.reserve(8);
    }

    void reset() {
      path_.clear();
      write_candidates_.clear();
      locked_.clear();
    }

    void recordPath(CCBtreeOLCNode* node, uint64_t version) {
      path_.emplace_back(node, version);
    }

    void addWriteCandidate(CCBtreeOLCNode* node) {
      if (!node || hasWriteCandidate(node)) {
        return;
      }
      write_candidates_.push_back(node);
    }

    bool hasWriteCandidate(CCBtreeOLCNode* node) const {
      if (!node) {
        return false;
      }
      return std::find(write_candidates_.begin(), write_candidates_.end(), node) != write_candidates_.end();
    }

    uint64_t versionFor(CCBtreeOLCNode* node) const {
      if (!node) {
        return 0;
      }
      for (const auto& entry : path_) {
        if (entry.first == node) {
          return entry.second;
        }
      }
      return 0;
    }

    void trackLocked(CCBtreeOLCNode* node) {
      if (node) {
        locked_.push_back(node);
      }
    }

    void unlockAndErase(CCBtreeOLCNode* node) {
      if (!node) {
        return;
      }
      for (auto it = locked_.begin(); it != locked_.end(); ++it) {
        if (*it == node) {
          node->writeUnlock();
          locked_.erase(it);
          return;
        }
      }
    }

    void unlockAll() {
      for (auto node : locked_) {
        node->writeUnlock();
      }
      locked_.clear();
    }

    size_t path_size() const { return path_.size(); }
    size_t locked_size() const { return locked_.size(); }

    const std::vector<PathEntry>& path() const { return path_; }
    std::vector<PathEntry>& path() { return path_; }

    const std::vector<CCBtreeOLCNode*>& write_candidates() const { return write_candidates_; }
    std::vector<CCBtreeOLCNode*>& write_candidates() { return write_candidates_; }

    std::vector<CCBtreeOLCNode*>& locked_nodes() { return locked_; }

   private:
    std::vector<PathEntry> path_;
    std::vector<CCBtreeOLCNode*> write_candidates_;
    std::vector<CCBtreeOLCNode*> locked_;
  };

 public:
  CCBtreeOLC(bool bulk = false, int leaf_empty = 1, int inner_empty = 0) {
    // Initialize the root node
    root_.store(new CCBtreeOLCLeafNode<KeyType, ValueType>());
    height_.store(1);
    leaf_empty_ = leaf_empty;
    inner_empty_ = inner_empty;
    leaf_entry_num_ = 0;
    leaf_num_ = 0;
    inner_entry_num_ = 0;
    inner_num_ = 0;
    read_cost_ = 0;
    write_cost_ = 0;
  }

  ~CCBtreeOLC() {
    std::stack<CCBtreeOLCNode*> nodes;
    nodes.push(root_.load());
    while (!nodes.empty()) {
      CCBtreeOLCNode* node = nodes.top();
      nodes.pop();
      if (node->node_type == PageType::BTreeLeaf) {
        auto l = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(node);
        delete l;
      } else {
        auto inner = static_cast<CCBtreeOLCInnerNode<KeyType>*>(node);
        for (int i = 0; i < inner->item_count; i++) {
          nodes.push(inner->children[i]);
        }
        delete inner;
      }
    }
  }

  int insert(KeyType key, ValueType val) {
    InsertCost insert_cost;
    insert_cost.read_cost_ = read_cost_;
    insert_cost.write_cost_ = write_cost_;

    auto& ctx = insert_context();
    int restartCount = 0;

    while (true) {
      restartCount++;
      if (restartCount > 300) {
        insert_wlock_impl(key, val, insert_cost);
        return restartCount;
      } else if (restartCount > 1) {
        yield_with_backoff(restartCount);
      }

      if (restartCount % 100 == 0) {
        fprintf(stderr, "[tid %lu] insert backoff attempt %d; path_len=%zu write_nodes=%zu\n",
                (unsigned long)pthread_self(), restartCount,
                ctx.path_size(), ctx.locked_size());
      }

      ctx.ensureCapacity();
      ctx.reset();

      if (try_fast_insert(key, val, insert_cost, ctx)) {
        insert_cost.flush();
        return restartCount;
      }
    }
    return restartCount;
  }

 private:
  bool try_fast_insert(KeyType key,
                       ValueType val,
                       InsertCost& insert_cost,
                       InsertContext& ctx) {
    bool needRestart = false;

    auto request_restart = [&]() {
      before_restart(ctx.locked_nodes());
      return false;
    };

    CCBtreeOLCNode* cur_node = root_;
    insert_cost.add_read(cur_node);
    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart || (cur_node != root_)) {
      return false;
    }

    CCBtreeOLCInnerNode<KeyType>* parent_node = nullptr;
    std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*> last_critical_pair = {nullptr, nullptr};
    std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*> last_tobe_critical_pair = {nullptr, nullptr};

    while (true) {
      ctx.recordPath(cur_node, versionNode);
      if (cur_node->is_critical) {
        last_critical_pair = {cur_node, parent_node};
      } else {
        examine_node(cur_node, parent_node, last_critical_pair, last_tobe_critical_pair);
      }

      if (cur_node->node_type == PageType::BTreeLeaf) {
        break;
      }

      auto inner_node = static_cast<CCBtreeOLCInnerNode<KeyType>*>(cur_node);
      parent_node = inner_node;
      int pos = inner_node->lowerBound(key);

      CCBtreeOLCNode* next_node = inner_node->children[pos];
      __builtin_prefetch(next_node, 0, 3);

      cur_node = next_node;
      insert_cost.add_read(cur_node);

      inner_node->checkOrRestart(versionNode, needRestart);
      if (needRestart) {
        return false;
      }

      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) {
        return false;
      }
    }

    ctx.addWriteCandidate(cur_node);
    ctx.addWriteCandidate(last_tobe_critical_pair.first);
    ctx.addWriteCandidate(last_tobe_critical_pair.second);
    ctx.addWriteCandidate(last_critical_pair.first);
    ctx.addWriteCandidate(last_critical_pair.second);

    for (auto node : ctx.write_candidates()) {
      uint64_t expected_version = ctx.versionFor(node);
      node->upgradeToWriteLockOrRestart(expected_version, needRestart);
      if (needRestart) {
        return request_restart();
      }
      ctx.trackLocked(node);
      insert_cost.add_dirty(node);
    }

    for (auto &entry : ctx.path()) {
      CCBtreeOLCNode* n = entry.first;
      if (!ctx.hasWriteCandidate(n)) {
        n->readUnlockOrRestart(entry.second, needRestart);
        if (needRestart) {
          return request_restart();
        }
      }
    }

    set_critical(last_tobe_critical_pair);

    if (last_tobe_critical_pair.first &&
        last_tobe_critical_pair.first != last_critical_pair.first &&
        last_tobe_critical_pair.first != cur_node) {
      ctx.unlockAndErase(last_tobe_critical_pair.first);
      if (last_tobe_critical_pair.second) {
        ctx.unlockAndErase(last_tobe_critical_pair.second);
      }
    }

    auto leaf_node = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(cur_node);
    KeyType sep;
    CCBtreeOLCNode* split_node = proactive_split(last_critical_pair, sep);

    if (last_critical_pair.first && last_critical_pair.first != cur_node) {
      ctx.unlockAndErase(last_critical_pair.first);
      if (last_critical_pair.second) {
        ctx.unlockAndErase(last_critical_pair.second);
      }
    }

    if (cur_node == last_critical_pair.first && split_node != nullptr) {
      if (!(key < sep)) {
        leaf_node = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(split_node);
      }
    }

    leaf_node->insert(key, val, leaf_empty_);

    ctx.unlockAll();
    return true;
  }

 public:
  bool lookup(KeyType k, ValueType& result) {
    int restartCount = 0;
    InsertCost read_cost;
    read_cost.read_cost_ = read_cost_;
    read_cost.write_cost_ = write_cost_;
  restart:
    if (restartCount++)
      yield_with_backoff(restartCount);
    bool needRestart = false;

    CCBtreeOLCNode* node = root_;
    read_cost.add_read(node);
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node!=root_)) goto restart;

    // Parent of current node
    CCBtreeOLCInnerNode<KeyType>* parent = nullptr;
    uint64_t versionParent;

    while (node->node_type == PageType::BTreeInner) {
      auto inner = static_cast<CCBtreeOLCInnerNode<KeyType>*>(node);
      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
      }
      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      read_cost.add_read(node);
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    }

    CCBtreeOLCLeafNode<KeyType, ValueType>* leaf = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success;
    if ((pos < leaf->item_count) && (leaf->keys[pos] == k)) {
      success = true;
      result = leaf->values[pos];
    }
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;

    return success;
  }

  uint64_t bounded_scan(KeyType low_key, KeyType high_key, std::vector<ValueType>& output) {
    int restartCount = 0;
  restart:
    if (restartCount++)
      yield_with_backoff(restartCount);
    bool needRestart = false;

    CCBtreeOLCNode* node = root_;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node!=root_)) goto restart;

    // Parent of current node
    CCBtreeOLCInnerNode<KeyType>* parent = nullptr;
    uint64_t versionParent;

    while (node->node_type == PageType::BTreeInner) {
      auto inner = static_cast<CCBtreeOLCInnerNode<KeyType>*>(node);
      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
      }
      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(low_key)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    }

    CCBtreeOLCLeafNode<KeyType, ValueType>* leaf = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(node);
    CCBtreeOLCLeafNode<KeyType, ValueType>* prev_leaf = nullptr;
    uint64_t versionPrevLeaf;
    bool reached_end = false;
    while (!reached_end) {
      unsigned pos = leaf->lowerBound(low_key);
      for (unsigned i = pos; i < leaf->item_count; i++) {
        if (leaf->keys[i] > high_key) {
          reached_end = true;
          break;
        }
        output.push_back(leaf->values[i]);
      }

      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
        parent = nullptr;
      }
      leaf->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      prev_leaf = leaf;
      versionPrevLeaf = versionNode;
      leaf = leaf->next;
      if (!leaf) {
        reached_end = true;
      } else {
        prev_leaf->checkOrRestart(versionPrevLeaf, needRestart);
        if (needRestart) goto restart;
        versionNode = leaf->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        prev_leaf = leaf;
        versionPrevLeaf = versionNode;
      }
    }
    return output.size();
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
  bool critical_node(CCBtreeOLCNode* node) {
    return node->is_critical || node->maybe_critical;
  }

  void enable_manual_cost(double read_cost, double write_cost) {
    read_cost_ = read_cost;
    write_cost_ = write_cost;
  }

 private:
  std::atomic<CCBtreeOLCNode*> root_;
  std::atomic<int> height_;
  static const int max_leaf_item = CCBtreeOLCLeafNode<KeyType, ValueType>::max_entries;
  static const int max_inner_item = CCBtreeOLCInnerNode<KeyType>::max_entries;
  int leaf_empty_;
  int inner_empty_;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;
  // For adding manual cost
  double read_cost_;
  double write_cost_;

  InsertContext& insert_context() {
    thread_local InsertContext ctx;
    ctx.ensureCapacity();
    return ctx;
  }
  

  void make_root(KeyType sep, CCBtreeOLCNode* left_node, CCBtreeOLCNode* right_node) {
    auto new_root = new CCBtreeOLCInnerNode<KeyType>();
    new_root->item_count = 1;
    new_root->keys[0] = sep;
    new_root->children[0] = left_node;
    new_root->children[1] = right_node;
    height_.fetch_add(1);
    root_.store(new_root);

    inner_num_++;
    inner_entry_num_++;
  }

  CCBtreeOLCNode* proactive_split(std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*>& critical,
                                  KeyType& sep) {
    if (critical.first == nullptr) {
      return nullptr;
    }
    CCBtreeOLCNode* cur_node = critical.first;
    CCBtreeOLCNode* split_node = nullptr;
    if (cur_node->node_type == PageType::BTreeInner) {
      auto inner_node = static_cast<CCBtreeOLCInnerNode<KeyType>*>(cur_node);
      split_node = inner_node->split(sep);
    } else {
      auto leaf_node = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(cur_node);
      split_node = leaf_node->split(sep);
    }
    if (critical.second == nullptr) {
      // Create a new root
      make_root(sep, critical.first, split_node);
    } else {
      // Insert the new node into the parent
      auto parent_node = static_cast<CCBtreeOLCInnerNode<KeyType>*>(critical.second);
      parent_node->insert(sep, split_node, inner_empty_);
    }
    return split_node;
  }

  void set_critical(std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*>& tobe_critical) {
    if (tobe_critical.first == nullptr) {
      return;
    }
    CCBtreeOLCNode* cur_node = tobe_critical.first;
    cur_node->is_critical = true;
    cur_node->maybe_critical = false;

    if (tobe_critical.second == nullptr) {
      return;
    }
    auto parent_node = static_cast<CCBtreeOLCInnerNode<KeyType>*>(tobe_critical.second);
    for (int i = 0; i < parent_node->item_count; i++) {
      if (parent_node->children[i] == cur_node) {
        BITMAP_SET(parent_node->critical, i);
        break;
      }
    }
    parent_node->maybe_critical = true;
  }

  void examine_node(CCBtreeOLCNode* node, 
                    CCBtreeOLCNode* parent_node, 
                   std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*>& critical,
                   std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*>&  tobe_critical) {
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
          critical = {node, parent_node};
          tobe_critical = {node, parent_node};
        }
      }
    }
  }

  // void yield(int count) {
  //   if (count > 100) {
  //     // std::this_thread::sleep_for(std::chrono::milliseconds(10));
  //     sched_yield();
  //   } else if (count > 30) {
  //     // std::this_thread::sleep_for(std::chrono::microseconds(10));
  //     sched_yield();
  //   } else {
  //     _mm_pause();
  //   }
  // }

  inline void cpu_relax() {
  #if defined(__x86_64__) || defined(__i386__)
    asm volatile("pause");
  #endif
  }
    
  void yield_with_backoff(int restartCount) {
    if (restartCount < 4) {
      // short spin
      _mm_pause();
    } else if (restartCount < 12) {
      // yield CPU to sibling threads; light backoff
      std::this_thread::yield();
    } else {
      // longer backoff with randomized sleep (microseconds)
      int max_us = std::min(1000, 1 << (restartCount - 12)); // cap at 1000us
      int sleep_us = (std::rand() & (max_us - 1)) + 1;
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
  }

  // We write lock from the root and do the analysis along the way.
  // When we reach the leaf node, we unlock the non-critical nodes
  // and other nodes that will not be written.
  void insert_wlock_impl(KeyType key, ValueType val, InsertCost& insert_cost) {
    // Small RAII helper to track write-locked nodes and unlock them exactly once.
    struct WriteLockGuard {
      std::vector<CCBtreeOLCNode*> locked;
      WriteLockGuard() { locked.reserve(16); }
      void track(CCBtreeOLCNode* n) {
        if (!n) return;
        for (auto p : locked) if (p == n) return;
        locked.push_back(n);
      }
      bool contains(CCBtreeOLCNode* n) const {
        if (!n) return false;
        for (auto p : locked) if (p == n) return true;
        return false;
      }
      // unlock n if tracked; return true if unlocked/removed
      bool unlock_and_remove(CCBtreeOLCNode* n) {
        if (!n) return false;
        for (size_t i = 0; i < locked.size(); ++i) {
          if (locked[i] == n) {
            n->writeUnlock();
            locked[i] = locked.back();
            locked.pop_back();
            return true;
          }
        }
        return false;
      }
      // return copy of tracked pointers (useful for before_restart)
      std::vector<CCBtreeOLCNode*> snapshot() const { return locked; }
      ~WriteLockGuard() {
        // best-effort unlock remaining
        for (auto n : locked) {
          n->writeUnlock();
        }
        // clear to be safe
        const_cast<std::vector<CCBtreeOLCNode*>&>(locked).clear();
      }
    };
  
    int restartCount = 0;
  restart:
    if (restartCount++) yield_with_backoff(restartCount);
  
    bool needRestart = false;
  
    // thread-local small buffers to avoid allocations inside locked region
    thread_local std::vector<CCBtreeOLCNode*> tls_path;
    thread_local std::vector<CCBtreeOLCNode*> tls_nodes_to_be_written;
    thread_local std::vector<CCBtreeOLCNode*> tls_successfully_locked;
    if (tls_path.capacity() < 32) tls_path.reserve(32);
    if (tls_nodes_to_be_written.capacity() < 8) tls_nodes_to_be_written.reserve(8);
    if (tls_successfully_locked.capacity() < 8) tls_successfully_locked.reserve(8);
    tls_path.clear();
    tls_nodes_to_be_written.clear();
    tls_successfully_locked.clear();
  
    WriteLockGuard guard;
  
    CCBtreeOLCNode* cur_node = root_;
    insert_cost.add_read(cur_node);
  
    // Acquire write lock on root (or restart)
    cur_node->writeLockOrRestart(needRestart);
    if (needRestart || cur_node != root_) {
      before_restart(tls_successfully_locked);
      goto restart;
    }
    // track it
    tls_path.push_back(cur_node);
    guard.track(cur_node);
  
    bool is_inner = (cur_node->node_type == PageType::BTreeInner);
    CCBtreeOLCInnerNode<KeyType>* parent_node = nullptr;
    uint64_t versionParent = 0;
    std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*> last_critical_pair = {nullptr, nullptr};
    std::pair<CCBtreeOLCNode*, CCBtreeOLCNode*> last_tobe_critical_pair = {nullptr, nullptr};
  
    // Descend while taking write locks on the path
    while (true) {
      if (cur_node->is_critical) {
        last_critical_pair = {cur_node, parent_node};
      }
      if (!cur_node->is_critical) {
        examine_node(cur_node, parent_node, last_critical_pair, last_tobe_critical_pair);
      }
      if (cur_node->node_type == PageType::BTreeLeaf) {
        break;
      }
  
      auto inner_node = static_cast<CCBtreeOLCInnerNode<KeyType>*>(cur_node);
      parent_node = inner_node;
      int pos = inner_node->lowerBound(key);
      CCBtreeOLCNode* next_node = inner_node->children[pos];
  
      // prefetch next child (as your caller did)
      __builtin_prefetch(next_node, 0, 3);
  
      cur_node = next_node;
      insert_cost.add_read(cur_node);
  
      // Acquire write lock on child
      cur_node->writeLockOrRestart(needRestart);
      if (needRestart) {
        // prepare list of currently-held write locks for before_restart
        auto snap = guard.snapshot();
        // copy into tls_successfully_locked to match caller expectation
        tls_successfully_locked = snap;
        before_restart(tls_successfully_locked);
        goto restart;
      }
      // successfully locked the child
      tls_path.push_back(cur_node);
      guard.track(cur_node);
    }
  
    // path size check (same as original behavior)
    if (tls_path.size() != static_cast<size_t>(height_.load())) {
      fprintf(stdout, "path size not equal to height\n");
      std::abort();
    }
  
    // Build unique nodes_to_be_written using tls vector (no std::set)
    auto push_unique = [&](CCBtreeOLCNode* n) {
      if (!n) return;
      for (auto x : tls_nodes_to_be_written) if (x == n) return;
      tls_nodes_to_be_written.push_back(n);
    };
  
    // leaf must be written
    push_unique(cur_node);
  
    if (last_tobe_critical_pair.first)  push_unique(last_tobe_critical_pair.first);
    if (last_tobe_critical_pair.second) push_unique(last_tobe_critical_pair.second);
    if (last_critical_pair.first)       push_unique(last_critical_pair.first);
    if (last_critical_pair.second)      push_unique(last_critical_pair.second);
  
    if (tls_nodes_to_be_written.size() > tls_path.size()) {
      fprintf(stdout, "nodes to be written size greater than path size\n");
      std::abort();
    }
  
    // Mark dirty and record successfully locked nodes (guard already tracks locks)
    for (auto n : tls_nodes_to_be_written) {
      insert_cost.add_dirty(n);
      // ensure it's in guard (it should be if it was write-locked)
      if (guard.contains(n)) {
        tls_successfully_locked.push_back(n);
      } else {
        // This shouldn't happen normally; if it does, try to upgrade it (defensive)
        // But we avoid extra allocations/complexity here; instead restart.
        // Prepare before_restart list: currently tracked
        tls_successfully_locked = guard.snapshot();
        before_restart(tls_successfully_locked);
        goto restart;
      }
    }
  
    // Release write locks of path nodes that are NOT in nodes_to_be_written
    for (auto node : tls_path) {
      bool keep = false;
      for (auto w : tls_nodes_to_be_written) { if (w == node) { keep = true; break; } }
      if (!keep) {
        // unlock once via guard
        guard.unlock_and_remove(node);
        // remove from tls_successfully_locked if present
        auto it = std::find(tls_successfully_locked.begin(), tls_successfully_locked.end(), node);
        if (it != tls_successfully_locked.end()) tls_successfully_locked.erase(it);
      }
    }
  
    // First pair: set critical as before
    set_critical(last_tobe_critical_pair);
  
    // Release them if nodes are different than last_critical_pair or current node
    if (last_tobe_critical_pair.first != nullptr &&
        last_tobe_critical_pair.first != last_critical_pair.first &&
        last_tobe_critical_pair.first != cur_node) {
      // use guard to unlock and remove from tracked list
      if (guard.unlock_and_remove(last_tobe_critical_pair.first)) {
        auto it = std::find(tls_successfully_locked.begin(), tls_successfully_locked.end(), last_tobe_critical_pair.first);
        if (it != tls_successfully_locked.end()) tls_successfully_locked.erase(it);
      }
      if (last_tobe_critical_pair.second != nullptr) {
        if (guard.unlock_and_remove(last_tobe_critical_pair.second)) {
          auto it = std::find(tls_successfully_locked.begin(), tls_successfully_locked.end(), last_tobe_critical_pair.second);
          if (it != tls_successfully_locked.end()) tls_successfully_locked.erase(it);
        }
      }
    }
  
    // Second pair: perform proactive split (may allocate internally)
    KeyType sep;
    CCBtreeOLCNode* split_node = proactive_split(last_critical_pair, sep);
  
    // Release the write lock of the last critical pair if not current node
    if (last_critical_pair.first != nullptr && last_critical_pair.first != cur_node) {
      if (guard.unlock_and_remove(last_critical_pair.first)) {
        auto it = std::find(tls_successfully_locked.begin(), tls_successfully_locked.end(), last_critical_pair.first);
        if (it != tls_successfully_locked.end()) tls_successfully_locked.erase(it);
      }
      if (last_critical_pair.second != nullptr) {
        if (guard.unlock_and_remove(last_critical_pair.second)) {
          auto it = std::find(tls_successfully_locked.begin(), tls_successfully_locked.end(), last_critical_pair.second);
          if (it != tls_successfully_locked.end()) tls_successfully_locked.erase(it);
        }
      }
    }
  
    // If cur_node was split proactively, pick the right leaf
    auto leaf_node = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(cur_node);
    if (cur_node == last_critical_pair.first && split_node != nullptr) {
      if (!(key < sep)) { // key >= sep
        leaf_node = static_cast<CCBtreeOLCLeafNode<KeyType, ValueType>*>(split_node);
      }
    }
  
    // Do the insert while holding required locks
    leaf_node->insert(key, val, leaf_empty_);
  
    // Release remaining write locks tracked in tls_successfully_locked via guard
    for (auto node : tls_successfully_locked) {
      guard.unlock_and_remove(node);
    }
  
    // guard destructor will unlock any nodes still tracked (safety net)
    insert_cost.flush();
  }

  inline void before_restart(std::vector<CCBtreeOLCNode*>& locked_nodes) {
    // Unlock in reverse acquisition order
    for (auto it = locked_nodes.rbegin(); it != locked_nodes.rend(); ++it) {
      if (*it) (*it)->writeUnlock();
    }
    locked_nodes.clear(); // prevent double unlocks
  }

};


} // namespace ccbtreeolc
