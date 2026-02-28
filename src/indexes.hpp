#include "btree/BTreeOLC.hpp"
#include "btree/btree_disk.hpp"
#include "blinktree/blinktree.hpp"
#include "blinktree/blinktree_disk.hpp"
#include "ccbtree/ccbtree.hpp"

#include "ccbtree/ccbtree_mem.hpp"
#include "ccbtree/ccbtree_olc.hpp"
#include "btree/btree_mem.hpp"

// Initialize a one-dimensional index parent class
template <typename Key, typename Value>
class OneDimDiskIndex {
public:
  virtual ~OneDimDiskIndex() {}
  virtual bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) = 0;
  virtual int Insert(Key k, Value v) = 0;
  virtual bool Remove(Key k) = 0;
  virtual bool Lookup(Key k, Value& result) = 0;
  virtual uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) = 0;
  virtual int GetSplitCount() = 0;
  virtual int GetHeight() = 0;
  virtual double GetLeafUtil() = 0;
  virtual double GetInnerUtil() = 0;
};

template <typename Key, typename Value>
class OneDimMemIndex {
public:
  virtual ~OneDimMemIndex() {}
  virtual bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) = 0;
  virtual int Insert(Key k, Value v) = 0;
  virtual bool Lookup(Key k, Value& result) = 0;
  virtual uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) = 0;
  virtual void EnableManualCost(double read_cost, double write_cost) = 0;
  virtual int GetSplitCount() = 0;
  virtual int GetHeight() = 0;
  virtual double GetLeafUtil() = 0;
  virtual double GetInnerUtil() = 0;
};

template <typename Key, typename Value>
class BtreeMemIndex : public OneDimMemIndex<Key, Value> {
 public:
  BtreeMemIndex() : btree(btreemem::InsertStrategy::Normal) {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(Key k, Value v) {
    btree.insert(k, v);
    return 0;
  }

  bool Remove(Key k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(Key k, Value& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  void EnableManualCost(double read_cost, double write_cost) {
    btree.enable_manual_cost(read_cost, write_cost);
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  ~BtreeMemIndex() {}

 private:
  btreemem::BTreeMem<Key, Value> btree;
};

template <typename Key, typename Value>
class BtreeOLCIndex : public OneDimMemIndex<Key, Value> {
 public:
  BtreeOLCIndex() : btree() {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(Key k, Value v) {
    return btree.insert(k, v);
  }

  bool Remove(Key k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  void EnableManualCost(double read_cost, double write_cost) {
    btree.enable_manual_cost(read_cost, write_cost);
  }

  bool Lookup(Key k, Value& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) {
    uint64_t count = btree.bounded_scan(low_k, high_k, output);
    return count;
  }

  int GetSplitCount() {
    return -1;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return -1;
  }

  double GetInnerUtil() {
    return -1;
  }

  ~BtreeOLCIndex() {}
  
 private:
  btreeolc::BTree<Key, Value> btree;
};

template <typename Key, typename Value>
class CCBtreeMemIndex : public OneDimMemIndex<Key, Value> {
 public:
  CCBtreeMemIndex() : btree(false, 1, 0) {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(Key k, Value v) {
    return btree.insert(k, v);
  }

  void EnableManualCost(double read_cost, double write_cost) {
    btree.enable_manual_cost(read_cost, write_cost);
  }

  bool Remove(Key k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(Key k, Value& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  ~CCBtreeMemIndex() {}

 private:
  ccbtreemem::CCBtreeMem<Key, Value> btree;
};

template <typename Key, typename Value>
class NormalBtreeDiskIndex : public OneDimDiskIndex<Key, Value> {
 public:
  NormalBtreeDiskIndex(int cache_size) 
  : btree("test.db", cache_size, btree::InsertStrategy::Normal) {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(int k, int v) {
    return btree.insert(k, v);
  }

  bool Remove(int k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, std::vector<int>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  int GetIO() {
    fprintf(stderr, "GetIO not implemented\n");
    return 0;
  }

  ~NormalBtreeDiskIndex() {}
  
 private:
  btree::BTreeDisk<Key, Value> btree;
};

template <typename Key, typename Value>
class FsplitNBtreeDiskIndex : public OneDimDiskIndex<Key, Value> {
 public:
  FsplitNBtreeDiskIndex(int cache_size) 
  : btree("test.db", cache_size, btree::InsertStrategy::FsplitN) {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(int k, int v) {
    return btree.insert(k, v);
  }

  bool Remove(int k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  int GetIO() {
    fprintf(stderr, "GetIO not implemented\n");
    return 0;
  }

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, std::vector<int>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  ~FsplitNBtreeDiskIndex() {}
  
 private:
  btree::BTreeDisk<Key, Value> btree;
};

template <typename Key, typename Value>
class BLinktreeMemIndex : public OneDimMemIndex<Key, Value> {
 public:
  BLinktreeMemIndex() : btree() {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(Key k, Value v) {
    static_assert(std::is_same<Value, uint64_t>::value, "Value type must be uint64_t");
    btree.insert(k, v);
    return 0;
  }

  void EnableManualCost(double read_cost, double write_cost) {
    fprintf(stderr, "EnableManualCost not implemented\n");
  }

  bool Remove(Key k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(Key k, Value& result) {
    static_assert(std::is_same<Value, uint64_t>::value, "Value type must be uint64_t");
    return btree.lookup(k, result);
  }

  uint64_t Scan(Key low_k, Key high_k, std::vector<Value>& output) {
    static_assert(std::is_same<Value, uint64_t>::value, "Value type must be uint64_t");
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return -1;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  int NumNodeAccess() {
    fprintf(stderr, "NumNodeAccess not implemented\n");
    return 0;
  }

  ~BLinktreeMemIndex() {}
  
 private:
  BLINK::btree_t<Key> btree;
};

template <typename Key, typename Value>
class BLinktreeDiskIndex : public OneDimDiskIndex<Key, Value> {
 public:
  BLinktreeDiskIndex() : btree("test.db") {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(int k, int v) {
    return btree.insert(k, v);
  }

  bool Remove(int k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, std::vector<int>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  int GetIO() {
    fprintf(stderr, "GetIO not implemented\n");
    return 0;
  }

  ~BLinktreeDiskIndex() {}
  
 private:
  blinktree::BLinkTreeDisk<Key, Value> btree;
};

template <typename Key, typename Value>
class CCBtreeDiskIndex : public OneDimDiskIndex<Key, Value> {
 public:
  CCBtreeDiskIndex(int cache_size) : btree("test.db", cache_size) {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(int k, int v) {
    return btree.insert(k, v);
  }

  bool Remove(int k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, std::vector<int>& output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  int GetIO() {
    fprintf(stderr, "GetIO not implemented\n");
    return 0;
  }

  ~CCBtreeDiskIndex() {}
  
 private:
  ccbtree::CCBtree<Key, Value> btree;
};

template <typename Key, typename Value>
class CCBtreeOLCIndex : public OneDimMemIndex<Key, Value> {
 public:
  CCBtreeOLCIndex() : btree() {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(int k, int v) {
    return btree.insert(k, v);
  }

  void EnableManualCost(double read_cost, double write_cost) {
    btree.enable_manual_cost(read_cost, write_cost);
  }

  bool Remove(int k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, std::vector<int>& output) {
    return btree.bounded_scan(low_k, high_k, output);
  }

  int GetSplitCount() {
    return 0;
  }

  int GetHeight() {
    return btree.get_height();
  }

  double GetLeafUtil() {
    return btree.GetLeafUtil();
  }

  double GetInnerUtil() {
    return btree.GetInnerUtil();
  }

  int GetIO() {
    fprintf(stderr, "GetIO not implemented\n");
    return 0;
  }

  ~CCBtreeOLCIndex() {}
  
 private:
  ccbtreeolc::CCBtreeOLC<Key, Value> btree;
};