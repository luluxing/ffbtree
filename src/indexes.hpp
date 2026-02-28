#include "btree/BTreeOLC.hpp"
#include "btree/btree_disk.hpp"
#include "blinktree/blinktree.hpp"
#include "blinktree/blinktree_disk.hpp"
#include "ccbtree/ccbtree.hpp"

// Initialize a one-dimensional index parent class
template <typename Key, typename Value>
class OneDimIndex {
public:
  virtual ~OneDimIndex() {}
  virtual bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) = 0;
  virtual int Insert(Key k, Value v) = 0;
  virtual bool Remove(Key k) = 0;
  virtual bool Lookup(Key k, Value& result) = 0;
  virtual uint64_t Scan(Key low_k, Key high_k, Value* output) = 0;
  virtual int GetSplitCount() = 0;
  virtual int GetHeight() = 0;
  virtual double GetLeafUtil() = 0;
  virtual double GetInnerUtil() = 0;
};

template <typename Key, typename Value>
class BtreeIndex : public OneDimIndex<Key, Value> {
 public:
  BtreeIndex() : btree() {}

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

  uint64_t Scan(Key low_k, Key high_k, Value* output) {
    fprintf(stderr, "Scan not implemented\n");
    return 0;
  }

  int GetSplitCount() {
    return -1;
  }

  int GetHeight() {
    return 0;
  }

  double GetLeafUtil() {
    return -1;
  }

  double GetInnerUtil() {
    return -1;
  }

  ~BtreeIndex() {}
  
 private:
  btreeolc::BTree<Key, Value> btree;
};

template <typename Key, typename Value>
class NormalBtreeDiskIndex : public OneDimIndex<Key, Value> {
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

  uint64_t Scan(int low_k, int high_k, int* output) {
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

  ~NormalBtreeDiskIndex() {}
  
 private:
  btree::BTreeDisk<Key, Value> btree;
};

template <typename Key, typename Value>
class FsplitNBtreeDiskIndex : public OneDimIndex<Key, Value> {
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

  bool Lookup(int k, int& result) {
    return btree.lookup(k, result);
  }

  uint64_t Scan(int low_k, int high_k, int* output) {
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
class BLinktreeIndex : public OneDimIndex<Key, Value> {
 public:
  BLinktreeIndex() : btree() {}

  bool BulkLoad(const std::vector<std::pair<Key, Value>>& data) {
    fprintf(stderr, "BulkLoad not implemented\n");
    return false;
  }

  int Insert(Key k, Value v) {
    static_assert(std::is_same<Value, uint64_t>::value, "Value type must be uint64_t");
    btree.insert(k, v);
    return 0;
  }

  bool Remove(Key k) {
    fprintf(stderr, "Remove not implemented\n");
    return false;
  }

  bool Lookup(Key k, Value& result) {
    static_assert(std::is_same<Value, uint64_t>::value, "Value type must be uint64_t");
    return btree.lookup(k, result);
  }

  uint64_t Scan(Key low_k, Key high_k, Value* output) {
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

  ~BLinktreeIndex() {}
  
 private:
  BLINK::btree_t<Key> btree;
};

template <typename Key, typename Value>
class BLinktreeDiskIndex : public OneDimIndex<Key, Value> {
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

  uint64_t Scan(int low_k, int high_k, int* output) {
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

  ~BLinktreeDiskIndex() {}
  
 private:
  blinktree::BLinkTreeDisk<Key, Value> btree;
};

template <typename Key, typename Value>
class CCBtreeDiskIndex : public OneDimIndex<Key, Value> {
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

  uint64_t Scan(int low_k, int high_k, int* output) {
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

  ~CCBtreeDiskIndex() {}
  
 private:
  ccbtree::CCBtree<Key, Value> btree;
};