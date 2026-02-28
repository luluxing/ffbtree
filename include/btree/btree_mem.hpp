#pragma once

#include <stack>
#include <thread>
#include <chrono>
#include <queue>
#include "../utils.hpp"
#include "../storage_manage.hpp"

namespace btree {

enum class InsertStrategy : uint8_t {
  Normal,
  Fsplit1,
  FsplitN,
  Ssplit
};

template <typename KeyType, typename ValueType>
class BTreeDisk;


// Base class for insert algorithms
template <typename KeyType, typename ValueType>
class InsertAlgorithm {
public:
  virtual int insert(BTreeDisk<KeyType, ValueType>& btree, 
                      KeyType& key, ValueType& val,
                      std::stack<int>& path,
                      InsertBuffer& buf) = 0;
  virtual ~InsertAlgorithm() = default;
};

// Normal insert algorithm
template <typename KeyType, typename ValueType>
class NormalInsert : public InsertAlgorithm<KeyType, ValueType> {
public:
  int insert(BTreeDisk<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<int>& path,
              InsertBuffer& buf) override {
    return btree.normal_insert(key, val, path, buf);
  }
};

// Fsplit1 insert algorithm
template <typename KeyType, typename ValueType>
class Fsplit1Insert : public InsertAlgorithm<KeyType, ValueType> {
public:
  int insert(BTreeDisk<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<int>& path,
              InsertBuffer& buf) override {
    return btree.fsplit1_insert(key, val, path, buf);
  }
};

// FsplitN insert algorithm
template <typename KeyType, typename ValueType>
class FsplitNInsert : public InsertAlgorithm<KeyType, ValueType> {
public:
  int insert(BTreeDisk<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<int>& path,
              InsertBuffer& buf) override {
    return btree.fsplitN_insert(key, val, path, buf);
  }
};

// Ssplit insert algorithm
template <typename KeyType, typename ValueType>
class SsplitInsert : public InsertAlgorithm<KeyType, ValueType> {
public:
  int insert(BTreeDisk<KeyType, ValueType>& btree, 
              KeyType& key, ValueType& val,
              std::stack<int>& path,
              InsertBuffer& buf) override {
    return btree.ssplit_insert(key, val, path, buf);
  }
};

template <typename KeyType, typename ValueType>
class BTreeDisk {

 public:
  BTreeDisk(const char *file_name,
            InsertStrategy is = InsertStrategy::FsplitN,
            int read_latency = 0,
            int write_latency = 0,
            bool bulk = false) {
    sm_ = new StorageManager(file_name, true, bulk);
    insert_strategy_ = is;
    // Initialize the root node
    Block block;
    NodeHeader root;
    root.item_count = 0;
    root.node_type = PageType::BTreeLeaf;
    root.block_id = 1;
    root.next_block_id = 0;
    memcpy(block.data, &root, NodeHeaderSize);
    sm_->write_block(1, block, nullptr);
    load_metanode();
    set_insert_algorithm();
    leaf_entry_num_ = 0;
    leaf_num_ = 0;
    inner_entry_num_ = 0;
    inner_num_ = 0;
    read_latency_ = read_latency;
    write_latency_ = write_latency;
  }

  ~BTreeDisk() {
    // Compute the leaf and nonleaf node utilization by scanning the index
    // fprintf(stdout, "Scanning the tree\n");
    // int cur_node_id = metanode.root_block_id;
    // fprintf(stdout, "Root node id: %d\n", cur_node_id);
    // std::queue<int> node_queue;
    // node_queue.push(cur_node_id);
    // int temp_leaf_num = 0;
    // int temp_inner_num = 0;
    // int temp_leaf_entry_num = 0;
    // int temp_inner_entry_num = 0;
    // while (!node_queue.empty()) {
    //   int cur_node_id = node_queue.front();
    //   node_queue.pop();
    //   Block cur_block = sm_->get_block(cur_node_id, nullptr);
    //   auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
    //   if (node->node_type == PageType::BTreeInner) {
    //     temp_inner_num++;
    //     temp_inner_entry_num += node->item_count;
    //     double cur_util = (double) (node->item_count) / (double)(max_inner_item - 1);
    //     if (cur_util < 0.5) {
    //       if (cur_node_id == metanode.root_block_id) {
    //         fprintf(stdout, "This is root: \t");
    //       }
    //       fprintf(stdout, "Inner node %d is less than 50%% full: %.5f\n", cur_node_id, cur_util);
    //     }
    //     auto inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
    //     for (int i = 0; i <= node->item_count; i++) {
    //       node_queue.push(inner_items[i].block_id);
    //     }
    //   } else {
    //     temp_leaf_num++;
    //     temp_leaf_entry_num += node->item_count;
    //     double cur_util = (double) (node->item_count) / (double)(max_leaf_item);
    //     if (cur_util < 0.5) {
    //       fprintf(stdout, "Leaf node %d is less than 50%% full\n", cur_node_id);
    //     }
    //   }
    // }
    // fprintf(stdout, "Leaf num: %d and %d\n", temp_leaf_num, leaf_num_);
    // fprintf(stdout, "Inner num: %d and %d\n", temp_inner_num, inner_num_);
    // fprintf(stdout, "Leaf entry num: %d and %d\n", temp_leaf_entry_num, leaf_entry_num_);
    // fprintf(stdout, "Inner entry num: %d and %d\n", temp_inner_entry_num, inner_entry_num_);
    // fprintf(stdout, "Final Leaf utilization: %.2f\n", ((double)temp_leaf_entry_num) / ((double)temp_leaf_num * (double)max_leaf_item));
    // fprintf(stdout, "Final Inner utilization: %.2f\n", ((double)temp_inner_entry_num) / ((double)temp_inner_num * ((double)max_inner_item - 1.0)));

    delete sm_;
    delete insert_algorithm_;
  }

  // Return the number of IOs
  int insert(KeyType key, ValueType val) {
    InsertBuffer insert_buffer;
    std::stack<int> path;
    int io_num = insert_algorithm_->insert(*this, key, val, path, insert_buffer);

    // Now we are at a leaf node
    int cur_node_id = path.top();
    path.pop();
    // Block cur_block = sm_->get_block(cur_node_id, &io_num);
    Block& cur_block = insert_buffer.get_block(cur_node_id);
    NodeHeader* leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
    if (leaf->item_count == max_leaf_item) {
      KeyType sep;
      int split_node_id = -1;
      Block new_blk;
      split_node_id = split_node(cur_block, new_blk, sep, &key, &val);
      insert_buffer.set_block(split_node_id, new_blk);
      // sm_->write_block(split_node_id, new_blk, &io_num);
      // sm_->write_block(cur_node_id, cur_block, &io_num);
      insert_buffer.mark_dirty(cur_node_id);
      insert_buffer.mark_dirty(split_node_id);
      // Propagate the split up the tree
      while (!path.empty()) {
        int parent_id = path.top();
        path.pop();
        // Block parent_block = sm_->get_block(parent_id, &io_num);
        Block& parent_block = insert_buffer.get_block(parent_id);
        auto parent_node = reinterpret_cast<NodeHeader*>(parent_block.data);
        InnerNodeItem<KeyType>* parent_items = 
          reinterpret_cast<InnerNodeItem<KeyType>*>(parent_block.data + NodeHeaderSize);
        if (parent_node->item_count == max_inner_item - 1) {
          KeyType new_sep;
          Block new_blk;
          int new_split_node_id = split_node(parent_block, new_blk, new_sep, &sep, &split_node_id);
          insert_buffer.set_block(new_split_node_id, new_blk);
          // sm_->write_block(new_split_node_id, new_blk, &io_num);
          // sm_->write_block(parent_id, parent_block, &io_num);
          insert_buffer.mark_dirty(parent_id);
          insert_buffer.mark_dirty(new_split_node_id);
          cur_node_id = parent_id;
          sep = new_sep;
          split_node_id = new_split_node_id;
        } else {
          insert_inner(parent_block, sep, split_node_id);
          // sm_->write_block(parent_id, parent_block, &io_num);
          insert_buffer.mark_dirty(parent_id);
          split_node_id = -1;
          break;
        }
      }
      if (split_node_id != -1) {
        make_root(sep, cur_node_id, split_node_id, insert_buffer);
      }
    } else {
      insert(cur_block, key, val);
      // sm_->write_block(cur_node_id, cur_block, &io_num);
      insert_buffer.mark_dirty(cur_node_id);
    }
    // Write all dirty blocks to disk
    io_num += insert_buffer.write_blocks(sm_);
    simulate_write(insert_buffer.dirty_blocks.size());
    // Clean up the insert buffer
    insert_buffer.blocks.clear();
    insert_buffer.dirty_blocks.clear();
    return io_num;
  }

  bool lookup(KeyType k, ValueType& result) {
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, nullptr);
    simulate_read();
    auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      NodeHeader* inner = reinterpret_cast<NodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, k);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      simulate_read();
      node = reinterpret_cast<NodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    NodeHeader* leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
    LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block.data + NodeHeaderSize);
    int pos = search_in_lnode(leaf_items, leaf->item_count, k);
    bool success;
    if ((pos < leaf->item_count) && (leaf_items[pos].key == k)) {
      success = true;
      result = leaf_items[pos].value;
    }
    
    return success;
  }

  uint64_t bounded_scan(KeyType low_key, KeyType high_key, std::vector<ValueType>& output) {
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, nullptr);
    simulate_read();
    auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      NodeHeader* inner = reinterpret_cast<NodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, low_key);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      simulate_read();
      node = reinterpret_cast<NodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    NodeHeader* leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
    bool reach_end = false;
    while (!reach_end) {
      LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block.data + NodeHeaderSize);
      int pos = search_in_lnode(leaf_items, leaf->item_count, low_key);
      for (int i = pos; i < leaf->item_count; i++) {
        if (leaf_items[i].key > high_key) {
          reach_end = true;
          break;
        }
        output.push_back(leaf_items[i].value);
      }
      if (leaf->next_block_id == 0) {
        reach_end = true;
      } else {
        cur_node_id = leaf->next_block_id;
        cur_block = sm_->get_block(cur_node_id, nullptr);
        simulate_read();
        leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
      }
    }
    return output.size();
  }

  int get_height() {
    return metanode.height;
  }

  double GetLeafUtil() {
    return ((double)leaf_entry_num_) / ((double)leaf_num_ * (double)max_leaf_item);
  }

  double GetInnerUtil() {
    return ((double)inner_entry_num_) / ((double)inner_num_ * ((double)max_inner_item - 1.0));
  }

 private:
  StorageManager *sm_;
  MetaNode metanode;
  static const int max_leaf_item = (pageSize - NodeHeaderSize) / LeafNodeItemSize<KeyType, ValueType>;
  static const int max_inner_item = (pageSize - NodeHeaderSize) / InnerNodeItemSize<KeyType>;
  InsertStrategy insert_strategy_;
  InsertAlgorithm<KeyType, ValueType>* insert_algorithm_;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;
  int read_latency_;
  int write_latency_;

  void simulate_read() {
    std::this_thread::sleep_for(std::chrono::microseconds(read_latency_));
  }

  void simulate_write(int io_num) {
    std::this_thread::sleep_for(std::chrono::microseconds(write_latency_ * io_num));
  }

  void load_metanode() {
    Block block = sm_->get_block(0, nullptr);
    memcpy(&metanode, block.data, MetaNodeSize);
  }

  void set_insert_algorithm() {
    switch (insert_strategy_) {
      case InsertStrategy::Normal:
        insert_algorithm_ = new NormalInsert<KeyType, ValueType>();
        break;
      case InsertStrategy::Fsplit1:
        insert_algorithm_ = new Fsplit1Insert<KeyType, ValueType>();
        break;
      case InsertStrategy::FsplitN:
        insert_algorithm_ = new FsplitNInsert<KeyType, ValueType>();
        break;
      case InsertStrategy::Ssplit:
        insert_algorithm_ = new SsplitInsert<KeyType, ValueType>();
        break;
    }
  }

  void make_root(KeyType sep, int left_id, int right_id, InsertBuffer& insert_buffer) {
    int new_root_id = metanode.block_count++;
    Block new_root_blk;
    auto new_root = reinterpret_cast<NodeHeader*>(new_root_blk.data);
    new_root->block_id = new_root_id;
    new_root->node_type = PageType::BTreeInner;
    new_root->item_count = 1;

    InnerNodeItem<KeyType>* items = reinterpret_cast<InnerNodeItem<KeyType>*>(new_root_blk.data + NodeHeaderSize);
    items[0].key = sep;
    items[0].block_id = left_id;
    items[1].block_id = right_id;

    metanode.root_block_id = new_root_id;
    metanode.height++;
    // sm_->write_block(new_root_id, new_root_blk, num_io);
    insert_buffer.set_block(new_root_id, new_root_blk);
    insert_buffer.mark_dirty(new_root_id);
    inner_num_++;
    inner_entry_num_++;
  }

  // Insert a new block id into the parent node
  void insert_inner(Block& blk, KeyType k, int insert_blk_id) {
    auto node = reinterpret_cast<NodeHeader*>(blk.data);
    InnerNodeItem<KeyType>* items = 
      reinterpret_cast<InnerNodeItem<KeyType>*>(blk.data + NodeHeaderSize);
    int pos = search_in_inode(items, node->item_count, k);
    memmove(items + pos + 1,
            items + pos,
            (node->item_count - pos + 1) * InnerNodeItemSize<KeyType>);
    items[pos].key = k;
    items[pos].block_id = insert_blk_id;
    std::swap(items[pos].block_id, items[pos + 1].block_id);
    node->item_count++;
    inner_entry_num_++;
  }

  void insert(Block& block, KeyType k, ValueType v) {
    LeafNodeItem<KeyType, ValueType>* leaf_items = 
      reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(block.data + NodeHeaderSize);
    auto leaf = reinterpret_cast<NodeHeader*>(block.data);
    if (leaf->item_count == 0) {
      leaf_items[0].key = k;
      leaf_items[0].value = v;
    } else {
      int pos = search_in_lnode(leaf_items, leaf->item_count, k);
      if ((pos < leaf->item_count) && (leaf_items[pos].key == k)) {
        // Update the value
        leaf_items[pos].value = v;
      } else {
        memmove(leaf_items + pos + 1,
                leaf_items + pos,
                (leaf->item_count - pos) * LeafNodeItemSize<KeyType, ValueType>);
        leaf_items[pos].key = k;
        leaf_items[pos].value = v;
      }
    }
    leaf->item_count++;
    leaf_entry_num_++;
  }

  int search_in_inode(InnerNodeItem<KeyType>* items, int item_count, KeyType k) {
    int low = 0;
    int high = item_count;
    do {
      int mid = low + (high - low) / 2;
      if (k < items[mid].key) {
        high = mid;
      } else if (k > items[mid].key) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }

  int search_in_lnode(LeafNodeItem<KeyType, ValueType>* items, int item_count, KeyType k) {
    int low = 0;
    int high = item_count;
    do {
      int mid = low + (high - low) / 2;
      if (k < items[mid].key) {
        high = mid;
      } else if (k > items[mid].key) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }

  int split_node(Block& blk, Block& new_blk, KeyType& sep, void* k, void* v) {
    auto node = reinterpret_cast<NodeHeader*>(blk.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);

    int new_block_id = metanode.block_count++;
    // Block new_blk = sm_->get_block(new_block_id);
    // Copy the header of the old node to the new one
    memcpy(new_blk.data, blk.data, NodeHeaderSize);

    auto new_node = reinterpret_cast<NodeHeader*>(new_blk.data);
    new_node->block_id = new_block_id;

    if (is_inner) {
      inner_num_++;
      int mid = node->item_count / 2;
      new_node->item_count = node->item_count - mid;
      node->item_count = node->item_count - new_node->item_count - 1;
      inner_entry_num_--;

      InnerNodeItem<KeyType>* items = 
        reinterpret_cast<InnerNodeItem<KeyType>*>(blk.data + NodeHeaderSize);
      sep = items[node->item_count].key;

      memcpy(new_blk.data + NodeHeaderSize, 
             blk.data + NodeHeaderSize + (node->item_count + 1) * InnerNodeItemSize<KeyType>,
             (new_node->item_count + 1) * InnerNodeItemSize<KeyType>);
      if (k) {
        KeyType* key = reinterpret_cast<KeyType*>(k);
        int* value = reinterpret_cast<int*>(v);
        if (*key < sep) {
          insert_inner(blk, *key, *value);
        } else {
          insert_inner(new_blk, *key, *value);
        }
      }
    } else {
      leaf_num_++;
      int mid = node->item_count / 2;
      new_node->item_count = node->item_count - mid;
      node->item_count = node->item_count - new_node->item_count;
      new_node->next_block_id = node->next_block_id;
      node->next_block_id = new_block_id;

      LeafNodeItem<KeyType, ValueType>* items = 
        reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(blk.data + NodeHeaderSize);
      sep = items[node->item_count - 1].key;

      memcpy(new_blk.data + NodeHeaderSize, 
             blk.data + NodeHeaderSize + node->item_count * LeafNodeItemSize<KeyType, ValueType>,
             new_node->item_count * LeafNodeItemSize<KeyType, ValueType>);
      if (k) {
        KeyType* key = reinterpret_cast<KeyType*>(k);
        ValueType* value = reinterpret_cast<ValueType*>(v);
        if (*key < sep) {
          insert(blk, *key, *value);
        } else {
          insert(new_blk, *key, *value);
        }
      }
    }
    // sm_->write_block(new_block_id, new_blk);
    // sm_->write_block(node->block_id, blk);
    return new_block_id;
  }

 public:
  // Normal insert implementation
  int normal_insert(KeyType key, ValueType val, std::stack<int>& path,
                    InsertBuffer& buf) {
    // Implement the normal insert logic here
    int io_num = 0;
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, &io_num);
    simulate_read();
    buf.set_block(cur_node_id, cur_block);
    auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    // Parent of current node
    int parent_node_id = -1;
    // std::stack<int> path;
    while (is_inner) {
      path.push(cur_node_id);
      parent_node_id = cur_node_id;
      NodeHeader* inner = reinterpret_cast<NodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, key);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, &io_num);
      simulate_read();
      buf.set_block(cur_node_id, cur_block);
      node = reinterpret_cast<NodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }
    path.push(cur_node_id);
    return io_num;
  }

  // Fsplit1 insert implementation
  int fsplit1_insert(KeyType key, ValueType val, std::stack<int>& path,
                    InsertBuffer& buf) {
    // Implement the Fsplit1 insert logic here
  //   int split_num = 0;
  // restart:
  //   int cur_node_id = metanode.root_block_id;
  //   Block cur_block = sm_->get_block(cur_node_id);
  //   auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
  //   bool is_inner = (node->node_type == PageType::BTreeInner);
  //   // Parent of current node
  //   int parent_node_id = -1;

  //   while (is_inner) {
  //     if (split_num == 0 && node->item_count == max_inner_item - 1) {
  //       // Split eagerly if full
  //       KeyType sep;
  //       int split_node_id = split_node(cur_block, sep, nullptr, nullptr);
  //       if (parent_node_id == -1) {
  //         // Create a new root
  //         make_root(sep, cur_node_id, split_node_id);
  //       } else {
  //         // Insert the new node into the parent
  //         Block parent_block = sm_->get_block(parent_node_id);
  //         insert_inner(parent_block, sep, split_node_id);
  //         sm_->write_block(parent_node_id, parent_block);
  //       }
  //       split_num++;
  //       goto restart;
  //     }

  //     parent_node_id = cur_node_id;
  //     NodeHeader* inner = reinterpret_cast<NodeHeader*>(cur_block.data);
  //     InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
  //     int pos = search_in_inode(inner_items, inner->item_count, key);
  //     cur_node_id = inner_items[pos].block_id;
  //     cur_block = sm_->get_block(cur_node_id);
  //     node = reinterpret_cast<NodeHeader*>(cur_block.data);
  //     is_inner = (node->node_type == PageType::BTreeInner);
  //   }

  //   // Now we are at a leaf node
  //   NodeHeader* leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
  //   if (split_num == 0 && leaf->item_count == max_leaf_item) {
  //     KeyType sep;
  //     int split_node_id = split_node(cur_block, sep, nullptr, nullptr);
  //     if (parent_node_id == -1) {
  //       // Create a new root
  //       make_root(sep, cur_node_id, split_node_id);
  //     } else {
  //       // Insert the new node into the parent
  //       Block parent_block = sm_->get_block(parent_node_id);
  //       insert_inner(parent_block, sep, split_node_id);
  //       sm_->write_block(parent_node_id, parent_block);
  //     }
  //     split_num++;
  //     goto restart;
  //   } else {
  //     insert(cur_block, key, val);
  //     sm_->write_block(cur_node_id, cur_block);
  //   }
    return 0;
  }

  // FsplitN insert implementation
  int fsplitN_insert(KeyType key, ValueType val, std::stack<int>& path,
                    InsertBuffer& buf) {
  // restart:
    int io_num = 0;
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, &io_num);
    simulate_read();
    buf.set_block(cur_node_id, cur_block);
    auto node = reinterpret_cast<NodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    // Parent of current node
    int parent_node_id = -1;

    while (is_inner) {
      if (node->item_count == max_inner_item - 1) {
        // Split eagerly if full
        KeyType sep;
        Block new_blk;
        Block& old = buf.get_block(cur_node_id);
        int split_node_id = split_node(old, new_blk, sep, nullptr, nullptr);
        buf.set_block(split_node_id, new_blk);
        // sm_->write_block(split_node_id, new_blk, &io_num);
        // sm_->write_block(cur_node_id, cur_block, &io_num);
        buf.mark_dirty(split_node_id);
        buf.mark_dirty(cur_node_id);
        if (parent_node_id == -1) {
          // Create a new root
          make_root(sep, cur_node_id, split_node_id, buf);
          path.push(metanode.root_block_id);
        } else {
          // Insert the new node into the parent
          // Block parent_block = sm_->get_block(parent_node_id, &io_num);
          Block& parent_block = buf.get_block(parent_node_id);
          insert_inner(parent_block, sep, split_node_id);
          // sm_->write_block(parent_node_id, parent_block, &io_num);
          buf.mark_dirty(parent_node_id);
        }
        // goto restart;
        if (key <= sep) {
          // Do nothing
        } else {
          cur_node_id = split_node_id;
          // cur_block = sm_->get_block(cur_node_id, &io_num);
          cur_block = new_blk;
        }
      }

      path.push(cur_node_id);
      parent_node_id = cur_node_id;
      NodeHeader* inner = reinterpret_cast<NodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + NodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, key);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, &io_num);
      simulate_read();
      buf.set_block(cur_node_id, cur_block);
      node = reinterpret_cast<NodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    NodeHeader* leaf = reinterpret_cast<NodeHeader*>(cur_block.data);
    if (leaf->item_count == max_leaf_item) {
      KeyType sep;
      Block new_blk;
      Block& old = buf.get_block(cur_node_id);
      int split_node_id = split_node(old, new_blk, sep, nullptr, nullptr);
      buf.set_block(split_node_id, new_blk);
      // sm_->write_block(split_node_id, new_blk, &io_num);
      // sm_->write_block(cur_node_id, cur_block, &io_num);
      buf.mark_dirty(split_node_id);
      buf.mark_dirty(cur_node_id);
      if (parent_node_id == -1) {
        // Create a new root
        make_root(sep, cur_node_id, split_node_id, buf);
      } else {
        // Insert the new node into the parent
        // Block parent_block = sm_->get_block(parent_node_id, &io_num);
        Block& parent_block = buf.get_block(parent_node_id);
        insert_inner(parent_block, sep, split_node_id);
        // sm_->write_block(parent_node_id, parent_block, &io_num);
        buf.mark_dirty(parent_node_id);
      }
      // goto restart;
      if (key <= sep) {
        path.push(cur_node_id);
      } else {
        path.push(split_node_id);
      }
    } else {
      path.push(cur_node_id);
    }
    return io_num;
  }

  // Ssplit insert implementation
  int ssplit_insert(KeyType key, ValueType val, std::stack<int>& path,
                    InsertBuffer& buf) {
    // Implement the Ssplit insert logic here
    return 0;
  }
};

} // namespace btree