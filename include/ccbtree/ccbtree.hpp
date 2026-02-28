// This tree is very similar to a disk-based B+-tree.
// The only difference is that tree nodes are checked for critical conditions
// when descending during the insertion process.

#include <stack>
#include <unordered_map>
#include "storage_manage.hpp"
#include "utils.hpp"

namespace ccbtree {

template <typename KeyType, typename ValueType>
class CCBtree {
 public:
  CCBtree(const char *file_name,
          bool bulk = false, int leaf_empty = 1, int inner_empty = 0) {
    sm_ = new StorageManager(file_name, true, bulk);
    // Initialize the root node
    Block block;
    CCNodeHeader root;
    root.item_count = 0;
    root.node_type = PageType::BTreeLeaf;
    root.block_id = 1;
    root.next_block_id = 0;
    // root.is_critical = false;
    // root.maybe_critical = false;
    is_critical_map[1] = false;
    maybe_critical_map[1] = false;
    memset(root.critical, 0, sizeof(uint8_t) * bitmapSize);
    memcpy(block.data, &root, CCNodeHeaderSize);
    sm_->write_block(1, block, nullptr);
    load_metanode();
    leaf_empty_ = leaf_empty;
    inner_empty_ = inner_empty;
    leaf_entry_num_ = 0;
    leaf_num_ = 0;
    inner_entry_num_ = 0;
    inner_num_ = 0;
  }

  ~CCBtree() {
    delete sm_;
  }

  int insert(KeyType key, ValueType val) {
    InsertBuffer insert_buffer;
    int cur_node_id = metanode.root_block_id;
    int io_num = 0;
    Block cur_block = sm_->get_block(cur_node_id, &io_num);
    insert_buffer.set_block(cur_node_id, cur_block);

    std::stack<int> path;
    auto node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    // Parent of current node
    int parent_node_id = -1;
    std::pair<int, int> last_critical_pair = {-1, -1};
    std::pair<int, int> last_tobe_critical_pair = {-1, -1};
    while (true) {
      path.push(cur_node_id);
      node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      if (is_critical_map[node->block_id]) {
        last_critical_pair = {cur_node_id, parent_node_id};
      }
      examine_node(cur_block, parent_node_id, last_critical_pair, last_tobe_critical_pair);
      if (node->node_type == PageType::BTreeLeaf) {
        break;
      }
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + CCNodeHeaderSize);
      int pos = search_in_inode(inner_items, node->item_count, key);
      parent_node_id = cur_node_id;
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, &io_num);
      insert_buffer.set_block(cur_node_id, cur_block);
      node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }
    set_critical(last_tobe_critical_pair, insert_buffer);
    KeyType sep;
    int split_node_id = proactive_split(last_critical_pair, sep, insert_buffer);
    // if the cur_block is proactively split, we need to check where to insert
    // cur_block = sm_->get_block(cur_node_id, &io_num);
    if (cur_node_id == last_critical_pair.first && split_node_id != -1) {
      if (key >= sep) {
        cur_node_id = split_node_id;
      }
    }
    Block& curblk = insert_buffer.get_block(cur_node_id);
    insert(curblk, key, val);
    
    // sm_->write_block(cur_node_id, cur_block, &io_num);
    insert_buffer.mark_dirty(cur_node_id);
    // Write all dirty blocks to disk
    io_num += insert_buffer.write_blocks(sm_);
    // Clean up the insert buffer
    insert_buffer.blocks.clear();
    insert_buffer.dirty_blocks.clear();
    return io_num;
  }

  bool lookup(KeyType k, ValueType& result) {
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, nullptr);
    auto node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      auto inner = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + CCNodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, k);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    auto leaf = reinterpret_cast<CCNodeHeader*>(cur_block.data);
    LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block.data + CCNodeHeaderSize);
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
    auto node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      auto inner = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block.data + CCNodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, low_key);
      cur_node_id = inner_items[pos].block_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      node = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    auto leaf = reinterpret_cast<CCNodeHeader*>(cur_block.data);
    bool reach_end = false;
    while (!reach_end) {
      LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block.data + CCNodeHeaderSize);
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
        leaf = reinterpret_cast<CCNodeHeader*>(cur_block.data);
      }
    }

    return output.size();
  }

  int get_height() {
    return metanode.height;
  }

  double GetLeafUtil() {
    return ((double)leaf_entry_num_) / ((double)(leaf_num_ * max_leaf_item));
  }

  double GetInnerUtil() {
    return ((double)inner_entry_num_) / ((double)(inner_num_ * (max_inner_item - 1)));
  }

  // Debugging purpose
  bool critical_node(int block_id) {
    Block block = sm_->get_block(block_id, nullptr);
    auto node = reinterpret_cast<CCNodeHeader*>(block.data);
    return is_critical_map[node->block_id] || maybe_critical_map[node->block_id];
  }

 private:
  StorageManager *sm_;
  MetaNode metanode;
  static const int max_leaf_item = (pageSize - CCNodeHeaderSize) / LeafNodeItemSize<KeyType, ValueType>;
  static const int max_inner_item = (pageSize - CCNodeHeaderSize) / InnerNodeItemSize<KeyType>;
  std::unordered_map<int, bool> is_critical_map;
  std::unordered_map<int, bool> maybe_critical_map;
  int leaf_empty_;
  int inner_empty_;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;

  void load_metanode() {
    Block block = sm_->get_block(0, nullptr);
    memcpy(&metanode, block.data, MetaNodeSize);
  }

  void make_root(KeyType sep, int left_id, int right_id, InsertBuffer& buf) {
    int new_root_id = metanode.block_count++;
    Block new_root_blk;// = sm_->get_block(new_root_id);
    auto new_root = reinterpret_cast<CCNodeHeader*>(new_root_blk.data);
    new_root->block_id = new_root_id;
    new_root->node_type = PageType::BTreeInner;
    new_root->item_count = 1;
    // When the old root node splits, the new root node cannot be critical
    // new_root->is_critical = false;
    // new_root->maybe_critical = false;
    is_critical_map[new_root_id] = false;
    maybe_critical_map[new_root_id] = false;
    // When the old root node splits, it and the new split node cannot be critical
    memset(new_root->critical, 0, sizeof(uint8_t) * bitmapSize);

    InnerNodeItem<KeyType>* items = reinterpret_cast<InnerNodeItem<KeyType>*>(new_root_blk.data + CCNodeHeaderSize);
    items[0].key = sep;
    items[0].block_id = left_id;
    items[1].block_id = right_id;

    metanode.root_block_id = new_root_id;
    metanode.height++;
    // sm_->write_block(new_root_id, new_root_blk, num_io);
    buf.set_block(new_root_id, new_root_blk);
    buf.mark_dirty(new_root_id);
    inner_num_++;
    inner_entry_num_++;
  }

  // Insert a new block id into the parent node. Return false if the node is full.
  void insert_inner(Block& blk, KeyType k, int insert_blk_id) {
    auto node = reinterpret_cast<CCNodeHeader*>(blk.data);
    if (node->item_count == max_inner_item - 1) {
      fprintf(stdout, "Inner node is full\n");
      std::exit(1);
    }
    if (node->item_count == max_inner_item - 1 - inner_empty_) {
      fprintf(stdout, "Inner node is almost full\n");
      std::exit(1);
    }
    InnerNodeItem<KeyType>* items = 
      reinterpret_cast<InnerNodeItem<KeyType>*>(blk.data + CCNodeHeaderSize);
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
      reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(block.data + CCNodeHeaderSize);
    auto leaf = reinterpret_cast<CCNodeHeader*>(block.data);
    if (leaf->item_count == max_leaf_item) {
      fprintf(stdout, "Leaf node is full\n");
      std::exit(1);
    }
    if (leaf->item_count == max_leaf_item - (leaf_empty_ - 1)) {
      fprintf(stdout, "Leaf node is almost full\n");
      std::exit(1);
    }
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

  int proactive_split(std::pair<int, int>& critical, KeyType& sep, InsertBuffer& buf) {
    if (critical.first == -1) {
      return -1;
    }
    // Block block = sm_->get_block(critical.first, io_num);
    Block& block = buf.get_block(critical.first);
    auto node = reinterpret_cast<CCNodeHeader*>(block.data);
    int split_node_id = -1;
    Block new_blk;
    split_node_id = split_node(block, new_blk, sep, nullptr, nullptr);
    buf.set_block(split_node_id, new_blk);
    // sm_->write_block(split_node_id, new_blk, io_num);
    // sm_->write_block(critical.first, block, io_num);
    buf.mark_dirty(critical.first);
    buf.mark_dirty(split_node_id);
    if (critical.second == -1) {
      // Create a new root
      make_root(sep, critical.first, split_node_id, buf);
    } else {
      // Insert the new node into the parent
      // Block parent_block = sm_->get_block(critical.second, io_num);
      Block& parent_block = buf.get_block(critical.second);
      auto parent_node = reinterpret_cast<CCNodeHeader*>(parent_block.data);
      insert_inner(parent_block, sep, split_node_id);
      // sm_->write_block(critical.second, parent_block, io_num);
      buf.mark_dirty(critical.second);
    }
    return split_node_id;
  }

  void set_critical(std::pair<int, int>& tobe_critical, InsertBuffer& buf) {
    if (tobe_critical.first == -1) {
      return;
    }
    int block_id = tobe_critical.first;
    
    // Block block = sm_->get_block(block_id, io_num);
    Block& block = buf.get_block(block_id);
    auto node = reinterpret_cast<CCNodeHeader*>(block.data);
    // node->is_critical = true;
    // node->maybe_critical = false;
    is_critical_map[block_id] = true;
    maybe_critical_map[block_id] = false;
    // sm_->write_block(block_id, block, io_num);
    // buf.mark_dirty(block_id);

    int parent = tobe_critical.second;
    if (parent == -1) {
      return;
    }
    // auto pblock = sm_->get_block(parent, io_num);
    Block& pblock = buf.get_block(parent);
    auto parent_node = reinterpret_cast<CCNodeHeader*>(pblock.data);
    // Iterate over parent's children ID and set the corresponding bits in critical
    InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(pblock.data + CCNodeHeaderSize);
    for (int i = 0; i < parent_node->item_count; i++) {
      if (inner_items[i].block_id == block_id) {
        BITMAP_SET(parent_node->critical, i);
        break;
      }
    }
    // parent_node->maybe_critical = true;
    maybe_critical_map[parent] = true;
    // sm_->write_block(parent, pblock, io_num);
    buf.mark_dirty(parent);
  }

  void examine_node(Block& block, int parent,
                   std::pair<int, int>& critical,
                   std::pair<int, int>&  tobe_critical) {
    auto node = reinterpret_cast<CCNodeHeader*>(block.data);
    if (is_critical_map[node->block_id]) {
      return;
    }
    if (node->node_type == PageType::BTreeLeaf) {
      if (node->item_count >= max_leaf_item - leaf_empty_) {
        tobe_critical.first = node->block_id;
        tobe_critical.second = parent;
      }
    } else {
      if (maybe_critical_map[node->block_id]) {
        // Count the critical children and check if the node is critical or not
        int num_cc = CountBitmapOnes(node->critical);
        if (node->item_count >= (max_inner_item - 1) - inner_empty_ - (num_cc)) {
          critical.first = node->block_id;
          critical.second = parent;
          tobe_critical.first = node->block_id;
          tobe_critical.second = parent;
        }
      }
    }
  }

  int split_node(Block& blk, Block& new_blk, KeyType& sep, void* k, void* v) {
    auto node = reinterpret_cast<CCNodeHeader*>(blk.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);

    int new_block_id = metanode.block_count++;
    // Block new_blk = sm_->get_block(new_block_id);
    // Copy the header of the old node to the new one
    memcpy(new_blk.data, blk.data, CCNodeHeaderSize);

    auto new_node = reinterpret_cast<CCNodeHeader*>(new_blk.data);
    new_node->block_id = new_block_id;

    // node->is_critical = false;
    // new_node->is_critical = false;
    // node->maybe_critical = false;
    // new_node->maybe_critical = false;
    is_critical_map[node->block_id] = false;
    is_critical_map[new_node->block_id] = false;
    maybe_critical_map[node->block_id] = false;
    maybe_critical_map[new_node->block_id] = false;

    if (is_inner) {
      inner_num_++;
      int mid = node->item_count / 2;
      new_node->item_count = node->item_count - mid;
      node->item_count = node->item_count - new_node->item_count - 1;
      inner_entry_num_--;

      split_bitmap(node, new_node);
      
      InnerNodeItem<KeyType>* items = 
        reinterpret_cast<InnerNodeItem<KeyType>*>(blk.data + CCNodeHeaderSize);
      sep = items[node->item_count].key;

      memcpy(new_blk.data + CCNodeHeaderSize, 
             blk.data + CCNodeHeaderSize + (node->item_count + 1) * InnerNodeItemSize<KeyType>,
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
        reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(blk.data + CCNodeHeaderSize);
      sep = items[node->item_count - 1].key;

      memcpy(new_blk.data + CCNodeHeaderSize, 
             blk.data + CCNodeHeaderSize + node->item_count * LeafNodeItemSize<KeyType, ValueType>,
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

  
};


} // namespace ccbtree