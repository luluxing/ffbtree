// This tree is very similar to a disk-based B+-tree.
// The only difference is that tree nodes are checked for critical conditions
// when descending during the insertion process.

#include <stack>
#include "storage_manage.hpp"
#include "utils.hpp"

namespace ccbtree {

template <typename KeyType, typename ValueType>
class CCBtree {
 public:
  CCBtree(const char *file_name, int cache_size,
          bool bulk = false, int leaf_empty = 1, int inner_empty = 0) {
    sm_ = new StorageManager(file_name, true, bulk);
    cache_ = new LRUCache(cache_size, sm_);
    // Initialize the root node
    Block* block = new Block();
    CCNodeHeader root;
    root.item_count = 0;
    root.node_type = PageType::BTreeLeaf;
    root.block_id = 1;
    root.is_critical = false;
    root.maybe_critical = false;
    memset(root.critical, 0, sizeof(uint8_t) * bitmapSize);
    memcpy(block->data, &root, CCNodeHeaderSize);
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
    delete cache_;
  }

  int insert(KeyType key, ValueType val) {
    cache_->reset_cache_stat();
    int cur_node_id = metanode.root_block_id;
    int io_num = 0;
    Block* cur_block = cache_->get_block(cur_node_id);

    std::stack<int> path;
    auto node = reinterpret_cast<CCNodeHeader*>(cur_block->data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    // Parent of current node
    int parent_node_id = -1;
    std::pair<int, int> last_critical_pair = {-1, -1};
    std::pair<int, int> last_tobe_critical_pair = {-1, -1};
    while (true) {
      path.push(cur_node_id);
      node = reinterpret_cast<CCNodeHeader*>(cur_block->data);
      if (node->is_critical) {
        last_critical_pair = {cur_node_id, parent_node_id};
      }
      examine_node(cur_block, parent_node_id, last_critical_pair, last_tobe_critical_pair);
      if (node->node_type == PageType::BTreeLeaf) {
        break;
      }
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block->data + CCNodeHeaderSize);
      int pos = search_in_inode(inner_items, node->item_count, key);
      parent_node_id = cur_node_id;
      cur_node_id = inner_items[pos].block_id;
      cur_block = cache_->get_block(cur_node_id);
      node = reinterpret_cast<CCNodeHeader*>(cur_block->data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }
    set_critical(last_tobe_critical_pair);
    KeyType sep;
    int split_node_id = proactive_split(last_critical_pair, sep);
    // if the cur_block is proactively split, we need to check where to insert
    // cur_block = sm_->get_block(cur_node_id, &io_num);
    if (cur_node_id == last_critical_pair.first && split_node_id != -1) {
      if (key >= sep) {
        cur_node_id = split_node_id;
      }
    }
    Block* curblk = cache_->get_block(cur_node_id);
    insert(curblk, key, val);
    cache_->write_block(cur_node_id);
    
    io_num += cache_->get_num_write_ios() + cache_->get_num_read_ios();
    return io_num;
  }

  bool lookup(KeyType k, ValueType& result) {
    int cur_node_id = metanode.root_block_id;
    Block* cur_block = cache_->get_block(cur_node_id);
    auto node = reinterpret_cast<CCNodeHeader*>(cur_block->data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      auto inner = reinterpret_cast<CCNodeHeader*>(cur_block->data);
      InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(cur_block->data + CCNodeHeaderSize);
      int pos = search_in_inode(inner_items, inner->item_count, k);
      cur_node_id = inner_items[pos].block_id;
      cur_block = cache_->get_block(cur_node_id);
      node = reinterpret_cast<CCNodeHeader*>(cur_block->data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    auto leaf = reinterpret_cast<CCNodeHeader*>(cur_block->data);
    LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block->data + CCNodeHeaderSize);
    int pos = search_in_lnode(leaf_items, leaf->item_count, k);
    bool success;
    if ((pos < leaf->item_count) && (leaf_items[pos].key == k)) {
      success = true;
      result = leaf_items[pos].value;
    }
    
    return success;
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
    Block* block = cache_->get_block(block_id);
    auto node = reinterpret_cast<CCNodeHeader*>(block->data);
    return node->is_critical || node->maybe_critical;
  }

 private:
  StorageManager *sm_;
  LRUCache* cache_;
  MetaNode metanode;
  static const int max_leaf_item = (pageSize - CCNodeHeaderSize) / LeafNodeItemSize<KeyType, ValueType>;
  static const int max_inner_item = (pageSize - CCNodeHeaderSize) / InnerNodeItemSize<KeyType>;
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

  void make_root(KeyType sep, int left_id, int right_id) {
    int new_root_id = metanode.block_count++;
    Block* new_root_blk = new Block();
    auto new_root = reinterpret_cast<CCNodeHeader*>(new_root_blk->data);
    new_root->block_id = new_root_id;
    new_root->node_type = PageType::BTreeInner;
    new_root->item_count = 1;
    // When the old root node splits, the new root node cannot be critical
    new_root->is_critical = false;
    new_root->maybe_critical = false;
    // When the old root node splits, it and the new split node cannot be critical
    memset(new_root->critical, 0, sizeof(uint8_t) * bitmapSize);

    InnerNodeItem<KeyType>* items = reinterpret_cast<InnerNodeItem<KeyType>*>(new_root_blk->data + CCNodeHeaderSize);
    items[0].key = sep;
    items[0].block_id = left_id;
    items[1].block_id = right_id;

    metanode.root_block_id = new_root_id;
    metanode.height++;

    cache_->allocate_block(new_root_id, new_root_blk);
    inner_num_++;
    inner_entry_num_++;
  }

  // Insert a new block id into the parent node. Return false if the node is full.
  void insert_inner(Block* blk, KeyType k, int insert_blk_id) {
    auto node = reinterpret_cast<CCNodeHeader*>(blk->data);
    if (node->item_count == max_inner_item - 1) {
      fprintf(stdout, "Inner node is full\n");
      std::exit(1);
    }
    if (node->item_count == max_inner_item - 1 - inner_empty_) {
      fprintf(stdout, "Inner node is almost full\n");
      std::exit(1);
    }
    InnerNodeItem<KeyType>* items = 
      reinterpret_cast<InnerNodeItem<KeyType>*>(blk->data + CCNodeHeaderSize);
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

  void insert(Block* block, KeyType k, ValueType v) {
    LeafNodeItem<KeyType, ValueType>* leaf_items = 
      reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(block->data + CCNodeHeaderSize);
    auto leaf = reinterpret_cast<CCNodeHeader*>(block->data);
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

  int proactive_split(std::pair<int, int>& critical, KeyType& sep) {
    if (critical.first == -1) {
      return -1;
    }
    Block* block = cache_->get_block(critical.first);
    auto node = reinterpret_cast<CCNodeHeader*>(block->data);
    int split_node_id = -1;
    Block* new_blk = new Block();
    split_node_id = split_node(block, new_blk, sep, nullptr, nullptr);
    cache_->allocate_block(split_node_id, new_blk);
    cache_->write_block(critical.first);
    if (critical.second == -1) {
      // Create a new root
      make_root(sep, critical.first, split_node_id);
    } else {
      // Insert the new node into the parent
      Block* parent_block = cache_->get_block(critical.second);
      auto parent_node = reinterpret_cast<CCNodeHeader*>(parent_block->data);
      insert_inner(parent_block, sep, split_node_id);
      cache_->write_block(critical.second);
    }
    return split_node_id;
  }

  void set_critical(std::pair<int, int>& tobe_critical) {
    if (tobe_critical.first == -1) {
      return;
    }
    int block_id = tobe_critical.first;
    
    Block* block = cache_->get_block(block_id);
    auto node = reinterpret_cast<CCNodeHeader*>(block->data);
    node->is_critical = true;
    node->maybe_critical = false;
    cache_->write_block(block_id);

    int parent = tobe_critical.second;
    if (parent == -1) {
      return;
    }
    Block* pblock = cache_->get_block(parent);
    auto parent_node = reinterpret_cast<CCNodeHeader*>(pblock->data);
    // Iterate over parent's children ID and set the corresponding bits in critical
    InnerNodeItem<KeyType>* inner_items = reinterpret_cast<InnerNodeItem<KeyType>*>(pblock->data + CCNodeHeaderSize);
    for (int i = 0; i < parent_node->item_count; i++) {
      if (inner_items[i].block_id == block_id) {
        BITMAP_SET(parent_node->critical, i);
        break;
      }
    }
    parent_node->maybe_critical = true;
    cache_->write_block(parent);
  }

  void examine_node(Block* block, int parent,
                   std::pair<int, int>& critical,
                   std::pair<int, int>&  tobe_critical) {
    auto node = reinterpret_cast<CCNodeHeader*>(block->data);
    if (node->is_critical) {
      return;
    }
    if (node->node_type == PageType::BTreeLeaf) {
      if (node->item_count >= max_leaf_item - leaf_empty_) {
        tobe_critical.first = node->block_id;
        tobe_critical.second = parent;
      }
    } else {
      if (node->maybe_critical) {
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

  int split_node(Block* blk, Block* new_blk, KeyType& sep, void* k, void* v) {
    auto node = reinterpret_cast<CCNodeHeader*>(blk->data);
    bool is_inner = (node->node_type == PageType::BTreeInner);

    int new_block_id = metanode.block_count++;
    // Copy the header of the old node to the new one
    memcpy(new_blk->data, blk->data, CCNodeHeaderSize);

    auto new_node = reinterpret_cast<CCNodeHeader*>(new_blk->data);
    new_node->block_id = new_block_id;

    node->is_critical = false;
    new_node->is_critical = false;
    node->maybe_critical = false;
    new_node->maybe_critical = false;

    if (is_inner) {
      inner_num_++;
      int mid = node->item_count / 2;
      new_node->item_count = node->item_count - mid;
      node->item_count = node->item_count - new_node->item_count - 1;
      inner_entry_num_--;

      split_bitmap(node, new_node);
      
      InnerNodeItem<KeyType>* items = 
        reinterpret_cast<InnerNodeItem<KeyType>*>(blk->data + CCNodeHeaderSize);
      sep = items[node->item_count].key;

      memcpy(new_blk->data + CCNodeHeaderSize, 
             blk->data + CCNodeHeaderSize + (node->item_count + 1) * InnerNodeItemSize<KeyType>,
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

      LeafNodeItem<KeyType, ValueType>* items = 
        reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(blk->data + CCNodeHeaderSize);
      sep = items[node->item_count - 1].key;

      memcpy(new_blk->data + CCNodeHeaderSize, 
             blk->data + CCNodeHeaderSize + node->item_count * LeafNodeItemSize<KeyType, ValueType>,
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
    return new_block_id;
  }

  
};


} // namespace ccbtree