#pragma once

#include <stack>
#include "../utils.hpp"
#include "../storage_manage.hpp"

namespace blinktree {

template<typename KeyType>
constexpr size_t BlinkNodeHeaderSize = sizeof(BlinkNodeHeader<KeyType>);


template <typename KeyType, typename ValueType>
class BLinkTreeDisk {
 public:
  BLinkTreeDisk(const char *file_name,
                bool bulk = false) {
    sm_ = new StorageManager(file_name, true, bulk);
    // Initialize the root node
    Block* block = new Block();
    BlinkNodeHeader<KeyType> root;
    root.item_count = 0;
    root.node_type = PageType::BTreeLeaf;
    root.block_id = 1;
    root.sibling_block_id = -1;
    root.high_key = std::numeric_limits<KeyType>::max();
    memcpy(block->data, &root, BlinkNodeHeaderSize<KeyType>);
    sm_->write_block(1, block, nullptr);

    load_metanode();
    leaf_entry_num_ = 0;
    leaf_num_ = 0;
    inner_entry_num_ = 0;
    inner_num_ = 0;
  }

  ~BLinkTreeDisk() {
    delete sm_;
  }
  int insert(KeyType key, ValueType val) {
    InsertBuffer insert_buffer;
    int io_num = 0;
    int cur_node_id = metanode.root_block_id;
    Block curblk = sm_->get_block(cur_node_id, &io_num);
    insert_buffer.set_block(cur_node_id, curblk);
    auto node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(curblk.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    std::stack<int> path;
    while (is_inner) {
      int next_node_id = scan_node(curblk, key);
      if (node->sibling_block_id != next_node_id) {
        path.push(cur_node_id);
      }
      cur_node_id = next_node_id;
      curblk = sm_->get_block(cur_node_id, &io_num);
      insert_buffer.set_block(cur_node_id, curblk);
      node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(curblk.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    Block& cur_block = insert_buffer.get_block(cur_node_id);
    BlinkNodeHeader<KeyType>* leaf = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
    while (leaf->sibling_block_id != -1 && leaf->high_key < key) {
      cur_node_id = leaf->sibling_block_id;
      cur_block = sm_->get_block(cur_node_id, &io_num);
      insert_buffer.set_block(cur_node_id, cur_block);
      leaf = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
    }
    if (leaf->item_count == max_leaf_item) {
      KeyType sep;
      int split_node_id = -1;
      Block new_blk;
      split_node_id = split_node(cur_block, new_blk, sep, &key, &val);
      insert_buffer.set_block(split_node_id, new_blk);
      insert_buffer.mark_dirty(cur_node_id);
      insert_buffer.mark_dirty(split_node_id);
      // sm_->write_block(split_node_id, new_blk, &io_num);
      // sm_->write_block(cur_node_id, cur_block, &io_num);
      // Propagate the split up the tree
      auto original_node = leaf;
      // auto new_block = sm_->get_block(split_node_id, &io_num);
      auto new_node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(new_blk.data);
      while (!path.empty()) {
        int parent_id = path.top();
        path.pop();
        // Block parent_block = sm_->get_block(parent_id, &io_num);
        Block& parent_block = insert_buffer.get_block(parent_id);
        auto old_parent_node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(parent_block.data);
        while (old_parent_node->sibling_block_id != -1 && old_parent_node->high_key < sep) {
          parent_id = old_parent_node->sibling_block_id;
          parent_block = sm_->get_block(parent_id, &io_num);
          insert_buffer.set_block(parent_id, parent_block);
          old_parent_node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(parent_block.data);
        }

        if (old_parent_node->item_count < max_inner_item - 1) {
          // Insert into non-leaf node
          insert_inner(parent_block, sep, split_node_id);
          // sm_->write_block(parent_id, parent_block, &io_num);
          insert_buffer.mark_dirty(parent_id);
          split_node_id = -1;
          break;
        } else {
          // Split the non-leaf node
          KeyType new_sep;
          Block nb;
          int new_split_node_id = split_node(parent_block, nb, new_sep, &sep, &split_node_id);
          // sm_->write_block(new_split_node_id, nb, &io_num);
          // sm_->write_block(parent_id, parent_block, &io_num);
          insert_buffer.set_block(new_split_node_id, nb);
          insert_buffer.mark_dirty(parent_id);
          insert_buffer.mark_dirty(new_split_node_id);
          cur_node_id = parent_id;
          sep = new_sep;
          split_node_id = new_split_node_id;
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
    // Clean up the insert buffer
    insert_buffer.blocks.clear();
    insert_buffer.dirty_blocks.clear();
    return io_num;
  }
  
  bool lookup(KeyType key, ValueType &val) {
    int cur_node_id = metanode.root_block_id;
    Block cur_block = sm_->get_block(cur_node_id, nullptr);
    auto node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);
    
    while (is_inner) {
      int next_node_id = scan_node(cur_block, key);
      cur_node_id = next_node_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
      is_inner = (node->node_type == PageType::BTreeInner);
    }

    // Now we are at a leaf node
    BlinkNodeHeader<KeyType>* leaf = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
    while (leaf->sibling_block_id != -1 && leaf->high_key < key) {
      cur_node_id = leaf->sibling_block_id;
      cur_block = sm_->get_block(cur_node_id, nullptr);
      leaf = reinterpret_cast<BlinkNodeHeader<KeyType>*>(cur_block.data);
    }
    LeafNodeItem<KeyType, ValueType>* leaf_items = reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(cur_block.data + BlinkNodeHeaderSize<KeyType>);
    int pos = search_in_lnode(leaf_items, leaf->item_count, key);
    bool success = false;
    if ((pos < leaf->item_count) && (leaf_items[pos].key == key)) {
      success = true;
      val = leaf_items[pos].value;
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

 private:
  StorageManager *sm_;
  MetaNode metanode;
  static const int max_leaf_item = (pageSize - BlinkNodeHeaderSize<KeyType>) / LeafNodeItemSize<KeyType, ValueType>;
  static const int max_inner_item = (pageSize - BlinkNodeHeaderSize<KeyType>) / InnerNodeItemSize<KeyType>;
  int leaf_entry_num_;
  int leaf_num_;
  int inner_entry_num_;
  int inner_num_;

  void load_metanode() {
    Block block = sm_->get_block(0, nullptr);
    memcpy(&metanode, block.data, MetaNodeSize);

    // // Initialize the root sibling pointer and high key
    // Block root_block = sm_->get_block(metanode.root_block_id);
    // auto root_node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(root_block.data);
    // root_node->sibling_block_id = -1;
    // root_node->high_key = std::numeric_limits<KeyType>::max();
    // sm_->write_block(metanode.root_block_id, root_block);
  }

  void make_root(KeyType key, int left_id, int right_id, InsertBuffer& insert_buffer) {
    // Implement the make_root logic here
    // This is a placeholder for the actual make_root implementation
    int new_root_id = metanode.block_count++;
    Block new_root_blk;// = sm_->get_block(new_root_id);
    auto new_root = reinterpret_cast<BlinkNodeHeader<KeyType>*>(new_root_blk.data);
    new_root->block_id = new_root_id;
    new_root->sibling_block_id = -1;
    new_root->high_key = std::numeric_limits<KeyType>::max();
    new_root->node_type = PageType::BTreeInner;
    new_root->item_count = 1;

    InnerNodeItem<KeyType>* items = reinterpret_cast<InnerNodeItem<KeyType>*>(new_root_blk.data + BlinkNodeHeaderSize<KeyType>);
    items[0].key = key;
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

  void insert_inner(Block &block, KeyType key, int block_id) {
    // Implement the insert_inner logic here
    // This is a placeholder for the actual insert_inner implementation
    auto node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(block.data);
    InnerNodeItem<KeyType>* items = 
      reinterpret_cast<InnerNodeItem<KeyType>*>(block.data + BlinkNodeHeaderSize<KeyType>);
    int pos = search_in_inode(items, node->item_count, key);
    memmove(items + pos + 1,
            items + pos,
            (node->item_count - pos + 1) * InnerNodeItemSize<KeyType>);
    items[pos].key = key;
    items[pos].block_id = block_id;
    std::swap(items[pos].block_id, items[pos + 1].block_id);
    node->item_count++;
    inner_entry_num_++;
  }

  void insert(Block &block, KeyType key, ValueType val) {
    // Implement the insert logic here
    // This is a placeholder for the actual insert implementation
    LeafNodeItem<KeyType, ValueType>* leaf_items = 
      reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(block.data + BlinkNodeHeaderSize<KeyType>);
    auto leaf = reinterpret_cast<BlinkNodeHeader<KeyType>*>(block.data);
    if (leaf->item_count == 0) {
      leaf_items[0].key = key;
      leaf_items[0].value = val;
    } else {
      int pos = search_in_lnode(leaf_items, leaf->item_count, key);
      if ((pos < leaf->item_count) && (leaf_items[pos].key == key)) {
        // Update the value
        leaf_items[pos].value = val;
      } else {
        memmove(leaf_items + pos + 1,
                leaf_items + pos,
                (leaf->item_count - pos) * LeafNodeItemSize<KeyType, ValueType>);
        leaf_items[pos].key = key;
        leaf_items[pos].value = val;
      }
    }
    leaf->item_count++;
    leaf_entry_num_++;
  }

  int search_in_inode(InnerNodeItem<KeyType> *items, int item_count, KeyType key) {
    int low = 0;
    int high = item_count;
    do {
      int mid = low + (high - low) / 2;
      if (key < items[mid].key) {
        high = mid;
      } else if (key > items[mid].key) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }

  int scan_node(Block &block, KeyType key) {
    auto node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(block.data);
    if (node->sibling_block_id != -1 && node->high_key < key) {
      return node->sibling_block_id;
    }
    InnerNodeItem<KeyType>* items = reinterpret_cast<InnerNodeItem<KeyType>*>(block.data + BlinkNodeHeaderSize<KeyType>);
    int pos = search_in_inode(items, node->item_count, key);
    return items[pos].block_id;
  }

  int search_in_lnode(LeafNodeItem<KeyType, ValueType> *items, int item_count, KeyType key) {
    int low = 0;
    int high = item_count;
    do {
      int mid = low + (high - low) / 2;
      if (key < items[mid].key) {
        high = mid;
      } else if (key > items[mid].key) {
        low = mid + 1;
      } else {
        return mid;
      }
    } while (low < high);
    return low;
  }

  int split_node(Block &blk, Block &new_blk, KeyType &sep, KeyType *k, ValueType *v) {
    auto node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(blk.data);
    bool is_inner = (node->node_type == PageType::BTreeInner);

    int new_block_id = metanode.block_count++;
    // Block new_blk = sm_->get_block(new_block_id);
    // Copy the header of the old node to the new one
    memcpy(new_blk.data, blk.data, BlinkNodeHeaderSize<KeyType>);

    auto new_node = reinterpret_cast<BlinkNodeHeader<KeyType>*>(new_blk.data);
    new_node->block_id = new_block_id;

    new_node->sibling_block_id = node->sibling_block_id;
    node->sibling_block_id = new_block_id;
    new_node->high_key = node->high_key;

    if (is_inner) {
      inner_num_++;
      int mid = node->item_count / 2;
      new_node->item_count = node->item_count - mid;
      node->item_count = node->item_count - new_node->item_count - 1;
      inner_entry_num_--;

      InnerNodeItem<KeyType>* items = 
        reinterpret_cast<InnerNodeItem<KeyType>*>(blk.data + BlinkNodeHeaderSize<KeyType>);
      sep = items[node->item_count].key;
      node->high_key = items[node->item_count].key;

      memcpy(new_blk.data + BlinkNodeHeaderSize<KeyType>, 
             blk.data + BlinkNodeHeaderSize<KeyType> + (node->item_count + 1) * InnerNodeItemSize<KeyType>,
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
        reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(blk.data + BlinkNodeHeaderSize<KeyType>);
      sep = items[node->item_count - 1].key;
      node->high_key = items[node->item_count - 1].key;

      memcpy(new_blk.data + BlinkNodeHeaderSize<KeyType>, 
             blk.data + BlinkNodeHeaderSize<KeyType> + node->item_count * LeafNodeItemSize<KeyType, ValueType>,
             new_node->item_count * LeafNodeItemSize<KeyType, ValueType>);
      LeafNodeItem<KeyType, ValueType>* nitems = 
        reinterpret_cast<LeafNodeItem<KeyType, ValueType>*>(new_blk.data + BlinkNodeHeaderSize<KeyType>);
      
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

} // namespace blinktree