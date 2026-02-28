/**
 * This file adopts the code from the following repository:
 * https://github.com/rmitbggroup/LearnedIndexDiskExp/blob/main/code/b%2B_tree/storage_management.h
 * 
 */
#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <stdint.h>
#include <cstring>
#include "utils.hpp"

#define Caching 0

class StorageManager {
 private:
  const char *file_name = nullptr;
  FILE *fp = nullptr;
#if Caching
  std::map<int, Block> block_cache; // LRU setting?
#endif

  void _write_block(void *data, int block_id) {
    fseek(fp, block_id * pageSize, SEEK_SET);
    fwrite(data, pageSize, 1, fp);
    return;
  }

  void _read_block(void *data, int block_id) {
    fseek(fp, block_id * pageSize, SEEK_SET);
    fread(data, pageSize, 1, fp);
    return;
  }

  char* _allocate_new_block() {
    char* ptr = nullptr;
    ptr = (char *)malloc(pageSize * sizeof(char));
    return ptr;
  }

  void _get_file_handle() {
    fp = fopen(file_name,"r+b");
  }

  void _create_file(bool bulk) {
    fp = fopen(file_name,"wb");
    char empty_block[pageSize];
    MetaNode mn;
    mn.block_count = 2;
    mn.root_block_id = 1;
    mn.height = 1;
    memcpy(empty_block, &mn, MetaNodeSize);
    _write_block(empty_block, 0);
    if (!bulk) {
      // NodeHeader lnh;
      // lnh.item_count = 0;
      // lnh.node_type = PageType::BTreeLeaf;
      // // lnh.level = 1; // level starts from 1
      // lnh.block_id = 1;
      // memcpy(empty_block, &lnh, NodeHeaderSize);
      // _write_block(empty_block, 1);
    }
    _close_file_handle();
    _get_file_handle();
  }

  void _close_file_handle() {
    fclose(fp);
  }

 public:
  StorageManager(const char *fn, bool first = false, bool bulk_load = false) {
    file_name = fn;
    if (first) {
      _create_file(bulk_load);
    } else {
      _get_file_handle();
    }
  }

  StorageManager(bool first, char *fn) {
    file_name = fn;

    if (first) {
      fp = fopen(file_name,"wb");

      char empty_block[pageSize];
      MetaNode mn;
      mn.block_count = 1;
      mn.height = 0;
      memcpy(empty_block, &mn, MetaNodeSize);
      _write_block(empty_block, 0);

      _close_file_handle();
    }
    _get_file_handle();
  }

  StorageManager() = default;

  ~StorageManager() {
    if (fp != nullptr) _close_file_handle();
  }  

  Block get_block(int block_id, int* num_io) {
    Block block;
    //char data[pageSize];
    _read_block(block.data, block_id);
    //memcpy(block.data, data, pageSize);
    if (num_io != nullptr)
      (*num_io)++;
    return block;
  }

  void get_block_into(int block_id, Block* block, int* num_io) {
    _read_block(block->data, block_id);
    if (num_io != nullptr)
      (*num_io)++;
  }

  // void get_block(int block_id, char *data) {
  //   _read_block(data, block_id);
  // }

  void write_block(int block_id, Block* block, int* num_io) {
    _write_block(block, block_id);
    if (num_io != nullptr)
      (*num_io)++;
  }

  void write_with_size(int block_id, void *data, long size) {
    fseek(fp, block_id * pageSize, SEEK_SET);
    fwrite(data, size, 1, fp);
    return;
  }

  size_t get_file_size() {
    fseek(fp, 0, SEEK_END);
    return ftell(fp);
  }

  void write_arbitrary(long offset, void *data, long size) {
    fseek(fp, offset, SEEK_SET);
    fwrite(data, size, 1, fp);
    return;
  }

};

struct InsertBuffer {
  std::map<int, Block> blocks;
  std::map<int, bool> dirty_blocks;

  Block& get_block(int block_id) {
    if (blocks.find(block_id) == blocks.end()) {
      fprintf(stderr, "Block %d not found in insert buffer\n", block_id);
      std::abort();
    }
    return blocks[block_id];
  }
  void set_block(int block_id, Block& block) {
    blocks[block_id] = block;
    dirty_blocks[block_id] = false;
  }
  void mark_dirty(int block_id) {
    if (blocks.find(block_id) == blocks.end()) {
      fprintf(stderr, "Block %d not found in insert buffer\n", block_id);
      std::abort();
    }
    dirty_blocks[block_id] = true;
  }
  int write_blocks(StorageManager* sm) {
    int num_io = 0;
    for (auto& pair : blocks) {
      if (dirty_blocks[pair.first]) {
        Block* block = &pair.second;
        sm->write_block(pair.first, block, &num_io);
      }
    }
    return num_io;
  }
};

class LRUCache {
 public:
  LRUCache(int capacity, StorageManager* sm)
  : capacity_(capacity), head_(nullptr), tail_(nullptr), sm_(sm) {
    block_dir_.clear();
    cache_stat_.num_hits_ = 0;
    cache_stat_.num_read_ios_ = 0;
    cache_stat_.num_write_ios_ = 0;
  }

  ~LRUCache() {
    while (head_ != nullptr) {
      BlockFrame* tmp = head_;
      head_ = head_->next_;
      delete tmp->block_;
      delete tmp;
    }
    block_dir_.clear();
  }

  Block* get_block(int block_id) {
    BlockFrame* block_frame;
    if (block_dir_.find(block_id) == block_dir_.end()) {
      cache_stat_.num_read_ios_++;
      Block* block_ptr = new Block();
      sm_->get_block_into(block_id, block_ptr, nullptr);
      block_frame = new BlockFrame{block_id, false, block_ptr, nullptr, nullptr};
      if (block_dir_.size() >= capacity_) {
        _remove_lru_block();
      }
      _add_block_to_tail(block_frame);
      block_dir_[block_id] = block_frame;
      return block_frame->block_;
    }
    cache_stat_.num_hits_++;
    block_frame = block_dir_[block_id];
    _update_block_to_tail(block_frame);
    return block_frame->block_;
  }

  void write_block(int block_id) {
    if (block_dir_.find(block_id) == block_dir_.end()) {
      fprintf(stderr, "Block %d not found in cache\n", block_id);
      std::abort();
    }
    block_dir_[block_id]->dirty_ = true;
    _update_block_to_tail(block_dir_[block_id]);
  }

  void allocate_block(int block_id, Block* block) {
    if (block_dir_.size() >= capacity_) {
      _remove_lru_block();
    }
    BlockFrame* block_frame = new BlockFrame{block_id, true, block, nullptr, nullptr};
    _add_block_to_tail(block_frame);
    block_dir_[block_id] = block_frame;
  }

  int get_num_hits() {
    return cache_stat_.num_hits_;
  }
  int get_num_read_ios() {
    return cache_stat_.num_read_ios_;
  }
  int get_num_write_ios() {
    return cache_stat_.num_write_ios_;
  }
  void reset_cache_stat() {
    cache_stat_.num_hits_ = 0;
    cache_stat_.num_read_ios_ = 0;
    cache_stat_.num_write_ios_ = 0;
  }

 private:
  struct BlockFrame {
    int block_id_;
    bool dirty_;
    Block* block_;
    BlockFrame* prev_;
    BlockFrame* next_;
  };
  
  std::map<int, BlockFrame*> block_dir_;
  BlockFrame* head_;
  BlockFrame* tail_;
  StorageManager* sm_;
  int capacity_;

  struct CacheStat {
    int num_hits_;
    int num_read_ios_;
    int num_write_ios_;
  } cache_stat_;

  void _remove_lru_block() {
    if (head_ == nullptr) {
      return;
    }
    BlockFrame* tmp = head_;
    _remove_block(tmp);
    int block_id = tmp->block_id_;
    block_dir_.erase(block_id);
    if (tmp->dirty_) {
      sm_->write_block(block_id, tmp->block_, nullptr);
      cache_stat_.num_write_ios_++;
    }
    delete tmp->block_;
    delete tmp;
  }
  // Block does not exist in cache, add to tail
  void _add_block_to_tail(BlockFrame* block_frame) {
    if (head_ == nullptr && tail_ == nullptr) {
      head_ = block_frame;
      tail_ = block_frame;
      return;
    }
    if (head_ == nullptr || tail_ == nullptr) {
      fprintf(stderr, "head_ == nullptr || tail_ == nullptr\n");
      std::abort();
    }
    tail_->next_ = block_frame;
    block_frame->prev_ = tail_;
    block_frame->next_ = nullptr;
    tail_ = block_frame;
  }
  void _update_block_to_tail(BlockFrame* block_frame) {
    if (tail_ == block_frame) {
      return;
    }
    _remove_block(block_frame);
    _add_block_to_tail(block_frame);
  }
  void _remove_block(BlockFrame* block_frame) {
    if (block_frame->next_ != nullptr) {
      block_frame->next_->prev_ = block_frame->prev_;
    }
    if (block_frame->prev_ != nullptr) {
      block_frame->prev_->next_ = block_frame->next_;
    }
    if (block_frame == head_) {
      head_ = block_frame->next_;
    }
    if (block_frame == tail_) {
      tail_ = block_frame->prev_;
    }
  }
};