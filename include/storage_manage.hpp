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
#include <unordered_map>
#include <cstring>
#include "utils.hpp"

#define Caching 0

class StorageManager {
 private:
  const char *file_name = nullptr;
  FILE *fp = nullptr;
  std::unordered_map<int, Block> memory_blocks;
#if Caching
  std::map<int, Block> block_cache; // LRU setting?
#endif

  void _write_block(void *data, int block_id) {
    Block blk;
    memcpy(blk.data, data, pageSize);
    memory_blocks[block_id] = blk;
  }

  void _read_block(void *data, int block_id) {
    auto it = memory_blocks.find(block_id);
    if (it == memory_blocks.end()) {
      memset(data, 0, pageSize);
    } else {
      memcpy(data, it->second.data, pageSize);
    }
  }

  char* _allocate_new_block() {
    char* ptr = nullptr;
    ptr = (char *)malloc(pageSize * sizeof(char));
    return ptr;
  }

  void _get_file_handle() {
    // in-memory: nothing to open
  }

  void _create_file(bool bulk) {
    Block meta{};
    MetaNode mn;
    mn.block_count = 2;
    mn.root_block_id = 1;
    mn.height = 1;
    memcpy(meta.data, &mn, MetaNodeSize);
    memory_blocks[0] = meta;
    if (!bulk) {
      // NodeHeader lnh;
      // lnh.item_count = 0;
      // lnh.node_type = PageType::BTreeLeaf;
      // // lnh.level = 1; // level starts from 1
      // lnh.block_id = 1;
      // memcpy(empty_block, &lnh, NodeHeaderSize);
      // _write_block(empty_block, 1);
    }
  }

  void _close_file_handle() {
    // in-memory: nothing to close
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
      Block empty_block{};
      MetaNode mn;
      mn.block_count = 1;
      mn.height = 0;
      memcpy(empty_block.data, &mn, MetaNodeSize);
      _write_block(empty_block.data, 0);
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

  // void get_block(int block_id, char *data) {
  //   _read_block(data, block_id);
  // }

  void write_block(int block_id, Block block, int* num_io) {
    _write_block(&block, block_id);
    if (num_io != nullptr)
      (*num_io)++;
  }

  void write_with_size(int block_id, void *data, long size) {
    Block blk{};
    memcpy(blk.data, data, size);
    memory_blocks[block_id] = blk;
  }

  size_t get_file_size() {
    return memory_blocks.size() * pageSize;
  }

  void write_arbitrary(long offset, void *data, long size) {
    int block_id = offset / pageSize;
    long inner_offset = offset % pageSize;
    Block blk = memory_blocks[block_id];
    memcpy(blk.data + inner_offset, data, size);
    memory_blocks[block_id] = blk;
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
    for (const auto& pair : blocks) {
      if (dirty_blocks[pair.first]) {
        sm->write_block(pair.first, pair.second, &num_io);
      }
    }
    return num_io;
  }
};
