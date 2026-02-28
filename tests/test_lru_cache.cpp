#include <gtest/gtest.h>
#include <algorithm>
#include "storage_manage.hpp"

// Test LRU cache can read from file
TEST(LRUCacheTest, ReadFromFile) {
  StorageManager sm("test.db", true, false);
  LRUCache cache(10, &sm);
  const char* test_data1 = "test_data1";
  Block block1;
  int block_id1 = 1;
  memcpy(block1.data, test_data1, 11);
  sm.write_block(block_id1, &block1, nullptr);
  Block* block2 = cache.get_block(1);
  EXPECT_TRUE(strcmp(block2->data, test_data1) == 0);
}

// Test LRU cache can read from file and evict properly
TEST(LRUCacheTest, ReadFromFileAndEvict) {
  int cache_cap = 8;
  StorageManager sm("test.db", true, false);
  LRUCache cache(cache_cap, &sm);
  size_t str_len = 12;

  for (int i = 0; i < cache_cap * 2; i++) {
    char* test_data1 = new char[str_len];
    sprintf(test_data1, "test_data%d", i);
    Block block1;
    memcpy(block1.data, test_data1, str_len);
    sm.write_block(i + 1, &block1, nullptr);
  }
  for (int i = 0; i < cache_cap; i++) {
    Block block1 = sm.get_block(i + 1, nullptr); 
    Block* block2 = cache.get_block(i + 1);
    EXPECT_TRUE(strcmp(block2->data, block1.data) == 0);
  }
  cache.reset_cache_stat();
  for (int i = 0; i < cache_cap * 2; i++) {
    Block block1 = sm.get_block(i + 1, nullptr); 
    Block* block2 = cache.get_block(i + 1);
    EXPECT_TRUE(strcmp(block2->data, block1.data) == 0);
  }
  EXPECT_EQ(cache.get_num_hits(), cache_cap);
  EXPECT_EQ(cache.get_num_read_ios(), cache_cap);
  EXPECT_EQ(cache.get_num_write_ios(), 0);
}

// Test LRU cache can update the content
TEST(LRUCacheTest, UpdateContent) {
  StorageManager sm("test.db", true, false);
  LRUCache cache(10, &sm);
  const char* test_data1 = "test_data1";
  const char* test_data2 = "test_data2";
  Block block1;
  memcpy(block1.data, test_data1, 11);
  sm.write_block(1, &block1, nullptr);

  Block* block2 = cache.get_block(1);
  memcpy(block2->data, test_data2, 12);
  cache.write_block(1);
  Block* block3 = cache.get_block(1);
  EXPECT_TRUE(strcmp(block3->data, test_data2) == 0);
  EXPECT_EQ(cache.get_num_write_ios(), 0);
}


