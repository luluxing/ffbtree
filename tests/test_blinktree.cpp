#include <gtest/gtest.h>
#include "blinktree/blinktree.hpp"

TEST(MemoryBlinkTreeTest, InitConstruction) {
  BLINK::btree_t<uint64_t>* btree = new BLINK::btree_t<uint64_t>();
  EXPECT_EQ(btree->check_height(), 0);
  delete btree;
}

TEST(MemoryBlinkTreeTest, SimpleInsert) {
  BLINK::btree_t<uint64_t>* btree = new BLINK::btree_t<uint64_t>();
  int num = 1000; // Node size 4096B
  for (int i = 0; i < num; i++) {
    btree->insert(i, i);
  }
  EXPECT_EQ(btree->check_height(), 1);
  delete btree;
}