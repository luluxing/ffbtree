#include <gtest/gtest.h>
#include <algorithm>
#include "blinktree/blinktree_disk.hpp"

using namespace blinktree;

TEST(DiskBlinkTreeTest, InitConstruction) {
  BLinkTreeDisk<uint64_t, uint64_t> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
}

TEST(DiskBlinkTreeTest, InsertAndLookupSingle) {
  BLinkTreeDisk<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
  btree.insert(1, 10);
  int val;
  EXPECT_TRUE(btree.lookup(1, val));
  EXPECT_EQ(val, 10);
}

TEST(DiskBlinkTreeTest, InsertAndLookupSequential) {
  BLinkTreeDisk<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
  int num = 50000;

  for (int i = 0; i < num; i++) {
    btree.insert(i, i + 42);
  }
  EXPECT_EQ(btree.get_height(), 2);
  for (int i = num - 1; i >= 0; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }

  for (int i = num; i < 2 * num; i++) {
    btree.insert(i, i + 43);
  }
  EXPECT_EQ(btree.get_height(), 2);
  for (int i = 2 * num - 1; i >= num; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 43);
  }

  for (int i = 2 * num; i < 3 * num; i++) {
    btree.insert(i, i + 45);
  }
  EXPECT_EQ(btree.get_height(), 3);
  for (int i = 3 * num - 1; i >= 2 * num; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 45);
  }
  for (int i = num - 1; i >= 0; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }
  for (int i = 2 * num - 1; i >= num; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 43);
  }
}

TEST(DiskBlinkTreeTest, InsertAndLookupRand) {
  BLinkTreeDisk<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
  int num = 150000;
  std::vector<int> data;
  for (int i = 0; i < num; i++) {
    data.push_back(i);
  }

  std::random_shuffle(data.begin(), data.end());

  for (int i = 0; i < data.size(); i++) {
    btree.insert(data[i], data[i]);

  }
  for (int i = 0; i < data.size(); i++) {
    int val;
    EXPECT_TRUE(btree.lookup(data[i], val));
    EXPECT_EQ(val, data[i]);
  }
}

TEST(DiskBlinkTreeTest, InsertionIOCost) {
  BLinkTreeDisk<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
  int num = 1;
  int io_num = btree.insert(num, num + 42);
  EXPECT_EQ(io_num, 2);

  num = 600;
  for (int i = 0; i < num; i++) {
    btree.insert(i, i + 42);
  }
  EXPECT_EQ(btree.get_height(), 2);
  io_num = btree.insert(num + 1, num + 42);
  EXPECT_EQ(io_num, 3);
}