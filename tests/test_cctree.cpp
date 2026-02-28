#include <gtest/gtest.h>
#include <algorithm>
#include "ccbtree/ccbtree.hpp"

using namespace ccbtree;

TEST(CCBTreeDiskTest, InitConstruction) {
  CCBtree<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);
}

TEST(CCBTreeDiskTest, SingleNode) {
  CCBtree<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);

  int num = 500;

  for (int i = 0; i < num; i++) {
    btree.insert(i, i + 42);
  }
  EXPECT_EQ(btree.get_height(), 1);
  for (int i = num - 1; i >= 0; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }
}

TEST(CCBTreeDiskTest, TestBitMap) {
  // Test bitmapSet, splitBitmap and CountBitmapOnes
  uint8_t bitmap[bitmapSize] = {0};
  CCNodeHeader n;
  CCNodeHeader new_n;
  memcpy(n.critical, bitmap, sizeof(uint8_t) * bitmapSize);
  memcpy(new_n.critical, bitmap, sizeof(uint8_t) * bitmapSize);

  int num = 17;
  int right = num / 2;
  int left = num - right - 1;
  for (int i = 0; i < num; i++) {
    BITMAP_SET(bitmap, i);
  }
  EXPECT_EQ(CountBitmapOnes(bitmap), num);

  n.item_count = left;
  new_n.item_count = right;
  memcpy(n.critical, bitmap, sizeof(uint8_t) * bitmapSize);
  split_bitmap(&n, &new_n);
  EXPECT_EQ(CountBitmapOnes(n.critical), left);
  EXPECT_EQ(CountBitmapOnes(new_n.critical), right);
}

TEST(CCBTreeDiskTest, TwoLevels) {
  CCBtree<int, int> btree("test.db", false);
  EXPECT_EQ(btree.get_height(), 1);

  int num = 600;

  for (int i = 0; i < num; i++) {
    btree.insert(i, i + 42);
  }
  EXPECT_EQ(btree.get_height(), 2);
  for (int i = num - 1; i >= 0; i--) {
    int val;
    EXPECT_TRUE(btree.lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }
}

TEST(CCBTreeDiskTest, MoreLevels) {
  CCBtree<int, int> btree("test.db", false);
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
  EXPECT_EQ(btree.get_height(), 3);
  for (int i = 0; i < data.size(); i++) {
    int val;
    EXPECT_TRUE(btree.lookup(data[i], val));
    EXPECT_EQ(val, data[i]);
  }
}

TEST(CCBTreeDiskTest, CriticalTest) {
  {
    // Single node
    CCBtree<int, int> btree("test.db", false);
    EXPECT_EQ(btree.get_height(), 1);

    int num = 501;
    for (int i = 0; i < num; i++) {
      btree.insert(i, i + 42);
    }
    EXPECT_EQ(btree.get_height(), 1);
    for (int i = num - 1; i >= 0; i--) {
      int val;
      EXPECT_TRUE(btree.lookup(i, val));
      EXPECT_EQ(val, i + 42);
    }
    EXPECT_TRUE(btree.critical_node(1));
  }

  {
    // Three nodes
    CCBtree<int, int> btree("test.db", false);
    EXPECT_EQ(btree.get_height(), 1);

    int num = 250 + 501;
    for (int i = 0; i < num; i++) {
      btree.insert(i, i + 42);
    }
    EXPECT_EQ(btree.get_height(), 2);
    for (int i = num - 1; i >= 0; i--) {
      int val;
      EXPECT_TRUE(btree.lookup(i, val));
      EXPECT_EQ(val, i + 42);
    }
    EXPECT_TRUE(btree.critical_node(2));
  }

  {
    // Two levels
    CCBtree<int, int> btree("test.db", false);
    EXPECT_EQ(btree.get_height(), 1);

    int num = 250*499 + 501;
    for (int i = 0; i < num; i++) {
      btree.insert(i, i + 42);
    }
    EXPECT_EQ(btree.get_height(), 2);
    for (int i = num - 1; i >= 0; i--) {
      int val;
      EXPECT_TRUE(btree.lookup(i, val));
      EXPECT_EQ(val, i + 42);
    }
    EXPECT_TRUE(btree.critical_node(3));
    EXPECT_TRUE(btree.critical_node(501));

    EXPECT_FALSE(btree.critical_node(2));
    EXPECT_FALSE(btree.critical_node(1));
    for (int i = 4; i < 501; i++) {
      EXPECT_FALSE(btree.critical_node(i));
    }
  }

}

TEST(CCBTreeDiskTest, InsertionIOCost) {
  {
    // Single node
    CCBtree<int, int> btree("test.db", false);
    EXPECT_EQ(btree.get_height(), 1);

    int num = 501;
    for (int i = 0; i < num; i++) {
      btree.insert(i, i + 42);
    }
    EXPECT_EQ(btree.get_height(), 1);
    for (int i = num - 1; i >= 0; i--) {
      int val;
      EXPECT_TRUE(btree.lookup(i, val));
      EXPECT_EQ(val, i + 42);
    }
    EXPECT_TRUE(btree.critical_node(1));
    int ionum = btree.insert(num + 1, num + 43);
    EXPECT_EQ(ionum, 4);
  }

  {
    // Three nodes
    CCBtree<int, int> btree("test.db", false);
    EXPECT_EQ(btree.get_height(), 1);

    int num = 250 + 501;
    for (int i = 0; i < num; i++) {
      btree.insert(i, i + 42);
    }
    EXPECT_EQ(btree.get_height(), 2);
    for (int i = num - 1; i >= 0; i--) {
      int val;
      EXPECT_TRUE(btree.lookup(i, val));
      EXPECT_EQ(val, i + 42);
    }
    EXPECT_TRUE(btree.critical_node(2));
    int ionum = btree.insert(num + 1, num + 43);
    EXPECT_EQ(ionum, 5);
  }
}

TEST(CCBTreeDiskTest, BoundedScan) {
  CCBtree<int, int> btree("test.db", false);
  int num = 1000000;
  std::vector<std::pair<int, int>> data;
  for (int i = 0; i < num; i++) {
    data.push_back(std::make_pair(i, i));
  }
  // Generate queries for short range scan: scan 1-2 nodes
  int query_num = 100;
  std::vector<std::pair<int, int>> short_queries;
  int short_range_max = 500;
  int short_range_min = 1;
  for (int i = 0; i < query_num; i++) {
    int low_key = rand() % num;
    int high_key = low_key + rand() % (short_range_max - short_range_min) + short_range_min;
    short_queries.push_back(std::make_pair(low_key, high_key));
  }
  // Generate queries for long range scan: scan >2 nodes
  std::vector<std::pair<int, int>> long_queries;
  int long_range_max = 2000;
  int long_range_min = 1000;
  for (int i = 0; i < query_num; i++) {
    int low_key = rand() % num;
    int high_key = low_key + rand() % (long_range_max - long_range_min) + long_range_min;
    long_queries.push_back(std::make_pair(low_key, high_key));
  }
  // Construct index
  std::random_shuffle(data.begin(), data.end());
  for (int i = 0; i < data.size(); i++) {
    btree.insert(data[i].first, data[i].second);
  }
  std::vector<int> output;
  // Run short range scan
  for (int i = 0; i < short_queries.size(); i++) {
    std::vector<int> output;
    btree.bounded_scan(short_queries[i].first, short_queries[i].second, output);
    int upper_bound = std::min(short_queries[i].second, num - 1);
    EXPECT_EQ(output.size(), upper_bound - short_queries[i].first + 1);
    for (int j = 0; j < output.size(); j++) {
      EXPECT_EQ(output[j], short_queries[i].first + j);
    }
  }

  // Run long range scan
  for (int i = 0; i < long_queries.size(); i++) {
    std::vector<int> output;
    btree.bounded_scan(long_queries[i].first, long_queries[i].second, output);
    int upper_bound = std::min(long_queries[i].second, num - 1);
    EXPECT_EQ(output.size(), upper_bound - long_queries[i].first + 1);
    for (int j = 0; j < output.size(); j++) {
      EXPECT_EQ(output[j], long_queries[i].first + j);
    }
  }
}