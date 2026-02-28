#include <gtest/gtest.h>
#include <algorithm>
#include "btree/btree_disk.hpp"

using namespace btree;

// Define a parameterized test fixture
class BTreeDiskTest : public ::testing::TestWithParam<InsertStrategy> {
 protected:
  void SetUp() override {}
  void TearDown() override {
    // Clean up the database file after each test
    std::remove("test.db");
  }
};

TEST_P(BTreeDiskTest, InitConstruction) {
  InsertStrategy strategy = GetParam();
  BTreeDisk<int, int> btree("test.db", 10, strategy);
  EXPECT_EQ(btree.get_height(), 1);
}

TEST_P(BTreeDiskTest, InsertAndLookupSingle) {
  InsertStrategy strategy = GetParam();
  BTreeDisk<int, int> btree("test.db", 10, strategy);
  EXPECT_EQ(btree.get_height(), 1);
  btree.insert(1, 10);
  int val;
  EXPECT_TRUE(btree.lookup(1, val));
  EXPECT_EQ(val, 10);
}

TEST_P(BTreeDiskTest, InsertAndLookupSequential) {
  InsertStrategy strategy = GetParam();
  BTreeDisk<int, int> btree("test.db", 10, strategy);
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

TEST_P(BTreeDiskTest, InsertAndLookupRand) {
  InsertStrategy strategy = GetParam();
  BTreeDisk<int, int> btree("test.db", 10, strategy);
  EXPECT_EQ(btree.get_height(), 1);
  int num = 519*260+520;
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

TEST(BTreeDiskTest, InsertionIOCost) {
  BTreeDisk<int, int> btree("test.db", 10, InsertStrategy::Normal);
  EXPECT_EQ(btree.get_height(), 1);
  int io_num = btree.insert(1, 10);
  EXPECT_EQ(io_num, 1);

  // int num = 519*260+520;
  // for (int i = 0; i < num; i++) {
  //   io_num = btree.insert(i, i + 42);
  // }
  // EXPECT_EQ(btree.get_height(), 3);
  // io_num = btree.insert(num + 1, num + 42);
  // EXPECT_EQ(io_num, 4);
}

TEST(BTreeDisk, BulkLoadAndLookup) {

}

TEST(BTreeDisk, RangeScan) {

}

// Instantiate the test suite with different insertion strategies
INSTANTIATE_TEST_SUITE_P(
  InsertStrategies,
  BTreeDiskTest,
  ::testing::Values(
      InsertStrategy::Normal,
      // InsertStrategy::Fsplit1,
      InsertStrategy::FsplitN
  )
);