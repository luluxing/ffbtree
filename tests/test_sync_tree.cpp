#include <gtest/gtest.h>
#include <algorithm>
#include <vector>
#include <memory>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include "indexes.hpp"

// Type-parameterized test fixture for OneDimMemIndex implementations
template <typename IndexType>
class OneDimMemIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index = std::make_unique<IndexType>();
  }
  
  void TearDown() override {
    index.reset();
  }
  
  std::unique_ptr<IndexType> index;
};

// Define the types we want to test
using OneDimMemIndexTypes = ::testing::Types<
  // BtreeMemIndex<int, int>,
  // BtreeOLCIndex<int, int>
  CCBtreeOLCIndex<int, int>
>;

TYPED_TEST_SUITE(OneDimMemIndexTest, OneDimMemIndexTypes);


TYPED_TEST(OneDimMemIndexTest, InsertAndLookupSingle) {
  this->index->Insert(1, 10);
  int val;
  EXPECT_TRUE(this->index->Lookup(1, val));
  EXPECT_EQ(val, 10);
}

TYPED_TEST(OneDimMemIndexTest, InsertAndLookupMultiple) {
  int num = 100;
  for (int i = 0; i < num; i++) {
    this->index->Insert(i, i + 42);
  }
  
  for (int i = 0; i < num; i++) {
    int val;
    EXPECT_TRUE(this->index->Lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }
}

TYPED_TEST(OneDimMemIndexTest, InsertAndLookupReverse) {
  int num = 100;
  for (int i = 0; i < num; i++) {
    this->index->Insert(i, i + 42);
  }
  
  for (int i = num - 1; i >= 0; i--) {
    int val;
    EXPECT_TRUE(this->index->Lookup(i, val));
    EXPECT_EQ(val, i + 42);
  }
}

TYPED_TEST(OneDimMemIndexTest, InsertAndLookupRandom) {
  int num = 1000;
  std::vector<int> data;
  for (int i = 0; i < num; i++) {
    data.push_back(i);
  }
  
  std::random_shuffle(data.begin(), data.end());
  
  for (int i = 0; i < data.size(); i++) {
    this->index->Insert(data[i], data[i] + 42);
  }
  
  for (int i = 0; i < data.size(); i++) {
    int val;
    EXPECT_TRUE(this->index->Lookup(data[i], val));
    EXPECT_EQ(val, data[i] + 42);
  }
}

TYPED_TEST(OneDimMemIndexTest, LargeSequentialInsert) {
  int num = 50000;
  for (int i = 0; i < num; i++) {
    this->index->Insert(i, i + 42);
  }

  EXPECT_GT(this->index->GetHeight(), 1);
  
  // Verify some random lookups
  for (int i = 0; i < 1000; i++) {
    int key = i * 50;
    int val;
    EXPECT_TRUE(this->index->Lookup(key, val));
    EXPECT_EQ(val, key + 42);
  }
}

TYPED_TEST(OneDimMemIndexTest, ConcurrentInsertSingleLookup) {
  const int num_keys = 10000;
  const int num_threads = 4;
  
  // Create a list of random keys
  std::vector<int> keys;
  for (int i = 0; i < num_keys; i++) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  
  // Calculate range per thread
  const int keys_per_thread = num_keys / num_threads;
  
  // Spawn threads to insert keys concurrently
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([this, &keys, t, keys_per_thread]() {
      int start_idx = t * keys_per_thread;
      int end_idx = (t == num_threads - 1) ? keys.size() : (t + 1) * keys_per_thread;
      
      for (int i = start_idx; i < end_idx; i++) {
        this->index->Insert(keys[i], keys[i] + 42);
      }
    });
  }
  
  // Wait for all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Single thread verifies all keys
  for (int i = 0; i < num_keys; i++) {
    int val;
    EXPECT_TRUE(this->index->Lookup(keys[i], val));
    EXPECT_EQ(val, keys[i] + 42);
  }
}

TYPED_TEST(OneDimMemIndexTest, ConcurrentInsertConcurrentLookup) {
  const int num_keys = 10000;
  const int num_threads = 4;
  
  // Create a list of random keys
  std::vector<int> keys;
  for (int i = 0; i < num_keys; i++) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  
  // Calculate range per thread
  const int keys_per_thread = num_keys / num_threads;
  
  // Spawn threads to insert keys concurrently
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([this, &keys, t, keys_per_thread]() {
      int start_idx = t * keys_per_thread;
      int end_idx = (t == num_threads - 1) ? keys.size() : (t + 1) * keys_per_thread;
      
      for (int i = start_idx; i < end_idx; i++) {
        this->index->Insert(keys[i], keys[i] + 42);
      }
    });
  }
  
  // Wait for all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Clear threads vector for lookup threads
  threads.clear();
  
  // Spawn threads to lookup keys concurrently
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([this, &keys, t, keys_per_thread]() {
      int start_idx = t * keys_per_thread;
      int end_idx = (t == num_threads - 1) ? keys.size() : (t + 1) * keys_per_thread;
      
      for (int i = start_idx; i < end_idx; i++) {
        int val;
        EXPECT_TRUE(this->index->Lookup(keys[i], val));
        EXPECT_EQ(val, keys[i] + 42);
      }
    });
  }
  
  // Wait for all lookup threads to finish
  for (auto& thread : threads) {
    thread.join();
  }
}


TYPED_TEST(OneDimMemIndexTest, RangeScan) {
  int num = 10000;
  std::vector<int> data;
  for (int i = 0; i < num; i++) {
    data.push_back(i);
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
    this->index->Insert(data[i], data[i] + 42);
  }

  // Run short range scan
  for (int i = 0; i < short_queries.size(); i++) {
    std::vector<int> output;
    this->index->Scan(short_queries[i].first, short_queries[i].second, output);
    int upper_bound = std::min(short_queries[i].second, num - 1);
    EXPECT_EQ(output.size(), upper_bound - short_queries[i].first + 1);
    for (int j = 0; j < output.size(); j++) {
      EXPECT_EQ(output[j], short_queries[i].first + j + 42);
    }
  }

  // Run long range scan
  for (int i = 0; i < long_queries.size(); i++) {
    std::vector<int> output;
    this->index->Scan(long_queries[i].first, long_queries[i].second, output);
    int upper_bound = std::min(long_queries[i].second, num - 1);
    EXPECT_EQ(output.size(), upper_bound - long_queries[i].first + 1);
    for (int j = 0; j < output.size(); j++) {
      EXPECT_EQ(output[j], long_queries[i].first + j + 42);
    }
  }
}