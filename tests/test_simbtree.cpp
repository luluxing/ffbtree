#include <gtest/gtest.h>
#include "simulation/sim_btree.hpp"
#include "simulation/dataset_builder.hpp"

TEST(SimBTree, InitConstruction) {
  {
    SimBTree sim_btree(4);
    EXPECT_EQ(sim_btree.GetHeight(), 1);
  }
}

TEST(SimBTree, BuildTree) {
  {
    SimBTree tree(6);
    Data data = {0, 1};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 1);
  }
  {
    SimBTree tree(6, 1, SplitType::FSplitN);
    Data data = {0, 1};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 1);
    EXPECT_EQ(tree.GetHeight(), 1);
  }
  {
    SimBTree tree(6, 1, SplitType::BFSplit);
    Data data = {0, 1};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 1);
    EXPECT_EQ(tree.GetHeight(), 1);
  }
}

TEST(SimBTree, InsertTreeDescending) {
  {
    SimBTree tree(6);
    Data data = {0, 7};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 4);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    SimBTree tree(6, 1, SplitType::FSplitN);
    Data data = {0, 7};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 4);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    SimBTree tree(6, 1, SplitType::BFSplit);
    Data data = {0, 7};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 3);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    SimBTree tree(6);
    Data data = {0, 27};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 3);
    for (int i = 1; i < 7; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 4);
    }
    EXPECT_EQ(tree.GetHeight(), 3);
  }
  {
    SimBTree tree(6, 1, SplitType::FSplitN);
    Data data = {0, 20};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 5);
    for (int i = 1; i < 6; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 3);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
  {
    SimBTree tree(6, 1, SplitType::BFSplit);
    Data data = {0, 20};
    tree.InsertDataItem(data);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 5);
    for (int i = 1; i < 6; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 3);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
}

TEST(SimBTree, InsertTreeAscending) {
  int node_cap = 6;
  DatasetBuilder dataset_builder(node_cap);
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(21, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap);
    tree.InsertDataset(dataset);
    for (int i = 0; i < 5; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetLeafNodeCount(5), 6);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(18, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap, 1, SplitType::FSplitN);
    tree.InsertDataset(dataset);
    for (int i = 0; i < 4; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetLeafNodeCount(4), 6);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 5);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(20, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap, 1, SplitType::BFSplit);
    tree.InsertDataset(dataset);
    for (int i = 0; i < 5; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetLeafNodeCount(5), 5);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 3);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(22, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap);
    tree.InsertDataset(dataset);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(2), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(3), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(4), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(5), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(6), 4);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(22, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap, 1, SplitType::FSplitN);
    tree.InsertDataset(dataset);
    for (int i = 0; i < 6; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetLeafNodeCount(6), 4);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 4);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(22, dataset, DatasetType::ASCENDING);
    SimBTree tree(node_cap, 1, SplitType::BFSplit);
    tree.InsertDataset(dataset);
    for (int i = 0; i < 6; i++) {
      EXPECT_EQ(tree.GetLeafNodeCount(i), 3);
    }
    EXPECT_EQ(tree.GetLeafNodeCount(6), 4);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 4);
    EXPECT_EQ(tree.GetHeight(), 3);
  }
}

TEST(SimBTree, InsertTreeRandom) {
  int node_cap = 6;
  DatasetBuilder dataset_builder(node_cap);
  {
    std::vector<Data> dataset;
    dataset_builder.GenDataset(20, dataset, DatasetType::RANDOM);
    SimBTree tree(node_cap);
    tree.InsertDataset(dataset);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 6);
    EXPECT_EQ(tree.GetLeafNodeCount(2), 5);
    EXPECT_EQ(tree.GetLeafNodeCount(3), 5);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    std::vector<Data> dataset;
    int num = 20;
    dataset_builder.GenDataset(num, dataset, DatasetType::RANDOM);
    SimBTree tree(node_cap, 1, SplitType::FSplitN);
    tree.InsertDataset(dataset);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(2), 5);
    EXPECT_EQ(tree.GetLeafNodeCount(3), 5);
    EXPECT_EQ(tree.GetLeafNodeCount(4), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 5);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
  {
    std::vector<Data> dataset;
    int num = 20;
    dataset_builder.GenDataset(num, dataset, DatasetType::RANDOM);
    SimBTree tree(node_cap, 1, SplitType::BFSplit);
    tree.InsertDataset(dataset);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(2), 5);
    EXPECT_EQ(tree.GetLeafNodeCount(3), 5);
    EXPECT_EQ(tree.GetLeafNodeCount(4), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 5);
    EXPECT_EQ(tree.GetHeight(), 2);
  }
}

TEST(SimBTree, InsertIllProactive) {
  int node_cap = 6;
  DatasetBuilder dataset_builder(node_cap);
  {
    std::vector<Data> dataset;
    int num = 23;
    dataset_builder.GenDataset(num, dataset, DatasetType::ILLPROACTIVE);
    SimBTree tree(node_cap, 1, SplitType::FSplitN);
    // tree.InsertDataset(dataset);
    tree.InsertDataItem(dataset[0]);
    tree.InsertDataItem(dataset[1]);
    tree.InsertDataItem(dataset[2]);
    tree.InsertDataItem(dataset[3]);
    EXPECT_EQ(tree.GetLeafNodeCount(0), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(1), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(2), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(3), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(4), 3);
    EXPECT_EQ(tree.GetLeafNodeCount(5), 4);
    EXPECT_EQ(tree.GetLeafNodeCount(6), 3);
    
    EXPECT_EQ(tree.GetInnerNodeCount(0, 3), 2);
    EXPECT_EQ(tree.GetInnerNodeCount(0, 2), 3);
    EXPECT_EQ(tree.GetInnerNodeCount(1, 2), 4);

    EXPECT_EQ(tree.GetHeight(), 3);
  }
}
