#include <gtest/gtest.h>
#include "rtree/rtree.hpp"

TEST(RTree, InitConstruction) {
  RTree* rtree = new RTree();
  EXPECT_EQ(rtree->height_, 1);
  delete rtree;
}