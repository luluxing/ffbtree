/**
 * This code adopts the code from the following repository:
 * https://github.com/rmitbggroup/LearnedIndexDiskExp/blob/main/code/b%2B_tree/utility.h
 */

#ifndef B_TREE_UTILS_HPP
#define B_TREE_UTILS_HPP

static const uint64_t pageSize=4*1024;
static const int bitmapSize=75; // A node stores more than 500 entries, so 75 bits are enough
enum class PageType : uint8_t { BTreeInner=1, BTreeLeaf=2 };

typedef struct MetaNode {
  int block_count;
  int root_block_id;
  int height;
  int offset;
  // use for hybrid case
  int last_block;
} MetaNode;
#define MetaNodeSize sizeof(MetaNode)

typedef struct {
  char data[pageSize];
} Block;

template<typename KeyType>
struct InnerNodeItem {
  KeyType key;
  int block_id;
};

template<typename KeyType, typename ValueType>
struct LeafNodeItem{
  KeyType key;
  ValueType value;
};

template<typename KeyType>
constexpr size_t InnerNodeItemSize = sizeof(InnerNodeItem<KeyType>);

template<typename KeyType, typename ValueType>
constexpr size_t LeafNodeItemSize = sizeof(LeafNodeItem<KeyType, ValueType>);


namespace btree {

typedef struct NodeHeader {
  PageType node_type;
  int block_id;
  short item_count;
  // int level;
} NodeHeader;
#define NodeHeaderSize sizeof(NodeHeader)

} // namespace btree

namespace blinktree {

template<typename KeyType>
struct BlinkNodeHeader {
  PageType node_type;
  int block_id;
  short item_count;
  int sibling_block_id;
  KeyType high_key;
};

} // namespace blinktree

namespace ccbtree {

struct CCNodeHeader {
  PageType node_type;
  int block_id;
  short item_count;
  bool is_critical;
  bool maybe_critical;
  uint8_t critical[bitmapSize];
};
#define CCNodeHeaderSize sizeof(CCNodeHeader)

#define BITMAP_SET(bitmap, pos) ((bitmap)[(pos) / 8] |= 1 << ((pos) % 8))

#define BITMAP_GET(bitmap, pos) (((bitmap)[(pos) / 8] >> ((pos) % 8)) & 1)

} // namespace ccbtree

int CountBitmapOnes(const uint8_t* bitmap) {
  int count = 0;
  for (int i = 0; i < bitmapSize; ++i) {
      uint8_t byte = bitmap[i];
      // Count bits in byte
      while (byte) {
          count += byte & 1;
          byte >>= 1;
      }
  }
  return count;
}

template<typename NodeType>
void split_bitmap(NodeType* n, NodeType* new_n) {
  uint8_t temp_bitmap[bitmapSize];
  memcpy(temp_bitmap, n->critical, sizeof(uint8_t) * bitmapSize);
  memset(new_n->critical, 0, sizeof(uint8_t) * bitmapSize);
  memset(n->critical, 0, sizeof(uint8_t) * bitmapSize);

  // Copy lower part to n->critical
  for (int i = 0; i < n->item_count; ++i) {
    if (BITMAP_GET(temp_bitmap, i)) {
      BITMAP_SET(n->critical, i);
    }
  }

  // Copy upper part to new_n->critical
  for (int j = 0; j < new_n->item_count; ++j) {
    if (BITMAP_GET(temp_bitmap, n->item_count + j)) {
      BITMAP_SET(new_n->critical, j);
    }
  }
}


#endif // B_TREE_UTILS_HPP