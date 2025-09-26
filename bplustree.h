#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <iomanip>
#include <string>

class Database;
struct Record;

#pragma pack(push, 1)
// header for a node
struct NodeHeader {
    uint8_t is_leaf; // 1 = leaf, 0 = internal
    uint16_t key_count; // number of separator keys
    uint32_t self_id;       
    uint32_t parent_id;     
    uint32_t next_leaf_id; // for leaf-level linked list
};
#pragma pack(pop)

// RID - Record ID: block number, slot number inside the block
// 4B + 4B = 8B
struct RID {
    uint32_t block; //4B
    uint32_t slot; //4B
    // define comparison operators
    bool operator<(const RID& o) const {
        return block < o.block || (block == o.block && slot < o.slot); // compare based on blocks, then slots
    }
    bool operator==(const RID& o) const {
        return block == o.block && slot == o.slot; // 2 RID equal if the block number AND the slot number is the same
    }
};

// (key, RID) pairs
// 4B + 8B = 12B
struct LeafEntry {
    float key; // chosn attribute, FT_PCT_home values
    RID   rid; // (block, slot)
};

// node representation
struct BPTNode {
    NodeHeader header{};
    std::vector<uint32_t> pointers; // child node ids
    std::vector<float> keys; // separator keys
    std::vector<LeafEntry> leaf; 
};

struct BPTree {
    uint32_t internal_n = 0; //max number of children
    uint32_t leaf_capacity = 0; //max number of entries

    std::vector<BPTNode> nodes;
    uint32_t root_id = UINT32_MAX;
    uint32_t levels = 0;

    void compute_capacities(size_t blockSizeBytes) {
        const size_t headerSize = sizeof(NodeHeader);
        // internal node:
        // (n key) + (n+1 pointer) + headerSize
        // -4 is the +1 pointer's size
        // key => 4B, pointer => 4B, so every (key, pointer) is 8B
        size_t kmax = (blockSizeBytes - headerSize - 4) / 8; 
        if (kmax < 1) kmax = 1;
        internal_n = static_cast<uint32_t>(kmax + 1);

        // leaf node:
        // m entries, each contains (key, RID)
        // the next leaf pointer is already in the header
        size_t mmax = (blockSizeBytes - headerSize) / sizeof(LeafEntry);
        if (mmax < 1) mmax = 1;
        leaf_capacity = static_cast<uint32_t>(mmax);
    }

    // create a new node
    uint32_t new_node(bool leaf) {
        BPTNode n;
        if (leaf) n.header.is_leaf = 1;
        else n.header.is_leaf = 0;
        n.header.key_count = 0;
        n.header.parent_id = UINT32_MAX; // root, no parent pointer
        n.header.next_leaf_id = UINT32_MAX; // not leaf, no next leaf pointer
        uint32_t id = static_cast<uint32_t>(nodes.size());
        n.header.self_id = id;
        nodes.push_back(std::move(n));
        return id;
    }

    void saveToBinaryFile(const std::string& filename) const;
    void loadFromBinaryFile(const std::string& filename);

};

void collect_pairs_ft_pct(const Database& db, std::vector<LeafEntry>& out_pairs);
std::vector<uint32_t> build_leaves(BPTree& tree, const std::vector<LeafEntry>& pairs);
std::vector<uint32_t> build_internal_level(BPTree& tree, const std::vector<uint32_t>& child_ids);

#endif 
