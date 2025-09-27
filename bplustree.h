#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <iomanip>

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

// (key, RID) pairs
struct LeafEntry {
    float key;
    uint32_t recno; 
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
        const size_t NodeHeader = sizeof(NodeHeader);
        size_t kmax = (blockSizeBytes - NodeHeader - 4) / 8;
        if (kmax < 1) kmax = 1;
        internal_n = static_cast<uint32_t>(kmax + 1);

        size_t mmax = (blockSizeBytes - NodeHeader) / sizeof(LeafEntry);
        if (mmax < 1) mmax = 1;
        leaf_capacity = static_cast<uint32_t>(mmax);
    }

    // create a new node
    uint32_t new_node(bool leaf) {
        BPTNode n;
        if (leaf) n.header.is_leaf = 1;
        else n.header.is_leaf = 0;
        n.header.key_count = 0;
        n.header.parent_id = UINT32_MAX; //root, no parent pointer
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