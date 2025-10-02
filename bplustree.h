#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <iomanip>
#include <string>
#include <chrono>

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
    
    bool isUnderflow(size_t min_keys) const {
        if (header.is_leaf) {
            return leaf.size() < min_keys;
        } else {
            return keys.size() < min_keys;
        }
    }
};

struct BPTree {
    uint32_t internal_n = 0; //max number of children
    uint32_t leaf_capacity = 0; //max number of entries

    std::vector<BPTNode> nodes;
    uint32_t root_id = UINT32_MAX;
    uint32_t levels = 0;

    // Task 3: Delete records with FT_PCT_home > 0.9
    struct DeletionStats {
        size_t index_nodes_accessed = 0;
        size_t data_blocks_accessed = 0;
        size_t games_deleted = 0;
        double average_ft_pct = 0.0;
        double running_time_ms = 0.0;
        size_t linear_scan_blocks = 0;
        double linear_scan_time_ms = 0.0;
    };

    void compute_capacities(size_t blockSizeBytes) {
        const size_t node_header_size = sizeof(NodeHeader);
        size_t kmax = (blockSizeBytes - node_header_size - sizeof(uint32_t)) / (sizeof(float) + sizeof(uint32_t));
        if (kmax < 1) kmax = 1;
        internal_n = static_cast<uint32_t>(kmax + 1);

        size_t mmax = (blockSizeBytes - node_header_size) / sizeof(LeafEntry);
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

    // Task 3 methods
    DeletionStats deleteHighFTPCT(Database& db, float threshold = 0.9f);
    
private:
    // Helper methods for deletion
    std::vector<LeafEntry> findRecordsGreaterThan(float threshold);
    void deleteFromDatabase(Database& db, const std::vector<LeafEntry>& to_delete);
    bool isNodeUnderflow(uint32_t node_id);
    void handleUnderflow(uint32_t node_id);
    void updateInternalKeys(uint32_t node_id);
    float findMinKeyInSubtree(uint32_t node_id);
    float findActualMinKey(uint32_t node_id);
};

void collect_pairs_ft_pct(const Database& db, std::vector<LeafEntry>& out_pairs);
std::vector<uint32_t> build_leaves(BPTree& tree, const std::vector<LeafEntry>& pairs);
std::vector<uint32_t> build_internal_level(BPTree& tree, const std::vector<uint32_t>& child_ids);

#endif