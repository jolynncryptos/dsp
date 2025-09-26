#include "bplustree.h"
#include "databasefile.h"
#include "block.h"
#include "record.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <cstdint>
#include <functional>
#include <iomanip>

// read heap file and collect (key, RID) pairs
void collect_pairs_ft_pct(const Database& db, std::vector<LeafEntry>& out_pairs) {
    const auto& blocks = db.getBlocks();
    uint32_t recno = 0;
    for (const auto& blk : blocks) {
        const size_t n = blk.getNumRecords();
        for (size_t i = 0; i < n; ++i) {
            const Record& r = blk.getRecord(i);
            out_pairs.push_back(LeafEntry{ static_cast<float>(r.FT_PCT_home), recno });
            recno++;
        }
    }

    // then sort
    std::stable_sort(out_pairs.begin(), out_pairs.end(),
        [](const LeafEntry& a, const LeafEntry& b){
            if (a.key != b.key) return a.key < b.key;
            return a.recno < b.recno;
        });
}

// bulk-load leaves
std::vector<uint32_t> build_leaves(BPTree& tree, const std::vector<LeafEntry>& pairs) {
    std::vector<uint32_t> leaf_ids;
    size_t i = 0, N = pairs.size();

    while (i < N) {
        const size_t take = std::min<size_t>(tree.leaf_capacity, N - i);

        uint32_t id = tree.new_node(true);
        BPTNode& leaf = tree.nodes[id];

        leaf.leaf.insert(leaf.leaf.end(), pairs.begin() + static_cast<long>(i), pairs.begin() + static_cast<long>(i + take));
        leaf.header.key_count = static_cast<uint16_t>(take);

        // link previous leaf
        if (!leaf_ids.empty()) tree.nodes[leaf_ids.back()].header.next_leaf_id = id;

        leaf_ids.push_back(id);
        i += take;
    }
    return leaf_ids;
}

// Return the minimal key (foor using it as a separator)
static float min_key_of_node(const BPTree& tree, uint32_t nid) {
    const BPTNode& c = tree.nodes[nid];
    if (c.header.is_leaf) return c.leaf.front().key;
    else return min_key_of_node(tree, c.pointers.front());
}

// Build one internal level above
std::vector<uint32_t> build_internal_level(BPTree& tree, const std::vector<uint32_t>& child_ids) {
    std::vector<uint32_t> level_ids;
    if (child_ids.empty()) return level_ids;

    const size_t fanout = tree.internal_n;
    size_t i = 0, N = child_ids.size();

    while (i < N) {
        const size_t take = std::min<size_t>(fanout, N - i);

        uint32_t id = tree.new_node(false);
        BPTNode& node = tree.nodes[id];

        node.pointers.reserve(take);
        for (size_t j = 0; j < take; ++j) {
            const uint32_t cid = child_ids[i + j];
            node.pointers.push_back(cid);
            tree.nodes[cid].header.parent_id = id;
        }

        // Separator keys are min of each right child
        if (take) node.keys.reserve(take - 1);
        else node.keys.reserve(0);

        for (size_t j = 1; j < node.pointers.size(); ++j) {
            node.keys.push_back(min_key_of_node(tree, node.pointers[j]));
        }
        node.header.key_count = static_cast<uint16_t>(node.keys.size());

        level_ids.push_back(id);
        i += take;
    }
    return level_ids;
}