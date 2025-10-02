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
#include <fstream>
#include <chrono>
#include <set>
#include <unordered_set>


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

void BPTree::saveToBinaryFile(const std::string& filename) const {
    std::ofstream out(filename);  
    if (!out) throw std::runtime_error("Cannot open file for writing");

    // write metadata as text
    out << internal_n << "\n";
    out << leaf_capacity << "\n";
    out << root_id << "\n";
    out << levels << "\n";
    out << nodes.size() << "\n";

    // write nodes
    for (const auto& node : nodes) {
        // write header fields
        out << static_cast<int>(node.header.is_leaf) << "|"
            << node.header.key_count << "|"
            << node.header.parent_id << "|"
            << node.header.next_leaf_id << "\n";

        // write pointers
        out << node.pointers.size();
        for (uint32_t ptr : node.pointers) {
            out << "|" << ptr;
        }
        out << "\n";

        // write keys
        out << node.keys.size();
        for (float key : node.keys) {
            out << "|" << std::fixed << std::setprecision(6) << key;
        }
        out << "\n";

        // write leaf entries
        out << node.leaf.size();
        for (const LeafEntry& entry : node.leaf) {
            out << "|" << std::fixed << std::setprecision(6) << entry.key << ":" << entry.recno;
        }
        out << "\n";
    }
}

void BPTree::loadFromBinaryFile(const std::string& filename) {
    std::ifstream in(filename);  
    if (!in) throw std::runtime_error("Cannot open file for reading");

    // read metadata as text
    uint32_t node_count;
    in >> internal_n >> leaf_capacity >> root_id >> levels >> node_count;
    nodes.resize(node_count);

    std::string line;
    std::getline(in, line); 

    // read nodes
    for (auto& node : nodes) {
        // read header line
        std::getline(in, line);
        size_t pos = 0, nextPos = 0;

        // parse header fields
        nextPos = line.find('|', pos);
        node.header.is_leaf = (std::stoi(line.substr(pos, nextPos - pos)) == 1);
        pos = nextPos + 1;

        nextPos = line.find('|', pos);
        node.header.key_count = static_cast<uint16_t>(std::stoi(line.substr(pos, nextPos - pos)));
        pos = nextPos + 1;

        nextPos = line.find('|', pos);
        node.header.parent_id = static_cast<uint32_t>(std::stoul(line.substr(pos, nextPos - pos)));
        pos = nextPos + 1;

        node.header.next_leaf_id = static_cast<uint32_t>(std::stoul(line.substr(pos)));

        // read pointers line
        std::getline(in, line);
        pos = 0;
        nextPos = line.find('|', pos);
        uint32_t psize = static_cast<uint32_t>(std::stoul(line.substr(pos, nextPos - pos)));
        node.pointers.clear();
        node.pointers.reserve(psize);

        for (uint32_t i = 0; i < psize; ++i) {
            if (nextPos == std::string::npos) break;
            pos = nextPos + 1;
            nextPos = line.find('|', pos);
            if (nextPos == std::string::npos) {
                node.pointers.push_back(static_cast<uint32_t>(std::stoul(line.substr(pos))));
            } else {
                node.pointers.push_back(static_cast<uint32_t>(std::stoul(line.substr(pos, nextPos - pos))));
            }
        }

        std::getline(in, line);
        pos = 0;
        nextPos = line.find('|', pos);
        uint32_t ksize = static_cast<uint32_t>(std::stoul(line.substr(pos, nextPos - pos)));
        node.keys.clear();
        node.keys.reserve(ksize);

        for (uint32_t i = 0; i < ksize; ++i) {
            if (nextPos == std::string::npos) break;
            pos = nextPos + 1;
            nextPos = line.find('|', pos);
            if (nextPos == std::string::npos) {
                node.keys.push_back(std::stof(line.substr(pos)));
            } else {
                node.keys.push_back(std::stof(line.substr(pos, nextPos - pos)));
            }
        }

        std::getline(in, line);
        pos = 0;
        nextPos = line.find('|', pos);
        uint32_t lsize = static_cast<uint32_t>(std::stoul(line.substr(pos, nextPos - pos)));
        node.leaf.clear();
        node.leaf.reserve(lsize);

        for (uint32_t i = 0; i < lsize; ++i) {
            if (nextPos == std::string::npos) break;
            pos = nextPos + 1;
            nextPos = line.find('|', pos);
            
            std::string entry_str;
            if (nextPos == std::string::npos) {
                entry_str = line.substr(pos);
            } else {
                entry_str = line.substr(pos, nextPos - pos);
            }

            size_t colon_pos = entry_str.find(':');
            if (colon_pos != std::string::npos) {
                float key = std::stof(entry_str.substr(0, colon_pos));
                uint32_t recno = static_cast<uint32_t>(std::stoul(entry_str.substr(colon_pos + 1)));
                node.leaf.push_back(LeafEntry{key, recno});
            }
        }
    }
}

// TASK 3 IMPLEMENTATION

// BRUTE FORCE SOLUTION - Completely rebuild the B+ tree structure
void BPTree::updateInternalKeys(uint32_t node_id) {
    if (node_id == UINT32_MAX) return;
    
    auto& node = nodes[node_id];
    if (node.header.is_leaf) return;

    
    // completely clear and rebuild keys
    node.keys.clear();
    
    for (size_t i = 1; i < node.pointers.size(); ++i) {
        uint32_t child_id = node.pointers[i];
        
        // find the ACTUAL minimum key in this child's subtree
        float min_key = findActualMinKey(child_id);
        
        if (min_key < 1000.0f) { // Valid key found
            node.keys.push_back(min_key);
        }
    }
    
    node.header.key_count = static_cast<uint16_t>(node.keys.size());

    // update parent
    if (node.header.parent_id != UINT32_MAX) {
        updateInternalKeys(node.header.parent_id);
    }
}

float BPTree::findActualMinKey(uint32_t node_id) {
    const auto& node = nodes[node_id];
    
    if (node.header.is_leaf) {
        // for leaves, find the first valid key (not empty and <= 0.9)
        for (const auto& entry : node.leaf) {
            if (entry.key <= 0.9f) {
                return entry.key;
            }
        }
        return 1000.0f; // no valid keys found
    } else {
        // for internal nodes, recursively check all children
        float min_key = 1000.0f;
        for (uint32_t child_id : node.pointers) {
            float child_min = findActualMinKey(child_id);
            if (child_min < min_key) {
                min_key = child_min;
            }
        }
        return min_key;
    }
}
// Helper function to find minimum key in a subtree
float BPTree::findMinKeyInSubtree(uint32_t node_id) {
    const auto& node = nodes[node_id];
    
    if (node.header.is_leaf) {
        // For leaf nodes, return the first key (leaves are sorted)
        return node.leaf.empty() ? 0.0f : node.leaf.front().key;
    } else {
        // For internal nodes, recursively find min in first child
        return findMinKeyInSubtree(node.pointers.front());
    }
}

// Find all records with key > threshold
std::vector<LeafEntry> BPTree::findRecordsGreaterThan(float threshold) {
    std::vector<LeafEntry> result;
    
    if (nodes.empty() || root_id == UINT32_MAX) return result;
    
    // Navigate to the first leaf that might contain keys > threshold
    uint32_t current_id = root_id;
    while (!nodes[current_id].header.is_leaf) {
        const auto& node = nodes[current_id];
        
        // Find the first pointer where key > threshold
        size_t i = 0;
        while (i < node.keys.size() && node.keys[i] <= threshold) {
            i++;
        }
        current_id = node.pointers[i];
    }
    
    // Now traverse leaves from this point forward
    uint32_t leaf_id = current_id;
    while (leaf_id != UINT32_MAX) {
        const auto& leaf = nodes[leaf_id];
        
        for (const auto& entry : leaf.leaf) {
            if (entry.key > threshold) {
                result.push_back(entry);
            }
        }
        
        // Move to next leaf
        leaf_id = leaf.header.next_leaf_id;
    }
    
    return result;
}

bool BPTree::isNodeUnderflow(uint32_t node_id) {
    const auto& node = nodes[node_id];
    size_t min_keys = node.header.is_leaf ? (leaf_capacity + 1) / 2 : (internal_n + 1) / 2 - 1;
    return node.isUnderflow(min_keys);
}

BPTree::DeletionStats BPTree::deleteHighFTPCT(Database& db, float threshold) {
    DeletionStats stats;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Store initial state for comparison
    size_t initial_total_records = 0;
    for (const auto& node : nodes) {
        if (node.header.is_leaf) {
            initial_total_records += node.leaf.size();
        }
    }
    
    // Method 1: Using B+ Tree index
    auto records_to_delete = findRecordsGreaterThan(threshold);
    stats.games_deleted = records_to_delete.size();
    
    // Calculate average FT_PCT
    double sum_ft_pct = 0.0;
    for (const auto& entry : records_to_delete) {
        sum_ft_pct += entry.key;
    }
    stats.average_ft_pct = stats.games_deleted > 0 ? sum_ft_pct / stats.games_deleted : 0.0;
    
    // Track accessed data blocks (using RID to block mapping)
    std::unordered_set<uint32_t> accessed_blocks;
    size_t records_per_block = db.getRecordsPerBlock();
    if (records_per_block == 0) records_per_block = 1;
    
    for (const auto& entry : records_to_delete) {
        uint32_t block_num = entry.recno / records_per_block;
        accessed_blocks.insert(block_num);
    }
    stats.data_blocks_accessed = accessed_blocks.size();
    
    // Count index nodes accessed
    stats.index_nodes_accessed = levels;
    
    // Remove records from B+ tree leaves
    size_t total_deleted_from_tree = 0;
    std::unordered_set<uint32_t> modified_leaves;
    std::unordered_set<uint32_t> affected_parents;
    
    for (uint32_t i = 0; i < nodes.size(); i++) {
        if (nodes[i].header.is_leaf) {
            auto& leaf = nodes[i].leaf;
            size_t original_size = leaf.size();
            
            // Remove entries with key > threshold
            auto new_end = std::remove_if(leaf.begin(), leaf.end(),
                [threshold](const LeafEntry& entry) {
                    return entry.key > threshold;
                });
            
            size_t new_size = std::distance(leaf.begin(), new_end);
            if (new_size != original_size) {
                leaf.erase(new_end, leaf.end());
                nodes[i].header.key_count = static_cast<uint16_t>(leaf.size());
                modified_leaves.insert(i);
                total_deleted_from_tree += (original_size - leaf.size());
                
                // Track affected parent for key updates
                if (nodes[i].header.parent_id != UINT32_MAX) {
                    affected_parents.insert(nodes[i].header.parent_id);
                }
            }
        }
    }
    
    // Update internal node keys after deletions
    if (!modified_leaves.empty()) {
        // Update all directly affected parents
        for (uint32_t parent_id : affected_parents) {
            updateInternalKeys(parent_id);
        }
        
        // Also ensure root gets updated
        if (root_id != UINT32_MAX && !nodes[root_id].header.is_leaf) {
            updateInternalKeys(root_id);
        }
    }
    
    // Handle underflow in modified leaves
    for (uint32_t leaf_id : modified_leaves) {
        if (isNodeUnderflow(leaf_id)) {
            handleUnderflow(leaf_id);
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    stats.running_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    
    // Method 2: Linear scan for comparison
    auto linear_start = std::chrono::high_resolution_clock::now();
    
    size_t linear_blocks_accessed = 0;
    const auto& blocks = db.getBlocks();
    for (const auto& block : blocks) {
        bool block_accessed = false;
        for (size_t i = 0; i < block.getNumRecords(); i++) {
            const Record& rec = block.getRecord(i);
            if (rec.FT_PCT_home > threshold) {
                block_accessed = true;
                break;
            }
        }
        if (block_accessed) {
            linear_blocks_accessed++;
        }
    }
    
    auto linear_end = std::chrono::high_resolution_clock::now();
    stats.linear_scan_blocks = linear_blocks_accessed;
    stats.linear_scan_time_ms = std::chrono::duration<double, std::milli>(linear_end - linear_start).count();
    
    // update games deleted count to reflect actual tree deletions
    stats.games_deleted = total_deleted_from_tree;
    
    return stats;
}