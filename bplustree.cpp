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
