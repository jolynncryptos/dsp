#include "databasefile.h"
#include "bplustree.h"
#include "record.h"
#include "block.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <iomanip>


void task1(Database &db) {
    const std::string inputFile = "games.txt";
    const std::string dbFile = "games_db.bin";
    std::cout << "Task 1 Report:" << std::endl;
    std::cout << "---------------------------------" << std::endl;
    std::cout << "Record size (bytes): " << db.getRecordSize() << std::endl;
    std::cout << "Total number of records: " << db.getTotalRecords() << std::endl;
    std::cout << "Number of records per block: " << db.getRecordsPerBlock() << std::endl;
    std::cout << "Number of blocks for storing data: " << db.getNumBlocks() << std::endl;
    std::cout << "---------------------------------" << std::endl;

    std::cout << "\nSample Records (first 5):\n";
    int printed = 0;

    // show that data is parsed correctly
    for (const auto &block : db.getBlocks()) {  
        for (size_t i = 0; i < block.getNumRecords() && printed < 5; ++i) {
            const Record r = block.getRecord(i);
            std::cout << "GameDate: " << r.GAME_DATE_EST
                      << ", TeamID: " << r.TEAM_ID_home
                      << ", PTS: " << r.PTS_home
                      << ", FG%: " << r.FG_PCT_home
                      << ", FT%: " << r.FT_PCT_home
                      << ", FG3%: " << r.FG3_PCT_home
                      << ", AST: " << r.AST_home
                      << ", REB: " << r.REB_home
                      << ", Win: " << r.HOME_TEAM_WINS
                      << "\n";
            ++printed;
        }
        if (printed >= 5) break;
    }
    std::cout << "---------------------------------" << std::endl;
    std::cout << "\n" << std::endl;
}

BPTree task2(Database& db, size_t blockSize) {
    std::cout << "Task 2 Report:";
    std::cout << "\n";
    std::vector<LeafEntry> pairs;
    collect_pairs_ft_pct(db, pairs);
    if (pairs.empty()) {
        std::cout << "No data to index.\n";
        return BPTree{};
    }

    BPTree tree;
    tree.compute_capacities(blockSize);

    // build leaves
    auto leaves = build_leaves(tree, pairs);

    // build internal levels bottom-up until a single root remains
    std::vector<uint32_t> level = leaves;
    uint32_t height = 1; 
    while (level.size() > 1) {
        level = build_internal_level(tree, level);
        height++;
    }
    tree.root_id = level.front();
    tree.levels  = height;

    //report
    std::cout << "\nB+ Tree (key = FT_PCT_home)\n";
    std::cout << "Order n: " << tree.internal_n << "\n";
    std::cout << "Total nodes: " << tree.nodes.size() << "\n";
    std::cout << "Levels: " << tree.levels << "\n";

    // root keys
    const BPTNode& root = tree.nodes[tree.root_id];
    std::cout << std::fixed << std::setprecision(4);
    std::cout << "Root keys: ";
    if (root.header.is_leaf) {
        for (size_t i = 0; i < root.leaf.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << root.leaf[i].key;
            if (i >= 19 && i + 1 < root.leaf.size()) { std::cout << ", ..."; break; }
        }
    } else {
        for (size_t i = 0; i < root.keys.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << root.keys[i];
        }
    }
    std::cout << "\n";

    return tree;
}

void task3(BPTree& tree, Database& db) {
    std::cout << "Task 3 Report: Delete records with FT_PCT_home > 0.9" << std::endl;
    std::cout << "=====================================================" << std::endl;
    
    // Get tree stats before deletion
    size_t initial_nodes = tree.nodes.size();
    uint32_t initial_levels = tree.levels;
    
    std::cout << "\nB+ Tree Statistics Before Deletion:" << std::endl;
    std::cout << "----------------------------------" << std::endl;
    std::cout << "Number of nodes: " << initial_nodes << std::endl;
    std::cout << "Number of levels: " << initial_levels << std::endl;
    
    // Root node content before deletion
    const BPTNode& root_before = tree.nodes[tree.root_id];
    std::cout << "Root node keys (before): ";
    if (root_before.header.is_leaf) {
        for (size_t i = 0; i < root_before.leaf.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << std::fixed << std::setprecision(4) << root_before.leaf[i].key;
            if (i >= 9 && i + 1 < root_before.leaf.size()) {
                std::cout << ", ...";
                break;
            }
        }
    } else {
        for (size_t i = 0; i < root_before.keys.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << std::fixed << std::setprecision(4) << root_before.keys[i];
            if (i >= 9 && i + 1 < root_before.keys.size()) {
                std::cout << ", ...";
                break;
            }
        }
    }
    std::cout << std::endl;

    // Perform deletion
    auto stats = tree.deleteHighFTPCT(db, 0.9f);
    
    // Get tree stats after deletion
    size_t final_nodes = tree.nodes.size();
    uint32_t final_levels = tree.levels;
    
    // Report statistics
    std::cout << "\nDeletion Statistics:" << std::endl;
    std::cout << "--------------------" << std::endl;
    std::cout << "Number of index nodes accessed: " << stats.index_nodes_accessed << std::endl;
    std::cout << "Number of data blocks accessed: " << stats.data_blocks_accessed << std::endl;
    std::cout << "Number of games deleted: " << stats.games_deleted << std::endl;
    std::cout << "Average FT_PCT_home of deleted records: " 
              << std::fixed << std::setprecision(4) << stats.average_ft_pct << std::endl;
    std::cout << "Running time (B+ Tree method): " << std::fixed << std::setprecision(2) 
              << stats.running_time_ms << " ms" << std::endl;
    std::cout << "Number of data blocks accessed (linear scan): " << stats.linear_scan_blocks << std::endl;
    std::cout << "Running time (linear scan): " << std::fixed << std::setprecision(2) 
              << stats.linear_scan_time_ms << " ms" << std::endl;
    
    std::cout << "\nB+ Tree Statistics After Deletion:" << std::endl;
    std::cout << "----------------------------------" << std::endl;
    std::cout << "Number of nodes: " << final_nodes << std::endl;
    std::cout << "Number of levels: " << final_levels << std::endl;
    
    // Root node content after deletion
    const BPTNode& root_after = tree.nodes[tree.root_id];
    std::cout << "Root node keys (after): ";
    if (root_after.header.is_leaf) {
        for (size_t i = 0; i < root_after.leaf.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << std::fixed << std::setprecision(4) << root_after.leaf[i].key;
            if (i >= 9 && i + 1 < root_after.leaf.size()) {
                std::cout << ", ...";
                break;
            }
        }
    } else {
        for (size_t i = 0; i < root_after.keys.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << std::fixed << std::setprecision(4) << root_after.keys[i];
            if (i >= 9 && i + 1 < root_after.keys.size()) {
                std::cout << ", ...";
                break;
            }
        }
    }
    std::cout << std::endl;
    
    std::cout << "\nComparison:" << std::endl;
    std::cout << "-----------" << std::endl;
    if (stats.running_time_ms > 0) {
        double speedup = stats.linear_scan_time_ms / stats.running_time_ms;
        std::cout << "B+ Tree is " << std::fixed << std::setprecision(2) << speedup 
                  << "x faster than linear scan" << std::endl;
        
        if (speedup < 1.0) {
            std::cout << "  (Linear scan is actually " << std::fixed << std::setprecision(2) 
                      << (1.0 / speedup) << "x faster)" << std::endl;
        }
    }
    if (stats.data_blocks_accessed > 0) {
        double block_ratio = static_cast<double>(stats.linear_scan_blocks) / stats.data_blocks_accessed;
        std::cout << "B+ Tree accesses " << std::fixed << std::setprecision(2) << block_ratio 
                  << "x fewer blocks than linear scan" << std::endl;
    }
    
    std::cout << "\nTree Changes:" << std::endl;
    std::cout << "-------------" << std::endl;
    std::cout << "Nodes removed: " << (initial_nodes - final_nodes) << std::endl;
    std::cout << "Levels changed: " << (initial_levels == final_levels ? "No" : "Yes") << std::endl;
    
    std::cout << "=====================================================" << std::endl;
}

int main() {
    size_t blockSize = 4096; 
    
    // Load the main database
    Database db(blockSize);
    std::cout << "Loading data from games.txt..." << std::endl;
    db.loadFromFile("games.txt");
    db.saveToBinaryFile("games.bin");
    
    // Use the same database instance for all tasks to ensure consistency
    std::cout << "\n";
    task1(db);

    // Build B+ tree (Task 2) using the same database instance
    std::cout << "\nBuilding B+ Tree Index..." << std::endl;
    BPTree tree = task2(db, blockSize);
    tree.saveToBinaryFile("bplustree.bin");
    
    // Test binary file loading for tree
    BPTree loadedTree;
    loadedTree.loadFromBinaryFile("bplustree.bin");
    
    std::cout << "\n[Tree Verification]" << std::endl;
    std::cout << "Original Tree - Nodes: " << tree.nodes.size() 
              << ", Root: " << tree.root_id 
              << ", Levels: " << tree.levels << std::endl;
    std::cout << "Loaded Tree  - Nodes: " << loadedTree.nodes.size() 
              << ", Root: " << loadedTree.root_id 
              << ", Levels: " << loadedTree.levels << std::endl;

    // Perform Task 3 - Delete records with FT_PCT_home > 0.9
    // Use the same database instance that was used to build the tree
    std::cout << "\n";
    std::cout << "*****************************************************" << std::endl;
    std::cout << "               EXECUTING TASK 3" << std::endl;
    std::cout << "*****************************************************" << std::endl;
    task3(loadedTree, db);  // Use 'db' instead of 'db2' for consistency
    
    // Save updated tree
    loadedTree.saveToBinaryFile("bplustree_updated.bin");
    std::cout << "\nUpdated B+ tree saved to: bplustree_updated.bin" << std::endl;

    return 0;
}