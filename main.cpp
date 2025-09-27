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
    const std::string dbFile = "games_db.bin"; //whERE IS THE DBFILEEEEEEEE
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
}


BPTree task2(Database& db, size_t blockSize) {
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

int main() {
    size_t blockSize = 4096; 
    Database db(blockSize);
    // std::cout << "\ndebug code\n" << std::endl;
    db.loadFromFile("games.txt");
    // std::cout << "\ndebug code2\n" << std::endl;
    db.saveToBinaryFile("games.bin");
    // task1(db);
    // std::cout << "\ndebug code3\n" << std::endl;
    
    // test to show binary file works
    Database db2(blockSize);
    db2.loadFromBinaryFile("games.bin");
    // std::cout << "\n[After reloading from binary]\n" << std::endl;
    task1(db2);

    BPTree tree = task2(db, blockSize);
    tree.saveToBinaryFile("bplustree.bin");
    // std::cout << "\ndebug code4\n" << std::endl;


    // test to show binary file works 
    BPTree loadedTree;
    loadedTree.loadFromBinaryFile("bplustree.bin");
    // std::cout << "\ndebug code5\n" << std::endl;
    std::cout << "\n[Original Tree]\n";
    std::cout << "Nodes: " << tree.nodes.size() 
              << ", Root: " << tree.root_id 
              << ", Levels: " << tree.levels << "\n";

    std::cout << "[Loaded Tree]\n";
    std::cout << "Nodes: " << loadedTree.nodes.size() 
              << ", Root: " << loadedTree.root_id 
              << ", Levels: " << loadedTree.levels << "\n";


    return 0;
}