// #include "databaseFile.h"
// #include <iostream>

// int main() {
//     DatabaseFile db;
//     db.loadData("games.txt");

//     std::cout << "Total records: " << db.numRecords() << "\n";
//     std::cout << "Total blocks: " << db.numBlocks() << "\n";
//     std::cout << "Record size (bytes): " << Record::getRecordSize() << "\n";
//     std::cout << "Records per block: " << BLOCK_SIZE / Record::getRecordSize() << "\n";
//     // db.writeToDisk("nba.bin");
//     return 0;
// }


#include "databasefile.h"
#include "bplustree.h"
#include "record.h"
#include "block.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>


// Task 1: Reporting statistics
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

    for (const auto &block : db.getBlocks()) {  // assuming you have a getBlocks() method
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

void task2(Database& db, size_t blockSize) {
    std::vector<LeafEntry> pairs;
    collect_pairs_ft_pct(db, pairs);
    if (pairs.empty()) {
        std::cout << "No data to index.\n";
        return;
    }

    BPTree tree;
    tree.compute_capacities(blockSize);


    // Build leaves
    auto leaves = build_leaves(tree, pairs);

    // Build internal levels bottom-up until a single root remains
    std::vector<uint32_t> level = leaves;
    uint32_t height = 1; 
    while (level.size() > 1) {
        level = build_internal_level(tree, level);
        height++;
    }
    tree.root_id = level.front();
    tree.levels  = height;

    //Report
    std::cout << "\nB+ Tree (key = FT_PCT_home)\n";
    std::cout << "Order n: " << tree.internal_n << "\n";
    std::cout << "Total nodes: " << tree.nodes.size() << "\n";
    std::cout << "Levels: " << tree.levels << "\n";

    // Root keys
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
}

int main() {
    size_t blockSize = 4096; // 4KB per block
    Database db(blockSize);

    db.loadFromFile("games.txt");
    task1(db);
    task2(blockSize, "games.txt");

    return 0;
}
