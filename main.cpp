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

void task2(size_t blockSize, const std::string &dataFile) {
    std::cout << "Task 2: Building B+ tree index on FT_PCT_home\n";
    BPlusTree tree(blockSize);

    std::ifstream file(dataFile);
    if (!file.is_open()) {
        std::cerr << "Cannot open dataset file: " << dataFile << "\n";
        return;
    }

    std::string line;
    if (!std::getline(file, line)) {
        std::cerr << "Empty file or no header\n";
        return;
    } // skip header

    uint32_t recordId = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        // parse 5th token (FT_PCT_home) using tab delimiter
        std::stringstream ss(line);
        std::string token;
        int col = 0;
        bool parsed = false;
        double ft_pct = 0.0;

        while (std::getline(ss, token, '\t')) {
            if (col == 4) { // 0-based: 0 date,1 team,2 pts,3 FG_PCT,4 FT_PCT
                // remove leading/trailing spaces
                while (!token.empty() && isspace((unsigned char)token.back())) token.pop_back();
                size_t p = 0;
                while (p < token.size() && isspace((unsigned char)token[p])) ++p;
                std::string trimmed = token.substr(p);
                try {
                    ft_pct = trimmed.empty() ? 0.0 : std::stod(trimmed);
                    parsed = true;
                } catch (...) {
                    parsed = false;
                }
                break;
            }
            ++col;
        }

        if (!parsed) {
            // skip malformed line
            ++recordId; // keep recordId consistent with file rows
            continue;
        }

        tree.insert(ft_pct, recordId);
        ++recordId;
    }

    // Save nodes to disk (bptree/node_X.txt)
    tree.saveToDisk("bptree");

    // Report statistics
    std::cout << "\nTask 2 Report:\n";
    std::cout << "---------------------------------\n";
    std::cout << "Order (n) of B+ tree: " << tree.getOrder() << "\n";
    std::cout << "Number of nodes: " << tree.getNumNodes() << "\n";
    std::cout << "Number of levels: " << tree.getLevels() << "\n";

    std::vector<double> rootKeys = tree.getKeysInRoot();
    std::cout << "Keys in root node (" << rootKeys.size() << "):\n";
    // print up to first 50 keys (avoid giant output)
    size_t limit = std::min<size_t>(rootKeys.size(), 50);
    for (size_t i = 0; i < limit; ++i) {
        std::cout << rootKeys[i];
        if (i + 1 < limit) std::cout << ", ";
    }
    if (rootKeys.size() > limit) std::cout << ", ...";
    std::cout << "\n---------------------------------\n";
}

int main() {
    size_t blockSize = 4096; // 4KB per block
    Database db(blockSize);

    db.loadFromFile("games.txt");
    task1(db);
    task2(blockSize, "games.txt");

    return 0;
}
