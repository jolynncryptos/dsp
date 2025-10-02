#include "databasefile.h"
#include "record.h"
#include <fstream>
#include <iostream>
#include <stdexcept>

Database::Database(size_t blkSize)
    : blockSize(blkSize), recordSize(0), totalRecords(0) {}

void Database::loadFromFile(const std::string &filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    std::string line;
    // Skip header row
    if (!std::getline(file, line)) return;

    Block currentBlock(blockSize);

    while (std::getline(file, line)) {
        if (line.empty()) continue;  // skip blank lines

        try {
            Record r = Record::fromCSV(line);

            if (recordSize == 0) {
                recordSize = r.size(); // record size from first real record
            }

            if (!currentBlock.addRecord(r)) {
                blocks.push_back(currentBlock);
                currentBlock = Block(blockSize);
                currentBlock.addRecord(r);
            }

            ++totalRecords;
        } catch (const std::exception &e) {
            std::cerr << "Skipping line due to parse error: " << e.what() << "\n";
            continue;
        }
    }

    if (currentBlock.getNumRecords() > 0) {
        blocks.push_back(currentBlock);
    }
}


// store data in dummy binary file 
void Database::saveToBinaryFile(const std::string &filename) const {
    std::ofstream out(filename); 
    if (!out) {
        std::cerr << "Error opening file for writing: " << filename << "\n";
        return;
    }

    // write metadata as text
    out << blockSize << "\n";
    out << recordSize << "\n"; 
    out << totalRecords << "\n";
    out << blocks.size() << "\n";

    // write blocks
    for (const auto& block : blocks) {
        out << block.getNumRecords() << "\n";

        for (size_t i = 0; i < block.getNumRecords(); ++i) {
            const Record& r = block.getRecord(i);

            // write all fields separated by |
            out << r.GAME_DATE_EST << "|"
                << r.TEAM_ID_home << "|"
                << r.PTS_home << "|"
                << r.FG_PCT_home << "|"
                << r.FT_PCT_home << "|"
                << r.FG3_PCT_home << "|"
                << r.AST_home << "|"
                << r.REB_home << "|"
                << r.HOME_TEAM_WINS << "\n";
        }
    }
}

// load binary file to test that it is working
void Database::loadFromBinaryFile(const std::string &dbFile) {
    std::ifstream in(dbFile);  
    if (!in) {
        std::cerr << "Error opening file for reading: " << dbFile << "\n";
        return;
    }

    // read metadata 
    in >> blockSize >> recordSize >> totalRecords;

    size_t numBlocks = 0;
    in >> numBlocks;

    blocks.clear();
    blocks.reserve(numBlocks);

    std::string line;
    std::getline(in, line); 

    for (size_t b = 0; b < numBlocks; ++b) {
        size_t numRecs = 0;
        in >> numRecs;
        std::getline(in, line); 

        Block block(blockSize);
        for (size_t i = 0; i < numRecs; ++i) {
            std::getline(in, line);
            if (line.empty()) continue;

            Record r;
            
            size_t pos = 0;
            size_t nextPos = 0;
            
            // parse data
            nextPos = line.find('|', pos);
            r.GAME_DATE_EST = line.substr(pos, nextPos - pos);
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.TEAM_ID_home = std::stoi(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.PTS_home = std::stoi(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.FG_PCT_home = std::stof(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.FT_PCT_home = std::stof(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.FG3_PCT_home = std::stof(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.AST_home = std::stoi(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            nextPos = line.find('|', pos);
            r.REB_home = std::stoi(line.substr(pos, nextPos - pos));
            pos = nextPos + 1;

            r.HOME_TEAM_WINS = std::stoi(line.substr(pos));

            block.addRecord(r);
        }
        blocks.push_back(block);
    }
}


size_t Database::getRecordSize() const {
    return recordSize;
}

size_t Database::getTotalRecords() const {
    return totalRecords;
}

size_t Database::getRecordsPerBlock() const {
    if (blocks.empty()) return 0;
    // average records per block (integer)
    return totalRecords / blocks.size();
}

size_t Database::getNumBlocks() const {
    return blocks.size();
}

