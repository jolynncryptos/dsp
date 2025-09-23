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

