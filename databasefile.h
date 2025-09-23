#ifndef DATABASEFILE_H
#define DATABASEFILE_H

#include "block.h"
#include <vector>
#include <string>

class Database {
private:
    std::vector<Block> blocks;
    size_t blockSize;
    size_t recordSize;
    size_t totalRecords;

public:
    explicit Database(size_t blockSize);
    void loadFromFile(const std::string &filename);

    // getters for task 1
    size_t getRecordSize() const;
    size_t getTotalRecords() const;
    size_t getRecordsPerBlock() const; // average records per block
    size_t getNumBlocks() const;
    const std::vector<Block>& getBlocks() const { 
        return blocks; 
    }
};

#endif // DATABASEFILE_H

