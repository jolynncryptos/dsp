#ifndef BLOCK_H
#define BLOCK_H

#include "record.h"
#include <vector>

class Block {
private:
    std::vector<Record> records;
    size_t blockSize; // max size in bytes

public:
    Block(size_t size);
    bool addRecord(const Record &record);
    size_t getNumRecords() const;
    size_t getBlockSize() const;
    const Record& getRecord(size_t idx) const { return records.at(idx); }
};

#endif
