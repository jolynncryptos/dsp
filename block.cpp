#include "block.h"

Block::Block(size_t size) : blockSize(size) {}

bool Block::addRecord(const Record &record) {
    size_t currentSize = 0;
    for (const auto &r : records) {
        currentSize += r.size();
    }
    if (currentSize + record.size() <= blockSize) {
        records.push_back(record);
        return true;
    }
    return false;
}

size_t Block::getNumRecords() const {
    return records.size();
}

size_t Block::getBlockSize() const {
    return blockSize;
}
