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

static void writeString(std::ofstream& out, const std::string& s) {
    size_t len = s.size();
    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
    out.write(s.data(), static_cast<std::streamsize>(len));
}

static std::string readString(std::ifstream& in) {
    size_t len;
    in.read(reinterpret_cast<char*>(&len), sizeof(len));
    std::string s(len, '\0');
    in.read(&s[0], static_cast<std::streamsize>(len));
    return s;
}



// Save blocks to binary file
void Database::saveToBinaryFile(const std::string &dbFile) const {
    std::ofstream out(dbFile, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open file for writing: " + dbFile);

    // Write metadata
    out.write(reinterpret_cast<const char*>(&blockSize), sizeof(blockSize));
    out.write(reinterpret_cast<const char*>(&recordSize), sizeof(recordSize));
    out.write(reinterpret_cast<const char*>(&totalRecords), sizeof(totalRecords));

    size_t numBlocks = blocks.size();
    out.write(reinterpret_cast<const char*>(&numBlocks), sizeof(numBlocks));

    for (const auto& blk : blocks) {
        size_t numRecords = blk.getNumRecords();
        out.write(reinterpret_cast<const char*>(&numRecords), sizeof(numRecords));

        for (size_t i = 0; i < numRecords; i++) {
            const Record& rec = blk.getRecord(i);

            // serialise 
            writeString(out, rec.GAME_DATE_EST);

            out.write(reinterpret_cast<const char*>(&rec.TEAM_ID_home), sizeof(rec.TEAM_ID_home));
            out.write(reinterpret_cast<const char*>(&rec.PTS_home), sizeof(rec.PTS_home));
            out.write(reinterpret_cast<const char*>(&rec.FG_PCT_home), sizeof(rec.FG_PCT_home));
            out.write(reinterpret_cast<const char*>(&rec.FT_PCT_home), sizeof(rec.FT_PCT_home));
            out.write(reinterpret_cast<const char*>(&rec.FG3_PCT_home), sizeof(rec.FG3_PCT_home));
            out.write(reinterpret_cast<const char*>(&rec.AST_home), sizeof(rec.AST_home));
            out.write(reinterpret_cast<const char*>(&rec.REB_home), sizeof(rec.REB_home));
            out.write(reinterpret_cast<const char*>(&rec.HOME_TEAM_WINS), sizeof(rec.HOME_TEAM_WINS));
        }
    }
}


// Load blocks from binary file (test if the binary file works)
void Database::loadFromBinaryFile(const std::string &dbFile) {
    std::ifstream in(dbFile, std::ios::binary);
    if (!in) throw std::runtime_error("Cannot open file for reading: " + dbFile);

    blocks.clear();
    totalRecords = 0;

    // Read metadata
    in.read(reinterpret_cast<char*>(&blockSize), sizeof(blockSize));
    in.read(reinterpret_cast<char*>(&recordSize), sizeof(recordSize));
    in.read(reinterpret_cast<char*>(&totalRecords), sizeof(totalRecords));

    size_t numBlocks;
    in.read(reinterpret_cast<char*>(&numBlocks), sizeof(numBlocks));

    for (size_t b = 0; b < numBlocks; b++) {
        Block blk(blockSize);

        size_t numRecords;
        in.read(reinterpret_cast<char*>(&numRecords), sizeof(numRecords));

        for (size_t i = 0; i < numRecords; i++) {
            Record rec;

            // deserialise
            rec.GAME_DATE_EST = readString(in);

            in.read(reinterpret_cast<char*>(&rec.TEAM_ID_home), sizeof(rec.TEAM_ID_home));
            in.read(reinterpret_cast<char*>(&rec.PTS_home), sizeof(rec.PTS_home));
            in.read(reinterpret_cast<char*>(&rec.FG_PCT_home), sizeof(rec.FG_PCT_home));
            in.read(reinterpret_cast<char*>(&rec.FT_PCT_home), sizeof(rec.FT_PCT_home));
            in.read(reinterpret_cast<char*>(&rec.FG3_PCT_home), sizeof(rec.FG3_PCT_home));
            in.read(reinterpret_cast<char*>(&rec.AST_home), sizeof(rec.AST_home));
            in.read(reinterpret_cast<char*>(&rec.REB_home), sizeof(rec.REB_home));
            in.read(reinterpret_cast<char*>(&rec.HOME_TEAM_WINS), sizeof(rec.HOME_TEAM_WINS));

            blk.addRecord(rec);
        }
        blocks.push_back(blk);
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

