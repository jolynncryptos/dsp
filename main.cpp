#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cmath>
#include <cstring>   

const int BLOCK_SIZE = 4096; 

#pragma pack(push, 1)
struct Record {
    char game_date[16]; 
    int team_id;
    int pts_home;
    float fg_pct_home;
    float ft_pct_home;
    float fg3_pct_home;
    int ast_home;
    int reb_home;
    int home_team_wins;
};
#pragma pack(pop)

// trim helpers
static inline std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

int toIntSafe(const std::string &s) {
    std::string t = trim(s);
    if (t.empty()) return 0;
    try { return std::stoi(t); }
    catch (...) { return 0; }
}

float toFloatSafe(const std::string &s) {
    std::string t = trim(s);
    if (t.empty()) return 0.0f;
    try { return std::stof(t); }
    catch (...) { return 0.0f; }
}

std::vector<std::string> splitBy(const std::string &line, char delim) {
    std::vector<std::string> cols;
    std::stringstream ss(line);
    std::string item;
    while (std::getline(ss, item, delim)) cols.push_back(trim(item));
    return cols;
}

// parse line given a delimiter (tab or comma)
Record parseLine(const std::string& line, char delim) {
    Record rec;
    std::memset(&rec, 0, sizeof(rec));

    auto cols = splitBy(line, delim);

    // If delimiter detection was wrong (very unlikely), try comma as fallback
    if (cols.size() < 9 && delim != ',') {
        auto cols2 = splitBy(line, ',');
        if (cols2.size() >= 9) cols = std::move(cols2);
    }

    // Now map columns (guarded with existence checks)
    if (cols.size() >= 1) {
        std::strncpy(rec.game_date, cols[0].c_str(), sizeof(rec.game_date) - 1);
        rec.game_date[sizeof(rec.game_date) - 1] = '\0';
    }
    if (cols.size() >= 2) rec.team_id = toIntSafe(cols[1]);
    if (cols.size() >= 3) rec.pts_home = toIntSafe(cols[2]);
    if (cols.size() >= 4) rec.fg_pct_home = toFloatSafe(cols[3]);
    if (cols.size() >= 5) rec.ft_pct_home = toFloatSafe(cols[4]);
    if (cols.size() >= 6) rec.fg3_pct_home = toFloatSafe(cols[5]);
    if (cols.size() >= 7) rec.ast_home = toIntSafe(cols[6]);
    if (cols.size() >= 8) rec.reb_home = toIntSafe(cols[7]);
    if (cols.size() >= 9) rec.home_team_wins = toIntSafe(cols[8]);

    return rec;
}

int main() {
    const std::string inputFile = "games.txt";
    const std::string dbFile = "games_db.bin";

    std::ifstream inFile(inputFile);
    if (!inFile.is_open()) {
        std::cerr << "Error: could not open " << inputFile << "\n";
        return 1;
    }

    std::string headerLine;
    if (!std::getline(inFile, headerLine)) {
        std::cerr << "Empty input file\n";
        return 1;
    }

    // detect delimiter from header: prefer tab, else comma
    char delim = (headerLine.find('\t') != std::string::npos) ? '\t' : ',';
    std::cerr << "Detected delimiter: " << (delim == '\t' ? "\\t (TAB)" : ", (COMMA)") << "\n";

    std::ofstream outFile(dbFile, std::ios::binary);
    if (!outFile.is_open()) {
        std::cerr << "Error: could not create " << dbFile << "\n";
        return 1;
    }

    std::string line;
    int total_records = 0;
    while (std::getline(inFile, line)) {
        if (trim(line).empty()) continue;
        Record rec = parseLine(line, delim);
        outFile.write(reinterpret_cast<const char*>(&rec), sizeof(Record));
        total_records++;
    }

    inFile.close();
    outFile.close();

    // statistics
    int record_size = static_cast<int>(sizeof(Record));
    int records_per_block = BLOCK_SIZE / record_size;
    int total_blocks = (total_records + records_per_block - 1) / records_per_block;

    std::cout << "===== Storage Statistics =====\n";
    std::cout << "Record size: " << record_size << " bytes\n";
    std::cout << "Total records: " << total_records << "\n";
    std::cout << "Records per block: " << records_per_block << "\n";
    std::cout << "Total blocks needed: " << total_blocks << "\n";

    // read back first 5
    std::ifstream dbInFile(dbFile, std::ios::binary);
    if (dbInFile.is_open()) {
        std::cout << "\nSample Records (first 5):\n";
        Record r;
        for (int i = 0; i < 5 && dbInFile.read(reinterpret_cast<char*>(&r), sizeof(Record)); ++i) {
            std::cout << "GameDate: " << r.game_date
                      << ", TeamID: " << r.team_id
                      << ", PTS: " << r.pts_home
                      << ", FG%: " << r.fg_pct_home
                      << ", FT%: " << r.ft_pct_home
                      << ", FG3%: " << r.fg3_pct_home
                      << ", AST: " << r.ast_home
                      << ", REB: " << r.reb_home
                      << ", Win: " << r.home_team_wins
                      << "\n";
        }
        dbInFile.close();
    } else {
        std::cerr << "Could not open binary DB file for reading\n";
    }

    return 0;
}
