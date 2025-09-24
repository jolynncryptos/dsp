#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cmath>
#include <cstring>


const int BLOCK_SIZE = 4096; 
void build_bpt_on_ft_pct(const std::string& dbFile);

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
    build_bpt_on_ft_pct("games_db.bin");

    return 0;
}

// ------------------------------------- B+ TREE (key = ft_pct_home) — Task 2 ---------------------------------------------
#include <algorithm>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#pragma pack(push, 1)
// header for a node
struct NodeHeader {
    uint8_t is_leaf; // 1 = leaf, 0 = internal
    uint16_t key_count; // number of separator keys
    uint32_t self_id;       
    uint32_t parent_id;     
    uint32_t next_leaf_id; // for leaf-level linked list
};
#pragma pack(pop)

// (key, RID) pairs
struct LeafEntry {
    float key;
    uint32_t recno; 
};

// node representation
struct BPTNode {
    NodeHeader header{};
    std::vector<uint32_t> pointers; // child node ids
    std::vector<float> keys; // separator keys
    std::vector<LeafEntry> leaf;
};

struct BPTree {
    uint32_t internal_n = 0; //max number of children
    uint32_t leaf_capacity = 0; //max number of entries

    std::vector<BPTNode> nodes;
    uint32_t root_id = UINT32_MAX;
    uint32_t levels = 0;

    void compute_capacities() {
        const size_t NodeHeader = sizeof(NodeHeader);
        size_t kmax = (BLOCK_SIZE - NodeHeader - 4) / 8;
        if (kmax < 1) kmax = 1;
        internal_n = static_cast<uint32_t>(kmax + 1);

        size_t mmax = (BLOCK_SIZE - NodeHeader) / sizeof(LeafEntry);
        if (mmax < 1) mmax = 1;
        leaf_capacity = static_cast<uint32_t>(mmax);
    }

    // create a new node
    uint32_t new_node(bool leaf) {
        BPTNode n;
        if (leaf) n.header.is_leaf = 1;
        else n.header.is_leaf = 0;
        n.header.key_count = 0;
        n.header.parent_id = UINT32_MAX; //root, no parent pointer
        n.header.next_leaf_id = UINT32_MAX; // not leaf, no next leaf pointer
        uint32_t id = static_cast<uint32_t>(nodes.size());
        n.header.self_id = id;
        nodes.push_back(std::move(n));
        return id;
    }
};

// read heap file and collect (key, RID) pairs
static void collect_pairs_ft_pct(const std::string& dbFile, std::vector<LeafEntry>& out_pairs) {
    std::ifstream db(dbFile, std::ios::binary);
    if (!db.is_open()) {
        std::cerr << "Error: cannot open " << dbFile << " for B+ build\n";
        return;
    }
    Record r;
    uint32_t recno = 0;
    while (db.read(reinterpret_cast<char*>(&r), sizeof(Record))) {
        out_pairs.push_back(LeafEntry{ r.ft_pct_home, recno });
        recno++;
    }
    db.close();

    // then sort
    std::stable_sort(out_pairs.begin(), out_pairs.end(),
        [](const LeafEntry& a, const LeafEntry& b){
            if (a.key != b.key) return a.key < b.key;
            return a.recno < b.recno;
        });
}

// bulk-load leaves
static std::vector<uint32_t> build_leaves(BPTree& tree, const std::vector<LeafEntry>& pairs) {
    std::vector<uint32_t> leaf_ids;
    size_t i = 0, N = pairs.size();

    while (i < N) {
        const size_t take = std::min<size_t>(tree.leaf_capacity, N - i);

        uint32_t id = tree.new_node(true);
        BPTNode& leaf = tree.nodes[id];

        leaf.leaf.insert(leaf.leaf.end(), pairs.begin() + static_cast<long>(i), pairs.begin() + static_cast<long>(i + take));
        leaf.header.key_count = static_cast<uint16_t>(take);

        // link previous leaf
        if (!leaf_ids.empty()) tree.nodes[leaf_ids.back()].header.next_leaf_id = id;

        leaf_ids.push_back(id);
        i += take;
    }
    return leaf_ids;
}

// Return the minimal key (foor using it as a separator)
static float min_key_of_node(const BPTree& tree, uint32_t nid) {
    const BPTNode& c = tree.nodes[nid];
    if (c.header.is_leaf) return c.leaf.front().key;
    else return min_key_of_node(tree, c.pointers.front());
}

// Build one internal level above
static std::vector<uint32_t> build_internal_level(BPTree& tree, const std::vector<uint32_t>& child_ids) {
    std::vector<uint32_t> level_ids;
    if (child_ids.empty()) return level_ids;

    const size_t fanout = tree.internal_n;
    size_t i = 0, N = child_ids.size();

    while (i < N) {
        const size_t take = std::min<size_t>(fanout, N - i);

        uint32_t id = tree.new_node(false);
        BPTNode& node = tree.nodes[id];

        node.pointers.reserve(take);
        for (size_t j = 0; j < take; ++j) {
            const uint32_t cid = child_ids[i + j];
            node.pointers.push_back(cid);
            tree.nodes[cid].header.parent_id = id;
        }

        // Separator keys are min of each right child
        if (take) node.keys.reserve(take - 1);
        else node.keys.reserve(0);

        for (size_t j = 1; j < node.pointers.size(); ++j) {
            node.keys.push_back(min_key_of_node(tree, node.pointers[j]));
        }
        node.header.key_count = static_cast<uint16_t>(node.keys.size());

        level_ids.push_back(id);
        i += take;
    }
    return level_ids;
}

void build_bpt_on_ft_pct(const std::string& dbFile) {
    std::vector<LeafEntry> pairs;
    collect_pairs_ft_pct(dbFile, pairs);
    if (pairs.empty()) {
        std::cerr << "No data to index.\n";
        return;
    }

    BPTree tree;
    tree.compute_capacities();

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
