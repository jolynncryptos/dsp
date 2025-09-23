#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <vector>
#include <cstddef>   // for size_t
#include <cstdint>   // for uint32_t
#include <string>

class BPlusTree {
public:
    struct Node {
        bool isLeaf;
        std::vector<double> keys;      // FT_PCT_home as key
        std::vector<uint32_t> values;  // record IDs (only used if leaf)
        std::vector<size_t> children;  // indices of child nodes
    };

    explicit BPlusTree(size_t treeOrder);

    bool insertRecursive(size_t nodeId, double key, uint32_t recordId, double &promotedKey, size_t &newNodeId);

    void insert(double key, uint32_t recordId);

    // Statistics
    size_t getOrder() const { return order; }
    size_t getNumNodes() const { return nodes.size(); }
    int getLevels() const;
    std::vector<double> getKeysInRoot() const;

    // Disk I/O (optional)
    void saveToDisk(const std::string& filename) const;

private:
    size_t order;   // maximum number of children in internal nodes
    size_t rootId;  // index of root node in nodes vector
    std::vector<Node> nodes;

    size_t createNode(bool isLeaf);

    void splitLeaf(size_t leafId, double& promotedKey, size_t& newNodeId);
    void splitInternal(size_t nodeId, double& promotedKey, size_t& newNodeId);
};

#endif // BPLUSTREE_H
