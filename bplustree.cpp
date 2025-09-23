#include "bplustree.h"
#include <fstream>
#include <algorithm>
#include <iostream>

BPlusTree::BPlusTree(size_t treeOrder) : order(treeOrder), rootId(0) {
    nodes.push_back(Node{true, {}, {}, {}});  // root is initially a leaf
}

size_t BPlusTree::createNode(bool isLeaf) {
    std::cout << "Creating node " << nodes.size() << " isLeaf=" << isLeaf << std::endl; //debug
    nodes.push_back(Node{isLeaf, {}, {}, {}});
    return nodes.size() - 1;
}

// not big enough
// void BPlusTree::insert(double key, uint32_t recordId) {
//     Node& root = nodes[rootId];

//     if (root.isLeaf) {
//         // Leaf insert
//         auto pos = std::lower_bound(root.keys.begin(), root.keys.end(), key);
//         size_t idx = static_cast<size_t>(pos - root.keys.begin());
//         root.keys.insert(root.keys.begin() + static_cast<std::ptrdiff_t>(idx), key);
//         root.values.insert(root.values.begin() + static_cast<std::ptrdiff_t>(idx), recordId);

//         if (root.keys.size() >= order) {
//             double promotedKey;
//             size_t newNodeId;
//             splitLeaf(rootId, promotedKey, newNodeId);

//             size_t newRoot = createNode(false);
//             nodes[newRoot].keys.push_back(promotedKey);
//             nodes[newRoot].children.push_back(rootId);
//             nodes[newRoot].children.push_back(newNodeId);
//             rootId = newRoot;
//         }
//         return;
//     }

//     // TODO: Recursively insert into the correct child
// }

bool BPlusTree::insertRecursive(size_t nodeId, double key, uint32_t recordId, double &promotedKey, size_t &newNodeId) {
    Node &node = nodes[nodeId];

    if (node.isLeaf) {
        // Insert key
        auto pos = std::lower_bound(node.keys.begin(), node.keys.end(), key);
        // size_t idx = pos - node.keys.begin();
        auto idx = static_cast<std::size_t>(pos - node.keys.begin());

        node.keys.insert(node.keys.begin() + static_cast<std::ptrdiff_t>(idx), key);
        node.values.insert(node.values.begin() + static_cast<std::ptrdiff_t>(idx), recordId);

        // Split if needed
        if (node.keys.size() > order) { // strictly > max keys
            splitLeaf(nodeId, promotedKey, newNodeId);
            return true;
        }
        return false;
    }

    // Internal node
    size_t childIdx = 0;
    while (childIdx < node.keys.size() && key >= node.keys[childIdx])
        ++childIdx;

    double childPromotedKey;
    size_t childNewNodeId;
    bool childSplit = insertRecursive(node.children[childIdx], key, recordId, childPromotedKey, childNewNodeId);

    if (childSplit) {
        // Insert promoted key into current node
        auto pos = std::lower_bound(node.keys.begin(), node.keys.end(), childPromotedKey);
        // size_t idx = pos - node.keys.begin();
        size_t idx = static_cast<std::size_t>(pos - node.keys.begin());

        node.keys.insert(node.keys.begin() + static_cast<std::ptrdiff_t>(idx), childPromotedKey);
        node.children.insert(node.children.begin() + static_cast<std::ptrdiff_t>(idx + 1), childNewNodeId);

        if (node.keys.size() > order) { // split if more than max keys
            splitInternal(nodeId, promotedKey, newNodeId);
            return true;
        }
    }

    return false;
}

void BPlusTree::insert(double key, uint32_t recordId) {
    double promotedKey;
    size_t newNodeId;
    bool split = insertRecursive(rootId, key, recordId, promotedKey, newNodeId);

    if (split) {
        // Root was split → create new root
        size_t newRoot = createNode(false);
        nodes[newRoot].keys.push_back(promotedKey);
        nodes[newRoot].children.push_back(rootId);
        nodes[newRoot].children.push_back(newNodeId);
        rootId = newRoot;
    }
}


void BPlusTree::splitLeaf(size_t leafId, double& promotedKey, size_t& newNodeId) {
    Node& leaf = nodes[leafId];
    size_t mid = leaf.keys.size() / 2;

    newNodeId = createNode(true);
    Node& right = nodes[newNodeId];

    right.keys.assign(leaf.keys.begin() + static_cast<std::ptrdiff_t>(mid), leaf.keys.end());
    right.values.assign(leaf.values.begin() + static_cast<std::ptrdiff_t>(mid), leaf.values.end());

    leaf.keys.erase(leaf.keys.begin() + static_cast<std::ptrdiff_t>(mid), leaf.keys.end());
    leaf.values.erase(leaf.values.begin() + static_cast<std::ptrdiff_t>(mid), leaf.values.end());

    promotedKey = right.keys.front();
}

void BPlusTree::splitInternal(size_t nodeId, double& promotedKey, size_t& newNodeId) {
    Node& node = nodes[nodeId];
    size_t mid = node.keys.size() / 2;

    newNodeId = createNode(false);
    Node& right = nodes[newNodeId];

    promotedKey = node.keys[mid];

    right.keys.assign(node.keys.begin() + static_cast<std::ptrdiff_t>(mid + 1), node.keys.end());
    right.children.assign(node.children.begin() + static_cast<std::ptrdiff_t>(mid + 1), node.children.end());

    node.keys.erase(node.keys.begin() + static_cast<std::ptrdiff_t>(mid), node.keys.end());
    node.children.erase(node.children.begin() + static_cast<std::ptrdiff_t>(mid + 1), node.children.end());
}

int BPlusTree::getLevels() const {
    int levels = 0;
    size_t cur = rootId;
    while (true) {
        levels++;
        if (nodes[cur].isLeaf) break;
        if (nodes[cur].children.empty()) break;
        cur = nodes[cur].children.front();
    }
    return levels;
}

std::vector<double> BPlusTree::getKeysInRoot() const {
    return nodes[rootId].keys;
}

void BPlusTree::saveToDisk(const std::string& filename) const {
    std::ofstream out(filename, std::ios::binary);
    if (!out) return;

    for (const auto& node : nodes) {
        out.write(reinterpret_cast<const char*>(&node.isLeaf), sizeof(node.isLeaf));

        size_t keyCount = node.keys.size();
        out.write(reinterpret_cast<const char*>(&keyCount), sizeof(keyCount));
        out.write(reinterpret_cast<const char*>(node.keys.data()), 
                  static_cast<std::streamsize>(keyCount * sizeof(double)));

        size_t valueCount = node.values.size();
        out.write(reinterpret_cast<const char*>(&valueCount), sizeof(valueCount));
        out.write(reinterpret_cast<const char*>(node.values.data()), 
                  static_cast<std::streamsize>(valueCount * sizeof(uint32_t)));

        size_t childCount = node.children.size();
        out.write(reinterpret_cast<const char*>(&childCount), sizeof(childCount));
        out.write(reinterpret_cast<const char*>(node.children.data()), 
                  static_cast<std::streamsize>(childCount * sizeof(size_t)));
    }
}

