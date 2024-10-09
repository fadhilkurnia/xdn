package edu.umass.cs.consistency.EventualConsistency.Test;

import java.util.*;

class GraphNode {
    int value;
    List<GraphNode> children;

    GraphNode(int value) {
        this.value = value;
        this.children = new ArrayList<>();
    }
}

public class Test {

    public static void main(String[] args) {
        // Create example graph nodes
        GraphNode root = new GraphNode(10);
        GraphNode node1 = new GraphNode(20);
        GraphNode node2 = new GraphNode(30);
        GraphNode node3 = new GraphNode(40);
        GraphNode node4 = new GraphNode(50);

        // Build the graph (as a tree structure)
        root.children.add(node1);
        root.children.add(node2);
        node1.children.add(node3);
        node1.children.add(node4);
        printGraph(root);

        // Get leaf nodes
        List<GraphNode> leafNodes = getLeafNodes(root);

        // Modify leaf nodes in the main function
        for (GraphNode leaf : leafNodes) {
            leaf.value = 100; // Set the new value for the leaf nodes
        }
        System.out.println("After modification");
        // Print the graph to check the modification
        printGraph(root);
    }

    // Recursive function to collect leaf nodes
    static List<GraphNode> getLeafNodes(GraphNode node) {
        List<GraphNode> leafNodes = new ArrayList<>();
        if (node == null) return leafNodes;

        if (node.children.isEmpty()) {
            // Node is a leaf node, add to the list
            leafNodes.add(node);
        } else {
            // Recursively call for all children
            for (GraphNode child : node.children) {
                leafNodes.addAll(getLeafNodes(child));
            }
        }
        return leafNodes;
    }

    // Function to print the graph (DFS)
    static void printGraph(GraphNode node) {
        if (node == null) return;

        System.out.println("Node Value: " + node.value);
        for (GraphNode child : node.children) {
            printGraph(child);
        }
    }
}
