#!/usr/bin/env python3
"""Dummy bandwidth map: replicas in a circle, bidirectional weighted edges.

Renders to PDF. Replace EDGES with real (src_node, dst_node, bytes) tuples
collected from the eBPF tcp_sendmsg / tcp_cleanup_rbuf hooks.
"""

import networkx as nx
import matplotlib.pyplot as plt

# Dummy app-byte traffic in MB between active replicas only.
# Primary-backup shape: AR0 (leader) fans out large state diffs to all backups;
# backups send small acks back. Backup<->backup traffic omitted.
EDGES = [
    ("AR0", "AR1", 240.0),
    ("AR1", "AR0", 12.0),
    ("AR0", "AR2", 240.0),
    ("AR2", "AR0", 12.0),
    ("AR0", "AR3", 240.0),
    ("AR3", "AR0", 12.0),
    ("AR0", "AR4", 240.0),
    ("AR4", "AR0", 12.0),
]

OUTPUT = "replica_traffic.pdf"


def main():
    G = nx.DiGraph()
    G.add_weighted_edges_from(EDGES)

    pos = nx.circular_layout(G)

    fig, ax = plt.subplots(figsize=(10, 10))

    nx.draw_networkx_nodes(
        G, pos,
        node_size=3200,
        node_color="#4C72B0",
        edgecolors="black",
        linewidths=1.5,
        ax=ax,
    )
    nx.draw_networkx_labels(
        G, pos,
        font_size=14,
        font_color="white",
        font_weight="bold",
        ax=ax,
    )

    weights = [G[u][v]["weight"] for u, v in G.edges()]
    max_w = max(weights)
    widths = [0.6 + 5.0 * (w / max_w) for w in weights]

    nx.draw_networkx_edges(
        G, pos,
        width=widths,
        edge_color="#444444",
        connectionstyle="arc3,rad=0.18",
        arrowsize=22,
        arrowstyle="-|>",
        node_size=3200,
        ax=ax,
    )

    edge_labels = {(u, v): f"{w:.1f} MB" for u, v, w in G.edges(data="weight")}
    nx.draw_networkx_edge_labels(
        G, pos,
        edge_labels=edge_labels,
        font_size=9,
        label_pos=0.3,
        connectionstyle="arc3,rad=0.18",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1.0),
        ax=ax,
    )

    ax.set_xlim(-1.4, 1.4)
    ax.set_ylim(-1.4, 1.4)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(OUTPUT, format="pdf", bbox_inches="tight")
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    main()
