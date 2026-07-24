#!/usr/bin/env python3
"""Render per-phase edge-delta matrices from measure_cluster_bw.py output."""
import json
import sys


def phase_delta(samples, ar, t0, t1):
    """Per-edge (tx,rx) byte deltas for one AR between the first/last sample in [t0,t1]."""
    win = [s for s in samples if s["ar"] == ar and "edges" in s and t0 <= s["t"] <= t1]
    if len(win) < 2:
        return {}
    first, last = win[0], win[-1]

    def edgemap(s):
        return {e["peer"]: (e["txBytes"], e["rxBytes"]) for e in s["edges"]}

    f, l = edgemap(first), edgemap(last)
    out = {}
    for peer in l:
        f_tx, f_rx = f.get(peer, (0, 0))
        out[peer] = (l[peer][0] - f_tx, l[peer][1] - f_rx)
    return out


def main(paths):
    for path in paths:
        d = json.load(open(path))
        print(f"\n===== {d['service']} ({d['kind']}) =====")
        for ph in d["phases"]:
            dur = ph["t1"] - ph["t0"]
            print(f"\n-- phase {ph['name']} ({dur:.0f}s, {ph['ops']} ops) --")
            print(f"{'AR':<12}{'edge':<12}{'tx(B)':>12}{'rx(B)':>12}{'tx B/s':>10}{'rx B/s':>10}")
            for ar in d["ars"]:
                self_name = f"replica-{d['ars'].index(ar)}"
                for peer, (tx, rx) in sorted(phase_delta(d["samples"], ar, ph["t0"], ph["t1"]).items()):
                    tag = " (self)" if peer == self_name else ""
                    print(f"{ar:<12}{peer + tag:<12}{tx:>12,}{rx:>12,}{tx/dur:>10,.0f}{rx/dur:>10,.0f}")
            if ph["name"] == "write" and ph["ops"]:
                # Coordination cost per write op: sum of peer-edge tx across
                # ARs / ops, MINUS the idle-phase baseline (heartbeats and
                # gossip continue during the write phase; at low op rates the
                # baseline otherwise dominates the per-op figure). Self-edges
                # (in-pod control clients hairpinning the member's own
                # overlay address) are excluded.
                idle = next((p for p in d["phases"] if p["name"] == "idle"), None)

                def peer_tx_rate(t0, t1):
                    tot = 0.0
                    for ar in d["ars"]:
                        self_name = f"replica-{d['ars'].index(ar)}"
                        for peer, (tx, _) in phase_delta(d["samples"], ar, t0, t1).items():
                            if peer.startswith("replica") and peer != self_name:
                                tot += tx
                    return tot / max(t1 - t0, 1e-9)

                w_rate = peer_tx_rate(ph["t0"], ph["t1"])
                base = peer_tx_rate(idle["t0"], idle["t1"]) if idle else 0.0
                raw = w_rate * dur / ph["ops"]
                net = max(w_rate - base, 0.0) * dur / ph["ops"]
                print(f"   coordination tx per write op: {net:,.0f} B"
                      f" (raw {raw:,.0f} B, idle baseline {base:,.0f} B/s)")


if __name__ == "__main__":
    main(sys.argv[1:])
