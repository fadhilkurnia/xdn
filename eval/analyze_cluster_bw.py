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
                # coordination cost per write op: sum of peer-edge tx across ARs
                # / ops; self-edges (in-container control clients, e.g. the
                # antidote rpc driver) are excluded
                tot = 0
                for ar in d["ars"]:
                    self_name = f"replica-{d['ars'].index(ar)}"
                    for peer, (tx, _) in phase_delta(d["samples"], ar, ph["t0"], ph["t1"]).items():
                        if peer.startswith("replica") and peer != self_name:
                            tot += tx
                print(f"   coordination tx per write op: {tot / ph['ops']:,.0f} B")


if __name__ == "__main__":
    main(sys.argv[1:])
