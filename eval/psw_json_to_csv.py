#!/usr/bin/env python3
"""Convert protocol_switch_demo.py JSON output to per-sample CSV.

  python3 eval/psw_json_to_csv.py results.json [more.json ...]
Writes <name>.csv next to each input.
"""
import json
import sys


def convert(path):
    d = json.load(open(path))
    sw = d.get("switch_at_rel", "")
    pad = d.get("pad_bytes", "")
    out = path.rsplit(".", 1)[0] + ".csv"
    with open(out, "w") as f:
        f.write("t_s,latency_ms,ok,phase,frontend,switch_at_s,pad_bytes\n")
        for s in sorted(d["samples"], key=lambda x: x["t"]):
            f.write("%s,%s,%d,%s,%s,%s,%s\n" % (
                s["t"], s["latency_ms"], 1 if s["ok"] else 0,
                s["phase"], s.get("frontend", ""), sw, pad))
    print(f"wrote {out}")


if __name__ == "__main__":
    for p in sys.argv[1:]:
        convert(p)
