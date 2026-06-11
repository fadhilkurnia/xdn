#!/usr/bin/env python3
"""Continuously dirty a fixed RAM working set with tiny (1-byte-per-page) changes,
at a capped rate. Such small-delta pages are the ideal case for QEMU XBZRLE, which
re-sends only the compressed XOR delta of each re-dirtied page instead of the full
4 KiB. Used to exercise live-migration convergence under load.

    python3 dirty.py [working_set_MB] [rate_MB_per_s]    # defaults: 256 MiB, 40 MB/s
"""
import sys, time

MB = 1 << 20
PAGE = 4096
ws = (int(sys.argv[1]) if len(sys.argv) > 1 else 256) * MB
rate = (int(sys.argv[2]) if len(sys.argv) > 2 else 40) * MB

buf = bytearray(ws)
npages = ws // PAGE
for i in range(npages):              # fault every page in once
    buf[i * PAGE] = 1

per_tick = max(1, (rate // PAGE) // 10)   # 10 ticks per second
idx = 0
while True:
    for _ in range(per_tick):
        o = (idx % npages) * PAGE
        buf[o] = (buf[o] + 1) & 0xFF      # 1-byte write dirties the whole 4K page
        idx += 1
    time.sleep(0.1)
