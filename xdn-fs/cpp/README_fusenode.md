# fusenode

`fusenode` is a state-diff recorder for XDN, implemented on the FUSE
**low-level API** (`fuse_lowlevel_ops`, nodeid/inode-based). It sits alongside
the other recorders: `fuselog` (C++, FUSE high-level API) and `fuserust`
(Rust). Source: `xdn-fs/cpp/fusenode.cpp`; binary: `bin/fusenode`.

## What it does

Like `fuselog`, it mounts over a service's state directory and records every
change as a state diff, which XDN replicates and re-applies on backups. It
emits the **same v3 wire format** (magic `FLG3`) and is replayed by the **same
`fuselog-apply` binary**, so it is a drop-in capture source.

The difference from the high-level recorder is that fusenode returns **one
nodeid per backing inode** (dedup by `(st_dev, st_ino)`). All hardlink names to
one file therefore share a single page cache and `i_size`, which keeps
`truncate` coherent across hardlinks under `writeback_cache`. The capture is
keyed by a monotonic per-inode `cap_id`, so a hardlinked file keeps one
identity across all its names.

The inode table, `(dev,ino)` lookup, and `lookup`/`forget` refcounting follow
libfuse's `example/passthrough_ll.c`.

## Use it in XDN

Select the recorder with `XDN_PB_STATEDIFF_RECORDER_TYPE=FUSENODE`
(`FusenodeStateDiffRecorder`). Writeback cache is on by default for this
recorder; disable it with `-DFUSELOG_WRITEBACK_CACHE=false` on the AR JVM.

## Environment variables

Reads `FUSENODE_*` with a `FUSELOG_*` fallback, so the launch/socket/capture
pipeline is shared with `fuselog`:

- `FUSENODE_ATTR_TIMEOUT` / `FUSELOG_ATTR_TIMEOUT` — stat-attribute cache
  duration in seconds (default `1.0`; `0` disables caching).
- `FUSELOG_WRITEBACK_CACHE` — enable/disable the kernel writeback cache
  (default on for fusenode).
- `FUSENODE_PROFILE` — set to `1` to emit lightweight upcall counters (off by
  default; for debugging only).

## Build & test

```bash
./bin/build_xdn_fuselog.sh        # builds fuselog, fuselog-apply, and fusenode
```

The FUSE differential/database fuzz harnesses under `xdn-fs/test/` cover
fusenode too; point them at the binary with `FUSELOG_BIN=bin/fusenode`
(see `xdn-fs/test/README.md`).
