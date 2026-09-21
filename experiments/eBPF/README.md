# eBPF VFS state-diff recorder

`statediff_vfs` captures filesystem mutations under one directory tree;
`statediff_apply_vfs` replays a captured `VFS1` batch under a target tree.

## Build

Requires kernel BTF, Clang/LLVM with the BPF target, libelf, zlib, and git.

```bash
make
```

libbpf and bpftool are cloned into `.deps/` on the first build, pinned to one
libbpf-bootstrap commit (`LIBBPF_BOOTSTRAP_COMMIT` in the Makefile), so that
first build needs network access. They are build-time only; the binaries link
libbpf statically. `make clean` keeps them, `make distclean` removes them.

## Standalone mode

Start the capturer, then stop it with `SIGINT` or `SIGTERM` to write the batch:

```bash
sudo ./statediff_vfs <target_dir> [output.sd]
./statediff_apply_vfs <output.sd> <backup_dir>
```

## Socket mode

Start the capturer and stop it with `SIGINT` or `SIGTERM`:

```bash
sudo ./statediff_vfs --socket <socket_path> <target_dir>
```

The client sends the byte `g` and reads an eight-byte little-endian length
followed by the batch, which also clears it; length `0` means no mutations and
`UINT64_MAX` means capture failed.
