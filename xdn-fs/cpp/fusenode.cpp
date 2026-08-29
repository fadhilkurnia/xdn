// fusenode.cpp - fuselog reimplemented on the FUSE *low-level* API
// (fuse_lowlevel_ops, nodeid/inode-based).
//
// Motivation:
//   1. Correctness under hardlink + truncate with writeback_cache. The
//      high-level API gives each hardlink NAME its own nodeid, so the kernel
//      keeps a separate page cache + i_size per name and truncating one name
//      leaves the other name's dirty tail stale. The low-level API lets us
//      return ONE nodeid per backing inode (dedup by (st_dev,st_ino)), so all
//      hardlink names share ONE page cache / i_size and truncate is coherent.
//   2. Performance: no per-op full-path reconstruction; direct nodeid dispatch.
//
// The statediff wire format is the SAME v3 identity-keyed format (magic FLG3)
// consumed by the existing fuselog-apply (apply3), so this binary is a
// drop-in capture source.
//
// Modelled closely on libfuse's example/passthrough_ll.c (inode table,
// lo_find by (dev,ino), lookup/forget refcounting). The mount-over-backing
// trick matches the high-level fuselog: the mountpoint's *underlying*
// directory is the backing store; we open it O_PATH BEFORE mounting so
// operations pass through to it via openat(parent_fd, name) and
// /proc/self/fd/<inode_fd>.

#define FUSE_USE_VERSION 31
#define _GNU_SOURCE

#include <fuse_lowlevel.h>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/statvfs.h>
#include <zstd.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fuselog_internal.h"

using std::string;

// ============================================================================
// Configuration (env-driven)
// ============================================================================
// Env-var namespace policy:
//   * fusenode-OWN knobs read the FUSENODE_* name FIRST, then fall back to the
//     legacy FUSELOG_* name so the shared fuzz harness (which hard-codes
//     FUSELOG_WRITEBACK_CACHE / WRITE_COALESCING / FUSELOG_PRUNE /
//     FUSELOG_COMPRESSION in launch_fuselog) keeps driving fusenode unchanged,
//     while A/B tests can override via the FUSENODE_* name from the outer env.
//   * FUSELOG_SOCKET_FILE / FUSELOG_CAPTURE stay FUSELOG_-named only: they are
//     part of the shared recorder+socket contract the Java
//     FuselogStateDiffRecorder and the harness both speak.
static const char* getenv_ns(const char* fusenode_name,
                             const char* fuselog_name) {
  const char* v = getenv(fusenode_name);
  if (v) return v;
  return fuselog_name ? getenv(fuselog_name) : nullptr;
}
static bool parse_bool(const char* v, bool dflt) {
  if (!v) return dflt;
  return strcmp(v, "1") == 0 || strcmp(v, "true") == 0 ||
         strcmp(v, "TRUE") == 0 || strcmp(v, "yes") == 0;
}
static bool getenv_bool(const char* name, bool dflt) {
  return parse_bool(getenv(name), dflt);
}
static bool getenv_bool_ns(const char* fusenode_name, const char* fuselog_name,
                           bool dflt) {
  return parse_bool(getenv_ns(fusenode_name, fuselog_name), dflt);
}

static bool  g_capture         = true;   // FUSELOG_CAPTURE (shared contract)
static bool  g_prune           = true;   // FUSENODE_PRUNE  (harvest pruning+merge)
static bool  g_coalesce        = false;  // FUSENODE_COALESCE (compute_diff; deferred)
static bool  g_compress        = true;   // FUSENODE_COMPRESSION (default ON)
static bool  g_writeback_cache = false;  // FUSENODE_WRITEBACK_CACHE
static const char* g_socket_file = "/tmp/fuselog.sock";  // FUSELOG_SOCKET_FILE

// Attribute/entry cache timeouts handed to the kernel.
//
// entry_timeout (name->nodeid): stays 0. The fuzzer churns unlink/recreate/
// rename heavily, and a nonzero entry cache would let the kernel resolve a
// stale name->nodeid.
//
// attr_timeout (cached stat attributes): DEFAULT 1.0s, matching the high-level
// fuselog. At attr_timeout=0 every stat/fstat the containerized SQLite does on
// the write path (it stats its db/-wal/-shm constantly) forces a userspace
// GETATTR upcall; caching attrs for 1s elides those. All access to the state
// dir goes through THIS single-writer mount, so nothing external can make a
// cached size/mtime stale; it does NOT touch the capture path (writes still
// traverse fn_write), so byte-exact capture is unaffected. Under writeback the
// kernel owns i_size authoritatively, so a cached attr cannot mislead the write
// path. Override with FUSENODE_ATTR_TIMEOUT / FUSELOG_ATTR_TIMEOUT (seconds, may
// be fractional; 0 restores a round-trip per stat).
static double g_attr_timeout  = 1.0;
static double g_entry_timeout = 0.0;

// ============================================================================
// Lightweight, env-gated profiling counters (FUSENODE_PROFILE=1). Off by
// default so the production hot path is untouched. On SIGUSR1 the totals are
// written to /tmp/fusenode_prof.txt so a harness can compare fn_write/fsync
// behavior against the high-level fuselog without strace perturbation.
// ============================================================================
static bool g_profile = false;
static std::atomic<uint64_t> p_nwrite{0}, p_write_bytes{0}, p_pwrite_ns{0},
    p_emit_ns{0}, p_nfsync{0}, p_fsync_ns{0}, p_nopen{0}, p_ncreate{0},
    p_nread{0}, p_read_bytes{0}, p_ngetattr{0}, p_nlookup{0};
static inline uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static void dump_profile(int) {
  if (!g_profile) return;
  char buf[1024];
  uint64_t nw = p_nwrite.load(), nf = p_nfsync.load();
  int n = snprintf(
      buf, sizeof(buf),
      "fusenode_profile: nwrite=%llu write_bytes=%llu pwrite_ns=%llu "
      "avg_wr_bytes=%.1f avg_pwrite_us=%.3f emit_ns=%llu avg_emit_us=%.3f "
      "nfsync=%llu fsync_ns=%llu avg_fsync_us=%.3f nopen=%llu ncreate=%llu "
      "ngetattr=%llu nlookup=%llu nread=%llu read_bytes=%llu\n",
      (unsigned long long)nw, (unsigned long long)p_write_bytes.load(),
      (unsigned long long)p_pwrite_ns.load(),
      nw ? (double)p_write_bytes.load() / nw : 0.0,
      nw ? (double)p_pwrite_ns.load() / nw / 1000.0 : 0.0,
      (unsigned long long)p_emit_ns.load(),
      nw ? (double)p_emit_ns.load() / nw / 1000.0 : 0.0,
      (unsigned long long)nf, (unsigned long long)p_fsync_ns.load(),
      nf ? (double)p_fsync_ns.load() / nf / 1000.0 : 0.0,
      (unsigned long long)p_nopen.load(), (unsigned long long)p_ncreate.load(),
      (unsigned long long)p_ngetattr.load(), (unsigned long long)p_nlookup.load(),
      (unsigned long long)p_nread.load(),
      (unsigned long long)p_read_bytes.load());
  int fd = open("/tmp/fusenode_prof.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd >= 0) {
    ssize_t wr = write(fd, buf, n > 0 ? (size_t)n : 0);
    (void)wr;
    close(fd);
  }
}

// ============================================================================
// Inode table
// ============================================================================
// nodeid (fuse_ino_t) is the address of the Inode. FUSE_ROOT_ID maps to
// g_root. A backing inode is identified by (dev, ino); the g_by_src map dedups
// so every hardlink NAME to one backing inode resolves to the SAME Inode (and
// hence the same nodeid + one kernel page cache) -- the structural hardlink
// fix.
struct Inode {
  int      fd = -1;         // O_PATH fd to the backing inode
  dev_t    dev = 0;
  ino_t    ino = 0;
  uint64_t nlookup = 0;     // kernel lookup count (lookup/link ++, forget --)
  uint64_t cap_id = 0;      // monotonic capture identity; 0 = none
  mode_t   mode = 0;        // cached type bits (for capture decisions)
  // One cached, real (non-O_PATH) I/O fd per inode, used under writeback: open
  // ONE O_RDWR fd lazily on first writable open, share it across all FUSE opens
  // of this inode, and service read/write/fsync via pread/pwrite (explicit
  // offsets -- safe to share because under writeback the kernel owns file
  // offsets and this is a single-writer passthrough). The fd is bound to the
  // backing INODE (not a path), so it tracks the right data across
  // rename/unlink; closed when the inode is freed (forget/last-unlink). Lazy
  // init + O_RDONLY->O_RDWR upgrade are serialized by g_iofd_mutex; the
  // steady-state read is a lock-free atomic load. The non-writeback path opens
  // the backing fd per open().
  std::atomic<int>  io_fd{-1};
  std::atomic<bool> io_rdwr{false};
  // Live names for this inode, as (parent_nodeid, leaf). A regular file with
  // >1 entry is a hardlink; the capture binding table emits one path per entry
  // so apply replays content to every name.
  std::set<std::pair<fuse_ino_t, string>> links;
};

static Inode g_root;
static std::mutex g_mutex;  // guards g_by_src + g_dentries + Inode nlookup/links
static std::map<std::pair<dev_t, ino_t>, Inode*> g_by_src;
// Forward directory-entry cache: (parent_nodeid, leaf) -> inode. The inverse
// of Inode::links; lets unlink/rename resolve the affected inode without a
// re-stat, and lets us keep both directions consistent under one lock.
static std::map<std::pair<fuse_ino_t, string>, Inode*> g_dentries;

static Inode* get_inode(fuse_ino_t ino) {
  if (ino == FUSE_ROOT_ID) return &g_root;
  return reinterpret_cast<Inode*>(static_cast<uintptr_t>(ino));
}
static int inode_fd(fuse_ino_t ino) { return get_inode(ino)->fd; }

// Serializes lazy-open / upgrade of Inode::io_fd. Deliberately separate from
// g_mutex (which guards the inode table + capture) so the I/O path never
// contends with lookup/forget/capture bookkeeping.
static std::mutex g_iofd_mutex;

// Get the cached real I/O fd for an inode, opening it lazily and upgrading
// O_RDONLY->O_RDWR on first write. Steady state is a lock-free atomic load;
// the slow path (first open, or a read-only fd that now needs writing) takes
// g_iofd_mutex. Returns a live fd >= 0, or -1 if the backing inode cannot be
// opened for the requested access. Only called on the writeback path.
static int get_io_fd(Inode* in, bool want_write) {
  int fd = in->io_fd.load(std::memory_order_acquire);
  if (fd >= 0 && (!want_write || in->io_rdwr.load(std::memory_order_acquire)))
    return fd;

  std::lock_guard<std::mutex> lk(g_iofd_mutex);
  fd = in->io_fd.load(std::memory_order_relaxed);
  char procname[64];
  snprintf(procname, sizeof(procname), "/proc/self/fd/%d", in->fd);
  if (fd < 0) {
    int nfd = open(procname, O_RDWR);
    if (nfd >= 0) {
      in->io_rdwr.store(true, std::memory_order_release);
    } else {
      nfd = open(procname, O_RDONLY);  // e.g. read-only-mode file
      in->io_rdwr.store(false, std::memory_order_release);
    }
    in->io_fd.store(nfd, std::memory_order_release);
    return nfd;
  }
  // Have a fd but it is O_RDONLY and a write now needs it: upgrade in place.
  if (want_write && !in->io_rdwr.load(std::memory_order_relaxed)) {
    int nfd = open(procname, O_RDWR);
    if (nfd >= 0) {
      int old = in->io_fd.exchange(nfd, std::memory_order_acq_rel);
      in->io_rdwr.store(true, std::memory_order_release);
      if (old >= 0) close(old);
    }
  }
  return in->io_fd.load(std::memory_order_relaxed);
}
static fuse_ino_t inode_nodeid(Inode* in) {
  return (in == &g_root) ? FUSE_ROOT_ID
                         : static_cast<fuse_ino_t>(
                               reinterpret_cast<uintptr_t>(in));
}

// Full path (relative to backing root, no leading slash, e.g. "x/y/a") of an
// inode, walking up one live link at a time. Caller MUST hold g_mutex.
// Directories have exactly one link (they can't be hardlinked), so the walk is
// deterministic; a regular file with >1 link resolves via its first name, and
// path_of_via() below enumerates all of them for the binding table.
static string path_of_inode_locked(Inode* in) {
  if (in == &g_root) return "";
  if (in->links.empty()) return "";  // orphan; callers skip these
  const auto& lk = *in->links.begin();
  string pp = path_of_inode_locked(get_inode(lk.first));
  return pp.empty() ? lk.second : pp + "/" + lk.second;
}
// Path for a specific (parent, leaf) directory entry. Caller holds g_mutex.
static string join_path_locked(fuse_ino_t parent, const string& name) {
  string pp = path_of_inode_locked(get_inode(parent));
  return pp.empty() ? name : pp + "/" + name;
}

// ============================================================================
// Capture identity + statediff records (v3).
// ============================================================================
static std::atomic<uint64_t> g_next_cap_id{1};
static uint64_t alloc_cap_id() {
  return g_next_cap_id.fetch_add(1, std::memory_order_relaxed);
}

#define SD_TYPE_WRITE    0
#define SD_TYPE_UNLINK   1
#define SD_TYPE_RENAME   2
#define SD_TYPE_TRUNCATE 3
#define SD_TYPE_CREATE   4
#define SD_TYPE_LINK     5
#define SD_TYPE_CHOWN    6
#define SD_TYPE_CHMOD    7
#define SD_TYPE_MKDIR    8
#define SD_TYPE_RMDIR    9
#define SD_TYPE_SYMLINK  10

struct StateDiff {
  uint8_t  sd_type = 0;
  uint64_t cap_id  = 0;   // for WRITE/TRUNCATE (identity)
  uint64_t offset  = 0;   // write offset / truncate size
  std::vector<unsigned char> buf;  // write bytes / symlink target
  uint32_t uid = 0, gid = 0, mode = 0;
  string   path_a, path_b;         // inline paths for namespace/metadata ops
  StateDiff* next = nullptr;
};

// Lock-free Treiber stack (newest-first); harvest reverses to chronological.
static std::atomic<StateDiff*> g_sd_head{nullptr};
static void push_sd(StateDiff* sd) {
  StateDiff* old = g_sd_head.load(std::memory_order_relaxed);
  do {
    sd->next = old;
  } while (!g_sd_head.compare_exchange_weak(old, sd, std::memory_order_release,
                                            std::memory_order_relaxed));
}

// Emit helpers (no-op when capture is disabled). Namespace/metadata records
// carry inline paths captured AT OP TIME; WRITE/TRUNCATE carry the target
// inode's cap_id (stable identity) and are bound to a final path at harvest.
static void emit_namespace(uint8_t type, const string& a, const string& b,
                           uint32_t uid, uint32_t gid, uint32_t mode,
                           const unsigned char* buf, size_t buflen) {
  if (!g_capture) return;
  StateDiff* sd = new StateDiff();
  sd->sd_type = type;
  sd->path_a = a;
  sd->path_b = b;
  sd->uid = uid;
  sd->gid = gid;
  sd->mode = mode;
  if (buf && buflen) sd->buf.assign(buf, buf + buflen);
  push_sd(sd);
}
static void emit_write(uint64_t cap_id, uint64_t offset,
                       const unsigned char* buf, size_t buflen) {
  if (!g_capture || cap_id == 0 || buflen == 0) return;
  StateDiff* sd = new StateDiff();
  sd->sd_type = SD_TYPE_WRITE;
  sd->cap_id = cap_id;
  sd->offset = offset;
  sd->buf.assign(buf, buf + buflen);
  push_sd(sd);
}
static void emit_truncate(uint64_t cap_id, uint64_t size) {
  if (!g_capture || cap_id == 0) return;
  StateDiff* sd = new StateDiff();
  sd->sd_type = SD_TYPE_TRUNCATE;
  sd->cap_id = cap_id;
  sd->offset = size;
  push_sd(sd);
}

static const uint32_t FUSELOG_V3_MAGIC = 0x33474C46;  // 'F''L''G''3' (LE)

// ============================================================================
// v3 payload serialization helpers
// ============================================================================
static inline void put_u8(std::vector<char>& b, uint8_t v) { b.push_back((char)v); }
static inline void put_u32(std::vector<char>& b, uint32_t v) {
  b.insert(b.end(), (char*)&v, (char*)&v + 4);
}
static inline void put_u64(std::vector<char>& b, uint64_t v) {
  b.insert(b.end(), (char*)&v, (char*)&v + 8);
}
static inline void put_str(std::vector<char>& b, const string& s) {
  put_u64(b, (uint64_t)s.size());
  b.insert(b.end(), s.data(), s.data() + s.size());
}

static std::vector<char> build_v3_payload();  // fwd decl (defined below)

// ============================================================================
// Unix-socket command channel (identical protocol to fuselogv2.cpp):
//   'g' -> send [8:size][payload]; 'y' -> ack "y\n".
// ============================================================================
static int g_socket_fd = -1;
static std::thread* g_sock_thread = nullptr;

static int send_all(int fd, const char* buf, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = send(fd, buf + sent, len - sent, 0);
    if (n == -1) { perror("send_all"); return -1; }
    sent += (size_t)n;
  }
  return 0;
}

static int send_gathered_statediffs(int conn_fd) {
  std::vector<char> payload = build_v3_payload();

  char* out = payload.data();
  uint64_t out_sz = payload.size();
  char* compressed = nullptr;
  if (g_compress && out_sz > 0) {
    static ZSTD_CCtx* cctx = nullptr;
    if (cctx == nullptr) cctx = ZSTD_createCCtx();
    if (cctx != nullptr) {
      size_t bound = ZSTD_compressBound(out_sz);
      compressed = (char*)malloc(bound);
      if (compressed) {
        size_t csz = ZSTD_compressCCtx(cctx, compressed, bound, payload.data(),
                                       out_sz, 1);
        if (!ZSTD_isError(csz)) { out = compressed; out_sz = (uint64_t)csz; }
        else { free(compressed); compressed = nullptr; }
      }
    }
  }

  if (send_all(conn_fd, (const char*)&out_sz, sizeof(uint64_t)) != 0) {
    if (compressed) free(compressed);
    return -1;
  }
  if (out_sz > 0 && send_all(conn_fd, out, out_sz) != 0) {
    if (compressed) free(compressed);
    return -1;
  }
  if (compressed) free(compressed);
  return 0;
}

static bool initialize_unix_socket() {
  unlink(g_socket_file);
  g_socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (g_socket_fd == -1) { perror("socket"); return false; }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, g_socket_file, sizeof(addr.sun_path) - 1);
  if (bind(g_socket_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    perror("bind"); return false;
  }
  if (listen(g_socket_fd, 16) != 0) { perror("listen"); return false; }

  auto listener = []() {
    while (true) {
      int conn_fd = accept(g_socket_fd, nullptr, nullptr);
      if (conn_fd == -1) continue;
      char recv_buf[128];
      int n;
      do {
        memset(recv_buf, 0, sizeof(recv_buf));
        n = recv(conn_fd, recv_buf, sizeof(recv_buf) - 1, 0);
        if (n <= 0) break;
        if (strstr(recv_buf, "g") != nullptr) {
          send_gathered_statediffs(conn_fd);
          continue;
        }
        // 'y' or anything else: just ack.
        send(conn_fd, "y\n", 2, 0);
      } while (n > 0);
      close(conn_fd);
    }
  };
  g_sock_thread = new std::thread(listener);
  g_sock_thread->detach();
  return true;
}

// ============================================================================
// FUSE handlers
// ============================================================================
static void fn_init(void* userdata, struct fuse_conn_info* conn) {
  (void)userdata;
  if (g_writeback_cache) {
    if (conn->capable & FUSE_CAP_WRITEBACK_CACHE) {
      conn->want |= FUSE_CAP_WRITEBACK_CACHE;
      fprintf(stderr, "fusenode: writeback_cache ENABLED\n");
    } else {
      fprintf(stderr, "fusenode: writeback_cache requested but kernel "
                      "does not advertise it\n");
    }
  }
  if (conn->capable & FUSE_CAP_EXPORT_SUPPORT)
    conn->want |= FUSE_CAP_EXPORT_SUPPORT;
  conn->max_write = 262144;  // parity with the high-level build (256 KiB)
}

static void fn_destroy(void* userdata) {
  (void)userdata;
  std::lock_guard<std::mutex> lk(g_mutex);
  for (auto& kv : g_by_src) {
    if (kv.second->fd >= 0) close(kv.second->fd);
    int iofd = kv.second->io_fd.load(std::memory_order_relaxed);
    if (iofd >= 0) close(iofd);
    delete kv.second;
  }
  g_by_src.clear();
}

// Resolve (parent,name) on the backing store, dedup into the inode table by
// (dev,ino), and record the directory entry (both g_dentries and Inode::links)
// so hardlink names share one nodeid + one cap_id. Returns 0 on success and
// fills *e; else returns positive errno.
static int do_lookup(fuse_ino_t parent, const char* name,
                     struct fuse_entry_param* e) {
  if (g_profile) p_nlookup.fetch_add(1, std::memory_order_relaxed);
  memset(e, 0, sizeof(*e));
  e->attr_timeout = g_attr_timeout;
  e->entry_timeout = g_entry_timeout;

  // Stat the entry through the parent's fd FIRST (one syscall). With
  // entry_timeout=0 the kernel re-issues LOOKUP for every path component on
  // every stat/open. We only need a NEW O_PATH fd when the backing inode is not
  // yet in our table; the common case (repeat lookup of a known inode) is a
  // single fstatat + a map hit -- no openat, no close.
  if (fstatat(inode_fd(parent), name, &e->attr, AT_SYMLINK_NOFOLLOW) == -1)
    return errno;

  std::unique_lock<std::mutex> lk(g_mutex);
  Inode* inode = nullptr;
  auto it = g_by_src.find({e->attr.st_dev, e->attr.st_ino});
  if (it != g_by_src.end()) {
    inode = it->second;
    inode->nlookup++;
  } else {
    // Mint a fresh inode: it needs a persistent O_PATH fd to the backing
    // inode. Drop the lock across the openat (a syscall), then re-check the
    // table under the lock in case a concurrent lookup won the race.
    lk.unlock();
    int newfd = openat(inode_fd(parent), name, O_PATH | O_NOFOLLOW);
    if (newfd == -1) return errno;
    // Re-stat via the fd so (dev,ino,attr) are authoritative for THIS fd
    // (guards the tiny window between the fstatat above and this openat).
    if (fstatat(newfd, "", &e->attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW) ==
        -1) {
      int saverr = errno;
      close(newfd);
      return saverr;
    }
    lk.lock();
    it = g_by_src.find({e->attr.st_dev, e->attr.st_ino});
    if (it != g_by_src.end()) {
      inode = it->second;
      inode->nlookup++;
      close(newfd);  // lost the race; reuse the existing inode
    } else {
      inode = new Inode();
      inode->fd = newfd;
      inode->dev = e->attr.st_dev;
      inode->ino = e->attr.st_ino;
      inode->mode = e->attr.st_mode;
      inode->nlookup = 1;
      inode->cap_id = alloc_cap_id();  // fresh identity for a fresh inode
      g_by_src[{inode->dev, inode->ino}] = inode;
    }
  }
  // Record this directory entry (idempotent for repeat lookups).
  auto key = std::make_pair(parent, string(name));
  g_dentries[key] = inode;
  inode->links.insert(key);
  e->ino = inode_nodeid(inode);
  return 0;
}

static void fn_lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  struct fuse_entry_param e;
  int err = do_lookup(parent, name, &e);
  if (err) fuse_reply_err(req, err);
  else fuse_reply_entry(req, &e);
}

static void unref_inode(Inode* inode, uint64_t n) {
  if (!inode || inode == &g_root) return;
  std::lock_guard<std::mutex> lk(g_mutex);
  if (inode->nlookup < n) inode->nlookup = n;  // defensive
  inode->nlookup -= n;
  if (inode->nlookup == 0) {
    g_by_src.erase({inode->dev, inode->ino});
    // Drop any residual forward dentries pointing at this inode. Normally
    // unlink/rename already removed them; this guards leaked entries.
    for (const auto& lk : inode->links) g_dentries.erase(lk);
    if (inode->fd >= 0) close(inode->fd);
    int iofd = inode->io_fd.load(std::memory_order_relaxed);  // shared I/O fd
    if (iofd >= 0) close(iofd);
    delete inode;
  }
}

static void fn_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
  unref_inode(get_inode(ino), nlookup);
  fuse_reply_none(req);
}

static void fn_forget_multi(fuse_req_t req, size_t count,
                             struct fuse_forget_data* forgets) {
  for (size_t i = 0; i < count; i++)
    unref_inode(get_inode(forgets[i].ino), forgets[i].nlookup);
  fuse_reply_none(req);
}

static void fn_getattr(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info* fi) {
  (void)fi;
  if (g_profile) p_ngetattr.fetch_add(1, std::memory_order_relaxed);
  struct stat buf;
  if (fstatat(inode_fd(ino), "", &buf, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW) ==
      -1)
    return (void)fuse_reply_err(req, errno);
  fuse_reply_attr(req, &buf, g_attr_timeout);
}

static void fn_setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                        int valid, struct fuse_file_info* fi) {
  Inode* inode = get_inode(ino);
  int ifd = inode->fd;
  char procname[64];
  int res;
  // When called with an open handle, target the inode's fd. Under writeback
  // that is the shared cached io_fd (fi->fh is the sentinel 0); non-writeback
  // uses the per-open fd in fi->fh. opfd < 0 => fall back to the /proc path.
  int opfd = fi ? (g_writeback_cache
                       ? get_io_fd(inode, (valid & FUSE_SET_ATTR_SIZE) != 0)
                       : (int)fi->fh)
                : -1;

  if (valid & FUSE_SET_ATTR_MODE) {
    if (opfd >= 0) res = fchmod(opfd, attr->st_mode);
    else {
      snprintf(procname, sizeof(procname), "/proc/self/fd/%i", ifd);
      res = chmod(procname, attr->st_mode);
    }
    if (res == -1) goto out_err;
  }
  if (valid & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
    uid_t uid = (valid & FUSE_SET_ATTR_UID) ? attr->st_uid : (uid_t)-1;
    gid_t gid = (valid & FUSE_SET_ATTR_GID) ? attr->st_gid : (gid_t)-1;
    if (fchownat(ifd, "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW) == -1)
      goto out_err;
  }
  if (valid & FUSE_SET_ATTR_SIZE) {
    if (opfd >= 0) res = ftruncate(opfd, attr->st_size);
    else {
      snprintf(procname, sizeof(procname), "/proc/self/fd/%i", ifd);
      res = truncate(procname, attr->st_size);
    }
    if (res == -1) goto out_err;
  }
  if (valid & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {
    struct timespec tv[2];
    tv[0].tv_sec = 0; tv[1].tv_sec = 0;
    tv[0].tv_nsec = UTIME_OMIT; tv[1].tv_nsec = UTIME_OMIT;
    if (valid & FUSE_SET_ATTR_ATIME_NOW) tv[0].tv_nsec = UTIME_NOW;
    else if (valid & FUSE_SET_ATTR_ATIME) tv[0] = attr->st_atim;
    if (valid & FUSE_SET_ATTR_MTIME_NOW) tv[1].tv_nsec = UTIME_NOW;
    else if (valid & FUSE_SET_ATTR_MTIME) tv[1] = attr->st_mtim;
    if (opfd >= 0) res = futimens(opfd, tv);
    else {
      snprintf(procname, sizeof(procname), "/proc/self/fd/%i", ifd);
      res = utimensat(AT_FDCWD, procname, tv, 0);
    }
    if (res == -1) goto out_err;
  }

  if (g_capture) {
    string path;
    uint64_t cap_id;
    { std::lock_guard<std::mutex> lk(g_mutex);
      path = path_of_inode_locked(inode);
      cap_id = inode->cap_id; }
    if (valid & FUSE_SET_ATTR_MODE)
      emit_namespace(SD_TYPE_CHMOD, path, "", 0, 0, attr->st_mode & 07777,
                     nullptr, 0);
    if (valid & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
      uint32_t uid = (valid & FUSE_SET_ATTR_UID) ? attr->st_uid : (uint32_t)-1;
      uint32_t gid = (valid & FUSE_SET_ATTR_GID) ? attr->st_gid : (uint32_t)-1;
      emit_namespace(SD_TYPE_CHOWN, path, "", uid, gid, 0, nullptr, 0);
    }
    if (valid & FUSE_SET_ATTR_SIZE)
      emit_truncate(cap_id, (uint64_t)attr->st_size);
  }
  return fn_getattr(req, ino, fi);

out_err:
  fuse_reply_err(req, errno);
}

static void fn_readlink(fuse_req_t req, fuse_ino_t ino) {
  char buf[PATH_MAX + 1];
  int res = readlinkat(inode_fd(ino), "", buf, sizeof(buf));
  if (res == -1) return (void)fuse_reply_err(req, errno);
  if (res == sizeof(buf)) return (void)fuse_reply_err(req, ENAMETOOLONG);
  buf[res] = '\0';
  fuse_reply_readlink(req, buf);
}

static int mknod_wrapper(int dirfd, const char* path, const char* link,
                         int mode, dev_t rdev) {
  if (S_ISREG(mode)) {
    int fd = openat(dirfd, path, O_CREAT | O_EXCL | O_WRONLY, mode);
    if (fd >= 0) return close(fd);
    return -1;
  } else if (S_ISDIR(mode)) {
    return mkdirat(dirfd, path, mode);
  } else if (S_ISLNK(mode) && link != nullptr) {
    return symlinkat(link, dirfd, path);
  } else if (S_ISFIFO(mode)) {
    return mkfifoat(dirfd, path, mode);
  } else {
    return mknodat(dirfd, path, mode, rdev);
  }
}

static void mknod_symlink(fuse_req_t req, fuse_ino_t parent, const char* name,
                          mode_t mode, dev_t rdev, const char* link) {
  int res = mknod_wrapper(inode_fd(parent), name, link, mode, rdev);
  if (res == -1) return (void)fuse_reply_err(req, errno);
  struct fuse_entry_param e;
  int err = do_lookup(parent, name, &e);
  if (err) return (void)fuse_reply_err(req, err);

  if (g_capture) {
    const struct fuse_ctx* ctx = fuse_req_ctx(req);
    string path;
    { std::lock_guard<std::mutex> lk(g_mutex); path = join_path_locked(parent, name); }
    if (S_ISDIR(mode)) {
      emit_namespace(SD_TYPE_MKDIR, path, "", 0, 0, mode, nullptr, 0);
    } else if (S_ISLNK(mode)) {
      emit_namespace(SD_TYPE_SYMLINK, path, "", ctx->uid, ctx->gid, 0,
                     (const unsigned char*)link, link ? strlen(link) : 0);
    } else {
      emit_namespace(SD_TYPE_CREATE, path, "", ctx->uid, ctx->gid, mode,
                     nullptr, 0);
    }
  }
  fuse_reply_entry(req, &e);
}

static void fn_mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
                      mode_t mode, dev_t rdev) {
  mknod_symlink(req, parent, name, mode, rdev, nullptr);
}
static void fn_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name,
                      mode_t mode) {
  mknod_symlink(req, parent, name, S_IFDIR | mode, 0, nullptr);
}
static void fn_symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                        const char* name) {
  mknod_symlink(req, parent, name, S_IFLNK, 0, link);
}

static void fn_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
                     const char* name) {
  Inode* inode = get_inode(ino);
  char procname[64];
  struct fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  e.attr_timeout = g_attr_timeout;
  e.entry_timeout = g_entry_timeout;

  snprintf(procname, sizeof(procname), "/proc/self/fd/%i", inode->fd);
  if (linkat(AT_FDCWD, procname, inode_fd(parent), name, AT_SYMLINK_FOLLOW) ==
      -1)
    return (void)fuse_reply_err(req, errno);
  if (fstatat(inode->fd, "", &e.attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW) ==
      -1)
    return (void)fuse_reply_err(req, errno);
  string from, to;
  {
    std::lock_guard<std::mutex> lk(g_mutex);
    from = path_of_inode_locked(inode);  // an existing name (before new link)
    inode->nlookup++;
    auto key = std::make_pair(parent, string(name));
    g_dentries[key] = inode;
    inode->links.insert(key);
    to = join_path_locked(parent, name);
  }
  // The new hardlink NAME shares the existing inode's nodeid AND cap_id, so
  // the harvest binding lists both paths under one cap_id -> apply replays
  // content to every name. Also record the LINK namespace op for replay.
  emit_namespace(SD_TYPE_LINK, from, to, 0, 0, 0, nullptr, 0);
  e.ino = inode_nodeid(inode);
  fuse_reply_entry(req, &e);
}

// Shared body for unlink (isdir=false) and rmdir (isdir=true).
static void do_remove(fuse_req_t req, fuse_ino_t parent, const char* name,
                      bool isdir) {
  string path;
  { std::lock_guard<std::mutex> lk(g_mutex); path = join_path_locked(parent, name); }
  int res = unlinkat(inode_fd(parent), name, isdir ? AT_REMOVEDIR : 0);
  if (res == -1) return (void)fuse_reply_err(req, errno);
  {
    std::lock_guard<std::mutex> lk(g_mutex);
    auto key = std::make_pair(parent, string(name));
    auto it = g_dentries.find(key);
    if (it != g_dentries.end()) {
      it->second->links.erase(key);  // one live name gone (hardlinks keep rest)
      g_dentries.erase(it);
    }
  }
  emit_namespace(isdir ? SD_TYPE_RMDIR : SD_TYPE_UNLINK, path, "", 0, 0, 0,
                 nullptr, 0);
  fuse_reply_err(req, 0);
}
static void fn_unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  do_remove(req, parent, name, false);
}
static void fn_rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  do_remove(req, parent, name, true);
}
static void fn_rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                       fuse_ino_t newparent, const char* newname,
                       unsigned int flags) {
  if (flags) return (void)fuse_reply_err(req, EINVAL);
  string from, to;
  { std::lock_guard<std::mutex> lk(g_mutex);
    from = join_path_locked(parent, name);
    to = join_path_locked(newparent, newname); }
  int res = renameat(inode_fd(parent), name, inode_fd(newparent), newname);
  if (res == -1) return (void)fuse_reply_err(req, errno);
  {
    std::lock_guard<std::mutex> lk(g_mutex);
    auto src_key = std::make_pair(parent, string(name));
    auto dst_key = std::make_pair(newparent, string(newname));
    // Victim at destination (overwritten) loses its name.
    auto vit = g_dentries.find(dst_key);
    if (vit != g_dentries.end()) {
      vit->second->links.erase(dst_key);
      g_dentries.erase(vit);
    }
    // Moved inode: retarget its name from src to dst.
    auto sit = g_dentries.find(src_key);
    if (sit != g_dentries.end()) {
      Inode* moved = sit->second;
      moved->links.erase(src_key);
      moved->links.insert(dst_key);
      g_dentries.erase(sit);
      g_dentries[dst_key] = moved;
    }
  }
  emit_namespace(SD_TYPE_RENAME, from, to, 0, 0, 0, nullptr, 0);
  fuse_reply_err(req, 0);
}

// ----- directory I/O -----
struct DirHandle {
  DIR* dp = nullptr;
  struct dirent* entry = nullptr;
  off_t offset = 0;
};
static DirHandle* dir_handle(struct fuse_file_info* fi) {
  return reinterpret_cast<DirHandle*>(static_cast<uintptr_t>(fi->fh));
}

static void fn_opendir(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info* fi) {
  DirHandle* d = new DirHandle();
  int fd = openat(inode_fd(ino), ".", O_RDONLY);
  if (fd == -1) { delete d; return (void)fuse_reply_err(req, errno); }
  d->dp = fdopendir(fd);
  if (d->dp == nullptr) {
    int saverr = errno;
    close(fd);
    delete d;
    return (void)fuse_reply_err(req, saverr);
  }
  fi->fh = reinterpret_cast<uintptr_t>(d);
  fuse_reply_open(req, fi);
}

static int is_dot_or_dotdot(const char* name) {
  return name[0] == '.' &&
         (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'));
}

static void do_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                       off_t offset, struct fuse_file_info* fi, int plus) {
  DirHandle* d = dir_handle(fi);
  char* buf = (char*)calloc(1, size);
  if (!buf) return (void)fuse_reply_err(req, ENOMEM);
  char* p = buf;
  size_t rem = size;
  int err = 0;

  if (offset != d->offset) {
    seekdir(d->dp, offset);
    d->entry = nullptr;
    d->offset = offset;
  }
  while (true) {
    if (!d->entry) {
      errno = 0;
      d->entry = readdir(d->dp);
      if (!d->entry) { if (errno) err = errno; break; }
    }
    off_t nextoff = d->entry->d_off;
    const char* name = d->entry->d_name;
    fuse_ino_t entry_ino = 0;
    size_t entsize;
    if (plus) {
      struct fuse_entry_param e;
      if (is_dot_or_dotdot(name)) {
        memset(&e, 0, sizeof(e));
        e.attr.st_ino = d->entry->d_ino;
        e.attr.st_mode = d->entry->d_type << 12;
      } else {
        err = do_lookup(ino, name, &e);
        if (err) break;
        entry_ino = e.ino;
      }
      entsize = fuse_add_direntry_plus(req, p, rem, name, &e, nextoff);
    } else {
      struct stat st;
      memset(&st, 0, sizeof(st));
      st.st_ino = d->entry->d_ino;
      st.st_mode = d->entry->d_type << 12;
      entsize = fuse_add_direntry(req, p, rem, name, &st, nextoff);
    }
    if (entsize > rem) {
      if (entry_ino != 0) unref_inode(get_inode(entry_ino), 1);
      break;
    }
    p += entsize;
    rem -= entsize;
    d->entry = nullptr;
    d->offset = nextoff;
  }

  if (err && rem == size) fuse_reply_err(req, err);
  else fuse_reply_buf(req, buf, size - rem);
  free(buf);
}

static void fn_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                        off_t offset, struct fuse_file_info* fi) {
  do_readdir(req, ino, size, offset, fi, 0);
}
static void fn_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                            off_t offset, struct fuse_file_info* fi) {
  do_readdir(req, ino, size, offset, fi, 1);
}
static void fn_releasedir(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info* fi) {
  (void)ino;
  DirHandle* d = dir_handle(fi);
  closedir(d->dp);
  delete d;
  fuse_reply_err(req, 0);
}
static void fn_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                         struct fuse_file_info* fi) {
  (void)ino;
  int fd = dirfd(dir_handle(fi)->dp);
  int res = datasync ? fdatasync(fd) : fsync(fd);
  fuse_reply_err(req, res == -1 ? errno : 0);
}

// ----- file I/O -----
static void fn_create(fuse_req_t req, fuse_ino_t parent, const char* name,
                       mode_t mode, struct fuse_file_info* fi) {
  if (g_profile) p_ncreate.fetch_add(1, std::memory_order_relaxed);
  // Under writeback promote O_WRONLY->O_RDWR so the created fd can seed the
  // shared I/O fd the kernel may also read through.
  int oflags = fi->flags;
  if (g_writeback_cache && (oflags & O_ACCMODE) == O_WRONLY)
    oflags = (oflags & ~O_ACCMODE) | O_RDWR;
  int fd = openat(inode_fd(parent), name, (oflags | O_CREAT) & ~O_NOFOLLOW,
                  mode);
  if (fd == -1) return (void)fuse_reply_err(req, errno);
  fi->fh = fd;
  struct fuse_entry_param e;
  int err = do_lookup(parent, name, &e);
  if (err) { close(fd); return (void)fuse_reply_err(req, err); }

  if (g_writeback_cache) {
    // Seed the inode's shared I/O fd with the just-created fd (no reopen). If
    // the inode already had one (O_CREAT on an existing name), keep it.
    Inode* in = get_inode(e.ino);
    int expected = -1;
    bool rdwr = (oflags & O_ACCMODE) == O_RDWR;
    if (in->io_fd.compare_exchange_strong(expected, fd,
                                          std::memory_order_acq_rel)) {
      in->io_rdwr.store(rdwr, std::memory_order_release);
    } else {
      close(fd);  // lost the race / already seeded; use the existing shared fd
    }
    fi->fh = 0;  // sentinel: I/O goes through Inode::io_fd
  }

  if (g_capture) {
    const struct fuse_ctx* ctx = fuse_req_ctx(req);
    string path;
    { std::lock_guard<std::mutex> lk(g_mutex); path = join_path_locked(parent, name); }
    emit_namespace(SD_TYPE_CREATE, path, "", ctx->uid, ctx->gid, mode, nullptr,
                   0);
  }
  fuse_reply_create(req, &e, fi);
}

static void fn_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  if (g_profile) p_nopen.fetch_add(1, std::memory_order_relaxed);
  // Under writeback the kernel may issue reads even on a write-only handle,
  // and handles O_APPEND itself -- mirror passthrough_ll's flag fixups.
  if (g_writeback_cache && (fi->flags & O_ACCMODE) == O_WRONLY) {
    fi->flags &= ~O_ACCMODE;
    fi->flags |= O_RDWR;
  }
  if (g_writeback_cache && (fi->flags & O_APPEND)) fi->flags &= ~O_APPEND;

  if (g_writeback_cache) {
    // No per-open procfs reopen: pre-open the shared I/O fd here (so an
    // open() error still surfaces to the caller) and mark fi->fh as the shared
    // sentinel; read/write/fsync resolve the inode's cached fd by `ino`.
    Inode* in = get_inode(ino);
    bool want_write = (fi->flags & O_ACCMODE) != O_RDONLY;
    int iofd = get_io_fd(in, want_write);
    if (iofd == -1) return (void)fuse_reply_err(req, errno);
    fi->fh = 0;  // sentinel: I/O goes through Inode::io_fd, not fi->fh
    fuse_reply_open(req, fi);
    return;
  }

  // Non-writeback: per-open reopen.
  char buf[64];
  snprintf(buf, sizeof(buf), "/proc/self/fd/%i", inode_fd(ino));
  int fd = open(buf, fi->flags & ~O_NOFOLLOW);
  if (fd == -1) return (void)fuse_reply_err(req, errno);
  fi->fh = fd;
  fuse_reply_open(req, fi);
}

// Under writeback, all I/O goes through the inode's cached shared fd (resolved
// by `ino`); non-writeback keeps the per-open fd in fi->fh.
static void fn_release(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info* fi) {
  // Under writeback the shared io_fd is owned by the inode (closed at forget),
  // so a per-handle release must NOT close it.
  if (!g_writeback_cache) close(fi->fh);
  (void)ino;
  fuse_reply_err(req, 0);
}
static void fn_flush(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  int fd = g_writeback_cache
               ? get_inode(ino)->io_fd.load(std::memory_order_acquire)
               : (int)fi->fh;
  int res = (fd >= 0) ? close(dup(fd)) : 0;
  fuse_reply_err(req, res == -1 ? errno : 0);
}
static void fn_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                      struct fuse_file_info* fi) {
  int fd = g_writeback_cache ? get_io_fd(get_inode(ino), false) : (int)fi->fh;
  if (fd < 0) return (void)fuse_reply_err(req, 0);
  uint64_t t0 = g_profile ? now_ns() : 0;
  int res = datasync ? fdatasync(fd) : fsync(fd);
  if (g_profile) {
    p_nfsync.fetch_add(1, std::memory_order_relaxed);
    p_fsync_ns.fetch_add(now_ns() - t0, std::memory_order_relaxed);
  }
  fuse_reply_err(req, res == -1 ? errno : 0);
}

static void fn_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset,
                     struct fuse_file_info* fi) {
  int fd = g_writeback_cache ? get_io_fd(get_inode(ino), false) : (int)fi->fh;
  if (fd < 0) return (void)fuse_reply_err(req, errno ? errno : EBADF);
  struct fuse_bufvec buf = FUSE_BUFVEC_INIT(size);
  buf.buf[0].flags =
      (fuse_buf_flags)(FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
  buf.buf[0].fd = fd;
  buf.buf[0].pos = offset;
  if (g_profile) {
    p_nread.fetch_add(1, std::memory_order_relaxed);
    p_read_bytes.fetch_add((uint64_t)size, std::memory_order_relaxed);
  }
  fuse_reply_data(req, &buf, FUSE_BUF_SPLICE_MOVE);
}

// Plain buffered write (not write_buf/splice) so capture can see the bytes.
static void fn_write(fuse_req_t req, fuse_ino_t ino, const char* buf,
                      size_t size, off_t off, struct fuse_file_info* fi) {
  int fd = g_writeback_cache ? get_io_fd(get_inode(ino), true) : (int)fi->fh;
  if (fd < 0) return (void)fuse_reply_err(req, errno ? errno : EBADF);
  uint64_t t0 = g_profile ? now_ns() : 0;
  ssize_t res = pwrite(fd, buf, size, off);
  if (g_profile) {
    uint64_t t1 = now_ns();
    p_nwrite.fetch_add(1, std::memory_order_relaxed);
    p_pwrite_ns.fetch_add(t1 - t0, std::memory_order_relaxed);
    if (res > 0) p_write_bytes.fetch_add((uint64_t)res, std::memory_order_relaxed);
  }
  if (res == -1) return (void)fuse_reply_err(req, errno);
  if (g_capture && res > 0) {
    // Key by the inode's stable cap_id (NOT any path): under writeback the
    // kernel may flush this write after an intervening rename, and the harvest
    // binding resolves cap_id -> final path. Emit the raw written bytes (apply3
    // pwrites them at off); byte-exact, no diff needed for correctness.
    Inode* inode = get_inode(ino);
    uint64_t cap_id;
    { std::lock_guard<std::mutex> lk(g_mutex); cap_id = inode->cap_id; }
    uint64_t e0 = g_profile ? now_ns() : 0;
    emit_write(cap_id, (uint64_t)off, (const unsigned char*)buf, (size_t)res);
    if (g_profile) p_emit_ns.fetch_add(now_ns() - e0, std::memory_order_relaxed);
  }
  fuse_reply_write(req, (size_t)res);
}

static void fn_statfs(fuse_req_t req, fuse_ino_t ino) {
  struct statvfs st;
  if (fstatvfs(inode_fd(ino), &st) == -1)
    return (void)fuse_reply_err(req, errno);
  fuse_reply_statfs(req, &st);
}

static void fn_access(fuse_req_t req, fuse_ino_t ino, int mask) {
  char procname[64];
  snprintf(procname, sizeof(procname), "/proc/self/fd/%i", inode_fd(ino));
  int res = access(procname, mask);
  fuse_reply_err(req, res == -1 ? errno : 0);
}

static void fn_flush_noop() {}  // silence unused warnings if any

// A coalesced content segment: absolute offset + merged bytes.
struct WriteSeg {
  uint64_t offset;
  std::vector<unsigned char> bytes;
};

// coalesce_writes: fold an ordered run of WRITE records for ONE cap_id (with NO
// intervening TRUNCATE -- the caller splits runs at truncate boundaries) into a
// minimal set of non-overlapping, last-writer-wins segments, then fuses exactly
// adjacent segments (end == next start) so contiguous rewrites become one
// record. This is byte-exact w.r.t. apply3's pass-2 replay: applying the run's
// writes in order lands the last writer at every touched byte and leaves gaps
// (bytes no write covered) untouched -- so we NEVER bridge holes (unlike the
// high-level compute_diff merge_adjacent_chunks, which may bridge < overhead
// gaps using a full current-buffer it owns; fusenode has no such buffer here).
// Under writeback the kernel flushes page-granular RMW buffers, so a hot page
// rewritten N times yields N same-offset WRITE records; this keeps only the
// final bytes.
static std::vector<WriteSeg> coalesce_writes(
    const std::vector<const StateDiff*>& writes) {
  std::map<uint64_t, std::vector<unsigned char>> m;  // start -> bytes, disjoint
  for (const StateDiff* w : writes) {
    uint64_t s = w->offset;
    uint64_t e = s + (uint64_t)w->buf.size();
    if (e == s) continue;
    // Collect trimmed leftovers of every existing segment overlapping [s, e).
    std::vector<std::pair<uint64_t, std::vector<unsigned char>>> readd;
    auto it = m.upper_bound(s);            // first start > s
    if (it != m.begin()) --it;             // step back to possible predecessor
    while (it != m.end() && it->first < e) {
      uint64_t ss = it->first;
      uint64_t se = ss + (uint64_t)it->second.size();
      if (se <= s) { ++it; continue; }     // predecessor that doesn't reach s
      if (ss < s)                          // left leftover survives
        readd.emplace_back(ss, std::vector<unsigned char>(
                                   it->second.begin(),
                                   it->second.begin() + (s - ss)));
      if (se > e)                          // right leftover survives
        readd.emplace_back(e, std::vector<unsigned char>(
                                  it->second.begin() + (e - ss),
                                  it->second.end()));
      it = m.erase(it);
    }
    m[s] = std::vector<unsigned char>(w->buf.begin(), w->buf.end());
    for (auto& r : readd) m[r.first] = std::move(r.second);
  }
  std::vector<WriteSeg> out;
  for (auto& kv : m) {
    if (!out.empty() &&
        out.back().offset + (uint64_t)out.back().bytes.size() == kv.first) {
      out.back().bytes.insert(out.back().bytes.end(), kv.second.begin(),
                              kv.second.end());
    } else {
      out.push_back(WriteSeg{kv.first, std::move(kv.second)});
    }
  }
  return out;
}

// ============================================================================
// v3 payload builder. Drains the capture stack, builds the cap_id->path
// binding table, then serializes records. When g_prune is set the harvest
// (a) drops WRITE/TRUNCATE whose cap_id has no live binding -- apply3 already
// skips such records (fuselog-apply.cpp: "identity has no live path: drop"),
// so this drops wire size with an identical apply result -- and
// (b) coalesces each identity's write runs (see coalesce_writes). Namespace/
// metadata records keep their original relative order (apply3 replays them in
// a separate structure pass), and content stays in per-cap_id chronological
// order (all apply3's content pass requires), so the transform is byte-exact.
// ============================================================================
static std::vector<char> build_v3_payload() {
  // Drain the capture stack (get-and-clear) and reverse to chronological.
  StateDiff* head = g_sd_head.exchange(nullptr, std::memory_order_acq_rel);
  StateDiff* chrono = nullptr;
  for (StateDiff* p = head; p;) {
    StateDiff* nxt = p->next;
    p->next = chrono;
    chrono = p;
    p = nxt;
  }

  std::vector<char> buf;
  put_u32(buf, FUSELOG_V3_MAGIC);
  put_u32(buf, 0u);  // minor

  // Binding table: cap_id -> EVERY live path, for every live regular file. A
  // hardlinked inode (>1 link) contributes multiple paths under ONE cap_id, so
  // apply3 replays its content to all names. Built from CURRENT inode state so
  // it reflects final paths (robust across renames + batches).
  std::vector<char> bindings;
  uint64_t num_binding = 0;
  std::unordered_set<uint64_t> live_caps;  // cap_ids with >=1 emitted binding
  {
    std::lock_guard<std::mutex> lk(g_mutex);
    for (auto& kv : g_by_src) {
      Inode* in = kv.second;
      if (in->cap_id == 0 || in->links.empty() || !S_ISREG(in->mode)) continue;
      for (const auto& link : in->links) {
        string path = join_path_locked(link.first, link.second);
        if (path.empty()) continue;
        put_u64(bindings, in->cap_id);
        put_str(bindings, path);
        num_binding++;
        live_caps.insert(in->cap_id);  // exactly apply3's capid_paths key set
      }
    }
  }
  put_u64(buf, num_binding);
  buf.insert(buf.end(), bindings.begin(), bindings.end());

  // Records. Same v3 encoding as fuselogv2. Emit helpers keep the wire format
  // in one place so the raw and coalesced write paths agree byte-for-byte.
  std::vector<char> rec;
  uint64_t num_sd = 0;
  auto emit_write_rec = [&](uint64_t cap_id, uint64_t offset,
                            const unsigned char* data, size_t len) {
    if (len == 0) return;
    put_u8(rec, SD_TYPE_WRITE);
    put_u64(rec, cap_id);
    put_u64(rec, (uint64_t)len);
    put_u64(rec, offset);
    rec.insert(rec.end(), (const char*)data, (const char*)data + len);
    num_sd++;
  };
  auto emit_truncate_rec = [&](uint64_t cap_id, uint64_t size) {
    put_u8(rec, SD_TYPE_TRUNCATE);
    put_u64(rec, cap_id);
    put_u64(rec, size);
    num_sd++;
  };
  // Serialize a namespace/metadata record (everything except WRITE/TRUNCATE).
  auto emit_ns_rec = [&](const StateDiff* p) {
    uint8_t t = p->sd_type;
    const unsigned char* bd = p->buf.empty() ? nullptr : p->buf.data();
    switch (t) {
      case SD_TYPE_CREATE:
        put_u8(rec, t); put_str(rec, p->path_a);
        put_u32(rec, p->uid); put_u32(rec, p->gid); put_u32(rec, p->mode);
        num_sd++;
        break;
      case SD_TYPE_MKDIR:
        put_u8(rec, t); put_str(rec, p->path_a); put_u32(rec, p->mode);
        num_sd++;
        break;
      case SD_TYPE_SYMLINK:
        put_u8(rec, t); put_str(rec, p->path_a);
        put_u32(rec, (uint32_t)p->buf.size());
        rec.insert(rec.end(), (const char*)bd, (const char*)bd + p->buf.size());
        put_u32(rec, p->uid); put_u32(rec, p->gid);
        num_sd++;
        break;
      case SD_TYPE_UNLINK:
      case SD_TYPE_RMDIR:
        put_u8(rec, t); put_str(rec, p->path_a);
        num_sd++;
        break;
      case SD_TYPE_RENAME:
      case SD_TYPE_LINK:
        put_u8(rec, t); put_str(rec, p->path_a); put_str(rec, p->path_b);
        num_sd++;
        break;
      case SD_TYPE_CHMOD:
        put_u8(rec, t); put_str(rec, p->path_a); put_u32(rec, p->mode);
        num_sd++;
        break;
      case SD_TYPE_CHOWN:
        put_u8(rec, t); put_str(rec, p->path_a);
        put_u32(rec, p->uid); put_u32(rec, p->gid);
        num_sd++;
        break;
      default:
        break;
    }
  };

  if (!g_prune) {
    // Baseline: chronological, every record, no drop/merge.
    for (StateDiff* p = chrono; p; p = p->next) {
      if (p->sd_type == SD_TYPE_WRITE)
        emit_write_rec(p->cap_id, p->offset,
                       p->buf.empty() ? nullptr : p->buf.data(), p->buf.size());
      else if (p->sd_type == SD_TYPE_TRUNCATE)
        emit_truncate_rec(p->cap_id, p->offset);
      else
        emit_ns_rec(p);
    }
  } else {
    // Pruned + coalesced. Namespace records go out first, in their original
    // relative order (apply3's pass 1). Content is grouped per cap_id, keeping
    // each cap_id's chronological order (apply3's pass 2), dropping cap_ids
    // with no live binding, and coalescing write runs between truncates.
    std::vector<uint64_t> cap_order;  // first-seen order, for stable output
    std::unordered_map<uint64_t, std::vector<const StateDiff*>> cap_content;
    for (StateDiff* p = chrono; p; p = p->next) {
      if (p->sd_type == SD_TYPE_WRITE || p->sd_type == SD_TYPE_TRUNCATE) {
        if (!live_caps.count(p->cap_id)) continue;  // unbound: apply3 drops it
        auto& v = cap_content[p->cap_id];
        if (v.empty()) cap_order.push_back(p->cap_id);
        v.push_back(p);
      } else {
        emit_ns_rec(p);
      }
    }
    for (uint64_t cap : cap_order) {
      const auto& seq = cap_content[cap];
      size_t i = 0;
      while (i < seq.size()) {
        if (seq[i]->sd_type == SD_TYPE_TRUNCATE) {
          emit_truncate_rec(cap, seq[i]->offset);
          i++;
          continue;
        }
        // Gather a maximal run of consecutive writes (no truncate inside).
        std::vector<const StateDiff*> run;
        while (i < seq.size() && seq[i]->sd_type == SD_TYPE_WRITE)
          run.push_back(seq[i++]);
        for (const WriteSeg& s : coalesce_writes(run))
          emit_write_rec(cap, s.offset, s.bytes.data(), s.bytes.size());
      }
    }
  }
  put_u64(buf, num_sd);
  buf.insert(buf.end(), rec.begin(), rec.end());

  // Free the drained records.
  while (chrono) { StateDiff* nxt = chrono->next; delete chrono; chrono = nxt; }
  return buf;
}

// ============================================================================
static const struct fuse_lowlevel_ops fn_ops = []() {
  struct fuse_lowlevel_ops o;
  memset(&o, 0, sizeof(o));
  o.init = fn_init;
  o.destroy = fn_destroy;
  o.lookup = fn_lookup;
  o.forget = fn_forget;
  o.forget_multi = fn_forget_multi;
  o.getattr = fn_getattr;
  o.setattr = fn_setattr;
  o.readlink = fn_readlink;
  o.mknod = fn_mknod;
  o.mkdir = fn_mkdir;
  o.symlink = fn_symlink;
  o.link = fn_link;
  o.unlink = fn_unlink;
  o.rmdir = fn_rmdir;
  o.rename = fn_rename;
  o.opendir = fn_opendir;
  o.readdir = fn_readdir;
  o.readdirplus = fn_readdirplus;
  o.releasedir = fn_releasedir;
  o.fsyncdir = fn_fsyncdir;
  o.create = fn_create;
  o.open = fn_open;
  o.release = fn_release;
  o.flush = fn_flush;
  o.fsync = fn_fsync;
  o.read = fn_read;
  o.write = fn_write;
  o.statfs = fn_statfs;
  o.access = fn_access;
  return o;
}();

int main(int argc, char* argv[]) {
  (void)&fn_flush_noop;
  umask(0);

  g_capture = getenv_bool("FUSELOG_CAPTURE", true);  // shared contract
  g_prune = getenv_bool_ns("FUSENODE_PRUNE", "FUSELOG_PRUNE", true);
  g_coalesce = getenv_bool_ns("FUSENODE_COALESCE", "WRITE_COALESCING", false);
  g_compress = getenv_bool_ns("FUSENODE_COMPRESSION", "FUSELOG_COMPRESSION", true);
  g_writeback_cache =
      getenv_bool_ns("FUSENODE_WRITEBACK_CACHE", "FUSELOG_WRITEBACK_CACHE", false);
  g_profile = getenv_bool("FUSENODE_PROFILE", false);
  if (const char* s = getenv("FUSELOG_SOCKET_FILE")) g_socket_file = s;
  if (const char* s = getenv_ns("FUSENODE_ATTR_TIMEOUT", "FUSELOG_ATTR_TIMEOUT"))
    g_attr_timeout = atof(s);
  if (const char* s = getenv_ns("FUSENODE_ENTRY_TIMEOUT", "FUSELOG_ENTRY_TIMEOUT"))
    g_entry_timeout = atof(s);

  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  struct fuse_cmdline_opts opts;
  if (fuse_parse_cmdline(&args, &opts) != 0) return 1;
  if (opts.show_help || opts.show_version) {
    printf("fusenode: low-level (inode-based) fuselog statediff recorder\n");
    return 0;
  }
  if (opts.mountpoint == nullptr) {
    fprintf(stderr, "usage: %s [options] <mountpoint>\n", argv[0]);
    return 1;
  }

  // Mount-over-backing: the backing store is the mountpoint's UNDERLYING dir.
  // Open it O_PATH BEFORE mounting so the fd survives the mount and addresses
  // the real backing (matches high-level fuselog's safe_fd).
  g_root.fd = open(opts.mountpoint, O_PATH);
  if (g_root.fd == -1) {
    fprintf(stderr, "fusenode: open backing '%s': %s\n", opts.mountpoint,
            strerror(errno));
    return 1;
  }
  {
    struct stat st;
    if (fstatat(g_root.fd, "", &st, AT_EMPTY_PATH) == 0) {
      g_root.dev = st.st_dev;
      g_root.ino = st.st_ino;
      g_root.mode = st.st_mode;
    }
  }
  g_root.nlookup = 2;

  fprintf(stderr, "fusenode: backing='%s' socket='%s' writeback=%d capture=%d\n",
          opts.mountpoint, g_socket_file, (int)g_writeback_cache,
          (int)g_capture);

  struct fuse_session* se =
      fuse_session_new(&args, &fn_ops, sizeof(fn_ops), nullptr);
  if (se == nullptr) { fuse_opt_free_args(&args); return 1; }
  if (fuse_set_signal_handlers(se) != 0) {
    fuse_session_destroy(se);
    fuse_opt_free_args(&args);
    return 1;
  }
  if (fuse_session_mount(se, opts.mountpoint) != 0) {
    fuse_remove_signal_handlers(se);
    fuse_session_destroy(se);
    fuse_opt_free_args(&args);
    return 1;
  }

  fuse_daemonize(opts.foreground);

  // Bring up the command socket AFTER a successful mount AND after
  // fuse_daemonize(). fuse_daemonize() fork()s when not in foreground, and only
  // the calling thread survives a fork -- so a detached socket-listener thread
  // created before it would vanish in the daemonized child (the child inherits
  // the listening fd, so clients still connect() out of the kernel accept queue,
  // but nothing ever accept()s or answers 'g', hanging the recorder's drain/
  // capture read forever and blocking XDN's PB init before `docker run`). The
  // high-level fuselog avoids this by initializing its socket from the FUSE
  // init callback, which already runs post-fork; here we simply order the call
  // after fuse_daemonize(). The fuzz harness (foreground, no fork) is
  // unaffected, and XDN's Java client retries the connect until the child binds.
  if (!initialize_unix_socket())
    fprintf(stderr, "fusenode: WARNING socket init failed\n");

  // Install the profiling dumper AFTER fuse_daemonize()'s fork so the handler
  // lives in the surviving daemon child. SIGUSR1 -> write totals to
  // /tmp/fusenode_prof.txt. No-op unless FUSENODE_PROFILE=1.
  if (g_profile) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = dump_profile;
    sigaction(SIGUSR1, &sa, nullptr);
    fprintf(stderr, "fusenode: PROFILE enabled (SIGUSR1 -> /tmp/fusenode_prof.txt)\n");
  }

  int ret;
  if (opts.singlethread) {
    ret = fuse_session_loop(se);
  } else {
    // libfuse 3.10 (FUSE_USE_VERSION 31): the mt-loop macro takes clone_fd.
    ret = fuse_session_loop_mt(se, opts.clone_fd);
  }

  fuse_session_unmount(se);
  fuse_remove_signal_handlers(se);
  fuse_session_destroy(se);
  fuse_opt_free_args(&args);
  if (g_root.fd >= 0) close(g_root.fd);
  return ret ? 1 : 0;
}
