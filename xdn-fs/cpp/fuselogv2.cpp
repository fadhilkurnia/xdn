// fuselog - a thin file system layer to capture all persistent state changes.
//
// This implementation takes inspirations from FUSE example code and loggedFS.
// Compared to v1, this v2 coalesce the statediffs and store them temporarily
// in memory. Additionally, another program (e.g., xdn-proxy) needs to get the
// captured statediff via Linux socket after each state update (e.g., web
// service execution: receiving http request, updating data in database, and
// sending the http response).
//
// Pure compute helpers used by the write path (diff capture and gap merging)
// live in fuselog_internal.h and are exercised by test_fuselog.cpp.
//
// Install dependencies:
//   apt install pkg-config libfuse3-dev libzstd-dev
//   apt install libgtest-dev                  (only needed for unit tests)
//
// How to compile (canonical path uses the build script):
//   ./bin/build_xdn_fuselog.sh cpp            # build fuselog + fuselog-apply
//   ./bin/build_xdn_fuselog.sh test           # build and run GoogleTest suite
//
// Equivalent raw g++ command (requires fuselog_internal.h in the same dir):
//   g++ -Wall fuselogv2.cpp -o fuselog -D_FILE_OFFSET_BITS=64
//   $(pkg-config fuse3 --cflags --libs)
//   $(pkg-config libzstd --cflags --libs)
//   -pthread -O3 -std=c++11
//
// How to use:
//   ./fuselog <mount_point>
//   example:
//      ./fuselog /app/data
//      ./fuselog -f /app/data                  (running in the foreground)
//      ./fuselog -o allow_other /app/data      (allowing other users)
//
// Initial developer: Fadhil I. Kurnia (fikurnia@cs.umass.edu)

#define FUSE_USE_VERSION 31

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fuse.h>
#include <grp.h>
#include <pwd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <zstd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fuselog_internal.h"

using namespace std;

/*******************************************************************************
 *                   GLOBAL/STATIC VARIABLE DECLARATIONS                       *
 ******************************************************************************/
static char *mount_point; // the file system mount point
static int   safe_fd;     // file description of the mount point directory
static mutex fid_mutex;   // protects filename_to_fid only

// configurations for logging purposes
#define LOG_DEBUG    0
#define LOG_INFO     1
#define LOG_WARNING  2
#define LOG_ERROR    3
const int log_level = LOG_INFO;

// helper to read boolean environment variables with a default value
static bool getenv_bool(const char* name, bool default_val) {
  const char* val = getenv(name);
  if (!val) return default_val;
  return strcmp(val, "1") == 0 || strcmp(val, "true") == 0 ||
         strcmp(val, "TRUE") == 0 || strcmp(val, "yes") == 0;
}

// important variables for storing statediff (configurable via env vars)
static bool is_sd_capture  = true;   // set from FUSELOG_CAPTURE
static bool is_sd_coalesce = true;   // set from WRITE_COALESCING
static bool is_sd_prune    = true;   // set from FUSELOG_PRUNE
static bool is_sd_compress = false;  // set from FUSELOG_COMPRESSION
static ZSTD_CCtx* zstd_cctx = nullptr;
static const int ZSTD_COMPRESS_LEVEL = 1;
// TODO: allocate fresh fids on recreate.
// Every handler that observes a new (or reappearing) path looks the path up
// here and **reuses the existing fid** if one was previously assigned. That
// conflates multiple incarnations of the same path within a single batch:
// e.g. a path cycled file → unlink → mkdir → rmdir → file all carries one
// fid in the captured statediff, and apply cannot tell which actions belong
// to which incarnation. Under concurrent workloads with path recycling, this
// produces ~5% A!=C divergence in fuzz_concurrent_overlap.py.
// Fix: on every CREATE/MKDIR/SYMLINK (and on the to_fid of LINK/RENAME when
// the destination is fresh), allocate a NEW fid instead of reusing the
// existing entry. Wire-format-compatible (just larger fid space); the apply
// side already keys actions by fid alone.
static unordered_map<string, uint64_t> filename_to_fid;
struct statediff_action {
  uint8_t                          sd_type;
  uint64_t                         fid;
  uint64_t                         offset;
  std::unique_ptr<unsigned char[]> buffer;
  size_t                           buffer_size = 0;
  uint32_t                         uid;
  uint32_t                         gid;
  uint32_t                         mode;
  statediff_action*                next = nullptr;  // intrusive linked list pointer

  // Take ownership of an existing buffer (e.g. from a statediff_write_unit).
  void take_buffer(std::unique_ptr<unsigned char[]> b, size_t n) {
    buffer = std::move(b);
    buffer_size = n;
  }
  // Allocate and copy from `src`.
  void copy_buffer(const unsigned char* src, size_t n) {
    if (n == 0) { buffer.reset(); buffer_size = 0; return; }
    buffer.reset(new unsigned char[n]);
    memcpy(buffer.get(), src, n);
    buffer_size = n;
  }
};

// Lock-free Treiber stack: prepend via CAS. The list ends up in
// reverse-chronological order (newest first). send_gathered_statediffs
// reverses it before serializing so apply replays in the original order.
static atomic<statediff_action*> statediff_head{nullptr};

// Cumulative throughput counters for compute_diff. Updated lock-free
// (atomic fetch_add) on each write; printed on every harvest so the
// SIMD vs scalar dispatch can be A/B'd against real workloads.
//
// All accesses are gated by `if (log_level <= LOG_DEBUG)` so that at
// log_level=LOG_INFO (the production default) the compiler DCEs both
// the timing reads and the atomic stores at -O3. The atomics still
// exist as zero-initialized globals (~24 bytes BSS, ignorable); but
// the per-write fetch_add cost is gone.
static atomic<uint64_t> total_cd_bytes{0};   // total bytes scanned
static atomic<uint64_t> total_cd_ns{0};      // total ns spent in compute_diff
static atomic<uint64_t> total_cd_calls{0};   // number of compute_diff calls
// statediff_write_unit and COALESCE_ACTION_OVERHEAD live in fuselog_internal.h
const uint32_t min_width_coelasce = 0;      // 0, 32, or other size
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

// Lock-free prepend. Concurrent callers are serialized by the CAS; the
// chronological order they observe is reconstructed by the harvest path.
static void push_statediff(statediff_action* sd) {
  statediff_action* old_head = statediff_head.load(memory_order_relaxed);
  do {
    sd->next = old_head;
  } while (!statediff_head.compare_exchange_weak(
      old_head, sd, memory_order_release, memory_order_relaxed));
}

// configurations for notify socket
const  bool        enable_notify_socket = true;
static const char *fuselog_socket_file  = "/tmp/fuselog.sock";
static int         socket_fd            = 0;
static thread     *socket_listener_thread;

/*******************************************************************************
 *                        HELPER FUNCTION DECLARATIONS                         *
 ******************************************************************************/
static char *get_absolute_path(const char *path);
static char *get_relative_path(const char *path);
static char *get_username(uid_t uid);
static char *get_groupname(gid_t gid);
static void  logging(const int level, const char *format, ...);

// initialize_unix_socket() returns true iff it successfully open and listen 
// to a unix domain socket so the XDN Proxy can notify the barriers between 
// each request processing. XDN Proxy notifies Fuselog before sending the 
// HTTP request to the Web Service. 
static bool initialize_unix_socket();

/*******************************************************************************
 *                     FUSE HANDLER FUNCTION DECLARATIONS                      *
 ******************************************************************************/
static void *fuselog_init(struct fuse_conn_info *info, struct fuse_config *cfg);
static void  fuselog_destroy(void *private_data);
static int   fuselog_getattr(const char *orig_path, struct stat *stbuf,
                             struct fuse_file_info *fi);
static int   fuselog_access(const char *orig_path, int mask);
static int   fuselog_readlink(const char *orig_path, char *buf, size_t size);
static int   fuselog_readdir(const char *orig_path, void *buf,
                             fuse_fill_dir_t filler, off_t offset,
                             struct fuse_file_info  *fi,
                             enum fuse_readdir_flags flags);
static int   fuselog_mknod(const char *orig_path, mode_t mode, dev_t rdev);
static int   fuselog_mkdir(const char *orig_path, mode_t mode);
static int   fuselog_symlink(const char *from, const char *orig_to);
static int   fuselog_unlink(const char *orig_path);
static int   fuselog_rmdir(const char *orig_path);
static int   fuselog_rename(const char *orig_from, const char *orig_to,
                            unsigned int flags);
static int   fuselog_link(const char *orig_from, const char *orig_to);
static int   fuselog_chmod(const char *orig_path, mode_t mode,
                           struct fuse_file_info *fi);
static int   fuselog_chown(const char *orig_path, uid_t uid, gid_t gid,
                           struct fuse_file_info *f);
static int   fuselog_truncate(const char *orig_path, off_t size,
                              struct fuse_file_info *fi);
static int   fuselog_utimens(const char *orig_path, const struct timespec ts[2],
                             struct fuse_file_info *fi);
static int   fuselog_open(const char *orig_path, struct fuse_file_info *fi);
static int   fuselog_read(const char *orig_path, char *buf, size_t size,
                          off_t offset, struct fuse_file_info *fi);
static int   fuselog_write(const char *orig_path, const char *buf, size_t size,
                           off_t offset, struct fuse_file_info *fi);
static int   fuselog_statfs(const char *orig_path, struct statvfs *stbuf);
static int   fuselog_release(const char *orig_path, struct fuse_file_info *fi);
static int   fuselog_fsync(const char *orig_path, int isdatasync,
                           struct fuse_file_info *fi);

/*******************************************************************************
 *                      THE MAIN FUNCTION - ENTRY POINT                        *
 ******************************************************************************/

int main(int argc, char *argv[]) {
  // read important environment variables, if any
  if (char* env_p = getenv("FUSELOG_SOCKET_FILE")) {
    fuselog_socket_file = env_p;
  }
  is_sd_capture  = getenv_bool("FUSELOG_CAPTURE", true);
  is_sd_coalesce = getenv_bool("WRITE_COALESCING", true);
  is_sd_prune    = getenv_bool("FUSELOG_PRUNE", true);
  is_sd_compress = getenv_bool("FUSELOG_COMPRESSION", false);

  logging(LOG_INFO, " Welcome to the FuselogFS for XDN\n");
  logging(LOG_INFO, " - fuselog socket file : %s\n", fuselog_socket_file);
  logging(LOG_INFO, " - sd_capture          : %s\n", is_sd_capture  ? "true" : "false");
  logging(LOG_INFO, " - sd_coalesce         : %s\n", is_sd_coalesce ? "true" : "false");
  logging(LOG_INFO, " - sd_prune            : %s\n", is_sd_prune    ? "true" : "false");
  logging(LOG_INFO, " - sd_compress         : %s\n", is_sd_compress ? "true" : "false");
  
  // initialize fuse handlers
  fuse_operations ops;
  memset(&ops, 0, sizeof(fuse_operations));
  ops.init     = fuselog_init;
  ops.destroy  = fuselog_destroy;
  ops.getattr  = fuselog_getattr;
  ops.access   = fuselog_access;
  ops.readlink = fuselog_readlink;
  ops.readdir  = fuselog_readdir;
  ops.mknod    = fuselog_mknod;
  ops.mkdir    = fuselog_mkdir;
  ops.symlink  = fuselog_symlink;
  ops.unlink   = fuselog_unlink;
  ops.rmdir    = fuselog_rmdir;
  ops.rename   = fuselog_rename;
  ops.link     = fuselog_link;
  ops.chmod    = fuselog_chmod;
  ops.chown    = fuselog_chown;
  ops.truncate = fuselog_truncate;
  ops.utimens  = fuselog_utimens;
  ops.open     = fuselog_open;
  ops.read     = fuselog_read;
  ops.write    = fuselog_write;
  ops.statfs   = fuselog_statfs;
  ops.release  = fuselog_release;
  ops.fsync    = fuselog_fsync;

  // optional FUSE methods
  // TODO: create, fallocate, setxattr, getxattr, listxattr, removexattr,
  // copy_file_range, lseek.

  // parse the mount_point, and change the directory to there
  if (argc > 1) {
    mount_point = argv[argc - 1];
    logging(LOG_INFO, " - mount point         : %s\n", mount_point);
    int chdir_err = chdir(mount_point);
    if (chdir_err == -1) {
      logging(LOG_ERROR, "failed to chdir to mount point.\n");
      perror("reason:");
      return 0;
    }
    safe_fd = open(".", 0);
    if (safe_fd < 0) {
      logging(LOG_ERROR, "failed to open the mount point. err=%d\n", safe_fd);
      return 0;
    }
  }

  fuse_main(argc, argv, &ops, NULL);

  cout << "fuselog exit.\n";
  return 0;
}

/*******************************************************************************
 *                      HELPER FUNCTION IMPLEMENTATION                         *
 ******************************************************************************/

static char *get_absolute_path(const char *path) {
  uint64_t sum_len = strlen(path) + strlen(mount_point) + 1;
  char *real_path = (char *) malloc(sum_len * sizeof(char));

  strcpy(real_path, mount_point);
  if (real_path[strlen(real_path) - 1] == '/')
    real_path[strlen(real_path) - 1] = '\0';
  strcat(real_path, path);
  return real_path;
}

// Detect FUSE's silly-rename names. When userspace unlinks a file that still
// has open fds, the kernel renames it to `.fuse_hiddenXXXXXXXXXXXXXXXX` (16
// hex chars) and later unlinks that silly name once all fds close. We treat
// silly-renamed paths as non-user-visible so the captured statediff matches
// the user's intent (which is: unlink the original path).
static bool is_silly_name(const char *name) {
  if (name == NULL) return false;
  const char prefix[] = ".fuse_hidden";
  const size_t prefix_len = sizeof(prefix) - 1;
  if (strncmp(name, prefix, prefix_len) != 0) return false;
  for (size_t i = prefix_len; name[i]; i++) {
    if (!isxdigit((unsigned char) name[i])) return false;
  }
  return name[prefix_len] != '\0';  // require at least one hex char
}

static bool is_silly_path(const char *path) {
  if (path == NULL) return false;
  const char *base = strrchr(path, '/');
  base = base ? base + 1 : path;
  return is_silly_name(base);
}

static char *get_relative_path(const char *path) {
  if (path[0] == '/') {
    if (strlen(path) == 1) {
      return strdup(".");
    }
    const char *substr = &path[1];
    return strdup(substr);
  }

  return strdup(path);
}

static char *get_username(uid_t uid) {
  struct passwd *p = getpwuid(uid);
  if (p != NULL)
    return p->pw_name;
  return NULL;
}

static char *get_groupname(gid_t gid) {
  struct group *g = getgrgid(gid);
  if (g != NULL)
    return g->gr_name;
  return NULL;
}

static void logging(const int level, const char *format, ...) {
  va_list args;
  va_start(args, format);
  if (level >= log_level) vprintf(format, args);
  va_end(args);
}

// Reliable send: loops until all bytes are sent or an error occurs.
static int send_all(int fd, const char* buf, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = send(fd, buf + sent, len - sent, 0);
    if (n == -1) { perror("send_all"); return -1; }
    sent += (size_t)n;
  }
  return 0;
}

// send_gathered_statediffs serializes and sends all the statediffs, from
// the last send, into a socket of `conn_fd`.
static int send_gathered_statediffs(int conn_fd) {
  // format:
  //  size       8 bytes
  //  num_file   8 bytes, followed by num_file of
  //   - fid       8 bytes
  //   - len_path  8 bytes
  //   - path      len_path bytes 
  //  num_statediff 8 bytes, followed by num_statediff of statediff
  //   for unlink:
  //   - sd_type   1 bytes (always 1)
  //   - fid       8 bytes
  //   for write:
  //   - sd_type   1 bytes (always 0)
  //   - fid       8 bytes
  //   - count     8 bytes
  //   - offset    8 bytes
  //   - buffer    count bytes
  //   for rename:
  //   - sd_type   1 bytes (always 2)
  //   - from_fid  8 bytes
  //   - to_fid    8 bytes

  // Atomically harvest the entire statediff list — O(1), no mutex.
  statediff_action* head = statediff_head.exchange(nullptr, memory_order_acq_rel);

  // Print compute_diff throughput so SIMD vs scalar runs can be A/B'd.
  // Gated by log_level so production builds (LOG_INFO) get DCE'd here.
  if (log_level <= LOG_DEBUG) {
    uint64_t b = total_cd_bytes.load(memory_order_relaxed);
    uint64_t n = total_cd_ns.load(memory_order_relaxed);
    uint64_t c = total_cd_calls.load(memory_order_relaxed);
    double mbps = (n > 0) ? ((double) b / (double) n * 1e9 / (1024.0 * 1024.0)) : 0.0;
    double avg_ns = (c > 0) ? ((double) n / (double) c) : 0.0;
    fprintf(stderr,
            "[fuselog] compute_diff cumulative: %lu calls, %lu MiB scanned, "
            "%lu ms total, %.0f MiB/s, %.1f ns/call avg, SIMD=%s\n",
            (unsigned long) c,
            (unsigned long) (b >> 20),
            (unsigned long) (n / 1000000),
            mbps, avg_ns,
            fuselog_simd_name());
  }

  // Snapshot filename_to_fid under fid_mutex — brief hold.
  unordered_map<string, uint64_t> local_filename_to_fid;
  {
    lock_guard<mutex> lk(fid_mutex);
    local_filename_to_fid = std::move(filename_to_fid);
  }

  /* Counting the required space */
  uint64_t sum_len_path = 0;
  uint64_t num_statediffs = 0;
  uint64_t num_unlink = 0;
  uint64_t num_write = 0;
  uint64_t num_rename = 0;
  uint64_t num_create = 0;
  uint64_t num_link = 0;
  uint64_t num_truncate = 0;
  uint64_t num_chown = 0;
  uint64_t num_chmod = 0;
  uint64_t num_mkdir = 0;
  uint64_t num_rmdir = 0;
  uint64_t num_symlink = 0;
  uint64_t sum_len_wr_buf = 0;
  uint64_t sum_len_symlink_target = 0;

  statediff_action* serialized = head;
  if (is_sd_prune) {
    // Pruning: O(n) two-pass scan. A fid's actions are only safe to drop
    // when its lifecycle in this batch is unambiguous: exactly one
    // create-equivalent action, an unlink-equivalent action, and no
    // rename/link references. Multi-lifecycle fids (file deleted then
    // recreated under the same fid) and rename-touched fids preserve all
    // their actions, because a proper lifetime/rename-chain analysis is
    // out of scope here.
    unordered_set<uint64_t> unlinked_fids;
    unordered_set<uint64_t> renamed_fids;
    unordered_map<uint64_t, int> create_count;
    for (statediff_action* p = head; p; p = p->next) {
      if (p->sd_type == SD_TYPE_UNLINK || p->sd_type == SD_TYPE_RMDIR) {
        unlinked_fids.insert(p->fid);
      } else if (p->sd_type == SD_TYPE_RENAME ||
                 p->sd_type == SD_TYPE_LINK) {
        renamed_fids.insert(p->fid);
        renamed_fids.insert(p->offset);
      } else if (p->sd_type == SD_TYPE_CREATE ||
                 p->sd_type == SD_TYPE_MKDIR ||
                 p->sd_type == SD_TYPE_SYMLINK) {
        create_count[p->fid]++;
      }
    }

    // Pass 2: drop is_prunable actions for fids with a clean single-life
    // cycle. Keep the UNLINK itself so the backup actually removes the
    // file. Apply tolerates ENOENT on unlink.
    statediff_action* pruned = nullptr;
    for (statediff_action* p = head; p; ) {
      statediff_action* nxt = p->next;
      bool is_prunable = (p->sd_type == SD_TYPE_WRITE ||
                          p->sd_type == SD_TYPE_TRUNCATE ||
                          p->sd_type == SD_TYPE_CHMOD ||
                          p->sd_type == SD_TYPE_CHOWN ||
                          p->sd_type == SD_TYPE_CREATE);
      auto cc = create_count.find(p->fid);
      int ccount = (cc == create_count.end()) ? 0 : cc->second;
      bool fid_prunable = unlinked_fids.count(p->fid) &&
                          !renamed_fids.count(p->fid) &&
                          ccount <= 1;
      bool keep = !(is_prunable && fid_prunable);
      if (keep) {
        p->next = pruned;
        pruned = p;
      } else {
        delete p;
      }
      p = nxt;
    }
    serialized = pruned;
  } else {
    // No prune: head is in reverse-chronological order from CAS prepends.
    // The prune branch above happens to reverse-during-rebuild and so
    // emits chronological order; here we have to do that reversal
    // explicitly so apply sees actions in the order they happened.
    statediff_action* reversed = nullptr;
    for (statediff_action* p = serialized; p; ) {
      statediff_action* nxt = p->next;
      p->next = reversed;
      reversed = p;
      p = nxt;
    }
    serialized = reversed;
  }

  for (statediff_action* p = serialized; p; p = p->next) {
    switch (p->sd_type) {
      case SD_TYPE_WRITE:    num_write++;    break;
      case SD_TYPE_UNLINK:   num_unlink++;   break;
      case SD_TYPE_RENAME:   num_rename++;   break;
      case SD_TYPE_CREATE:   num_create++;   break;
      case SD_TYPE_LINK:     num_link++;     break;
      case SD_TYPE_TRUNCATE: num_truncate++; break;
      case SD_TYPE_CHOWN:    num_chown++;    break;
      case SD_TYPE_CHMOD:    num_chmod++;    break;
      case SD_TYPE_MKDIR:    num_mkdir++;    break;
      case SD_TYPE_RMDIR:    num_rmdir++;    break;
      case SD_TYPE_SYMLINK:  num_symlink++;  break;
    }
  }

  uint64_t num_file = local_filename_to_fid.size();
  for (auto& m : local_filename_to_fid) {
    sum_len_path += m.first.size();
  }
  for (statediff_action* p = serialized; p; p = p->next) {
    if (p->sd_type == SD_TYPE_WRITE) {
      sum_len_wr_buf += p->buffer_size;
    }
    if (p->sd_type == SD_TYPE_SYMLINK) {
      sum_len_symlink_target += p->buffer_size;
    }
  }
  num_statediffs = num_write + num_unlink + num_rename +
                   num_create + num_link + num_truncate + num_chown + num_chmod +
                   num_mkdir + num_rmdir + num_symlink;

  uint64_t data_size_head = 8 +                                // num_file
                            (num_file * 16) + sum_len_path;    // fid to filename
  uint64_t data_size_all =  data_size_head +
                            8 +                                // num_statediff
                            (num_unlink * 9) +                 // unlink: type+fid
                            (num_write * 25) + sum_len_wr_buf+ // write: type+fid+count+offset+buf
                            (num_rename * 17) +                // rename: type+from_fid+to_fid
                            (num_truncate * 17) +              // truncate: type+fid+size
                            (num_create * 21) +                // create: type+fid+uid+gid+mode
                            (num_link * 17) +                  // link: type+src_fid+new_fid
                            (num_chown * 17) +                 // chown: type+fid+uid+gid
                            (num_chmod * 13) +                 // chmod: type+fid+mode
                            (num_mkdir * 13) +                 // mkdir: type+fid+mode
                            (num_rmdir * 9) +                  // rmdir: type+fid
                            (num_symlink * 21) +               // symlink: type+fid+target_len+uid+gid
                            sum_len_symlink_target;            // symlink target data
  /* Serialize everything into one contiguous buffer */
  char *buffer = (char *) malloc(data_size_all);
  if (buffer == NULL) {
    logging(LOG_ERROR, "failed to get memory space for %lu bytes\n", data_size_all);
    return -1;
  }

  uint64_t pos = 0;

  /* filling up the filename to fid mapping */
  memcpy(buffer + pos, (void *)&num_file, sizeof(uint64_t));   // num_file
  pos += 8;
  for (auto& m : local_filename_to_fid) {
    uint64_t fid = m.second;
    uint64_t len_path = m.first.size();
    memcpy(buffer + pos, (void*)&fid, sizeof(uint64_t));
    memcpy(buffer + pos + 8, (void*)&len_path, sizeof(uint64_t));
    memcpy(buffer + pos + 16, m.first.c_str(), len_path);
    pos += 16 + len_path;
  }

  /* filling up the number of statediffs */
  memcpy(buffer + pos, (void*)&num_statediffs, sizeof(uint64_t));
  pos += 8;

  /* filling up the actual statediffs */
  uint64_t write_diff_sz = 0;
  for (statediff_action* sdp = serialized; sdp; sdp = sdp->next) {
    auto& sd = *sdp;
    uint8_t sd_type = sd.sd_type;
    uint64_t fid = sd.fid;

    switch (sd_type) {
    case SD_TYPE_UNLINK:
    case SD_TYPE_RMDIR:
      // [1:type][8:fid]
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      pos += 9;
      break;

    case SD_TYPE_WRITE: {
      // [1:type][8:fid][8:count][8:offset][count:buffer]
      uint64_t count = sd.buffer_size;
      uint64_t offset = sd.offset;
      if (count == 0) {
        logging(LOG_ERROR, "found write with empty buffer!\n");
        free(buffer);
        return -1;
      }
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&count, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8 + 8, (void*)&offset, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8 + 8 + 8, sd.buffer.get(), count);
      pos += 25 + count;
      write_diff_sz += count;
      break;
    }

    case SD_TYPE_RENAME:
    case SD_TYPE_LINK: {
      // [1:type][8:from_fid][8:to_fid]
      uint64_t from_fid = sd.fid;
      uint64_t to_fid = sd.offset;
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&from_fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&to_fid, sizeof(uint64_t));
      pos += 17;
      break;
    }

    case SD_TYPE_TRUNCATE: {
      // [1:type][8:fid][8:size]
      uint64_t truncate_size = sd.offset;
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&truncate_size, sizeof(uint64_t));
      pos += 17;
      break;
    }

    case SD_TYPE_CREATE: {
      // [1:type][8:fid][4:uid][4:gid][4:mode]
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&sd.uid, sizeof(uint32_t));
      memcpy(buffer + pos + 1 + 8 + 4, (void*)&sd.gid, sizeof(uint32_t));
      memcpy(buffer + pos + 1 + 8 + 4 + 4, (void*)&sd.mode, sizeof(uint32_t));
      pos += 21;
      break;
    }

    case SD_TYPE_CHOWN: {
      // [1:type][8:fid][4:uid][4:gid]
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&sd.uid, sizeof(uint32_t));
      memcpy(buffer + pos + 1 + 8 + 4, (void*)&sd.gid, sizeof(uint32_t));
      pos += 17;
      break;
    }

    case SD_TYPE_CHMOD:
    case SD_TYPE_MKDIR: {
      // [1:type][8:fid][4:mode]
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&sd.mode, sizeof(uint32_t));
      pos += 13;
      break;
    }

    case SD_TYPE_SYMLINK: {
      // [1:type][8:fid][4:target_len][target_len:target][4:uid][4:gid]
      uint32_t target_len = (uint32_t) sd.buffer_size;
      memcpy(buffer + pos, (void*)&sd_type, sizeof(uint8_t));
      memcpy(buffer + pos + 1, (void*)&fid, sizeof(uint64_t));
      memcpy(buffer + pos + 1 + 8, (void*)&target_len, sizeof(uint32_t));
      memcpy(buffer + pos + 1 + 8 + 4, sd.buffer.get(), target_len);
      memcpy(buffer + pos + 1 + 8 + 4 + target_len, (void*)&sd.uid, sizeof(uint32_t));
      memcpy(buffer + pos + 1 + 8 + 4 + target_len + 4, (void*)&sd.gid, sizeof(uint32_t));
      pos += 21 + target_len;
      break;
    }

    default:
      logging(LOG_ERROR, "unknown statediff type (%d)!\n", sd_type);
      break;
    } // end switch
  }

  // Free the pruned linked list nodes (no longer needed after serialization).
  while (serialized) { statediff_action* nxt = serialized->next; delete serialized; serialized = nxt; }

  // Sanity check: serialized size must match expected size.
  if (pos != data_size_all) {
    logging(LOG_ERROR, "serialized size %lu != expected %lu\n", pos, data_size_all);
    free(buffer);
    return -1;
  }

  // Determine what to send: compressed or uncompressed payload.
  char *payload = buffer;
  uint64_t payload_size = data_size_all;

  char *compressed = NULL;
  if (is_sd_compress && data_size_all > 0) {
    // Lazily initialize the compression context (reused across calls).
    if (zstd_cctx == NULL) {
      zstd_cctx = ZSTD_createCCtx();
      if (zstd_cctx == NULL) {
        logging(LOG_ERROR, "failed to create ZSTD_CCtx\n");
        free(buffer);
        return -1;
      }
    }

    size_t compress_bound = ZSTD_compressBound(data_size_all);
    compressed = (char *) malloc(compress_bound);
    if (compressed == NULL) {
      logging(LOG_ERROR, "failed to allocate compression buffer (%lu bytes)\n", compress_bound);
      free(buffer);
      return -1;
    }

    size_t compressed_size = ZSTD_compressCCtx(
        zstd_cctx, compressed, compress_bound,
        buffer, data_size_all, ZSTD_COMPRESS_LEVEL);
    if (ZSTD_isError(compressed_size)) {
      logging(LOG_ERROR, "zstd compression failed: %s\n", ZSTD_getErrorName(compressed_size));
      free(compressed);
      free(buffer);
      return -1;
    }

    payload = compressed;
    payload_size = (uint64_t) compressed_size;
  }

  /* Send: [8-byte payload_size] [payload] */
  if (send_all(conn_fd, (const char*)&payload_size, sizeof(uint64_t)) != 0) {
    logging(LOG_ERROR, "failed to send payload size\n");
    free(buffer);
    if (compressed) free(compressed);
    return -1;
  }
  if (payload_size > 0 && send_all(conn_fd, payload, payload_size) != 0) {
    logging(LOG_ERROR, "failed to send payload\n");
    free(buffer);
    if (compressed) free(compressed);
    return -1;
  }

  free(buffer);
  if (compressed) free(compressed);

  return 0;
}

static bool initialize_unix_socket() {
  logging(LOG_INFO, ">> open and listen to a unix socket in '%s'\n", 
          fuselog_socket_file);
  
  // prepare a socket file descriptor
  struct sockaddr_un address;
  socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_fd == -1) {
    logging(LOG_ERROR, "failed to open a unix domain socket.\n");
    return false;
  }

  // listen to the socket
  int address_len    = 0;
  int bind_code      = 0;
  int num_conn       = 1;
  address.sun_family = AF_UNIX;
  strcpy(address.sun_path, fuselog_socket_file);
  unlink(address.sun_path);
  address_len = strlen(address.sun_path) + sizeof(address.sun_family);
  bind_code   = bind(socket_fd, (struct sockaddr*)&address, address_len);
  if (bind_code != 0) {
    logging(LOG_ERROR, "failed to bind to a unix domain socket. err=%d\n", 
            bind_code);
    return false;
  }
  if (listen(socket_fd, num_conn) != 0) {
    logging(LOG_ERROR, "failed to listen to a unix domain socket\n");
    return false;
  }
  
  // listen to the incoming connections in different thread.
  // WARNING: current implementation assume a single client (xdn-proxy)
  auto sock_listener = [&]() {
    bool is_waiting = true;
    while (is_waiting) {
      unsigned int socket_len;
      int          conn_fd;
      logging(LOG_INFO, ">> waiting for a connection ...\n");
      
      conn_fd = accept(socket_fd, (struct sockaddr*)&address, &socket_len);
      if (conn_fd == -1) {
        logging(LOG_ERROR, "failed to accept a connection\n");
        continue;
      }
      
      int  data_recv = 0;
      int  send_err  = 0;
      char recv_buf[100];
      char send_buf[200];
      do {
        memset(recv_buf, 0, 100*sizeof(char));
        memset(send_buf, 0, 200*sizeof(char));
        
        data_recv = recv(conn_fd, recv_buf, 100, 0);
        if (data_recv == 0) {
          logging(LOG_ERROR, "failed receiving data\n");
          continue;
        }
        
        if (strstr(recv_buf, "y") != 0) {
          // proxy is going to do execution — nothing to do
        } else if (strstr(recv_buf, "g") != 0) {
          send_err = send_gathered_statediffs(conn_fd);
          if (send_err == -1) {
            logging(LOG_ERROR, "failed send statediffs to proxy\n");
            continue;
          }
          continue;
        } else {
          logging(LOG_WARNING, "unknown notification: '%s' (%d bytes)\n",
            recv_buf, data_recv);
          is_waiting = false;
        }

        strcpy(send_buf, "y\n");
        send_err = send(conn_fd, send_buf, strlen(send_buf)*sizeof(char), 0);
        if (send_err == -1) {
          logging(LOG_ERROR, "failed send ack to proxy\n");
          continue;
        }
        
      } while (data_recv > 0);
      
      close(conn_fd);
    }

    close(socket_fd);
  };
  socket_listener_thread = new thread(sock_listener);
  socket_listener_thread->detach();

  return true;
}

/*******************************************************************************
 *                    FUSE HANDLER FUNCTION IMPLEMENTATIONS                    *
 ******************************************************************************/

static void *fuselog_init(struct fuse_conn_info *info,
                          struct fuse_config    *cfg) {
  logging(LOG_INFO, ">> initialization ...\n");
  (void) info;
  cfg->use_ino = 1;
  
  /* Pick up changes from lower filesystem right away. This is
     also necessary for better hardlink support. When the kernel
     calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;
  
  // cfg->direct_io = 1;
  // cfg->kernel_cache = 1;

  info->max_write = 262144;
  
  // changing the working directory, as specified in the following link.
  // https://github.com/libfuse/libfuse/wiki/FAQ#if-a-filesystem-is-mounted-over-a-directory-how-can-i-access-the-old-contents
  logging(LOG_INFO, ">> go into the working directory '%s' ...\n", mount_point);
  int err = fchdir(safe_fd);
  if (err == -1) {
    logging(LOG_ERROR, "  + failed to open into the working directory \n");
    perror("reason: ");
    return NULL;
  }
  err = close(safe_fd);
  if (err != 0) {
    logging(LOG_ERROR, "  + failed to close fd of the working directory \n");
    perror("reason: ");
    return NULL;
  }

  // prepare socket and thread to send the captured statediff
  if (enable_notify_socket) {
    logging(LOG_INFO, ">> preparing socket and statediff sender ...\n");
    bool is_success = initialize_unix_socket();
    if (!is_success) {
      logging(LOG_ERROR, "    failed to initilize socket :(\n");
    }
  }

  return NULL;
}

static void fuselog_destroy(void *private_data) {
  // close socket and statediff sender thread
  if (enable_notify_socket) {
    logging(LOG_INFO, "closing socket and statediff sender ...\n");
    int err = close(socket_fd);
    if (err != 0) {
      logging(LOG_ERROR,
              "  + failed to close fd of the statediff sender's socket \n");
      perror("reason: ");
      return;
    }
  }

  // Drain any remaining statediff nodes to avoid memory leaks at teardown.
  statediff_action* p = statediff_head.exchange(nullptr, memory_order_acq_rel);
  while (p) { statediff_action* nxt = p->next; delete p; p = nxt; }

  return;
}

static int fuselog_getattr(const char *orig_path, struct stat *stbuf,
                           struct fuse_file_info *fi) {
  logging(LOG_DEBUG, ">> getattr %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = lstat(path, stbuf);
  free(path);
  if (res == -1) {
    return -errno;
  }

  return 0;
}

static int fuselog_access(const char *orig_path, int mask) {
  logging(LOG_DEBUG, ">> access %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = access(path, mask);
  free(path);
  if (res == -1) {
    return -errno;
  }

  return 0;
}

static int fuselog_readlink(const char *orig_path, char *buf, size_t size) {
  logging(LOG_DEBUG, ">> readlink %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = readlink(path, buf, size - 1);
  free(path);
  if (res == -1) {
    return -errno;
  }

  buf[res] = '\0';

  return 0;
}

static int fuselog_readdir(const char *orig_path, void *buf,
                           fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info  *fi,
                           enum fuse_readdir_flags flags) {
  logging(LOG_DEBUG, ">> readdir %s\n", orig_path);

  int            res;
  DIR           *dp;
  struct dirent *de;
  char          *path;

  path = get_relative_path(orig_path);
  dp   = opendir(path);
  if (dp == NULL) {
    free(path);
    res = -errno;
    return res;
  }

  while ((de = readdir(dp)) != NULL) {
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_ino  = de->d_ino;
    st.st_mode = de->d_type << 12;
    if (filler(buf, de->d_name, &st, 0, FUSE_FILL_DIR_PLUS))
      break;
  }

  closedir(dp);
  free(path);
  return 0;
}

static int fuselog_mknod(const char *orig_path, mode_t mode, dev_t rdev) {
  logging(LOG_INFO, ">> mknod %s\n", orig_path);

  int   res;
  char *path = get_relative_path(orig_path);

  if (S_ISREG(mode)) {
    logging(LOG_INFO, "  + %s %o S_IFRAG (normal file creation)\n", path, mode);

    res = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
    if (res >= 0) {
      res = close(res);
    }

  } else if (S_ISFIFO(mode)) {
    logging(LOG_INFO, "  + %s %o S_IFFIFO (fifo creation)\n", path, mode);
    res = mkfifo(path, mode);

  } else {
    res = mknod(path, mode, rdev);
    if (S_ISCHR(mode)) {
      logging(LOG_INFO, "  + %s %o (character device creation)\n", path, mode);

    } else if (S_ISBLK(mode)) {
      logging(LOG_INFO, "  + %s %o (block device creation)\n", path, mode);

    } else {
      logging(LOG_INFO, "  + %s %o\n", path, mode);
    }
  }

  if (res == -1) {
    free(path);
    return -errno;

  } else {
    int err = lchown(path, fuse_get_context()->uid, fuse_get_context()->gid);
    if (err == -1) {
      logging(LOG_ERROR, "  + failed to change owner\n");
      perror("reason: ");
      free(path);
      return -errno;
    }
    logging(LOG_INFO, "  + lchown uid:%d gid:%d\n", fuse_get_context()->uid,
           fuse_get_context()->gid);
  }

  if (is_sd_capture && fuse_get_context()->pid > 0) {
    string path_str(path);
    uint64_t cur_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(path_str)) {
      cur_fid = filename_to_fid[path_str];
    } else {
      cur_fid = filename_to_fid.size();
      filename_to_fid[path_str] = cur_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_CREATE;
    sd->fid     = cur_fid;
    sd->uid     = fuse_get_context()->uid;
    sd->gid     = fuse_get_context()->gid;
    sd->mode    = mode;
    push_statediff(sd);
  }

  free(path);
  return 0;
}

static int fuselog_mkdir(const char *orig_path, mode_t mode) {
  logging(LOG_INFO, ">> mkdir %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = mkdir(path, mode);

  if (res == -1) {
    free(path);
    return -errno;
  } else {
    int err = lchown(path, fuse_get_context()->uid, fuse_get_context()->gid);
    if (err == -1) {
      logging(LOG_ERROR, "  + failed to change owner\n");
      perror("reason: ");
      free(path);
      return -errno;
    }
    logging(LOG_INFO, "  + lchown uid:%d gid:%d\n", fuse_get_context()->uid,
           fuse_get_context()->gid);
  }

  if (is_sd_capture && fuse_get_context()->pid > 0) {
    string path_str(path);
    uint64_t cur_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(path_str)) {
      cur_fid = filename_to_fid[path_str];
    } else {
      cur_fid = filename_to_fid.size();
      filename_to_fid[path_str] = cur_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_MKDIR;
    sd->fid     = cur_fid;
    sd->mode    = mode;
    push_statediff(sd);
  }

  free(path);
  return 0;
}

static int fuselog_symlink(const char *from, const char *orig_to) {
  logging(LOG_INFO, ">> symlink %s %s\n", from, orig_to);

  char *to  = get_relative_path(orig_to);
  int   res = symlink(from, to);

  if (res == -1) {
    free(to);
    return -errno;
  } else {
    int err = lchown(to, fuse_get_context()->uid, fuse_get_context()->gid);
    if (err == -1) {
      logging(LOG_ERROR, "  + failed to change owner\n");
      perror("reason: ");
      free(to);
      return -errno;
    }
    logging(LOG_INFO, "  + lchown uid:%d gid:%d\n", fuse_get_context()->uid,
           fuse_get_context()->gid);
  }

  if (is_sd_capture && fuse_get_context()->pid > 0) {
    string to_str(to);
    uint64_t link_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(to_str)) {
      link_fid = filename_to_fid[to_str];
    } else {
      link_fid = filename_to_fid.size();
      filename_to_fid[to_str] = link_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_SYMLINK;
    sd->fid     = link_fid;
    sd->uid     = fuse_get_context()->uid;
    sd->gid     = fuse_get_context()->gid;
    // store symlink target path in buffer
    string from_str(from);
    sd->copy_buffer(
        reinterpret_cast<const unsigned char*>(from_str.data()),
        from_str.size());
    push_statediff(sd);
  }

  free(to);
  return 0;
}

static int fuselog_unlink(const char *orig_path) {
  logging(LOG_DEBUG, ">> unlink %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = unlink(path);

  if (res == -1) {
    free(path);
    return -errno;
  } 
  
  if (is_sd_capture == true && !is_silly_path(path)) {
    auto start_time = chrono::high_resolution_clock::now();
    if (fuse_get_context()->pid > 0) {

      // get the file id (fid)
      uint64_t cur_fid = 0;
      string path_str(path);
      fid_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        cur_fid = filename_to_fid[path_str];
      } else {
        cur_fid = filename_to_fid.size();
        filename_to_fid[path_str] = cur_fid;
      }
      fid_mutex.unlock();

      // capture and gather the statediff
      auto* sd = new statediff_action{};
      sd->sd_type = SD_TYPE_UNLINK;
      sd->fid     = cur_fid;
      sd->offset  = 0;
      push_statediff(sd);
    }
    auto end_time     = chrono::high_resolution_clock::now();
    auto sd_cap_time  = chrono::duration_cast<chrono::nanoseconds>
                        (end_time-start_time);
    logging(LOG_INFO, ">> unlink %s, sd-cap-time: %ldns\n" ,
            path, sd_cap_time.count());
  } else {
    logging(LOG_INFO, ">> unlink %s%s\n", path,
            is_silly_path(path) ? " (silly-rename cleanup; not captured)" : "");
  }

  free(path);
  return 0;
}

static int fuselog_rmdir(const char *orig_path) {
  logging(LOG_INFO, ">> rmdir %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = rmdir(path);

  if (res == -1) {
    free(path);
    return -errno;
  }

  if (is_sd_capture && fuse_get_context()->pid > 0) {
    string path_str(path);
    uint64_t cur_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(path_str)) {
      cur_fid = filename_to_fid[path_str];
    } else {
      cur_fid = filename_to_fid.size();
      filename_to_fid[path_str] = cur_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_RMDIR;
    sd->fid     = cur_fid;
    push_statediff(sd);
  }

  free(path);
  return 0;
}

static int fuselog_rename(const char *orig_from, const char *orig_to,
                          unsigned int flags) {
  logging(LOG_INFO, ">> rename %s %s\n", orig_from, orig_to);

  if (flags) {
    return -EINVAL;
  }

  char *from = get_relative_path(orig_from);
  char *to   = get_relative_path(orig_to);
  int   res  = rename(from, to);

  if (res != -1 && is_sd_capture == true) {
    auto start_time = chrono::high_resolution_clock::now();
    if (fuse_get_context()->pid > 0) {

      // Silly-rename detection: when the kernel renames X -> .fuse_hiddenZ
      // because something still has X open, capture the user-visible effect
      // (UNLINK X) rather than the FUSE-internal rename. The subsequent
      // ops on the silly name will be skipped by their handlers.
      bool to_is_silly   = is_silly_path(to);
      bool from_is_silly = is_silly_path(from);

      if (to_is_silly && !from_is_silly) {
        // Treat as UNLINK(from).
        uint64_t from_fid = 0;
        string   from_path_str(from);
        fid_mutex.lock();
        if (filename_to_fid.count(from_path_str)) {
          from_fid = filename_to_fid[from_path_str];
        } else {
          from_fid = filename_to_fid.size();
          filename_to_fid[from_path_str] = from_fid;
        }
        fid_mutex.unlock();

        auto* sd = new statediff_action{};
        sd->sd_type = SD_TYPE_UNLINK;
        sd->fid     = from_fid;
        push_statediff(sd);
      } else if (from_is_silly) {
        // Reverse silly-rename (rare). Skip capture entirely; the file
        // never had user-visible content at the silly name.
      } else {
        // Normal user-initiated rename.
        uint64_t from_fid = 0;
        string   from_path_str(from);
        uint64_t to_fid = 0;
        string   to_path_str(to);
        fid_mutex.lock();
        if (filename_to_fid.count(from_path_str)) {
          from_fid = filename_to_fid[from_path_str];
        } else {
          from_fid = filename_to_fid.size();
          filename_to_fid[from_path_str] = from_fid;
        }
        if (filename_to_fid.count(to_path_str)) {
          to_fid = filename_to_fid[to_path_str];
        } else {
          to_fid = filename_to_fid.size();
          filename_to_fid[to_path_str] = to_fid;
        }
        fid_mutex.unlock();

        auto* sd = new statediff_action{};
        sd->sd_type = SD_TYPE_RENAME;
        sd->fid     = from_fid;
        sd->offset  = to_fid;          // TODO: use another field!!!
        push_statediff(sd);
      }
    }
    auto end_time     = chrono::high_resolution_clock::now();
    auto sd_cap_time  = chrono::duration_cast<chrono::nanoseconds>
                        (end_time-start_time);
    logging(LOG_INFO, "  + sd-cap-time: %ldns\n", sd_cap_time.count());
  }

  free(from);
  free(to);

  if (res == -1) {
    return -errno;
  }

  return 0;
}

static int fuselog_link(const char *orig_from, const char *orig_to) {
  logging(LOG_INFO, ">> link %s %s\n", orig_from, orig_to);

  char *from = get_relative_path(orig_from);
  char *to   = get_relative_path(orig_to);
  int   res  = link(from, to);

  if (res == -1) {
    free(from);
    free(to);
    return -errno;
  } else {
    int err = lchown(to, fuse_get_context()->uid, fuse_get_context()->gid);
    if (err == -1) {
      logging(LOG_ERROR, "  + failed to change owner\n");
      perror("reason: ");
      free(from);
      free(to);
      return -errno;
    }
    logging(LOG_INFO, "  + lchown uid:%d gid:%d\n", fuse_get_context()->uid,
            fuse_get_context()->gid);
  }

  if (is_sd_capture && fuse_get_context()->pid > 0) {
    string from_str(from);
    string to_str(to);
    uint64_t from_fid = 0, to_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(from_str)) {
      from_fid = filename_to_fid[from_str];
    } else {
      from_fid = filename_to_fid.size();
      filename_to_fid[from_str] = from_fid;
    }
    if (filename_to_fid.count(to_str)) {
      to_fid = filename_to_fid[to_str];
    } else {
      to_fid = filename_to_fid.size();
      filename_to_fid[to_str] = to_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_LINK;
    sd->fid     = from_fid;
    sd->offset  = to_fid;
    push_statediff(sd);
  }

  free(from);
  free(to);

  return 0;
}

static int fuselog_chmod(const char *orig_path, mode_t mode,
                         struct fuse_file_info *fi) {
  logging(LOG_INFO, ">> chmod %s %o\n", orig_path, mode);

  (void)fi;
  char *path = get_relative_path(orig_path);
  int   res  = chmod(path, mode);

  if (res == -1) {
    free(path);
    return -errno;
  }

  if (is_sd_capture && fuse_get_context()->pid > 0 && !is_silly_path(path)) {
    string path_str(path);
    uint64_t cur_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(path_str)) {
      cur_fid = filename_to_fid[path_str];
    } else {
      cur_fid = filename_to_fid.size();
      filename_to_fid[path_str] = cur_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_CHMOD;
    sd->fid     = cur_fid;
    sd->mode    = mode;
    push_statediff(sd);
  }

  free(path);
  return 0;
}

static int fuselog_chown(const char *orig_path, uid_t uid, gid_t gid,
                         struct fuse_file_info *fi) {
  logging(LOG_INFO, ">> chown %s uid:%d gid:%d\n", orig_path, uid, gid);

  (void)fi;
  char *path = get_relative_path(orig_path);
  int   res  = lchown(path, uid, gid) == -1 ? -errno : 0;

  char *username  = get_username(uid);
  char *groupname = get_groupname(gid);
  if (username != NULL && groupname != NULL) {
    logging(LOG_INFO, "  + username:%s groupname:%s\n", username, groupname);
  }

  if (res == 0 && is_sd_capture && fuse_get_context()->pid > 0 &&
      !is_silly_path(path)) {
    string path_str(path);
    uint64_t cur_fid = 0;
    fid_mutex.lock();
    if (filename_to_fid.count(path_str)) {
      cur_fid = filename_to_fid[path_str];
    } else {
      cur_fid = filename_to_fid.size();
      filename_to_fid[path_str] = cur_fid;
    }
    fid_mutex.unlock();

    auto* sd = new statediff_action{};
    sd->sd_type = SD_TYPE_CHOWN;
    sd->fid     = cur_fid;
    sd->uid     = uid;
    sd->gid     = gid;
    push_statediff(sd);
  }

  free(path);
  return res;
}

static int fuselog_truncate(const char *orig_path, off_t size,
                            struct fuse_file_info *fi) {
  logging(LOG_INFO, ">> truncate %s size:%ld bytes\n", orig_path, size);

  char *path = get_relative_path(orig_path);
  int   res;

  if (fi != NULL)
    res = ftruncate(fi->fh, size);
  else
    res = truncate(path, size);

  if (res != -1 && is_sd_capture == true && fuse_get_context()->pid > 0 &&
      !is_silly_path(path)) {
    auto start_time = chrono::high_resolution_clock::now();
    {
      // get the file id (fid)
      uint64_t fid = 0;
      string   path_str(path);
      fid_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        fid = filename_to_fid[path_str];
      } else {
        fid = filename_to_fid.size();
        filename_to_fid[path_str] = fid;
      }
      fid_mutex.unlock();

      // capture and gather the statediff
      auto* sd = new statediff_action{};
      sd->sd_type = SD_TYPE_TRUNCATE;
      sd->fid     = fid;
      sd->offset  = size;          // TODO: use another field!!!
      push_statediff(sd);
    }
    auto end_time     = chrono::high_resolution_clock::now();
    auto sd_cap_time  = chrono::duration_cast<chrono::nanoseconds>
                        (end_time-start_time);
    logging(LOG_INFO, "  + sd-cap-time: %ldns\n", sd_cap_time.count());
  }

  free(path);
  if (res == -1)
    return -errno;

  return 0;
}

static int fuselog_utimens(const char *orig_path, const struct timespec ts[2],
                           struct fuse_file_info *fi) {
  logging(LOG_INFO, ">> truncate %s\n", orig_path);

  (void)fi;
  char *path = get_relative_path(orig_path);
  int   res  = utimensat(AT_FDCWD, path, ts, AT_SYMLINK_NOFOLLOW);

  free(path);
  if (res == -1)
    return -errno;

  return 0;
}

static int fuselog_open(const char *orig_path, struct fuse_file_info *fi) {
  char *path = get_relative_path(orig_path);

  // Strip O_DIRECT from the flags before opening the underlying file.
  // O_DIRECT requires the caller's buffer, file offset, and length to all
  // be aligned to the device block size; the buffer libfuse hands to our
  // fuselog_write is not 512-byte aligned, so a downstream pwrite would
  // fail with EINVAL. Since fuselog already sits between the app and the
  // backing FS, we transparently demote the open to buffered I/O. The
  // app's durability is unaffected because COMMIT-time fsync still flows
  // through fuselog_fsync.
  int open_flags = fi->flags & ~O_DIRECT;
  int   res  = open(path, open_flags);

  if (fi->flags & O_DIRECT) {
    logging(LOG_DEBUG, ">> open %s with O_DIRECT requested; stripped\n",
            orig_path);
  }

  // what type of open ? read, write, or read-write ?
  if (fi->flags & O_RDONLY) {
    logging(LOG_DEBUG, ">> open %s with mode: readonly\n", orig_path);
  } else if (fi->flags & O_WRONLY) {
    logging(LOG_DEBUG, ">> open %s with mode: writeonly\n", orig_path);
  } else if (fi->flags & O_RDWR) {
    logging(LOG_DEBUG, ">> open %s with mode: readwrite\n", orig_path);
  } else {
    logging(LOG_DEBUG, ">> open %s\n", orig_path);
  }
  if (fi->flags & O_TRUNC) {
    logging(LOG_DEBUG, ">> open %s with truncate\n", orig_path);
  }

  free(path);
  if (res == -1)
    return -errno;

  fi->fh = res;
  return 0;
}

static int fuselog_read(const char *orig_path, char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi) {
  logging(LOG_DEBUG, ">> read %s size: %ld bytes, offset: %ld\n", 
          orig_path, size, offset);

  char *path = get_relative_path(orig_path);
  int   res  = pread(fi->fh, buf, size, offset);

  if (res == -1) {
    res = -errno;
    logging(LOG_DEBUG, ">> read %s returns -1\n", orig_path);
  } else {
    logging(LOG_DEBUG, ">> read %s returns %d\n", orig_path, res);
  }

  free(path);
  return res;
}

static unsigned char printChar(char c) {
  unsigned char _c = (unsigned char) c;
  if (_c >= 32 && _c <= 126) {
    return _c;
  } else {
    return '?';
  }
}

static int fuselog_write(const char *orig_path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi) {
  int   fd;
  int   res;
  char *path = get_relative_path(orig_path);
  (void)fi;

  logging(LOG_INFO, ">> write size: %ld bytes, offset: %ld, path: %s\n", 
          size, offset, path);

  if (fi == NULL) {
    fd = open(path, O_WRONLY);
    logging(LOG_INFO, "   + use a new fd=%d O_WRONLY\n", fd);
  } else {
    fd = fi->fh;
    logging(LOG_INFO, "   + use an existing fd=%d\n", fd);
  }
  
  if (fd == -1) {
    logging(LOG_ERROR, "   + error: [OPEN-FAIL]\n", 
            path, size, offset);
    perror("   + reason: ");
    res = -errno;
    if (fi == NULL) close(fd);
    free(path);
    return res;
  }

  /* Capturing the actual differences by getting the diff between existing data
   * and the to-be-written data.
  */
  int64_t num_diff = -1;            // the number of actual differences
  vector<struct statediff_write_unit> coalesced_write;
  if (is_sd_capture && is_sd_coalesce && fuse_get_context()->pid > 0) {
    int      fd_ro;
    char     sd_buffer_rd[size];
    ssize_t  rd_size;
    
    fd_ro = open(path, O_RDONLY); // we need to get new fd (fd_ro) since the 
                                  // existing fd can be in write-only mode.
                                  // TODO: handle if file doesn't exist (ENOENT)
    if (fd_ro == -1) {
      logging(LOG_ERROR, "   + error: fail to open file to read old values\n");
      perror("   + reason: ");
      res = -errno;
      if (fi == NULL) close(fd);
      free(path);
      return res;
    }
    
    /* read the existing values in the file, if any */
    rd_size = pread(fd_ro, sd_buffer_rd, size, offset);
    if (rd_size == -1) {
      logging(LOG_ERROR, "   + error: failed to read old data\n");
      perror("   + reason: ");
      res = -errno;
      if (fi == NULL) close(fd);
      close(fd_ro);
      free(path);
      return res;
    }
    close(fd_ro); // close the read-only fd
    logging(LOG_INFO, "   + read old data w/ fd_ro=%d read_size=%ld "
            "from %d\n", 
            fd_ro, rd_size, size, sd_buffer_rd);

    chrono::steady_clock::time_point cd_t0;
    if (log_level <= LOG_DEBUG) cd_t0 = chrono::steady_clock::now();
    coalesced_write = compute_diff(
        (const unsigned char*) sd_buffer_rd,
        (const unsigned char*) buf,
        (size_t) rd_size, size, (uint64_t) offset, min_width_coelasce);
    if (log_level <= LOG_DEBUG) {
      auto cd_t1 = chrono::steady_clock::now();
      total_cd_ns.fetch_add(
          (uint64_t) chrono::duration_cast<chrono::nanoseconds>(
              cd_t1 - cd_t0).count(),
          memory_order_relaxed);
      total_cd_bytes.fetch_add((uint64_t) rd_size, memory_order_relaxed);
      total_cd_calls.fetch_add(1, memory_order_relaxed);
    }

    /* Calculate the number of different bytes */
    if (coalesced_write.size() > 0) num_diff = 0;
    for (const auto& cur_diff : coalesced_write) {
      num_diff += cur_diff.size;
    }

  } // end of coalescing write statediff

  /* do the actual write */
  // The underlying fd does not carry O_DIRECT: fuselog_open strips it
  // before opening so this pwrite is always against a buffered fd, which
  // has no alignment requirements on `buf`. A future enhancement could
  // honour O_DIRECT via posix_memalign'd bounce buffers if cache-bypass
  // semantics matter to some app — for now they don't.
  res = pwrite(fd, buf, size, offset);
  if (res == -1) {
    int err = errno;
    logging(LOG_ERROR, "   + write %s size: %ld bytes, offset: %ld "
            "[WRITE-FAIL] errno=%d (%s)\n",
            path, size, offset, err, strerror(err));
    res = -err;
    if (fi == NULL) close(fd);
    free(path);
    return res;
  }
  
  /* Store the statediff into an external file */
  if (is_sd_capture) {
    auto start_time = chrono::high_resolution_clock::now();

    /* Ignore statediff from pid == 0, and writes that land on a
       silly-renamed (.fuse_hiddenXXXX) inode — those bytes are about to
       be discarded when the last fd closes. */
    if (fuse_get_context()->pid > 0 && !is_silly_path(path)) {
      
      // get the file id (fid)
      uint64_t cur_fid = 0;
      string path_str(path);
      fid_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        cur_fid = filename_to_fid[path_str];
      } else {
        cur_fid = filename_to_fid.size();
        filename_to_fid[path_str] = cur_fid;
      }
      fid_mutex.unlock();

      if (is_sd_coalesce && coalesced_write.size() > 0) {
        // Phase 1: Merge adjacent chunks whose gap < COALESCE_ACTION_OVERHEAD.
        vector<statediff_write_unit> merged = merge_adjacent_chunks(
            std::move(coalesced_write),
            (const unsigned char*) buf, (uint64_t) offset,
            COALESCE_ACTION_OVERHEAD);

        // Phase 2: Compare coalesced cost vs raw cost. Use whichever is smaller.
        // Coalesced cost: N * 25 + D (N actions, D total diff bytes)
        // Raw cost:       1 * 25 + size (one action, full write buffer)
        uint64_t coalesced_data = 0;
        for (auto& m : merged) coalesced_data += m.size;
        uint64_t coalesced_cost = merged.size() * COALESCE_ACTION_OVERHEAD + coalesced_data;
        uint64_t raw_cost       = COALESCE_ACTION_OVERHEAD + size;

        if (coalesced_cost < raw_cost) {
          // Coalescing wins: push merged chunks
          for (auto& m : merged) {
            auto* sd = new statediff_action{};
            sd->sd_type = SD_TYPE_WRITE;
            sd->fid     = cur_fid;
            sd->offset  = m.offset;
            sd->take_buffer(std::move(m.buffer), m.size);
            push_statediff(sd);
          }
        } else {
          // Raw wins: push the full write buffer as one action
          auto* sd = new statediff_action{};
          sd->sd_type = SD_TYPE_WRITE;
          sd->fid     = cur_fid;
          sd->offset  = offset;
          sd->copy_buffer(reinterpret_cast<const unsigned char*>(buf), size);
          push_statediff(sd);
        }
      } else if (is_sd_coalesce) {
        // Coalescing produced no diff chunks — no data changed, nothing to push.
      }

      if (!is_sd_coalesce) {
        auto* sd = new statediff_action{};
        sd->sd_type = SD_TYPE_WRITE;
        sd->fid     = cur_fid;
        sd->offset  = offset;
        sd->copy_buffer(reinterpret_cast<const unsigned char*>(buf), size);
        push_statediff(sd);
      }
    }

    auto end_time     = chrono::high_resolution_clock::now();
    auto sd_cap_time  = chrono::duration_cast<chrono::nanoseconds>
                        (end_time-start_time);

    logging(LOG_INFO, "   + sd-cap-time: %ldns [WRITE-SUCCESS]\n", 
            sd_cap_time.count());

    /* Debugging purposes: Logging the actual state differences */
    if (coalesced_write.size() > 0) {
      logging(LOG_INFO, "   + written %d diff bytes of %ld\n", num_diff, size);

      uint64_t print_count = 0;
      for (const auto& cur_diff : coalesced_write) {
        logging(LOG_DEBUG, "     + %5ld..%5ld: \n",
                cur_diff.offset,
                cur_diff.offset + cur_diff.size - 1);

        uint64_t i = 0;
        uint64_t max_print_count = 5;
        while (i < cur_diff.size && i < max_print_count) {
        logging(LOG_DEBUG, "                      %4d(%1c) ==> %4d(%1c) \n",
                (unsigned char) cur_diff.buffer.get()[i],
                printChar((char) cur_diff.buffer.get()[i]),
                (unsigned char) buf[i],
                printChar((char) buf[i]));

        i++;
        if (i == max_print_count && cur_diff.size != max_print_count) {
          logging(LOG_DEBUG, "                      <...>\n");
          break;
        }
        }
        
        print_count++;
        if (print_count > 25) {
          logging(LOG_DEBUG, "     <...>\n");
          break;
        }
      }
      if (num_diff > (ssize_t) size) {
        logging(LOG_ERROR, " ===== diff (%ld) is larger than size (%ld)\n\n\n", 
                num_diff, size);
        if (fi == NULL) close(fd);
        free(path);
        return -1;
      }
    }
    if (is_sd_coalesce && num_diff == -1) {
      logging(LOG_INFO, "   + all written data is new\n");
    }

  } // end is_sd_capture == true
  
  if (!is_sd_capture) {
    logging(LOG_INFO, "   + [WRITE-SUCCESS]\n");
  }

  if (fi == NULL) close(fd);
  free(path);
  return res;
}

static int fuselog_statfs(const char *orig_path, struct statvfs *stbuf) {
  logging(LOG_DEBUG, ">> statfs %s\n", orig_path);

  char *aPath = get_absolute_path(orig_path);
  char *path  = get_relative_path(orig_path);
  int   res   = statvfs(path, stbuf);

  free(path);
  free(aPath);
  if (res == -1)
    return -errno;

  return 0;
}

static int fuselog_release(const char *orig_path, struct fuse_file_info *fi) {
  logging(LOG_DEBUG, ">> release %s\n", orig_path);

  (void)orig_path;
  close(fi->fh);
  return 0;
}

static int fuselog_fsync(const char *orig_path, int isdatasync,
                         struct fuse_file_info *fi) {
  logging(LOG_INFO, ">> fsync %s, isdatasync: %d\n", orig_path, isdatasync);

  (void)orig_path;
  (void)isdatasync;
  (void)fi;

  int res;

  auto start_time = chrono::high_resolution_clock::now();
  if (isdatasync) {
    res = fdatasync(fi->fh);
  } else {
    res = fsync(fi->fh);
  }
  if (res == -1)
    return -errno;
  auto end_time   = chrono::high_resolution_clock::now();
  auto spent_time = chrono::duration_cast<chrono::nanoseconds>
                    (end_time-start_time);
  logging(LOG_INFO, "   + time-spent: %ldns\n", spent_time.count());

  return 0;
}
