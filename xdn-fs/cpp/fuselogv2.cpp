// fuselog - a thin file system layer to capture all persistent state changes.
//
// This implementation takes inspirations from FUSE example code and loggedFS.
// Compared to v1, this v2 coalesce the statediffs and store them temporarily
// in memory. Additionally, another program (e.g., xdn-proxy) needs to get the
// captured statediff via Linux socket after each state update (e.g., web
// service execution: receiving http request, updating data in database, and
// sending the http response).
//
// Other than the fuselib library, we intentionally do not use other third party
// libraries (e.g., boost, abseil, etc) so the compilation process is simple:
// it is only this single file. We might revisit this in the future if this
// research prototype becomes more complex.
//
// Install dependencies:
//   apt install pkg-config libfuse3-dev
//
// How to compile:
//   g++ -Wall fuselogv2.cpp -o fuselog -D_FILE_OFFSET_BITS=64 \
//       `pkg-config fuse3 --cflags --libs` -pthread -O3 -std=c++11
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

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

/*******************************************************************************
 *                   GLOBAL/STATIC VARIABLE DECLARATIONS                       *
 ******************************************************************************/
static char *mount_point; // the file system mount point
static int   safe_fd;     // file description of the mount point directory
static mutex sd_mutex;    // mutex to prevents concurrent update of statediff

// configurations for logging purposes
#define LOG_DEBUG    0
#define LOG_INFO     1
#define LOG_WARNING  2
#define LOG_ERROR    3
const int log_level = LOG_INFO;

// important variables for storing statediff
const bool is_sd_capture  = true;
const bool is_sd_coalesce = true;
const bool is_sd_prune    = true;
const bool is_sd_compress = false;
static unordered_map<string, uint64_t> filename_to_fid;
struct statediff_action {
  uint8_t                sd_type;
  uint64_t               fid;
  uint64_t               offset;
  vector<unsigned char>  buffer;
};
vector<struct statediff_action> statediffs;  // per-request statediffs
struct statediff_write_unit {                // for coalescing
  uint64_t               offset;
  vector<unsigned char>  buffer;
};
const uint32_t min_width_coelasce = 0;      // 0, 32, or other size
#define SD_TYPE_WRITE    0
#define SD_TYPE_UNLINK   1
#define SD_TYPE_RENAME   2
#define SD_TYPE_TRUNCATE 3

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

  logging(LOG_INFO, " Welcome to the FuselogFS for XDN\n");
  logging(LOG_INFO, " - fuselog socket file : %s\n", fuselog_socket_file);
  
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

  sd_mutex.lock();

  /* Counting the required space */
  uint64_t num_file = filename_to_fid.size();
  uint64_t sum_len_path = 0;
  uint64_t num_statediffs = 0;
  uint64_t num_unlink = 0;
  uint64_t num_write = 0;
  uint64_t num_rename = 0;
  uint64_t sum_len_wr_buf = 0;

  // prunning: remove writes to a deleted file
  if (is_sd_prune) {
    vector<struct statediff_action> pruned_statediffs;
    unordered_set<uint64_t> removed_files;
    for (ssize_t i = statediffs.size()-1; i >= 0; i--) {
      auto sd = statediffs[i];
      if (sd.sd_type == SD_TYPE_UNLINK) {
        removed_files.insert(sd.fid);
        pruned_statediffs.push_back(sd);
        continue;
      }
      if (sd.sd_type == SD_TYPE_WRITE || sd.sd_type == SD_TYPE_TRUNCATE) {
        if (removed_files.count(sd.fid) == 0) {
          pruned_statediffs.push_back(sd);
        }
        continue;
      }
      if (sd.sd_type == SD_TYPE_RENAME) {
        uint64_t from_fid = sd.fid;
        uint64_t to_fid = sd.offset;
        if (removed_files.count(to_fid) == 1) {
          removed_files.insert(from_fid);
        }
        pruned_statediffs.push_back(sd);
        continue;
      }
    }
    /* remove the removed file from the `filename_to_fid` map */
    // unordered_set<string> removed_filenames;
    // for (auto f : filename_to_fid) {
    //   if (removed_files.count(f.second) > 0) {
    //     removed_filenames.insert(f.first);
    //   }
    // }
    // for (auto f : removed_filenames) {
    //   filename_to_fid.erase(f);
    // }
    statediffs.clear();
    reverse(pruned_statediffs.begin(), pruned_statediffs.end());
    statediffs = pruned_statediffs; // reinitialize the statediffs
  }
  
  for (auto m : filename_to_fid) {
    sum_len_path += m.first.size();
  }
  for (auto sd : statediffs) {
    if (sd.sd_type == SD_TYPE_UNLINK) {
      num_unlink += 1;
    }
    if (sd.sd_type == SD_TYPE_RENAME) {
      num_rename += 1;
    }
    if (sd.sd_type == SD_TYPE_WRITE) {
      num_write += 1;
      sum_len_wr_buf += sd.buffer.size();
    }
  }
  num_statediffs = num_write + num_unlink + num_rename;

  uint64_t data_size_head = 8 +                                // num_file
                            (num_file * 16) + sum_len_path;    // fid to filename
  uint64_t data_size_all =  data_size_head +
                            8 +                                // num_statediff
                            (num_unlink * 9) +                 // unlink statediff
                            (num_write * 25) + sum_len_wr_buf+ // write statediff
                            (num_rename * 17);                 // rename statediff
  printf(">>>>>> overall size %lu\n", data_size_all);
  printf(">>>>>> # write %lu\n", num_write);
  printf(">>>>>> # unlink %lu\n", num_unlink);
  printf(">>>>>> # rename %lu\n", num_rename);

  /* allocating the space */
  char *buffer = (char *) malloc(8 + (data_size_head * sizeof(char)));
  if (buffer == NULL) {
    printf("error: failed to get memory space\n");
    sd_mutex.unlock();
    return -1;
  }

  printf(">>>>>> filling up space\n");
  /* filling up the overall size */
  memcpy(buffer, (void *)&data_size_all, sizeof(uint64_t));  // size overall
  
  /* filling up the filename to fid mapping */
  printf(">>>>>> filename-to-fid map\n");
  memcpy(buffer + 8, (void *)&num_file, sizeof(uint64_t));   // num_file
  uint64_t cur_offset = 16;
  for (auto m : filename_to_fid) {
    uint64_t fid = m.second;
    uint64_t len_path = m.first.size();
    string path = m.first;
    memcpy(buffer + cur_offset, (void*)&fid, sizeof(uint64_t));           // fid
    memcpy(buffer + cur_offset + 8, (void*)&len_path, sizeof(uint64_t));  // len_path
    memcpy(buffer + cur_offset + 16 , path.c_str(), len_path);            // len_path
    cur_offset += 16 + len_path;
  }

  /* sending the overall size and header mapping */
  ssize_t send_err = send(conn_fd, buffer, cur_offset, 0);
  if (send_err == -1) {
    printf("error: failed to send size and header mapping\n");
    perror(" reason: ");
    sd_mutex.unlock();
    return -1;
  }
  if (send_err != (ssize_t) cur_offset) {
    printf("error: failed to send all header\n");
    sd_mutex.unlock();
    return -1;
  }
  free(buffer);

  /* sending the number of statediffs */
  printf(">>>>>> statdiffs size \n");
  send_err = send(conn_fd, (char*)&num_statediffs, sizeof(uint64_t), 0);
    if (send_err == -1) {
    printf("error: failed to send number of statediffs\n");
    perror(" reason: ");
    sd_mutex.unlock();
    return -1;
  }
  if (send_err != (ssize_t) sizeof(uint64_t)) {
    printf("error: failed to send all #statediffs\n");
    sd_mutex.unlock();
    return -1;
  }
  
  /* sending the actual statediffs */
  uint64_t write_diff_sz = 0;
  printf(">>>>>> statdiffs \n");
  char *sd_buffer = NULL; uint64_t max_data_sz = 0;
  for (auto sd : statediffs) {
    uint8_t sd_type = sd.sd_type;
    uint64_t fid = sd.fid;

    if (sd_type != SD_TYPE_UNLINK && 
        sd_type != SD_TYPE_WRITE && 
        sd_type != SD_TYPE_RENAME && 
        sd_type != SD_TYPE_TRUNCATE) {
        printf("error: unknown statediff type (%d)! \n", sd_type);
        continue;
    }

    /* get the required size and allocate the buffer (sd_buffer), if needed */
    uint64_t sd_buf_len = 9;           // default: size for unlink statediff = 9
    if (sd_type == SD_TYPE_WRITE) {    // size for write depends on the buffer
      sd_buf_len = 25 + sd.buffer.size();
      if (sd.buffer.size() == 0) {
        printf("error: found write with empty buffer!\n");
        sd_mutex.unlock();
        return -1;
      }
    }
    if (sd_type == SD_TYPE_RENAME || sd_type == SD_TYPE_TRUNCATE) {
      sd_buf_len = 17;
    }
    if (max_data_sz < sd_buf_len) {
      if (sd_buffer != NULL) free(sd_buffer);
      sd_buffer = (char *) malloc(sd_buf_len * sizeof(char));
      if (sd_buffer == NULL) {
        printf("error: failed to allocate mem for statediffs\n");
        sd_mutex.unlock();
        return -1;
      }
      max_data_sz = sd_buf_len;
    }
    
    /* send the actual statediffs */
    if (sd_type == SD_TYPE_UNLINK) {
      memcpy(sd_buffer, (void*)&sd_type, sizeof(uint8_t));     // sd_type
      memcpy(sd_buffer + 1, (void*)&fid, sizeof(uint64_t));    // fid
      size_t num_sent = 0;
      while (num_sent < sd_buf_len) {
        send_err = send(conn_fd, sd_buffer + num_sent, 
                       (size_t) sd_buf_len - num_sent, 0);
        if (send_err == -1) {
          printf("error: failed to send an unlink statediff\n");
          perror(" reason");
          sd_mutex.unlock();
          break;
        }
        num_sent = num_sent + (size_t) send_err;
      }
      if (num_sent != (size_t) sd_buf_len) {
        printf("error: failed to send an unlink statediff. %ld!=%ld\n",
                num_sent, sd_buf_len);
        sd_mutex.unlock();
        return -1;
      }
    }
    if (sd_type == SD_TYPE_WRITE) {
      uint64_t count = sd.buffer.size();
      uint64_t offset = sd.offset;
      unsigned char *wbuffer = sd.buffer.data();
      memcpy(sd_buffer, (void*)&sd_type, sizeof(uint8_t));              // sd_type
      memcpy(sd_buffer + 1, (void*)&fid, sizeof(uint64_t));             // fid
      memcpy(sd_buffer + 1 + 8, (void*)&count, sizeof(uint64_t));       // count
      memcpy(sd_buffer + 1 + 8 + 8, (void*)&offset, sizeof(uint64_t));  // offset
      memcpy(sd_buffer + 1 + 8 + 8 + 8, (void *) wbuffer, count);       // write buffer
      size_t num_sent = 0;
      while (num_sent < sd_buf_len) {
        send_err = send(conn_fd, sd_buffer + num_sent, 
                       (size_t) sd_buf_len - num_sent, 0);
        if (send_err == -1) {
          printf("error: failed to send a write statediff\n");
          perror(" reason");
          break;
        }
        num_sent = num_sent + (size_t) send_err;
      }
      if (num_sent != (size_t) sd_buf_len) {
        printf("error: failed to send a write statediff. %ld!=%ld\n",
                num_sent, sd_buf_len);
        sd_mutex.unlock();
        return -1;
      }
      write_diff_sz += count;
    }
    if (sd_type == SD_TYPE_RENAME) {
      uint64_t from_fid = sd.fid;
      uint64_t to_fid = sd.offset;
      memcpy(sd_buffer, (void*)&sd_type, sizeof(uint8_t));         // sd_type
      memcpy(sd_buffer + 1, (void*)&from_fid, sizeof(uint64_t));   // from_fid
      memcpy(sd_buffer + 1 + 8, (void*)&to_fid, sizeof(uint64_t)); // to_fid
      size_t num_sent = 0;
      while (num_sent < sd_buf_len) {
        send_err = send(conn_fd, sd_buffer + num_sent, 
                       (size_t) sd_buf_len - num_sent, 0);
        if (send_err == -1) {
          printf("error: failed to send a rename statediff\n");
          perror(" reason");
          sd_mutex.unlock();
          break;
        }
        num_sent = num_sent + (size_t) send_err;
      }
      if (num_sent != (size_t) sd_buf_len) {
        printf("error: failed to send a rename statediff. %ld!=%ld\n",
                num_sent, sd_buf_len);
        sd_mutex.unlock();
        return -1;
      }
    }
    if (sd_type == SD_TYPE_TRUNCATE) {
      uint64_t fid = sd.fid;
      uint64_t truncate_size = sd.offset;
      memcpy(sd_buffer, (void*)&sd_type, sizeof(uint8_t));                // sd_type
      memcpy(sd_buffer + 1, (void*)&fid, sizeof(uint64_t));               // fid
      memcpy(sd_buffer + 1 + 8, (void*)&truncate_size, sizeof(uint64_t)); // size
      size_t num_sent = 0;
      while (num_sent < sd_buf_len) {
        send_err = send(conn_fd, sd_buffer + num_sent, 
                       (size_t) sd_buf_len - num_sent, 0);
        if (send_err == -1) {
          printf("error: failed to send a truncate statediff\n");
          perror(" reason");
          sd_mutex.unlock();
          break;
        }
        num_sent = num_sent + (size_t) send_err;
      }
      if (num_sent != (size_t) sd_buf_len) {
        printf("error: failed to send a truncate statediff. %ld!=%ld\n",
                num_sent, sd_buf_len);
        sd_mutex.unlock();
        return -1;
      }
    }

  }
  if (sd_buffer != NULL) free(sd_buffer);

  printf(">>>>>> total write statediff buffer size: %ld bytes\n", write_diff_sz);
  printf(">>>>>> reset global\n");
  filename_to_fid.clear();
  statediffs.clear();
  printf(">>>>>> reset global %ld %ld\n", filename_to_fid.size(), statediffs.size());

  printf(">>>>>> send\n");
  sd_mutex.unlock();
  return 0; //send(conn_fd, buffer, data_size, 0);
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
        printf("failed to accept a connection\n");
        continue;
      }
      
      printf("received a connection.\n");
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
          printf("  proxy is going to do execution\n");
          printf("  ------------------------------\n");
        } else if (strstr(recv_buf, "g") != 0) {
          printf("  XDN: send the gathered statediff\n");
          printf("  ------------------------------\n");
          send_err = send_gathered_statediffs(conn_fd);
          if (send_err == -1) {
            printf("error failed send statediffs to proxy\n");
            continue;
          }
          continue;
        } else {
          printf("  unknown notification: '%s' (%d bytes)\n", 
            recv_buf, data_recv);
          is_waiting = false;
        }

        strcpy(send_buf, "y\n");
        send_err = send(conn_fd, send_buf, strlen(send_buf)*sizeof(char), 0);
        if (send_err == -1) {
          printf("failed send ack to proxy\n");
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
  
  if (is_sd_capture == true) {
    auto start_time = chrono::high_resolution_clock::now();
    if (fuse_get_context()->pid > 0) {
      
      // get the file id (fid)
      uint64_t cur_fid = 0;
      string path_str(path);
      sd_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        cur_fid = filename_to_fid[path_str];
      } else {
        cur_fid = filename_to_fid.size();
        filename_to_fid[path_str] = cur_fid;
      }
      sd_mutex.unlock();

      // capture the statediff
      struct statediff_action cur_sd;
      cur_sd.sd_type = SD_TYPE_UNLINK;
      cur_sd.fid     = cur_fid;
      cur_sd.offset  = 0;

      // gather the statediff
      sd_mutex.lock();
      statediffs.push_back(cur_sd);
      sd_mutex.unlock();
    }
    auto end_time     = chrono::high_resolution_clock::now();
    auto sd_cap_time  = chrono::duration_cast<chrono::nanoseconds>
                        (end_time-start_time);
    logging(LOG_INFO, ">> unlink %s, sd-cap-time: %ldns\n" , 
            path, sd_cap_time.count());
  } else {
    logging(LOG_INFO, ">> unlink %s\n", path);
  }

  free(path);
  return 0;
}

static int fuselog_rmdir(const char *orig_path) {
  logging(LOG_INFO, ">> rmdir %s\n", orig_path);

  char *path = get_relative_path(orig_path);
  int   res  = rmdir(path);

  free(path);
  if (res == -1)
    return -errno;
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
      
      // get the file id (fid)
      uint64_t from_fid = 0;
      string   from_path_str(from);
      uint64_t to_fid = 0;
      string   to_path_str(to);
      sd_mutex.lock();
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
      sd_mutex.unlock();

      // capture the statediff
      struct statediff_action cur_sd;
      cur_sd.sd_type = SD_TYPE_RENAME;
      cur_sd.fid     = from_fid;
      cur_sd.offset  = to_fid;          // TODO: use another field!!!

      // gather the statediff
      sd_mutex.lock();
      statediffs.push_back(cur_sd);
      sd_mutex.unlock();
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

  free(path);

  if (res == -1) {
    return -errno;
  }

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

  if (res != -1 && is_sd_capture == true && fuse_get_context()->pid > 0) {
    auto start_time = chrono::high_resolution_clock::now();
    {
      // get the file id (fid)
      uint64_t fid = 0;
      string   path_str(path);
      sd_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        fid = filename_to_fid[path_str];
      } else {
        fid = filename_to_fid.size();
        filename_to_fid[path_str] = fid;
      }
      sd_mutex.unlock();

      // capture the statediff
      struct statediff_action cur_sd;
      cur_sd.sd_type = SD_TYPE_TRUNCATE;
      cur_sd.fid     = fid;
      cur_sd.offset  = size;          // TODO: use another field!!!

      // gather the statediff
      sd_mutex.lock();
      statediffs.push_back(cur_sd);
      sd_mutex.unlock();
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
  int   res  = open(path, fi->flags);

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
   * WARNING: Current implementation assumes single-threaded FUSE!
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

    /* compare old bytes (in buf) and new bytes (in sd_buffer_rd) */
    ssize_t i = 0;
    while (i < rd_size) {
      struct statediff_write_unit cur;
      cur.offset = offset + i;

      /* keep iterating through non-equal contiguous values, if they are
         equal we dont need to keep the statediff.
         Coalesce with a minimum length of `min_width_coelasce` */
      if ((unsigned char) sd_buffer_rd[i] != (unsigned char) buf[i]) {
        while (((unsigned char) sd_buffer_rd[i] != (unsigned char) buf[i] ||
                ((unsigned char) sd_buffer_rd[i] == (unsigned char) buf[i] &&
                cur.buffer.size() < min_width_coelasce)) &&
              i < rd_size) {
          cur.buffer.push_back((unsigned char) buf[i]);
          i++;
        }
      }

      /* if buf and sd_buffer_rd are equal, we dont need to gather the 
         statediff */
      if (cur.buffer.size() > 0) {
        coalesced_write.push_back(cur);
      }

      i++;
    }
    // if i == rd_size < size, process the remaining new values
    if (i < (ssize_t) size) {
      struct statediff_write_unit cur;
      cur.offset = offset + i;
      
      // handle if the last contiguous diff includes the last byte
      if (coalesced_write.size() > 0) {
        struct statediff_write_unit last;
        last = coalesced_write.back();
        if ((ssize_t) (last.offset + last.buffer.size()) == rd_size) {
          coalesced_write.pop_back();
          cur.offset = last.offset;
          cur.buffer = last.buffer;
        }
      }

      while (i < (ssize_t) size) {
        cur.buffer.push_back((unsigned char) buf[i]);
        i++;
      }

      coalesced_write.push_back(cur);
    }

    /* Calculate the number of different bytes */
    if (coalesced_write.size() > 0) num_diff = 0;
    for (auto cur_diff : coalesced_write) {
      num_diff += cur_diff.buffer.size();
    }

  } // end of coalescing write statediff

  /* do the actual write */
  res = pwrite(fd, buf, size, offset);
  if (res == -1) {
    logging(LOG_ERROR, "   + write %s size: %ld bytes, offset: %ld "
            "[WRITE-FAIL]\n", path, size, offset);
    res = -errno;
    if (fi == NULL) close(fd);
    free(path);
    return res;
  } 
  
  /* Store the statediff into an external file */
  if (is_sd_capture) {
    auto start_time = chrono::high_resolution_clock::now();

    /* Ignore statediff from pid == 0*/
    if (fuse_get_context()->pid > 0) {
      
      // get the file id (fid)
      uint64_t cur_fid = 0;
      string path_str(path);
      sd_mutex.lock();
      if (filename_to_fid.count(path_str)) {
        cur_fid = filename_to_fid[path_str];
      } else {
        cur_fid = filename_to_fid.size();
        filename_to_fid[path_str] = cur_fid;
      }
      sd_mutex.unlock();
      
      // gather all the coallesced write
      if (is_sd_coalesce) {
        for (auto sd_coalesce : coalesced_write) {
          struct statediff_action cur_sd;
          cur_sd.sd_type = SD_TYPE_WRITE;
          cur_sd.fid = cur_fid;
          cur_sd.offset = sd_coalesce.offset;
          cur_sd.buffer = sd_coalesce.buffer;
          sd_mutex.lock();
          statediffs.push_back(cur_sd);
          sd_mutex.unlock();
        }
      }

      if (!is_sd_coalesce) {
        struct statediff_action cur_sd;
        cur_sd.sd_type = SD_TYPE_WRITE;
        cur_sd.fid = cur_fid;
        cur_sd.offset = offset;
        for (size_t i = 0; i < size; i++) {
          cur_sd.buffer.push_back(buf[i]);
        }
        sd_mutex.lock();
        statediffs.push_back(cur_sd);
        sd_mutex.unlock();
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
      for (auto cur_diff : coalesced_write) {
        logging(LOG_DEBUG, "     + %5ld..%5ld: \n",
                cur_diff.offset,
                cur_diff.offset + cur_diff.buffer.size() - 1);

        uint64_t i = 0;
        uint64_t max_print_count = 5;
        while (i < cur_diff.buffer.size() && i < max_print_count) {
        logging(LOG_DEBUG, "                      %4d(%1c) ==> %4d(%1c) \n",
                (unsigned char) cur_diff.buffer[i],
                printChar((char) cur_diff.buffer[i]),
                (unsigned char) buf[i],
                printChar((char) buf[i]));

        i++;
        if (i == max_print_count && cur_diff.buffer.size() != max_print_count) {
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
