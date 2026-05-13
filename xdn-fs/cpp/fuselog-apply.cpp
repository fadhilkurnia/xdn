// fuselog-apply - a program to apply statediffs to a target directory
//
// This is a program to apply statediffs captured by fuselog, for the xdn
// prototype.
//
// How to compile:
//   g++ -Wall fuselog-apply.cpp -o fuselog-apply -O3 -std=c++20
//
// How to run:
//   ./fuselog-apply <target_dir>
//   example: 
//      ./fuselog-apply /backup/app/data/
//      ./fuselog-apply /backup/app/data/ --silent
//      ./fuselog-apply /backup/app/data/ --statediff=/tmp/app.diff
//   
//   Note that the <target_dir> must be in an absolute path (i.e., starts 
//   with '/') and ends with '/' (indicating a directory, not a file).
//   By default, this program reads the statediff from /tmp/statediffv2 file,
//   which you can change using the --statediff flag.
// 
// Initial developer: Fadhil I. Kurnia (fikurnia@cs.umass.edu)

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>

#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>

using namespace std;

/*******************************************************************************
 *                   GLOBAL/STATIC VARIABLE DECLARATIONS                       *
 ******************************************************************************/
static unordered_set<string> updated_files;
static string                statediff_file_path = "/tmp/statediffv2";
static string                target_dir_path;
static char                  target_path[256];
const bool                   is_dry_run = false;
const int                    version = 2;
static const uint32_t        ZSTD_MAGIC = 0xFD2FB528;

// configurations for logging purposes
#define LOG_DEBUG   1
#define LOG_INFO    2
#define LOG_WARNING 3
#define LOG_ERROR   4
static int log_level = LOG_INFO;

// all the statediff operation (i.e., system call)
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

/*******************************************************************************
 *                           FUNCTIONS DECLARATION                             *
 ******************************************************************************/
void print_usage();
bool parse_flags(int argc, char *argv[]);
bool validate_target_dir_path(string path);
static void logging(const int level, const char *format, ...);

void apply(char *target_root);
void apply2(char *target_root);
void apply_write(const char *sd_buffer, const char *target_root);
void apply_unlink(const char *sd_buffer, const char *target_root);

/*******************************************************************************
 *                      THE MAIN FUNCTION - ENTRY POINT                        *
 ******************************************************************************/
int main(int argc, char *argv[]) {
  char *target_root = nullptr;

  if (argc < 2) {
    print_usage();
    return -1;
  }

  target_dir_path = string(argv[1]);
  bool is_success = validate_target_dir_path(target_dir_path);
  if (!is_success) {
    print_usage();
    return -1;
  }

  is_success = parse_flags(argc, argv);
  if (!is_success) {
    print_usage();
    return -1;
  }

  // print out important paths
  logging(LOG_INFO, "fuselog-apply v2\n");
  logging(LOG_INFO, "- target directory      : %s\n", target_dir_path.c_str());
  logging(LOG_INFO, "- statediff source file : %s\n", 
          statediff_file_path.c_str());
  target_root = target_dir_path.data();

  switch (version) {
    case 1: 
      apply(target_root);
      break;

    case 2:
      apply2(target_root);
      break;

    default:
      printf("unknown fuselog-apply or the statediff version");
      return -1;
  }

  return 0;
}

void print_usage() {
  printf("usage: apply <target_dir>\n");
  printf("       <target_dir> must be in an absolute path\n");
}

bool parse_flags(int argc, char *argv[]) {
  bool is_statediff_file_in_flag = false;
  
  for (int i = 2; i < argc; ++i) {
    string flag = argv[i];

    if (flag.compare(0, 8, "--silent") == 0) {
      log_level = LOG_ERROR;
      continue;
    }

    if (flag.compare(0, 12, "--statediff=") == 0) {
      statediff_file_path = flag.substr(12, flag.length());
      is_statediff_file_in_flag = true;
      continue;
    }

    cout << "error: unknown flag " << flag << "\n";
    return false;
  }

  // read important environment variables, if any.
  // this is mainly for backward compatibility since the --statediff flag was
  // just added recently.
  if (!is_statediff_file_in_flag) {
    if (char* env_p = getenv("FUSELOG_STATEDIFF_FILE")) {
      statediff_file_path = env_p != NULL ? string(env_p) : "";
    }
  }

  return true;
}

bool validate_target_dir_path(string path) {
  if (path.empty()) {
    printf("error: empty target_dir\n");
    return false;
  }
  if (path.back() != '/') {
    printf("error: target-dir must be a directory with '/' ending.\n");
    return false;
  }
  return true;
}

static void logging(const int level, const char *format, ...) {
  va_list args;
  va_start(args, format);
  if (level >= log_level) vprintf(format, args);
  va_end(args);
}

void apply(char *target_root) {
  char     sd_sz_buf[8];
  char    *sd_buffer;
  FILE    *fd;
  uint8_t  sd_type;
  uint64_t sd_size, sd_count;
  size_t   rw_count;

  // get the size of the file
  fd = fopen(statediff_file_path.c_str(), "rb");
  fseek(fd, 0L, SEEK_END);
  size_t sz = ftell(fd);
  fseek(fd, 0L, SEEK_SET);
  size_t cur_byte = 0;
  sd_count        = 0;

  // read and apply all the statediff updates
  while (cur_byte < sz) {

    memset(sd_sz_buf, 0, 8);
    // memset(sd_buffer, 0, 10240);

    // get the length of the statediff data
    rw_count = fread(sd_sz_buf, sizeof(uint64_t), 1, fd);
    if (rw_count != sizeof(uint64_t)) {
      logging(LOG_ERROR, "error: failed to read %ld bytes\n", sizeof(uint64_t));
      return;
    }
    cur_byte += 8;

    // parse the statediff size
    memcpy(&sd_size, sd_sz_buf, sizeof(uint64_t));
    if (sd_size == 0) {
      printf("error: parse an empty statediff!\n");
      break;
    }
    // if (sd_size > 10240) {
    //   printf("error: found large statediff!\n");
    //   break;
    // }
    logging(LOG_INFO, "[%lu] >> statediff size: %lu\n", sd_count, sd_size);

    // prepare buffer
    sd_buffer = (char *) malloc(sizeof(char) * sd_size);

    // read the whole statediff data
    rw_count = fread(sd_buffer, sizeof(char), sd_size, fd);
    if (rw_count != sizeof(char)) {
      logging(LOG_ERROR, "error: failed to read %ld bytes\n", sizeof(char));
      return;
    }
    cur_byte += sd_size;

    // get the statediff's type
    memcpy(&sd_type, sd_buffer, sizeof(uint8_t));
    printf(">> sd_type: %u\n", sd_type);

    
  switch (sd_type) {
  case SD_TYPE_WRITE: // sd_type == 0: write (to an exist or non-exist file)
    apply_write(sd_buffer, target_root);
    sd_count = sd_count + 1;
    break;

  case SD_TYPE_UNLINK: // sd_type == 1: unlink
    apply_unlink(sd_buffer, target_root);
    break;

  default:
    printf("undefined statediff type %u\n", sd_type);
    fclose(fd);
    return;
  }

    free(sd_buffer);
  }

  logging(LOG_INFO, "applying %lu write updates in these files:\n", sd_count);
  for (const auto &p : updated_files) {
    logging(LOG_INFO, "   - %s\n", p.c_str());
  }

  fclose(fd);
  return;
}

void apply2(char *target_root) {
  logging(LOG_INFO, "running apply v2 ...\n");

  // Read entire file into memory.
  FILE *fd = fopen(statediff_file_path.c_str(), "rb");
  if (!fd) {
    logging(LOG_ERROR, "error: cannot open statediff file %s\n",
            statediff_file_path.c_str());
    return;
  }
  fseek(fd, 0L, SEEK_END);
  size_t file_sz = ftell(fd);
  fseek(fd, 0L, SEEK_SET);
  logging(LOG_INFO, "  filesize: %lu bytes\n", file_sz);

  if (file_sz == 0) {
    logging(LOG_INFO, "  empty statediff file, nothing to apply\n");
    fclose(fd);
    return;
  }

  char *raw_data = (char *) malloc(file_sz);
  if (!raw_data) {
    logging(LOG_ERROR, "error: failed to allocate %lu bytes\n", file_sz);
    fclose(fd);
    return;
  }
  size_t rw_count = fread(raw_data, 1, file_sz, fd);
  fclose(fd);
  if (rw_count != file_sz) {
    logging(LOG_ERROR, "error: short read %lu/%lu bytes\n", rw_count, file_sz);
    free(raw_data);
    return;
  }

  // Auto-detect zstd compression by checking the magic number.
  char *data = raw_data;
  size_t data_sz = file_sz;
  char *decompressed = NULL;

  if (file_sz >= 4) {
    uint32_t magic;
    memcpy(&magic, raw_data, sizeof(uint32_t));
    if (magic == ZSTD_MAGIC) {
      logging(LOG_INFO, "  detected zstd-compressed statediff\n");
      unsigned long long decompressed_sz = ZSTD_getFrameContentSize(raw_data, file_sz);
      if (decompressed_sz == ZSTD_CONTENTSIZE_UNKNOWN ||
          decompressed_sz == ZSTD_CONTENTSIZE_ERROR) {
        logging(LOG_ERROR, "error: cannot determine decompressed size\n");
        free(raw_data);
        return;
      }
      decompressed = (char *) malloc((size_t) decompressed_sz);
      if (!decompressed) {
        logging(LOG_ERROR, "error: failed to allocate %llu bytes for decompression\n",
                decompressed_sz);
        free(raw_data);
        return;
      }
      size_t result = ZSTD_decompress(decompressed, (size_t) decompressed_sz,
                                      raw_data, file_sz);
      if (ZSTD_isError(result)) {
        logging(LOG_ERROR, "error: zstd decompression failed: %s\n",
                ZSTD_getErrorName(result));
        free(decompressed);
        free(raw_data);
        return;
      }
      logging(LOG_INFO, "  decompressed %lu -> %lu bytes\n", file_sz, result);
      data = decompressed;
      data_sz = result;
      free(raw_data);
      raw_data = NULL;
    }
  }

  // Parse from in-memory buffer using cursor.
  size_t cur = 0;
  uint64_t sd_count = 0;

  // Helper macro to safely read from buffer.
  #define READ_BUF(dst, n) do { \
    if (cur + (n) > data_sz) { \
      logging(LOG_ERROR, "error: unexpected end of data at offset %lu (need %lu)\n", cur, (size_t)(n)); \
      goto cleanup; \
    } \
    memcpy((dst), data + cur, (n)); \
    cur += (n); \
  } while(0)

  // Read the fid to filename mapping in the header.
  unordered_map<uint64_t, string> fid_to_filename;
  {
    uint64_t num_file;
    READ_BUF(&num_file, sizeof(uint64_t));
    logging(LOG_INFO, "  #files: %lu\n", num_file);

    char path_buffer[256];
    for (size_t i = 0; i < num_file; i++) {
      uint64_t fid = 0;
      uint64_t path_len = 0;
      READ_BUF(&fid, sizeof(uint64_t));
      READ_BUF(&path_len, sizeof(uint64_t));
      if (path_len >= sizeof(path_buffer)) {
        logging(LOG_ERROR, "error: path_len %lu too large\n", path_len);
        goto cleanup;
      }
      memset(path_buffer, 0, sizeof(path_buffer));
      READ_BUF(path_buffer, path_len);
      fid_to_filename[fid] = string(path_buffer);
      logging(LOG_INFO, ">> fid: %lu, len:%lu, path: %s\n",
              fid, path_len, path_buffer);
    }
  }

  for (auto& m : fid_to_filename) {
    logging(LOG_INFO, ">> fid: %lu, path: %s\n", m.first, m.second.c_str());
  }

  // Read and apply all statediff updates.
  {
    uint64_t num_statediff = 0;
    READ_BUF(&num_statediff, sizeof(uint64_t));
    logging(LOG_INFO, "  #statediff: %lu\n", num_statediff);

    for (size_t i = 0; i < num_statediff; i++) {
      uint8_t sd_type;
      READ_BUF(&sd_type, sizeof(uint8_t));

      logging(LOG_DEBUG, "current byte offset: %ld\n", cur);

      if (sd_type == SD_TYPE_WRITE) {
        sd_count += 1;
        uint64_t sd_fid, sd_size, sd_offset;
        READ_BUF(&sd_fid, sizeof(uint64_t));
        READ_BUF(&sd_size, sizeof(uint64_t));
        READ_BUF(&sd_offset, sizeof(uint64_t));

        string filename = fid_to_filename[sd_fid];
        uint64_t path_len = filename.size();

        // Build sd_buffer in the format apply_write expects.
        char *sd_buffer = (char *) malloc(1 + 8 + path_len + 8 + 8 + sd_size);
        // Copy the write data from the in-memory buffer.
        if (cur + sd_size > data_sz) {
          logging(LOG_ERROR, "error: unexpected end of data reading write buffer\n");
          free(sd_buffer);
          goto cleanup;
        }
        memcpy(sd_buffer + 1 + 8 + path_len + 16, data + cur, sd_size);
        cur += sd_size;

        logging(LOG_INFO, ">>> write size:%lu offset:%lu path:%s\n",
                sd_size, sd_offset, filename.c_str());

        memcpy(sd_buffer, (void *) &sd_type, sizeof(uint8_t));
        memcpy(sd_buffer + 1, (void *) &path_len, sizeof(uint64_t));
        memcpy(sd_buffer + 1 + 8, filename.c_str(), path_len);
        memcpy(sd_buffer + 1 + 8 + path_len, (void *) &sd_offset, sizeof(uint64_t));
        memcpy(sd_buffer + 1 + 8 + path_len + 8, (void *) &sd_size, sizeof(uint64_t));

        apply_write(sd_buffer, target_root);
        free(sd_buffer);

      } else if (sd_type == SD_TYPE_UNLINK) {
        uint64_t sd_fid;
        READ_BUF(&sd_fid, sizeof(uint64_t));
        string filename = fid_to_filename[sd_fid];
        uint64_t path_len = filename.size();

        logging(LOG_INFO, ">>> unlink %s\n", filename.c_str());

        char *sd_buffer = (char *) malloc(1 + 8 + path_len);
        memcpy(sd_buffer, (void *) &sd_type, sizeof(uint8_t));
        memcpy(sd_buffer + 1, (void *) &path_len, sizeof(uint64_t));
        memcpy(sd_buffer + 1 + 8, filename.c_str(), path_len);
        apply_unlink(sd_buffer, target_root);
        free(sd_buffer);

      } else if (sd_type == SD_TYPE_RENAME) {
        uint64_t from_fid, to_fid;
        READ_BUF(&from_fid, sizeof(uint64_t));
        READ_BUF(&to_fid, sizeof(uint64_t));

        string from_filename = fid_to_filename[from_fid];
        string to_filename = fid_to_filename[to_fid];

        char from_abs_path[512], to_abs_path[512];
        strcpy(from_abs_path, target_root);
        strcat(from_abs_path, from_filename.c_str());
        strcpy(to_abs_path, target_root);
        strcat(to_abs_path, to_filename.c_str());

        logging(LOG_INFO, ">>> rename \n      from: %s\n      to: %s\n",
               from_abs_path, to_abs_path);
        int err = rename(from_abs_path, to_abs_path);
        if (err == -1 && errno != ENOENT) {
          logging(LOG_ERROR, "error: failed to rename file %s -> %s\n",
                  from_filename.c_str(), to_filename.c_str());
          perror("reason");
          goto cleanup;
        }
        logging(LOG_INFO, "\n\n");

      } else if (sd_type == SD_TYPE_TRUNCATE) {
        uint64_t fid, truncate_size;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&truncate_size, sizeof(uint64_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> truncate \n      path: %s\n      size: %ld\n",
          abs_path, truncate_size);
        int err = truncate(abs_path, truncate_size);
        if (err == -1) {
          logging(LOG_ERROR, "error: failed to truncate file %s (%ld)\n",
                  filename.c_str(), truncate_size);
          perror("reason");
          goto cleanup;
        }
        logging(LOG_INFO, "\n\n");

      } else if (sd_type == SD_TYPE_CREATE) {
        uint64_t fid;
        uint32_t sd_uid, sd_gid, sd_mode;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&sd_uid, sizeof(uint32_t));
        READ_BUF(&sd_gid, sizeof(uint32_t));
        READ_BUF(&sd_mode, sizeof(uint32_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> create path:%s uid:%u gid:%u mode:%o\n",
                abs_path, sd_uid, sd_gid, sd_mode);

        int tfd = open(abs_path, O_CREAT | O_EXCL | O_WRONLY, sd_mode);
        if (tfd >= 0) {
          close(tfd);
          if (lchown(abs_path, sd_uid, sd_gid) == -1) {
            logging(LOG_ERROR, "error: failed to chown created file %s\n", abs_path);
          }
          chmod(abs_path, sd_mode);
        } else if (errno != EEXIST) {
          logging(LOG_ERROR, "error: failed to create %s\n", abs_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_LINK) {
        uint64_t src_fid, new_fid;
        READ_BUF(&src_fid, sizeof(uint64_t));
        READ_BUF(&new_fid, sizeof(uint64_t));

        string src_filename = fid_to_filename[src_fid];
        string new_filename = fid_to_filename[new_fid];
        char src_path[512], new_path[512];
        strcpy(src_path, target_root); strcat(src_path, src_filename.c_str());
        strcpy(new_path, target_root); strcat(new_path, new_filename.c_str());

        logging(LOG_INFO, ">>> link from:%s to:%s\n", src_path, new_path);
        int err = link(src_path, new_path);
        if (err == -1) {
          logging(LOG_ERROR, "error: failed to link %s -> %s\n", src_path, new_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_CHOWN) {
        uint64_t fid;
        uint32_t sd_uid, sd_gid;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&sd_uid, sizeof(uint32_t));
        READ_BUF(&sd_gid, sizeof(uint32_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> chown path:%s uid:%u gid:%u\n", abs_path, sd_uid, sd_gid);
        int err = lchown(abs_path, sd_uid, sd_gid);
        if (err == -1) {
          logging(LOG_ERROR, "error: failed to chown %s\n", abs_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_CHMOD) {
        uint64_t fid;
        uint32_t sd_mode;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&sd_mode, sizeof(uint32_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> chmod path:%s mode:%o\n", abs_path, sd_mode);
        int err = chmod(abs_path, sd_mode);
        if (err == -1) {
          logging(LOG_ERROR, "error: failed to chmod %s\n", abs_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_MKDIR) {
        uint64_t fid;
        uint32_t sd_mode;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&sd_mode, sizeof(uint32_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> mkdir path:%s mode:%o\n", abs_path, sd_mode);
        int err = mkdir(abs_path, sd_mode);
        if (err == -1 && errno != EEXIST) {
          logging(LOG_ERROR, "error: failed to mkdir %s\n", abs_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_RMDIR) {
        uint64_t fid;
        READ_BUF(&fid, sizeof(uint64_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> rmdir path:%s\n", abs_path);
        int err = rmdir(abs_path);
        if (err == -1 && errno != ENOENT) {
          logging(LOG_ERROR, "error: failed to rmdir %s\n", abs_path);
          perror("reason");
        }

      } else if (sd_type == SD_TYPE_SYMLINK) {
        uint64_t fid;
        uint32_t target_len, sd_uid, sd_gid;
        READ_BUF(&fid, sizeof(uint64_t));
        READ_BUF(&target_len, sizeof(uint32_t));

        char target_buf[512];
        memset(target_buf, 0, 512);
        READ_BUF(target_buf, target_len);

        READ_BUF(&sd_uid, sizeof(uint32_t));
        READ_BUF(&sd_gid, sizeof(uint32_t));

        string filename = fid_to_filename[fid];
        char abs_path[512];
        strcpy(abs_path, target_root);
        strcat(abs_path, filename.c_str());

        logging(LOG_INFO, ">>> symlink link:%s -> target:%s uid:%u gid:%u\n",
                abs_path, target_buf, sd_uid, sd_gid);
        int err = symlink(target_buf, abs_path);
        if (err == -1 && errno != EEXIST) {
          logging(LOG_ERROR, "error: failed to symlink %s -> %s\n", abs_path, target_buf);
          perror("reason");
        }
        if (lchown(abs_path, sd_uid, sd_gid) == -1) {
          logging(LOG_ERROR, "error: failed to chown symlink %s\n", abs_path);
        }

      } else {
        logging(LOG_ERROR, "undefined statediff type %u\n", sd_type);
        goto cleanup;
      }
    }
  }

  logging(LOG_INFO, "applying %lu write updates in these files:\n", sd_count);
  for (const auto &p : updated_files) {
    logging(LOG_INFO, "   - %s\n", p.c_str());
  }

  #undef READ_BUF

cleanup:
  if (raw_data) free(raw_data);
  if (decompressed) free(decompressed);
  return;
}

void apply_write(const char *sd_buffer, const char *target_root) {
  uint64_t len_path, offset, len_data;

  // get the length of the path
  memcpy(&len_path, sd_buffer + 1, sizeof(uint64_t));
  logging(LOG_INFO, ">> len_path: %lu\n", len_path);

  // get the path
  memset(target_path, 0, 256);
  memcpy(target_path, sd_buffer + 1 + 8, len_path);
  logging(LOG_INFO, ">> path: %s\n", target_path);
  std::string target_path_str(target_path);
  updated_files.insert(target_path_str);

  // get the offset
  memcpy(&offset, sd_buffer + 1 + 8 + len_path, sizeof(uint64_t));
  logging(LOG_INFO, ">> offset: %lu\n", offset);

  // get the size of the data
  memcpy(&len_data, sd_buffer + 1 + 8 + len_path + 8, sizeof(uint64_t));
  logging(LOG_INFO, ">> size: %lu\n", len_data);

  // get the written data
  const char *data = sd_buffer + 1 + 8 + len_path + 8 + 8;
  
  logging(LOG_DEBUG, ">> data:\n");
  for (size_t i = 0; i < len_data; i++) {
    logging(LOG_DEBUG, "%c", data[i]);
  }

  // get the absolute path of the target file
  char temp[512];
  strcpy(temp, target_root);
  strcat(temp, target_path);

  if (is_dry_run) {
    return;
  }

  // apply the write to the target file
  int tfd = open(temp, O_WRONLY | O_CREAT, 0644);
  ssize_t err = pwrite(tfd, data, len_data, offset);
  close(tfd);
  if (err == -1) {
    logging(LOG_ERROR, "  error: failed to write\n");
    perror("  reason: ");
  }
  logging(LOG_INFO, ">> write of %ld bytes is applied to %s\n", err, temp);

  // set the owner of the written file
  struct stat owner_info;
  int ret;
  stat(target_root, &owner_info);
  ret = lchown(temp, owner_info.st_uid, owner_info.st_gid);
  if (ret != 0) {
    logging(LOG_ERROR, "  error: failed to change owner of %s\n", temp);
    perror("  reason: ");
    logging(LOG_ERROR, "\n");
    return;
  }
  logging(LOG_INFO, "\n\n");
}

void apply_unlink(const char *sd_buffer, const char *target_root) {
  uint64_t len_path;

  // get the length of the path
  memcpy(&len_path, sd_buffer + 1, sizeof(uint64_t));
  logging(LOG_INFO, ">> len_path: %lu\n", len_path);

  // get the path
  memset(target_path, 0, 256);
  memcpy(target_path, sd_buffer + 1 + 8, len_path);
  logging(LOG_INFO, ">> path: %s\n", target_path);

  if (is_dry_run) {
    return;
  }

  // get the absolute path of the target file
  char temp[512];
  strcpy(temp, target_root);
  strcat(temp, target_path);
  int res = unlink(temp);
  // handle error, except error due to nonexistent file
  if (res == -1 && errno != ENOENT) {
    logging(LOG_INFO, "failed to unlink the file %s\n", temp);
    perror("  reason: ");
    logging(LOG_ERROR, "\n");
  }
  logging(LOG_INFO, "\n\n");
}
