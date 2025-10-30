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
  
  FILE    *fd;
  uint64_t sd_count;
  size_t   rw_count;
  // char    *sd_buffer;
  // uint8_t  sd_type;

  // get the size of the file
  fd = fopen(statediff_file_path.c_str(), "rb");
  fseek(fd, 0L, SEEK_END);
  size_t sz = ftell(fd);
  fseek(fd, 0L, SEEK_SET);
  size_t cur_byte = 0;
  sd_count        = 0;
  logging(LOG_INFO, "  filesize: %lu bytes\n", sz);

  // read the fid to filename mapping in the header
  unordered_map<uint64_t, string> fid_to_filename;

  // first, read the number of files
  uint64_t num_file;
  rw_count = fread(&num_file, sizeof(uint64_t), 1, fd);
  if (rw_count != 1) {
    logging(LOG_ERROR, 
            "error: fread.num_file failed to read %ld bytes (%ld)\n",
            sizeof(uint64_t), rw_count);
    return;
  }
  cur_byte += 8;
  logging(LOG_INFO, "  #files: %lu\n", num_file);

  // then, read all files
  char uint64_buffer[8];
  char path_buffer[256];
  for (size_t i = 0; i < num_file; i++) {
    uint64_t fid = 0;
    uint64_t path_len = 0;
    
    // read the file id (fid)
    memset(uint64_buffer, 0, 8); // reset
    rw_count = fread(uint64_buffer, sizeof(uint64_t), 1, fd);
    if (rw_count != 1) {
      logging(LOG_ERROR, "error: fread.fid failed to read %ld bytes (%ld)\n",
              sizeof(uint64_t), rw_count);
      return;
    }
    memcpy(&fid, uint64_buffer, sizeof(uint64_t));
    cur_byte += 8;

    // read the path len
    memset(uint64_buffer, 0, 8); // reset
    rw_count = fread(uint64_buffer, sizeof(uint64_t), 1, fd);
    if (rw_count != 1) {
      logging(LOG_ERROR, 
              "error: fread.path_len failed to read %ld bytes (%ld)\n", 
              sizeof(uint64_t), rw_count);
      return;
    }
    memcpy(&path_len, uint64_buffer, sizeof(uint64_t));
    cur_byte += 8;

    // read the whole path
    memset(uint64_buffer, 0, 8); // reset
    rw_count = fread(path_buffer, sizeof(char), path_len, fd);
    if (rw_count != path_len) {
      logging(LOG_ERROR, "error: fread.path failed to read %ld bytes (%ld)\n",
              sizeof(char), rw_count);
      return;
    }
    cur_byte += path_len;
    
    fid_to_filename[fid] = string(path_buffer);

    logging(LOG_INFO, ">> fid: %lu, len:%lu, path: %s\n",
            fid, path_len, path_buffer);
  }

  for (auto m : fid_to_filename) {
    logging(LOG_INFO, ">> fid: %lu, path: %s\n", m.first, m.second.c_str());
  }
  
  // read and apply all the statediff updates

  // read the file id (fid)
  uint64_t num_statediff = 0;
  memset(uint64_buffer, 0, 8); // reset
  rw_count = fread(uint64_buffer, sizeof(uint64_t), 1, fd);
  if (rw_count != 1) {
    logging(LOG_ERROR, "error: fread.fid failed to read %ld bytes (%ld)\n",
            sizeof(uint64_t), rw_count);
    return;
  }
  memcpy(&num_statediff, uint64_buffer, sizeof(uint64_t));
  cur_byte += 8;
  logging(LOG_INFO, "  #statediff: %lu\n", num_statediff);

  for (size_t i = 0; i < num_statediff; i++) {
    uint8_t sd_type = SD_TYPE_UNLINK;

    logging(LOG_DEBUG, "current byte offset: %ld\n", cur_byte);

    // read the statediff type
    rw_count = fread(&sd_type, sizeof(uint8_t), 1, fd);
    if (rw_count != 1) {
      logging(LOG_ERROR, "error: failed to read %ld byte\ns", sizeof(uint8_t));
      return;
    }
    cur_byte += 1;

    // prepare the statediff buffer
    if (sd_type == SD_TYPE_WRITE) {
      sd_count += 1;
      uint64_t sd_fid;
      uint64_t sd_size;
      uint64_t sd_offset;
      char *sd_buffer;

      // read fid
      rw_count = fread(&sd_fid, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      string filename = fid_to_filename[sd_fid];
      uint64_t path_len = filename.size();
      cur_byte += 8;

      // read count
      rw_count = fread(&sd_size, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      cur_byte += 8;

      // read offset
      rw_count = fread(&sd_offset, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      cur_byte += 8;

      // read buffer
      sd_buffer = (char *) malloc(1 + 8 + path_len + 8 + 8 + sd_size);
      rw_count = fread(sd_buffer + 1 + 8 + path_len + 16 , 
                       sizeof(char), sd_size, fd);
      if (rw_count != sd_size) {
        logging(LOG_ERROR, "error: failed to read %ld bytes\n", sizeof(char));
        return;
      }
      cur_byte += sd_size;
      
      logging(LOG_INFO, ">>> write size:%lu offset:%lu path:%s\n",
              sd_size, sd_offset, filename.c_str());

      // prepare the statediff buffer
      memcpy(sd_buffer, (void *) &sd_type, sizeof(uint8_t));
      memcpy(sd_buffer + 1, (void *) &path_len, sizeof(uint64_t));
      memcpy(sd_buffer + 1 + 8, filename.c_str(), path_len * sizeof(char));
      memcpy(sd_buffer + 1 + 8 + path_len, (void *) &sd_offset,
             sizeof(uint64_t));
      memcpy(sd_buffer + 1 + 8 + path_len + 8, (void *) &sd_size,
             sizeof(uint64_t));
      
      apply_write(sd_buffer, target_root);

      free(sd_buffer);
    } else if (sd_type == SD_TYPE_UNLINK) {
      uint64_t sd_fid;

      // read the fid
      rw_count = fread(&sd_fid, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      string filename = fid_to_filename[sd_fid];
      uint64_t path_len = filename.size();
      cur_byte += 8;

      logging(LOG_INFO, ">>> unlink %s\n", filename.c_str());

      // prepare the statediff buffer
      char *sd_buffer = (char *) malloc(1 + 8 + path_len);
      memcpy(sd_buffer, (void *) &sd_type, sizeof(uint8_t));
      memcpy(sd_buffer + 1, (void *) &path_len, sizeof(uint64_t));
      memcpy(sd_buffer + 1 + 8, filename.c_str(), path_len * sizeof(char));
      
      apply_unlink(sd_buffer, target_root);
      
      free(sd_buffer);
    } else if (sd_type == SD_TYPE_RENAME) {
      uint64_t from_fid;
      uint64_t to_fid;

      // read the from and to fid
      rw_count = fread(&from_fid, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      rw_count = fread(&to_fid, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: failed to read %ld bytes\n", sizeof(uint64_t));
        return;
      }
      string from_filename = fid_to_filename[from_fid];
      string to_filename = fid_to_filename[to_fid];
      cur_byte += 16;

      // get the absolute path
      char from_abs_path[512];
      strcpy(from_abs_path, target_root);
      strcat(from_abs_path, from_filename.c_str());
      char to_abs_path[512];
      strcpy(to_abs_path, target_root);
      strcat(to_abs_path, to_filename.c_str());

      logging(LOG_INFO, ">>> rename \n      from: %s\n      to: %s\n", 
             from_abs_path, to_abs_path);
      int err = rename(from_abs_path, to_abs_path);
      if (err == -1) {
        logging(LOG_ERROR, "error: failed to rename file %s -> %s\n",
                from_filename.c_str(), to_filename.c_str());
        perror("reason");
        return;
      }

      logging(LOG_INFO, "\n\n");

    } else if (sd_type == SD_TYPE_TRUNCATE) {
      uint64_t fid;
      uint64_t truncate_size;

      // read the fid and size
      rw_count = fread(&fid, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: fread.truncate_fid failed to read %ld bytes (%ld)\n",
                sizeof(uint64_t), rw_count);
        return;
      }
      rw_count = fread(&truncate_size, sizeof(uint64_t), 1, fd);
      if (rw_count != 1) {
        logging(LOG_ERROR,
                "error: fread.truncate_size failed to read %ld bytes (%ld)\n",
                sizeof(uint64_t), rw_count);
        return;
      }
      cur_byte += 16;

      // get the absolute path
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
        return;
      }

      logging(LOG_INFO, "\n\n");

    } else {
      logging(LOG_ERROR, "undefined statediff type %u\n", sd_type);
      fclose(fd);
      return;
    }

  }

  logging(LOG_INFO, "applying %lu write updates in these files:\n", sd_count);
  for (const auto &p : updated_files) {
    logging(LOG_INFO, "   - %s\n", p.c_str());
  }

  fclose(fd);
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
