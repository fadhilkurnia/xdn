// SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "statediff_vfs.h"

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1U << 0)
#endif

#define MAX_REPLAY_DATA (16U * 1024U * 1024U)
#define MAX_REPLAY_RECORDS 1000000ULL
#define ZERO_RANGE_CHUNK (64U * 1024U)

static int read_all(FILE *fp, void *buf, size_t len)
{
	return fread(buf, 1, len, fp) == len ? 0 : -1;
}

static int write_fd_all(int fd, const void *buf, size_t len,
			unsigned long long offset)
{
	const char *p = buf;
	size_t done = 0;

	while (done < len) {
		ssize_t n = pwrite(fd, p + done, len - done,
				   (off_t)(offset + done));

		if (n < 0)
			return -1;
		if (n == 0) {
			errno = EIO;
			return -1;
		}
		done += (size_t)n;
	}
	return 0;
}

// Reject absolute paths and any "." or ".." component so a corrupt or hostile
// batch cannot escape the target root.
static int path_is_safe(const char *path)
{
	char tmp[PATH_MAX];
	char *saveptr;
	char *token;

	if (!path || !path[0] || path[0] == '/' || strlen(path) >= sizeof(tmp))
		return 0;
	snprintf(tmp, sizeof(tmp), "%s", path);
	token = strtok_r(tmp, "/", &saveptr);
	while (token) {
		if (strcmp(token, ".") == 0 || strcmp(token, "..") == 0)
			return 0;
		token = strtok_r(NULL, "/", &saveptr);
	}
	return 1;
}

static int join_path(const char *root, const char *rel, char *out,
		     size_t out_size)
{
	int n;

	if (!path_is_safe(rel)) {
		errno = EINVAL;
		return -1;
	}

	if (root[0] && root[strlen(root) - 1] == '/')
		n = snprintf(out, out_size, "%s%s", root, rel);
	else
		n = snprintf(out, out_size, "%s/%s", root, rel);
	return n >= 0 && (size_t)n < out_size ? 0 : -1;
}

// A batch may reference a path whose parent directories were never captured,
// so materialize them before applying the record.
static int ensure_parent_dirs(const char *path)
{
	char tmp[PATH_MAX];
	char *p;

	if (snprintf(tmp, sizeof(tmp), "%s", path) < 0 ||
	    strlen(path) >= sizeof(tmp)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	for (p = tmp + 1; *p; p++) {
		if (*p != '/')
			continue;
		*p = '\0';
		if (mkdir(tmp, 0755) < 0 && errno != EEXIST)
			return -1;
		*p = '/';
	}
	return 0;
}

static int read_string(FILE *fp, unsigned int len, char **out)
{
	char *s;

	if (len == 0 || len >= PATH_MAX) {
		errno = EINVAL;
		return -1;
	}

	s = malloc((size_t)len + 1);
	if (!s)
		return -1;
	if (read_all(fp, s, len) < 0) {
		free(s);
		return -1;
	}
	s[len] = '\0';
	*out = s;
	return 0;
}

static int apply_create(const char *path, unsigned int mode)
{
	int fd;

	if (ensure_parent_dirs(path) < 0)
		return -1;
	fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, mode & 07777);
	if (fd < 0)
		return -1;
	if (close(fd) < 0)
		return -1;
	return chmod(path, mode & 07777);
}

static int apply_write(const char *path, unsigned long long offset,
		       const void *data, unsigned int data_len)
{
	int fd;

	if (ensure_parent_dirs(path) < 0)
		return -1;
	fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
	if (fd < 0)
		return -1;
	if (write_fd_all(fd, data, data_len, offset) < 0) {
		close(fd);
		return -1;
	}
	return close(fd);
}

static int apply_truncate(const char *path, unsigned long long size)
{
	int fd;

	if (ensure_parent_dirs(path) < 0)
		return -1;
	fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
	if (fd < 0)
		return -1;
	if (ftruncate(fd, (off_t)size) < 0) {
		close(fd);
		return -1;
	}
	return close(fd);
}

static int apply_zero_range(const char *path, unsigned long long offset,
			    unsigned long long size, unsigned int flags)
{
	static const char zeros[ZERO_RANGE_CHUNK];
	unsigned long long current_size;
	unsigned long long end;
	unsigned long long zero_end;
	struct stat st;
	int saved_errno;
	int fd;

	if (!size || (flags & ~SD_VFS_ZERO_RANGE_F_KEEP_SIZE) ||
	    offset > ULLONG_MAX - size) {
		errno = EINVAL;
		return -1;
	}
	end = offset + size;
	if (end > (unsigned long long)LLONG_MAX) {
		errno = EOVERFLOW;
		return -1;
	}
	if (ensure_parent_dirs(path) < 0)
		return -1;
	fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) < 0 || st.st_size < 0)
		goto err;

	// Only the part of the range that lies inside the current file can be
	// written. Anything past EOF is materialized by the ftruncate below.
	current_size = (unsigned long long)st.st_size;
	zero_end = end < current_size ? end : current_size;
	while (offset < zero_end) {
		unsigned long long remaining = zero_end - offset;
		size_t chunk = remaining < sizeof(zeros) ?
			(size_t)remaining : sizeof(zeros);

		if (write_fd_all(fd, zeros, chunk, offset) < 0)
			goto err;
		offset += chunk;
	}

	if (!(flags & SD_VFS_ZERO_RANGE_F_KEEP_SIZE) && end > current_size &&
	    ftruncate(fd, (off_t)end) < 0)
		goto err;
	return close(fd);

err:
	saved_errno = errno;
	close(fd);
	errno = saved_errno;
	return -1;
}

// Removals are idempotent, because a missing target means the batch already
// reflects the desired state.
static int apply_unlink(const char *path)
{
	if (unlink(path) < 0 && errno != ENOENT)
		return -1;
	return 0;
}

static int apply_mkdir(const char *path, unsigned int mode)
{
	struct stat st;

	if (ensure_parent_dirs(path) < 0)
		return -1;
	// Tolerate an existing directory, but not an existing non-directory.
	if (mkdir(path, mode & 07777) < 0) {
		if (errno != EEXIST || lstat(path, &st) < 0 || !S_ISDIR(st.st_mode))
			return -1;
	}
	return chmod(path, mode & 07777);
}

static int apply_rmdir(const char *path)
{
	if (rmdir(path) < 0 && errno != ENOENT)
		return -1;
	return 0;
}

static int apply_rename(const char *old_path, const char *new_path,
			unsigned int flags)
{
	struct stat st;

	if (flags & ~RENAME_NOREPLACE) {
		errno = ENOTSUP;
		return -1;
	}
	if (ensure_parent_dirs(new_path) < 0)
		return -1;
	if ((flags & RENAME_NOREPLACE) && lstat(new_path, &st) == 0) {
		errno = EEXIST;
		return -1;
	}
	return rename(old_path, new_path);
}

// Reports the type of each component of a path, so an ENOTDIR or EEXIST can
// be attributed to the exact component that holds the wrong kind of object.
static void describe_path(const char *root, const char *rel)
{
	char acc[PATH_MAX];
	char full[PATH_MAX];
	struct stat st;
	const char *p = rel;
	size_t len = 0;

	while (*p && len < sizeof(acc) - 1) {
		const char *slash = strchr(p, '/');
		size_t seg = slash ? (size_t)(slash - p) : strlen(p);

		if (len + seg + 2 >= sizeof(acc))
			return;
		if (len)
			acc[len++] = '/';
		memcpy(acc + len, p, seg);
		len += seg;
		acc[len] = '\0';

		if (join_path(root, acc, full, sizeof(full)) == 0) {
			if (lstat(full, &st) < 0)
				fprintf(stderr, "    %-28s <absent>\n", acc);
			else
				fprintf(stderr, "    %-28s %s mode=0%o\n", acc,
					S_ISDIR(st.st_mode) ? "dir " :
					S_ISREG(st.st_mode) ? "file" : "othr",
					st.st_mode & 07777);
		}
		if (!slash)
			break;
		p = slash + 1;
	}
}

static const char *op_name(unsigned int op)
{
	switch (op) {
	case SD_VFS_OP_CREATE:
		return "CREATE";
	case SD_VFS_OP_WRITE:
		return "WRITE";
	case SD_VFS_OP_TRUNCATE:
		return "TRUNC";
	case SD_VFS_OP_UNLINK:
		return "UNLINK";
	case SD_VFS_OP_RENAME:
		return "RENAME";
	case SD_VFS_OP_MKDIR:
		return "MKDIR";
	case SD_VFS_OP_RMDIR:
		return "RMDIR";
	case SD_VFS_OP_ZERO_RANGE:
		return "ZERO";
	default:
		return "UNKNOWN";
	}
}

static void usage(const char *prog)
{
	fprintf(stderr, "Usage: %s <vfs_batch.sd> <target_root>\n", prog);
}

int main(int argc, char **argv)
{
	struct statediff_vfs_file_header header;
	const char *batch_path;
	const char *target_root;
	unsigned long long i;
	char resolved_root[PATH_MAX];
	FILE *fp;
	int err = 0;

	if (argc != 3) {
		usage(argv[0]);
		return 1;
	}

	batch_path = argv[1];
	target_root = argv[2];

	if (!realpath(target_root, resolved_root)) {
		if (errno != ENOENT) {
			fprintf(stderr, "Failed to resolve %s: %s\n",
				target_root, strerror(errno));
			return 1;
		}
		if (mkdir(target_root, 0755) < 0 && errno != EEXIST) {
			fprintf(stderr, "Failed to create %s: %s\n",
				target_root, strerror(errno));
			return 1;
		}
		if (!realpath(target_root, resolved_root)) {
			fprintf(stderr, "Failed to resolve %s: %s\n",
				target_root, strerror(errno));
			return 1;
		}
	}

	fp = fopen(batch_path, "rb");
	if (!fp) {
		fprintf(stderr, "Failed to open %s: %s\n",
			batch_path, strerror(errno));
		return 1;
	}

	if (read_all(fp, &header, sizeof(header)) < 0 ||
	    header.magic != STATEDIFF_VFS_MAGIC ||
	    header.version != STATEDIFF_VFS_VERSION ||
	    header.record_count > MAX_REPLAY_RECORDS) {
		fprintf(stderr, "Invalid VFS statediff file: %s\n", batch_path);
		err = 1;
		goto cleanup;
	}

	for (i = 0; i < header.record_count; i++) {
		struct statediff_vfs_record_header record;
		char old_full[PATH_MAX];
		char new_full[PATH_MAX];
		char *path = NULL;
		char *new_path = NULL;
		void *data = NULL;

		if (read_all(fp, &record, sizeof(record)) < 0 ||
		    read_string(fp, record.path_len, &path) < 0 ||
		    join_path(resolved_root, path, old_full, sizeof(old_full)) < 0) {
			fprintf(stderr, "Invalid record %llu\n", i + 1);
			err = 1;
			free(path);
			goto cleanup;
		}

		if (record.new_path_len) {
			if (read_string(fp, record.new_path_len, &new_path) < 0 ||
			    join_path(resolved_root, new_path, new_full,
				      sizeof(new_full)) < 0) {
				fprintf(stderr, "Invalid new path in record %llu\n",
					i + 1);
				err = 1;
				free(path);
				free(new_path);
				goto cleanup;
			}
		}

		if (record.data_len) {
			if (record.data_len > MAX_REPLAY_DATA) {
				fprintf(stderr, "Record %llu data is too large\n",
					i + 1);
				err = 1;
				free(path);
				free(new_path);
				goto cleanup;
			}
			data = malloc(record.data_len);
			if (!data || read_all(fp, data, record.data_len) < 0) {
				fprintf(stderr, "Invalid data in record %llu\n",
					i + 1);
				err = 1;
				free(path);
				free(new_path);
				free(data);
				goto cleanup;
			}
		}

		if (getenv("SD_APPLY_TRACE"))
			fprintf(stderr, "[apply] %llu %s %s%s%s\n", i + 1,
				op_name(record.op), path,
				new_path ? " -> " : "",
				new_path ? new_path : "");

		// Each arm also asserts the record's shape. A field the operation
		// does not use must be absent, so a malformed batch fails closed
		// instead of being applied with a silently ignored field.
		switch (record.op) {
		case SD_VFS_OP_CREATE:
			if (record.new_path_len || record.data_len ||
			    apply_create(old_full, record.mode) < 0)
				err = 1;
			break;
		case SD_VFS_OP_WRITE:
			if (record.new_path_len || record.data_len != record.size ||
			    apply_write(old_full, record.offset, data,
					record.data_len) < 0)
				err = 1;
			break;
		case SD_VFS_OP_TRUNCATE:
			if (record.new_path_len || record.data_len ||
			    apply_truncate(old_full, record.size) < 0)
				err = 1;
			break;
		case SD_VFS_OP_ZERO_RANGE:
			if (record.new_path_len || record.data_len ||
			    apply_zero_range(old_full, record.offset, record.size,
					     record.flags) < 0)
				err = 1;
			break;
		case SD_VFS_OP_UNLINK:
			if (record.new_path_len || record.data_len ||
			    apply_unlink(old_full) < 0)
				err = 1;
			break;
		case SD_VFS_OP_MKDIR:
			if (record.new_path_len || record.data_len ||
			    apply_mkdir(old_full, record.mode) < 0)
				err = 1;
			break;
		case SD_VFS_OP_RMDIR:
			if (record.new_path_len || record.data_len ||
			    apply_rmdir(old_full) < 0)
				err = 1;
			break;
		case SD_VFS_OP_RENAME:
			if (!new_path || record.data_len ||
			    apply_rename(old_full, new_full, record.flags) < 0)
				err = 1;
			break;
		default:
			err = 1;
			break;
		}

		if (err) {
			fprintf(stderr,
				"record %llu/%llu: %s %s%s%s failed: %s\n",
				i + 1, header.record_count, op_name(record.op),
				path, new_path ? " -> " : "",
				new_path ? new_path : "", strerror(errno));
			fprintf(stderr, "  seq=%llu offset=%llu size=%llu mode=0%o flags=0x%x data_len=%u\n",
				record.seq, record.offset, record.size,
				record.mode & 07777, record.flags,
				record.data_len);
			fprintf(stderr, "  path components of '%s':\n", path);
			describe_path(resolved_root, path);
			if (new_path) {
				fprintf(stderr, "  path components of '%s':\n",
					new_path);
				describe_path(resolved_root, new_path);
			}
			free(path);
			free(new_path);
			free(data);
			goto cleanup;
		}

		free(path);
		free(new_path);
		free(data);
	}

	printf("Applied %llu VFS records from %s into %s\n",
	       header.record_count, batch_path, resolved_root);

cleanup:
	fclose(fp);
	return err;
}
