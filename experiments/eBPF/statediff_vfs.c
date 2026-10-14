// SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)
#define _GNU_SOURCE
#include <dirent.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <bpf/bpf.h>
#include <bpf/btf.h>
#include <bpf/libbpf.h>
#include "statediff_vfs.h"
#include "statediff_vfs.skel.h"

#define KERNEL_MINORBITS 20
#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

static volatile sig_atomic_t exiting;

/*
 * Capture of writable-shared mmap content (SQLite's WAL-index/shm being the
 * motivating case). When on, each capture calls sync_file_range() on every
 * mmap'd file and pread()s the resulting writeback ranges out of the page
 * cache. That is the only way those bytes are seen -- mmap stores never reach
 * a VFS write hook -- but it also puts a flush barrier plus a userspace read
 * on the capture path. Off by default because it costs real latency. Turn it
 * on with --mmap-snapshot whenever capture completeness matters.
 */
static int mmap_snapshot = 0;

// Inode-to-path tables resolve kernel events within the tracked tree.
struct dir_entry {
	struct statediff_vfs_inode_key key;
	char *path;
};

struct dir_table {
	struct dir_entry *entries;
	size_t count;
	size_t cap;
};

// Batch records hold mutations until they are written or sent.
struct batch_record {
	unsigned long long seq;
	unsigned int op;
	unsigned int flags;
	unsigned long long offset;
	unsigned long long size;
	unsigned int mode;
	char *path;
	char *new_path;
	void *data;
	unsigned int data_len;
};

struct event_batch {
	struct batch_record *records;
	size_t count;
	size_t cap;
	unsigned long long next_seq;
};

// The capture socket serves incremental batches to one client.
struct capture_socket {
	int listen_fd;
	int client_fd;
	int epoll_fd;
	const char *path;
};

/*
 * Native-AIO capture is an order-independent flow. fexit ships DIO_SUBMIT
 * segments followed by DIO_SUBMIT_DONE, while DIO_COMPLETE may arrive before or
 * after them. A submission becomes a durable WRITE only when both terminal
 * events exist. Ready cookies are committed in completion order so overlapping
 * writes retain last-completer-wins behavior.
 */
struct dio_segment {
	unsigned long long offset;
	void *data;
	unsigned int data_len;
	unsigned int flags;
};

struct dio_tentative {
	unsigned long long cookie;
	struct dio_segment *segs;
	size_t seg_count;
	size_t seg_cap;
	unsigned long long staged_bytes;
	unsigned long long submit_size;
	unsigned long long completion_order;
	long long completion_res;
	char *path;
	int submit_done;
	int completion_seen;
	int aborted;
};

struct dio_table {
	struct dio_tentative *entries;
	size_t count;
	size_t cap;
	unsigned long long next_completion_order;
	unsigned long long next_commit_order;
};

// Runtime state owns path indexes, buffered mutations, and BPF map handles.
struct runtime_state {
	struct dir_table dirs;
	struct dir_table files;
	struct event_batch batch;
	struct dio_table dios;
	int tracked_dirs_fd;
	int batch_error;
	char root[PATH_MAX];
	const char *output_path;
};

static int libbpf_print_fn(enum libbpf_print_level level,
			   const char *format, va_list args)
{
	if (level == LIBBPF_DEBUG)
		return 0;
	return vfprintf(stderr, format, args);
}

static void sig_handler(int sig)
{
	(void)sig;
	exiting = 1;
}

static int key_equal(const struct statediff_vfs_inode_key *a,
		     const struct statediff_vfs_inode_key *b)
{
	return a->dev == b->dev && a->ino == b->ino;
}

static int key_is_zero(const struct statediff_vfs_inode_key *key)
{
	return key->dev == 0 && key->ino == 0;
}

static void stat_to_key(const struct stat *st,
			struct statediff_vfs_inode_key *key)
{
	unsigned long long maj = major(st->st_dev);
	unsigned long long min = minor(st->st_dev);

	key->dev = (maj << KERNEL_MINORBITS) | min;
	key->ino = (unsigned long long)st->st_ino;
}

static void free_dir_table(struct dir_table *t)
{
	size_t i;

	for (i = 0; i < t->count; i++)
		free(t->entries[i].path);
	free(t->entries);
	memset(t, 0, sizeof(*t));
}

static void free_event_batch(struct event_batch *b)
{
	size_t i;

	for (i = 0; i < b->count; i++) {
		free(b->records[i].path);
		free(b->records[i].new_path);
		free(b->records[i].data);
	}
	free(b->records);
	memset(b, 0, sizeof(*b));
}

static void reset_event_batch(struct event_batch *b)
{
	free_event_batch(b);
	b->next_seq = 1;
}

static int grow_dir_table(struct dir_table *t)
{
	size_t new_cap = t->cap ? t->cap * 2 : 64;
	struct dir_entry *entries;

	entries = realloc(t->entries, new_cap * sizeof(*entries));
	if (!entries)
		return -1;
	t->entries = entries;
	t->cap = new_cap;
	return 0;
}

static int grow_event_batch(struct event_batch *b)
{
	size_t new_cap = b->cap ? b->cap * 2 : 128;
	struct batch_record *records;

	records = realloc(b->records, new_cap * sizeof(*records));
	if (!records)
		return -1;
	b->records = records;
	b->cap = new_cap;
	return 0;
}

/*
 * The kernel side emits one WRITE record per underlying event by design (see
 * writev_emit_seg()'s comment in statediff_vfs.bpf.c for why that is a
 * BPF-verifier constraint, not a choice) -- a PostgreSQL WAL flush issuing a
 * single pwritev() across several pages produces one record per page. Rather
 * than pay that record count through the rest of the pipeline (Paxos proposal
 * framing, replay), fold it back together here. If the record about to be
 * appended is a WRITE that starts exactly where the most recently appended
 * record for the same path ends, extend that record's payload instead of
 * appending a new one. Segments within one writev(2) are contiguous in the file
 * by construction (only their *source* buffers are scattered), and
 * handle_vfs_write_ret()'s own chunking of an oversized plain write() is equally
 * contiguous, so this recovers both cases -- and, incidentally, any two
 * genuinely separate writes that happen to abut.
 */
static int merge_into_prev_write(struct event_batch *b, const char *path,
				 unsigned int flags, unsigned long long offset,
				 unsigned long long size, const void *data,
				 unsigned int data_len)
{
	struct batch_record *prev;
	void *grown;

	if (!b->count)
		return -1;

	prev = &b->records[b->count - 1];
	if (prev->op != SD_VFS_OP_WRITE)
		return -1;
	/*
	 * A truncated record marks a segment the staging buffer could not hold
	 * in full (see SD_VFS_EVENT_F_TRUNCATED). Its true end in the file is
	 * not `offset + size`, so nothing can be proven contiguous with it.
	 */
	if (prev->flags & SD_VFS_EVENT_F_TRUNCATED)
		return -1;
	if (prev->offset + prev->size != offset)
		return -1;
	if (strcmp(prev->path, path) != 0)
		return -1;

	if (!data_len) {
		prev->size += size;
		prev->flags |= flags;
		return 0;
	}

	grown = realloc(prev->data, prev->data_len + data_len);
	if (!grown)
		return -1;
	memcpy((char *)grown + prev->data_len, data, data_len);
	prev->data = grown;
	prev->data_len += data_len;
	prev->size += size;
	prev->flags |= flags;
	return 0;
}

static int append_batch_record(struct runtime_state *rt, unsigned int op,
			       const char *path, const char *new_path,
			       unsigned int flags, unsigned long long offset,
			       unsigned long long size, unsigned int mode,
			       const void *data, unsigned int data_len)
{
	struct batch_record *record;

	if (!path || !path[0])
		return -1;
	if (op == SD_VFS_OP_WRITE &&
	    merge_into_prev_write(&rt->batch, path, flags, offset, size,
				  data, data_len) == 0)
		return 0;
	if (rt->batch.count == rt->batch.cap && grow_event_batch(&rt->batch) < 0)
		return -1;

	record = &rt->batch.records[rt->batch.count];
	memset(record, 0, sizeof(*record));
	record->seq = rt->batch.next_seq++;
	record->op = op;
	record->flags = flags;
	record->offset = offset;
	record->size = size;
	record->mode = mode;
	record->path = strdup(path);
	if (!record->path)
		return -1;
	if (new_path && new_path[0]) {
		record->new_path = strdup(new_path);
		if (!record->new_path)
			goto err;
	}
	if (data_len) {
		record->data = malloc(data_len);
		if (!record->data)
			goto err;
		memcpy(record->data, data, data_len);
		record->data_len = data_len;
	}

	rt->batch.count++;
	return 0;

err:
	free(record->path);
	free(record->new_path);
	free(record->data);
	memset(record, 0, sizeof(*record));
	return -1;
}

static struct dio_tentative *dio_find(struct dio_table *t,
				      unsigned long long cookie)
{
	size_t i;

	for (i = 0; i < t->count; i++) {
		if (t->entries[i].cookie == cookie)
			return &t->entries[i];
	}
	return NULL;
}

static struct dio_tentative *dio_get_or_create(struct dio_table *t,
					       unsigned long long cookie)
{
	struct dio_tentative *e = dio_find(t, cookie);

	if (e)
		return e;
	if (t->count == t->cap) {
		size_t new_cap = t->cap ? t->cap * 2 : 64;
		struct dio_tentative *entries =
			realloc(t->entries, new_cap * sizeof(*entries));

		if (!entries)
			return NULL;
		t->entries = entries;
		t->cap = new_cap;
	}
	e = &t->entries[t->count++];
	memset(e, 0, sizeof(*e));
	e->cookie = cookie;
	return e;
}

static void dio_free_tentative(struct dio_tentative *e)
{
	size_t i;

	for (i = 0; i < e->seg_count; i++)
		free(e->segs[i].data);
	free(e->segs);
	free(e->path);
	memset(e, 0, sizeof(*e));
}

static void dio_remove(struct dio_table *t, struct dio_tentative *e)
{
	size_t idx = (size_t)(e - t->entries);

	dio_free_tentative(e);
	if (idx + 1 < t->count)
		t->entries[idx] = t->entries[t->count - 1];
	t->count--;
}

static void dio_free_all(struct dio_table *t)
{
	size_t i;

	for (i = 0; i < t->count; i++)
		dio_free_tentative(&t->entries[i]);
	free(t->entries);
	memset(t, 0, sizeof(*t));
}

/*
 * Stash one DIO_SUBMIT segment (its file offset and a private copy of the
 * payload) under its cookie. The copy is required because the ring buffer slot
 * is reused the moment handle_event returns, but the segment must survive until the
 * matching DIO_COMPLETE, which may arrive before or after the segment stream.
 * The path is not known yet, so it is stored when completion arrives.
 */
static int dio_stage_segment(struct dio_table *t, unsigned long long cookie,
			     unsigned long long offset, const void *data,
			     unsigned int data_len, unsigned int flags)
{
	struct dio_tentative *e = dio_get_or_create(t, cookie);
	struct dio_segment *seg;

	if (!e || e->submit_done || e->aborted)
		return -1;
	if (e->seg_count == e->seg_cap) {
		size_t new_cap = e->seg_cap ? e->seg_cap * 2 : 4;
		struct dio_segment *segs =
			realloc(e->segs, new_cap * sizeof(*segs));

		if (!segs)
			return -1;
		e->segs = segs;
		e->seg_cap = new_cap;
	}

	seg = &e->segs[e->seg_count];
	memset(seg, 0, sizeof(*seg));
	seg->offset = offset;
	seg->data_len = data_len;
	seg->flags = flags;
	if (data_len) {
		seg->data = malloc(data_len);
		if (!seg->data)
			return -1;
		memcpy(seg->data, data, data_len);
	}
	e->seg_count++;
	e->staged_bytes += data_len;
	return 0;
}

/*
 * DIO_SUBMIT, DIO_SUBMIT_DONE, and DIO_COMPLETE can arrive in either staging-
 * first or completion-first order. Keep a per-cookie state until both terminal
 * events arrive, then commit ready writes in their original completion order.
 */
static struct dio_tentative *dio_find_completion_order(struct dio_table *t,
						       unsigned long long order)
{
	size_t i;

	for (i = 0; i < t->count; i++) {
		if (t->entries[i].completion_seen &&
		    t->entries[i].completion_order == order)
			return &t->entries[i];
	}
	return NULL;
}

// Append one fully paired write. The caller enforces completion order.
static void dio_commit(struct runtime_state *rt, struct dio_tentative *e)
{
	unsigned long long remaining;
	size_t i;

	if (e->aborted)
		return;
	if (e->staged_bytes != e->submit_size) {
		fprintf(stderr,
			"DIO cookie 0x%llx staged %llu bytes but DONE reported %llu\n",
			e->cookie, e->staged_bytes, e->submit_size);
		rt->batch_error = 1;
		return;
	}
	if (!e->path || e->completion_res <= 0)
		return;

	remaining = (unsigned long long)e->completion_res;
	for (i = 0; i < e->seg_count && remaining; i++) {
		struct dio_segment *s = &e->segs[i];
		unsigned int n = s->data_len;

		if ((unsigned long long)n > remaining)
			n = (unsigned int)remaining;
		if (!n)
			continue;
		if (append_batch_record(rt, SD_VFS_OP_WRITE, e->path, NULL,
					s->flags, s->offset, n, 0, s->data, n) < 0) {
			fprintf(stderr,
				"Failed to append DIO WRITE record for %s\n",
				e->path);
			rt->batch_error = 1;
		}
		if (s->flags & SD_VFS_EVENT_F_TRUNCATED) {
			fprintf(stderr,
				"DIO WRITE for %s exceeded capture limits at offset %llu\n",
				e->path, s->offset);
			rt->batch_error = 1;
		}
		remaining -= n;
	}
	if (remaining) {
		fprintf(stderr,
			"DIO cookie 0x%llx completed %lld bytes with only %llu staged\n",
			e->cookie, e->completion_res, e->staged_bytes);
		rt->batch_error = 1;
	}
}

/*
 * Commit only the oldest completed cookie that is also fully staged. This
 * prevents a synchronous completion waiting for fexit/DONE from being overtaken
 * by a later asynchronous completion whose payload was already available.
 */
static void dio_flush_completed(struct runtime_state *rt)
{
	struct dio_tentative *e;

	for (;;) {
		e = dio_find_completion_order(&rt->dios,
					      rt->dios.next_commit_order);
		if (!e || !e->submit_done)
			return;
		dio_commit(rt, e);
		rt->dios.next_commit_order++;
		dio_remove(&rt->dios, e);
	}
}

static void dio_submit_done(struct runtime_state *rt,
			    unsigned long long cookie,
			    unsigned long long submit_size)
{
	struct dio_tentative *e = dio_get_or_create(&rt->dios, cookie);

	if (!e) {
		rt->batch_error = 1;
		return;
	}
	if (e->submit_done || e->aborted) {
		fprintf(stderr, "Duplicate/late DIO DONE for cookie 0x%llx\n",
			cookie);
		rt->batch_error = 1;
		return;
	}
	e->submit_done = 1;
	e->submit_size = submit_size;
	dio_flush_completed(rt);
}

static void dio_note_completion(struct runtime_state *rt,
				unsigned long long cookie,
				long long res, const char *path)
{
	struct dio_tentative *e = dio_get_or_create(&rt->dios, cookie);

	if (!e) {
		rt->batch_error = 1;
		return;
	}
	if (e->completion_seen) {
		fprintf(stderr, "Duplicate DIO completion for cookie 0x%llx\n",
			cookie);
		rt->batch_error = 1;
		return;
	}
	e->completion_seen = 1;
	e->completion_res = res;
	e->completion_order = rt->dios.next_completion_order++;
	if (path) {
		e->path = strdup(path);
		if (!e->path)
			rt->batch_error = 1;
	}
	dio_flush_completed(rt);
}

static void dio_abort(struct runtime_state *rt, unsigned long long cookie)
{
	struct dio_tentative *e = dio_get_or_create(&rt->dios, cookie);

	fprintf(stderr, "DIO capture aborted for cookie 0x%llx\n", cookie);
	rt->batch_error = 1;
	if (!e)
		return;
	e->aborted = 1;
	e->submit_done = 1;
	dio_flush_completed(rt);
}

static int write_all(FILE *fp, const void *buf, size_t len)
{
	return fwrite(buf, 1, len, fp) == len ? 0 : -1;
}

static int send_all(int fd, const void *buf, size_t len)
{
	const char *bytes = buf;
	size_t sent = 0;

	while (sent < len) {
		ssize_t n = send(fd, bytes + sent, len - sent, MSG_NOSIGNAL);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (n == 0) {
			errno = EPIPE;
			return -1;
		}
		sent += (size_t)n;
	}
	return 0;
}

static unsigned long long batch_payload_size(const struct event_batch *b)
{
	unsigned long long size = 0;
	size_t i;

	for (i = 0; i < b->count; i++) {
		const struct batch_record *record = &b->records[i];

		size += sizeof(struct statediff_vfs_record_header);
		size += strlen(record->path);
		if (record->new_path)
			size += strlen(record->new_path);
		size += record->data_len;
	}
	return size;
}

static void fill_batch_header(const struct event_batch *b,
			      struct statediff_vfs_file_header *header)
{
	memset(header, 0, sizeof(*header));
	header->magic = STATEDIFF_VFS_MAGIC;
	header->version = STATEDIFF_VFS_VERSION;
	header->record_count = b->count;
	header->payload_size = batch_payload_size(b);
}

static int fill_record_header(const struct batch_record *record,
			      struct statediff_vfs_record_header *entry,
			      size_t *path_len, size_t *new_path_len)
{
	*path_len = strlen(record->path);
	*new_path_len = record->new_path ? strlen(record->new_path) : 0;
	if (*path_len > UINT32_MAX || *new_path_len > UINT32_MAX) {
		errno = EOVERFLOW;
		return -1;
	}

	memset(entry, 0, sizeof(*entry));
	entry->seq = record->seq;
	entry->op = record->op;
	entry->flags = record->flags;
	entry->offset = record->offset;
	entry->size = record->size;
	entry->mode = record->mode;
	entry->path_len = (unsigned int)*path_len;
	entry->new_path_len = (unsigned int)*new_path_len;
	entry->data_len = record->data_len;
	return 0;
}

static int send_batch(int fd, const struct event_batch *b)
{
	struct statediff_vfs_file_header header;
	unsigned long long encoded_size;
	uint64_t wire_size;
	size_t i;

	// Avoid proposing a header-only batch when no filesystem mutation occurred.
	if (!b->count) {
		wire_size = htole64(0);
		return send_all(fd, &wire_size, sizeof(wire_size));
	}

	fill_batch_header(b, &header);
	if (header.payload_size > ULLONG_MAX - sizeof(header)) {
		errno = EOVERFLOW;
		return -1;
	}
	encoded_size = sizeof(header) + header.payload_size;
	wire_size = htole64(encoded_size);

	if (send_all(fd, &wire_size, sizeof(wire_size)) < 0 ||
	    send_all(fd, &header, sizeof(header)) < 0)
		return -1;

	for (i = 0; i < b->count; i++) {
		const struct batch_record *record = &b->records[i];
		struct statediff_vfs_record_header entry;
		size_t path_len;
		size_t new_path_len;

		if (fill_record_header(record, &entry, &path_len,
				       &new_path_len) < 0 ||
		    send_all(fd, &entry, sizeof(entry)) < 0 ||
		    send_all(fd, record->path, path_len) < 0)
			return -1;
		if (new_path_len &&
		    send_all(fd, record->new_path, new_path_len) < 0)
			return -1;
		if (record->data_len &&
		    send_all(fd, record->data, record->data_len) < 0)
			return -1;
	}
	return 0;
}

static int write_batch_file(const struct event_batch *b, const char *path)
{
	struct statediff_vfs_file_header header;
	FILE *fp;
	size_t i;

	fp = fopen(path, "wb");
	if (!fp)
		return -1;

	fill_batch_header(b, &header);

	if (write_all(fp, &header, sizeof(header)) < 0)
		goto err;

	for (i = 0; i < b->count; i++) {
		const struct batch_record *record = &b->records[i];
		struct statediff_vfs_record_header entry;
		size_t path_len;
		size_t new_path_len;

		if (fill_record_header(record, &entry, &path_len,
				       &new_path_len) < 0)
			goto err;

		if (write_all(fp, &entry, sizeof(entry)) < 0 ||
		    write_all(fp, record->path, path_len) < 0)
			goto err;
		if (new_path_len &&
		    write_all(fp, record->new_path, new_path_len) < 0)
			goto err;
		if (record->data_len &&
		    write_all(fp, record->data, record->data_len) < 0)
			goto err;
	}

	if (fclose(fp) < 0)
		return -1;
	printf("Wrote %zu VFS records to %s\n", b->count, path);
	return 0;

err:
	fclose(fp);
	return -1;
}

static struct dir_entry *find_dir_entry(struct dir_table *t,
					const struct statediff_vfs_inode_key *key)
{
	size_t i;

	for (i = 0; i < t->count; i++) {
		if (key_equal(&t->entries[i].key, key))
			return &t->entries[i];
	}
	return NULL;
}

static int upsert_dir_entry(struct dir_table *t,
			    const struct statediff_vfs_inode_key *key,
			    const char *path)
{
	struct dir_entry *entry;
	char *copy;

	if (key_is_zero(key))
		return -1;
	entry = find_dir_entry(t, key);
	if (!entry) {
		if (t->count == t->cap && grow_dir_table(t) < 0)
			return -1;
		entry = &t->entries[t->count++];
		memset(entry, 0, sizeof(*entry));
		entry->key = *key;
	}

	copy = strdup(path);
	if (!copy)
		return -1;
	free(entry->path);
	entry->path = copy;
	return 0;
}

static int path_is_child_or_same(const char *path, const char *prefix)
{
	size_t len = strlen(prefix);

	if (strcmp(path, prefix) == 0)
		return 1;
	if (strcmp(prefix, ".") == 0)
		return 1;
	return strncmp(path, prefix, len) == 0 && path[len] == '/';
}

static int replace_prefix(char **path, const char *old_prefix,
			  const char *new_prefix)
{
	const char *suffix;
	char tmp[PATH_MAX];
	int n;

	if (strcmp(*path, old_prefix) == 0) {
		n = snprintf(tmp, sizeof(tmp), "%s", new_prefix);
	} else {
		suffix = *path + strlen(old_prefix);
		if (*suffix == '/')
			suffix++;
		if (strcmp(new_prefix, ".") == 0)
			n = snprintf(tmp, sizeof(tmp), "%s", suffix);
		else
			n = snprintf(tmp, sizeof(tmp), "%s/%s", new_prefix, suffix);
	}
	if (n < 0 || (size_t)n >= sizeof(tmp))
		return -1;

	free(*path);
	*path = strdup(tmp);
	return *path ? 0 : -1;
}

/*
 * Dropping a directory invalidates the cached path of everything beneath it,
 * so the whole subtree is forgotten rather than just the directory itself.
 *
 * Only the userspace path table is pruned here. The BPF tracked_dirs map must
 * never be modified from this path: it is maintained synchronously by the
 * hooks, whereas this runs whenever the ring buffer is drained. An inode freed
 * by an rmdir may have been reissued to a different, live directory by then,
 * so deleting by (dev, ino) here would untrack that directory instead --
 * mutations inside it are then filtered out in-kernel and the batch is
 * reported complete while missing them.
 *
 * Leaving the map to the BPF side can only over-track, which costs extra
 * events; under-tracking corrupts the batch silently.
 */
static int remove_dir_subtree(struct runtime_state *rt, const char *prefix)
{
	size_t i = 0;

	while (i < rt->dirs.count) {
		if (!path_is_child_or_same(rt->dirs.entries[i].path, prefix)) {
			i++;
			continue;
		}
		free(rt->dirs.entries[i].path);
		if (i + 1 < rt->dirs.count)
			rt->dirs.entries[i] = rt->dirs.entries[rt->dirs.count - 1];
		rt->dirs.count--;
	}
	return 0;
}

// A directory rename moves every descendant path at once, so rewrite the
// cached prefix in place and later events under the subtree still resolve.
static int move_dir_subtree(struct runtime_state *rt,
			    const struct statediff_vfs_inode_key *key,
			    const char *old_path, const char *new_path)
{
	size_t i;

	if (upsert_dir_entry(&rt->dirs, key, new_path) < 0)
		return -1;

	for (i = 0; i < rt->dirs.count; i++) {
		if (key_equal(&rt->dirs.entries[i].key, key))
			continue;
		if (!path_is_child_or_same(rt->dirs.entries[i].path, old_path))
			continue;
		if (replace_prefix(&rt->dirs.entries[i].path, old_path,
				   new_path) < 0)
			return -1;
	}
	return 0;
}

static int join_rel(const char *parent, const char *name, char *out,
		    size_t out_size)
{
	int n;

	if (strcmp(parent, ".") == 0)
		n = snprintf(out, out_size, "%s", name);
	else
		n = snprintf(out, out_size, "%s/%s", parent, name);
	return n >= 0 && (size_t)n < out_size ? 0 : -1;
}

static int join_abs(const char *root, const char *rel, char *out,
		    size_t out_size)
{
	int n;

	if (strcmp(rel, ".") == 0)
		n = snprintf(out, out_size, "%s", root);
	else
		n = snprintf(out, out_size, "%s/%s", root, rel);
	return n >= 0 && (size_t)n < out_size ? 0 : -1;
}

// Turn a kernel (parent inode, name) pair into a path relative to the tracked
// root. An unknown parent yields a placeholder rather than a failure, so one
// unresolved event cannot abort the capture.
static int resolve_event_path(struct runtime_state *rt,
			      const struct statediff_vfs_inode_key *parent,
			      const char *name, char *out, size_t out_size)
{
	struct dir_entry *entry;
	int n;

	entry = find_dir_entry(&rt->dirs, parent);
	if (!entry) {
		/*
		 * Records under an unresolvable parent are skipped by every
		 * caller. That is reported but not treated as capture loss: a
		 * directory renamed out of the tree keeps its descendants in
		 * tracked_dirs (only the directory itself is untracked), so
		 * activity genuinely outside the tree can arrive here and must
		 * not fail the batch. The warning is what makes the skip
		 * visible, since it is otherwise silent.
		 */
		fprintf(stderr,
			"Skipping event under unresolved parent %llu:%llu name=%s\n",
			parent->dev, parent->ino, name);
		n = snprintf(out, out_size, "<unknown:%llu:%llu>/%s",
			     parent->dev, parent->ino, name);
		return n >= 0 && (size_t)n < out_size ? 1 : -1;
	}
	return join_rel(entry->path, name, out, out_size);
}

// Walk the tree once at startup so the inode-to-path table is populated before
// any hook fires. Without this, early events would resolve to placeholders.
static int seed_dir(struct runtime_state *rt, const char *abs_path,
		    const char *rel_path)
{
	struct statediff_vfs_inode_key key;
	unsigned char tracked = 1;
	struct dirent *de;
	struct stat st;
	DIR *dir;
	int rc = 0;

	if (lstat(abs_path, &st) < 0)
		return -1;
	if (!S_ISDIR(st.st_mode))
		return 0;

	stat_to_key(&st, &key);
	if (bpf_map_update_elem(rt->tracked_dirs_fd, &key, &tracked, BPF_ANY) < 0)
		return -1;
	if (upsert_dir_entry(&rt->dirs, &key, rel_path) < 0)
		return -1;

	dir = opendir(abs_path);
	if (!dir)
		return -1;

	while ((de = readdir(dir))) {
		char child_abs[PATH_MAX];
		char child_rel[PATH_MAX];

		if (strcmp(de->d_name, ".") == 0 ||
		    strcmp(de->d_name, "..") == 0)
			continue;
		if (join_abs(abs_path, de->d_name, child_abs,
			     sizeof(child_abs)) < 0 ||
		    join_rel(rel_path, de->d_name, child_rel,
			     sizeof(child_rel)) < 0) {
			rc = -1;
			break;
		}
		if (lstat(child_abs, &st) < 0) {
			rc = -1;
			break;
		}
		if (!S_ISDIR(st.st_mode))
			continue;
		if (seed_dir(rt, child_abs, child_rel) < 0) {
			rc = -1;
			break;
		}
	}

	if (closedir(dir) < 0)
		rc = -1;
	return rc;
}

/*
 * Read [offset, offset+size) of a regular file straight from the page cache and
 * append it as a WRITE record. mmap(MAP_SHARED) stores land in the same page-
 * cache pages pread() reads, so the bytes are coherent without any flush. These
 * records are appended at capture time, i.e. after the request's VFS records, so
 * on replay they overwrite whatever the (invisible-write) file held before.
 */
static int snapshot_range(struct runtime_state *rt, const char *rel_path,
			  unsigned long long offset, unsigned long long size)
{
	char abs_path[PATH_MAX];
	char *buf;
	size_t got = 0;
	int fd;

	if (!size || size > STATEDIFF_VFS_MAX_WRITE_DATA_LEN)
		return 0;
	if (join_abs(rt->root, rel_path, abs_path, sizeof(abs_path)) < 0)
		return -1;

	fd = open(abs_path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		// The file may disappear while the snapshot is being created.
		return errno == ENOENT ? 0 : -1;
	}

	buf = malloc(size);
	if (!buf) {
		close(fd);
		return -1;
	}

	while (got < size) {
		ssize_t n = pread(fd, buf + got, size - got,
				  (off_t)(offset + got));

		if (n < 0) {
			if (errno == EINTR)
				continue;
			break;
		}
		if (n == 0) {
			// A writeback range can extend beyond the current file size.
			break;
		}
		got += (size_t)n;
	}

	if (got && append_batch_record(rt, SD_VFS_OP_WRITE, rel_path, NULL, 0,
				       offset, (unsigned long long)got, 0, buf,
				       (unsigned int)got) < 0) {
		fprintf(stderr, "Failed to append mmap WRITE record for %s\n",
			rel_path);
		rt->batch_error = 1;
	}

	free(buf);
	close(fd);
	return 0;
}

/*
 * At capture time, force every file with a writable shared mapping through
 * writeback. sync_file_range(..., WAIT_AFTER) blocks until the dirty (incl.
 * mmap-written) pages are flushed, which makes the BPF writeback hook emit a
 * {inode, offset, length} event per dirtied folio, and userspace drains those
 * next and pread()s the bytes. The flush also cleans the pages so a subsequent
 * capture only sees ranges dirtied since now. Runs in this process's context,
 * which is why the writeback hook must not filter by pid.
 */
static int snapshot_mmap_files(struct runtime_state *rt, int mmap_files_fd)
{
	struct statediff_vfs_inode_key cur, next;
	int first = 1;

	while (bpf_map_get_next_key(mmap_files_fd, first ? NULL : &cur,
				    &next) == 0) {
		char abs_path[PATH_MAX];
		struct dir_entry *entry;
		int fd;

		first = 0;
		cur = next;

		entry = find_dir_entry(&rt->files, &cur);
		if (!entry)
			continue;
		if (join_abs(rt->root, entry->path, abs_path,
			     sizeof(abs_path)) < 0)
			continue;
		fd = open(abs_path, O_RDONLY | O_CLOEXEC);
		if (fd < 0)
			continue;
		sync_file_range(fd, 0, 0,
				SYNC_FILE_RANGE_WAIT_BEFORE |
				SYNC_FILE_RANGE_WRITE |
				SYNC_FILE_RANGE_WAIT_AFTER);
		close(fd);
	}
	return 0;
}

// Ring-buffer callback. Validate the event's declared lengths against what the
// kernel actually delivered, resolve its paths, then fold it into the batch.
static int handle_event(void *ctx, void *data, size_t data_sz)
{
	struct runtime_state *rt = ctx;
	const struct statediff_vfs_event *e = data;
	char old_path[PATH_MAX];
	char new_path[PATH_MAX];
	int path_rc;
	int old_rc;
	int new_rc;
	size_t expected_sz;

	if (data_sz < STATEDIFF_VFS_EVENT_HEADER_LEN)
		return 0;
	if (e->data_len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN) {
		fprintf(stderr,
			"Invalid VFS event payload length: data_sz=%zu data_len=%u\n",
			data_sz, e->data_len);
		rt->batch_error = 1;
		return 0;
	}
	expected_sz = STATEDIFF_VFS_EVENT_HEADER_LEN + e->data_len;
	if (data_sz < expected_sz) {
		fprintf(stderr,
			"Short VFS event from BPF: data_sz=%zu expected=%zu data_len=%u\n",
			data_sz, expected_sz, e->data_len);
		rt->batch_error = 1;
		return 0;
	}

	old_path[0] = '\0';
	new_path[0] = '\0';

	switch (e->op) {
	case SD_VFS_OP_CREATE:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 &&
		    append_batch_record(rt, e->op, old_path, NULL, 0, 0, 0,
					e->mode, NULL, 0) < 0) {
			fprintf(stderr, "Failed to append CREATE record\n");
			rt->batch_error = 1;
		}
		if (path_rc == 0 && !key_is_zero(&e->object))
			upsert_dir_entry(&rt->files, &e->object, old_path);
		break;
	case SD_VFS_OP_WRITE:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0) {
			if (!e->data_len || e->data_len != e->size ||
			    e->data_len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN) {
				fprintf(stderr,
					"Invalid WRITE payload from BPF for %s: size=%llu data_len=%u\n",
					old_path, e->size, e->data_len);
				rt->batch_error = 1;
			} else if (append_batch_record(rt, e->op, old_path, NULL,
					       e->flags, e->offset, e->size, 0,
					       e->data, e->data_len) < 0) {
				fprintf(stderr, "Failed to append WRITE record\n");
				rt->batch_error = 1;
			}
			if (e->flags & SD_VFS_EVENT_F_TRUNCATED) {
				fprintf(stderr,
					"WRITE for %s exceeded BPF capture limits at offset %llu\n",
					old_path, e->offset);
				rt->batch_error = 1;
			}
			if (!key_is_zero(&e->object))
				upsert_dir_entry(&rt->files, &e->object, old_path);
		}
		break;
	case SD_VFS_OP_TRUNCATE:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 &&
		    append_batch_record(rt, e->op, old_path, NULL, 0, 0,
					e->size, 0, NULL, 0) < 0) {
			fprintf(stderr, "Failed to append TRUNCATE record\n");
			rt->batch_error = 1;
		}
		break;
	case SD_VFS_OP_ZERO_RANGE:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0) {
			if (e->data_len || !e->size ||
			    (e->flags & ~SD_VFS_ZERO_RANGE_F_KEEP_SIZE)) {
				fprintf(stderr,
					"Invalid ZERO_RANGE event for %s: offset=%llu size=%llu flags=0x%x data_len=%u\n",
					old_path, e->offset, e->size, e->flags,
					e->data_len);
				rt->batch_error = 1;
			} else if (append_batch_record(rt, e->op, old_path, NULL,
						       e->flags, e->offset,
						       e->size, 0, NULL, 0) < 0) {
				fprintf(stderr, "Failed to append ZERO_RANGE record\n");
				rt->batch_error = 1;
			}
			if (!key_is_zero(&e->object))
				upsert_dir_entry(&rt->files, &e->object, old_path);
		}
		break;
	case SD_VFS_OP_UNLINK:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 &&
		    append_batch_record(rt, e->op, old_path, NULL, 0, 0, 0, 0,
					NULL, 0) < 0) {
			fprintf(stderr, "Failed to append UNLINK record\n");
			rt->batch_error = 1;
		}
		break;
	case SD_VFS_OP_MKDIR:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 &&
		    append_batch_record(rt, e->op, old_path, NULL, 0, 0, 0,
					e->mode, NULL, 0) < 0) {
			fprintf(stderr, "Failed to append MKDIR record\n");
			rt->batch_error = 1;
		}
		if (!key_is_zero(&e->object))
			upsert_dir_entry(&rt->dirs, &e->object, old_path);
		break;
	case SD_VFS_OP_RMDIR:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 &&
		    append_batch_record(rt, e->op, old_path, NULL, 0, 0, 0, 0,
					NULL, 0) < 0) {
			fprintf(stderr, "Failed to append RMDIR record\n");
			rt->batch_error = 1;
		}
		remove_dir_subtree(rt, old_path);
		break;
	case SD_VFS_OP_RENAME:
		old_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					    sizeof(old_path));
		new_rc = resolve_event_path(rt, &e->new_parent, e->new_name,
					    new_path, sizeof(new_path));
		if (old_rc == 0 && new_rc == 0) {
			if (append_batch_record(rt, e->op, old_path, new_path,
						e->flags, 0, 0, 0, NULL, 0) < 0) {
				fprintf(stderr, "Failed to append RENAME record\n");
				rt->batch_error = 1;
			}
		} else if (old_rc == 0) {
			if (append_batch_record(rt, SD_VFS_OP_UNLINK, old_path,
						NULL, 0, 0, 0, 0, NULL, 0) < 0) {
				fprintf(stderr,
					"Failed to append external RENAME unlink record\n");
				rt->batch_error = 1;
			}
		} else if (new_rc == 0) {
			fprintf(stderr,
				"Skipping RENAME into tracked tree; source content is unavailable for replay: %s\n",
				new_path);
			rt->batch_error = 1;
		}
		if (e->is_dir) {
			if (old_rc == 0 && new_rc == 0) {
				move_dir_subtree(rt, &e->object, old_path, new_path);
			} else if (old_rc == 0) {
				remove_dir_subtree(rt, old_path);
			} else if (new_rc == 0) {
				char abs_path[PATH_MAX];

				if (join_abs(rt->root, new_path, abs_path,
					     sizeof(abs_path)) == 0)
					seed_dir(rt, abs_path, new_path);
			}
		}
		break;
	case SD_VFS_OP_MMAP:
		/*
		 * Learn the path so writeback/snapshot can resolve the inode.
		 * No batch record -- mmap itself changes no persistent state.
		 */
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		if (path_rc == 0 && !key_is_zero(&e->object))
			upsert_dir_entry(&rt->files, &e->object, old_path);
		break;
	case SD_VFS_OP_WRITEBACK: {
		struct dir_entry *fe = find_dir_entry(&rt->files, &e->object);

		if (fe)
			snapshot_range(rt, fe->path, e->offset, e->size);
		else
			fprintf(stderr,
				"WRITEBACK for unresolved inode %llu:%llu offset=%llu\n",
				e->object.dev, e->object.ino, e->offset);
		break;
	}
	case SD_VFS_OP_DIO_SUBMIT:
		if (!e->data_len || e->data_len != e->size ||
		    e->data_len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN) {
			fprintf(stderr,
				"Invalid DIO submit payload: size=%llu data_len=%u cookie=0x%llx\n",
				e->size, e->data_len, e->cookie);
			rt->batch_error = 1;
		} else if (dio_stage_segment(&rt->dios, e->cookie, e->offset,
					     e->data, e->data_len,
					     e->flags) < 0) {
			fprintf(stderr,
				"Failed to stage DIO segment for cookie 0x%llx\n",
				e->cookie);
			rt->batch_error = 1;
		}
		break;
	case SD_VFS_OP_DIO_SUBMIT_DONE:
		if (e->data_len) {
			fprintf(stderr,
				"Invalid DIO DONE payload for cookie 0x%llx\n",
				e->cookie);
			rt->batch_error = 1;
		} else {
			dio_submit_done(rt, e->cookie, e->size);
		}
		break;
	case SD_VFS_OP_DIO_COMPLETE:
		path_rc = resolve_event_path(rt, &e->parent, e->name, old_path,
					     sizeof(old_path));
		dio_note_completion(rt, e->cookie, e->ret,
				    (path_rc == 0 && e->name[0]) ? old_path : NULL);
		break;
	case SD_VFS_OP_DIO_ABORT:
		if (e->data_len) {
			fprintf(stderr,
				"Invalid DIO ABORT payload for cookie 0x%llx\n",
				e->cookie);
			rt->batch_error = 1;
		}
		dio_abort(rt, e->cookie);
		break;
	default:
		break;
	}

	return 0;
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage:\n"
		"  sudo %s [flags] <target_dir> [output_file]\n"
		"  sudo %s [flags] --socket <socket_path> <target_dir>\n"
		"\n"
		"  target_dir  Directory tree to seed and capture.\n"
		"  output_file Default: " STATEDIFF_VFS_DEFAULT_OUTPUT "\n"
		"  socket_path Unix socket for incremental get-and-clear capture.\n"
		"\n"
		"Flags:\n"
		"  --no-parent-check  Keep running after the launching process exits.\n"
		"                     Socket mode otherwise stops once reparented.\n"
		"  --mmap-snapshot    Capture writable-shared mmap content (e.g. SQLite\n"
		"                     shm/WAL-index). Each capture flushes mmap'd files\n"
		"                     with sync_file_range() and pread()s the dirtied\n"
		"                     ranges. Off by default: mmap stores are NOT\n"
		"                     captured unless this flag is given.\n",
		prog, prog);
}

static int read_stats(struct statediff_vfs_bpf *skel,
		      struct statediff_vfs_stats *stats)
{
	unsigned int key = 0;
	int fd;

	if (!skel)
		return -1;
	fd = bpf_map__fd(skel->maps.stats);
	if (fd < 0)
		return -1;
	memset(stats, 0, sizeof(*stats));
	return bpf_map_lookup_elem(fd, &key, stats);
}

static unsigned long long stats_loss_count(
	const struct statediff_vfs_stats *stats)
{
	return stats->ringbuf_drops + stats->write_bytes_dropped +
		stats->path_read_failures + stats->payload_read_failures +
		stats->dio_iocb_read_failures +
		stats->dio_iovec_read_failures +
		stats->dio_payload_read_failures +
		stats->fallocate_unsupported + stats->internal_failures;
}

// Fail closed. Any non-zero counter means the batch no longer describes the
// tree, so the caller must report an error instead of shipping a partial diff.
/*
 * vfs_mkdir() returns int on older kernels and the created dentry on newer
 * ones. Two fexit programs cover both shapes, and only the one whose argument
 * list matches the running kernel can be loaded, so the other is disabled
 * here. The running kernel's BTF is the authority on which applies.
 */
static int select_mkdir_program(struct statediff_vfs_bpf *skel)
{
	const struct btf_type *proto;
	const struct btf_type *ret;
	const struct btf_type *func;
	struct btf *btf;
	int returns_ptr = -1;
	int id;

	btf = btf__load_vmlinux_btf();
	if (!btf) {
		fprintf(stderr, "Failed to load kernel BTF: %s\n",
			strerror(errno));
		return -1;
	}

	id = btf__find_by_name_kind(btf, "vfs_mkdir", BTF_KIND_FUNC);
	if (id > 0) {
		func = btf__type_by_id(btf, id);
		proto = func ? btf__type_by_id(btf, func->type) : NULL;
		if (proto) {
			ret = btf__type_by_id(btf, proto->type);
			while (ret && (btf_is_mod(ret) || btf_is_typedef(ret)))
				ret = btf__type_by_id(btf, ret->type);
			returns_ptr = ret && btf_is_ptr(ret);
		}
	}
	btf__free(btf);

	if (returns_ptr < 0) {
		fprintf(stderr,
			"Failed to determine the return type of vfs_mkdir from kernel BTF\n");
		return -1;
	}

	bpf_program__set_autoload(skel->progs.handle_vfs_mkdir_ret,
				  !returns_ptr);
	bpf_program__set_autoload(skel->progs.handle_vfs_mkdir_ret_dentry,
				  returns_ptr);
	return 0;
}

static int check_capture_loss(struct statediff_vfs_bpf *skel)
{
	struct statediff_vfs_stats stats;
	unsigned long long lost;

	if (read_stats(skel, &stats) < 0) {
		fprintf(stderr, "Failed to read BPF capture status: %s\n",
			strerror(errno));
		return -1;
	}

	lost = stats_loss_count(&stats);
	if (!lost)
		return 0;

	fprintf(stderr,
		"VFS capture is incomplete: ringbuf_drops=%llu write_bytes_dropped=%llu path_read_failures=%llu payload_read_failures=%llu dio_iocb_read_failures=%llu dio_iovec_read_failures=%llu dio_payload_read_failures=%llu fallocate_unsupported=%llu internal_failures=%llu\n",
		stats.ringbuf_drops, stats.write_bytes_dropped,
		stats.path_read_failures, stats.payload_read_failures,
		stats.dio_iocb_read_failures, stats.dio_iovec_read_failures,
		stats.dio_payload_read_failures, stats.fallocate_unsupported,
		stats.internal_failures);
	return -1;
}

/*
 * Level-triggered EPOLLIN registration. Readiness is re-reported until the fd
 * is actually drained, so a partially serviced socket cannot be missed.
 */
static int epoll_watch(int epoll_fd, int fd)
{
	struct epoll_event ev = {};

	if (epoll_fd < 0 || fd < 0)
		return 0;
	ev.events = EPOLLIN;
	ev.data.fd = fd;
	return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

static void epoll_unwatch(int epoll_fd, int fd)
{
	if (epoll_fd >= 0 && fd >= 0)
		epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
}

static void close_capture_client(struct capture_socket *sock)
{
	if (sock->client_fd >= 0) {
		// Deregister before the descriptor number can be reused.
		epoll_unwatch(sock->epoll_fd, sock->client_fd);
		close(sock->client_fd);
	}
	sock->client_fd = -1;
}

static void close_capture_socket(struct capture_socket *sock)
{
	close_capture_client(sock);
	if (sock->listen_fd >= 0)
		close(sock->listen_fd);
	sock->listen_fd = -1;
	if (sock->path)
		unlink(sock->path);
}

static int initialize_capture_socket(struct capture_socket *sock,
				     const char *path)
{
	struct sockaddr_un addr = {};
	int flags;

	if (!path || !path[0] || strlen(path) >= sizeof(addr.sun_path)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	sock->path = path;
	sock->listen_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (sock->listen_fd < 0)
		return -1;

	addr.sun_family = AF_UNIX;
	memcpy(addr.sun_path, path, strlen(path) + 1);
	unlink(path);
	if (bind(sock->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
	    chmod(path, 0600) < 0 || listen(sock->listen_fd, 1) < 0)
		goto err;

	flags = fcntl(sock->listen_fd, F_GETFL, 0);
	if (flags < 0 ||
	    fcntl(sock->listen_fd, F_SETFL, flags | O_NONBLOCK) < 0)
		goto err;
	return 0;

err:
	close_capture_socket(sock);
	return -1;
}

// Send one complete batch and retain it if the client disconnects.
static int capture_and_send(struct capture_socket *sock,
			    struct statediff_vfs_bpf *skel,
			    struct ring_buffer *rb, struct runtime_state *rt)
{
	uint64_t wire_error = htole64(STATEDIFF_VFS_SOCKET_ERROR_SIZE);
	int mmap_fd = mmap_snapshot ? bpf_map__fd(skel->maps.mmap_files) : -1;
	int consumed;

	consumed = ring_buffer__consume(rb);
	if (consumed < 0) {
		fprintf(stderr, "Failed to drain VFS events: %d\n", consumed);
		rt->batch_error = 1;
	}
	if (mmap_fd >= 0 && snapshot_mmap_files(rt, mmap_fd) < 0) {
		fprintf(stderr, "Failed to snapshot mmap-backed files: %s\n",
			strerror(errno));
		rt->batch_error = 1;
	}
	if (mmap_fd >= 0) {
		consumed = ring_buffer__consume(rb);
		if (consumed < 0) {
			fprintf(stderr,
				"Failed to drain mmap writeback events: %d\n",
				consumed);
			rt->batch_error = 1;
		}
	}
	if (check_capture_loss(skel) < 0)
		rt->batch_error = 1;

	if (rt->batch_error) {
		// Tell the client that the batch is incomplete before closing.
		send_all(sock->client_fd, &wire_error, sizeof(wire_error));
		return -1;
	}

	if (send_batch(sock->client_fd, &rt->batch) < 0) {
		fprintf(stderr,
			"Failed to send VFS batch; retaining %zu record(s): %s\n",
			rt->batch.count, strerror(errno));
		return 1;
	}

	reset_event_batch(&rt->batch);
	return 0;
}

static int service_capture_socket(struct capture_socket *sock,
				  struct statediff_vfs_bpf *skel,
				  struct ring_buffer *rb,
				  struct runtime_state *rt)
{
	char commands[32];
	ssize_t received;
	size_t i;

	if (sock->client_fd < 0) {
		struct timeval send_timeout = { .tv_sec = 10, .tv_usec = 0 };

		sock->client_fd = accept4(sock->listen_fd, NULL, NULL, SOCK_CLOEXEC);
		if (sock->client_fd < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return 0;
			fprintf(stderr, "Failed to accept capture client: %s\n",
				strerror(errno));
			return -1;
		}
		setsockopt(sock->client_fd, SOL_SOCKET, SO_SNDTIMEO,
			   &send_timeout, sizeof(send_timeout));
		// Watch the client descriptor for capture commands.
		if (epoll_watch(sock->epoll_fd, sock->client_fd) < 0) {
			fprintf(stderr, "Failed to watch capture client: %s\n",
				strerror(errno));
			close_capture_client(sock);
			return -1;
		}
	}

	received = recv(sock->client_fd, commands, sizeof(commands), MSG_DONTWAIT);
	if (received < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return 0;
		fprintf(stderr, "Capture client receive failed: %s\n",
			strerror(errno));
		close_capture_client(sock);
		return 0;
	}
	if (received == 0) {
		close_capture_client(sock);
		return 0;
	}

	for (i = 0; i < (size_t)received; i++) {
		int rc;

		if (commands[i] != STATEDIFF_VFS_SOCKET_GET) {
			fprintf(stderr, "Unknown capture command: 0x%02x\n",
				(unsigned char)commands[i]);
			close_capture_client(sock);
			return 0;
		}

		rc = capture_and_send(sock, skel, rb, rt);
		if (rc < 0)
			return -1;
		if (rc > 0) {
			close_capture_client(sock);
			return 0;
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	struct statediff_vfs_bpf *skel = NULL;
	struct ring_buffer *rb = NULL;
	struct runtime_state rt = {};
	struct capture_socket capture_socket = {
		.listen_fd = -1,
		.client_fd = -1,
		.epoll_fd = -1,
	};
	const char *target_dir;
	const char *socket_path = NULL;
	pid_t parent_pid = 0;
	char resolved[PATH_MAX];
	struct stat st;
	int no_parent_check = 0;
	int epoll_fd = -1;
	int rb_epoll_fd = -1;
	int i, j;
	int err = 0;

	/*
	 * Strip the optional leading flags so the positional parsing below keeps
	 * accepting exactly the argument forms it always has.
	 */
	for (i = 1; i < argc; i++) {
		int consumed_args = 1;

		if (strcmp(argv[i], "--no-parent-check") == 0) {
			no_parent_check = 1;
		} else if (strcmp(argv[i], "--mmap-snapshot") == 0) {
			mmap_snapshot = 1;
		} else {
			continue;
		}
		for (j = i; j + consumed_args < argc; j++)
			argv[j] = argv[j + consumed_args];
		argc -= consumed_args;
		i--;
	}

	if (argc == 4 && strcmp(argv[1], "--socket") == 0) {
		socket_path = argv[2];
		target_dir = argv[3];
	} else if (argc >= 2 && argc <= 3) {
		target_dir = argv[1];
		rt.output_path = argc == 3 ? argv[2] :
			STATEDIFF_VFS_DEFAULT_OUTPUT;
	} else {
		usage(argv[0]);
		return 1;
	}

	rt.batch.next_seq = 1;

	if (!realpath(target_dir, resolved)) {
		fprintf(stderr, "Failed to resolve %s: %s\n",
			target_dir, strerror(errno));
		return 1;
	}
	if (stat(resolved, &st) < 0) {
		fprintf(stderr, "Failed to stat %s: %s\n",
			resolved, strerror(errno));
		return 1;
	}
	if (!S_ISDIR(st.st_mode)) {
		fprintf(stderr, "Target is not a directory: %s\n", resolved);
		return 1;
	}
	if (snprintf(rt.root, sizeof(rt.root), "%s", resolved) < 0 ||
	    strlen(resolved) >= sizeof(rt.root)) {
		fprintf(stderr, "Target path is too long: %s\n", resolved);
		return 1;
	}

	libbpf_set_print(libbpf_print_fn);
	signal(SIGINT, sig_handler);
	signal(SIGTERM, sig_handler);
	signal(SIGPIPE, SIG_IGN);
	if (socket_path && !no_parent_check) {
		/*
		 * A supervisor may stop its child with SIGKILL, which bypasses
		 * any shutdown hook. Remember the launching pid and stop once
		 * this process is reparented, so the probes and the socket
		 * cannot be orphaned.
		 */
		parent_pid = getppid();
		if (parent_pid == 1) {
			fprintf(stderr, "Failed to monitor parent: parent already exited\n");
			return 1;
		}
	}

	skel = statediff_vfs_bpf__open();
	if (!skel) {
		fprintf(stderr, "Failed to open BPF skeleton\n");
		err = -1;
		goto cleanup;
	}

	skel->rodata->ignored_pid = (unsigned int)getpid();

	// Allocate one large event scratch slot per possible CPU.
	{
		int ncpus = libbpf_num_possible_cpus();

		if (ncpus < 1)
			ncpus = 1;
		err = bpf_map__set_max_entries(skel->maps.event_scratch, ncpus);
		if (err) {
			fprintf(stderr,
				"Failed to size event_scratch map: %d\n", err);
			goto cleanup;
		}
	}

	// Skip mmap-specific hooks unless snapshotting is enabled.
	if (!mmap_snapshot) {
		bpf_program__set_autoload(skel->progs.handle_security_mmap_file,
					  false);
		bpf_program__set_autoload(
			skel->progs.handle_folio_start_writeback, false);
	}

	if (select_mkdir_program(skel) < 0) {
		err = -1;
		goto cleanup;
	}

	err = statediff_vfs_bpf__load(skel);
	if (err) {
		fprintf(stderr, "Failed to load BPF skeleton: %d\n", err);
		goto cleanup;
	}

	rt.tracked_dirs_fd = bpf_map__fd(skel->maps.tracked_dirs);
	if (rt.tracked_dirs_fd < 0) {
		err = -1;
		fprintf(stderr, "Failed to access tracked_dirs map\n");
		goto cleanup;
	}
	if (seed_dir(&rt, resolved, ".") < 0) {
		err = -1;
		fprintf(stderr, "Failed to seed tracked directories under %s: %s\n",
			resolved, strerror(errno));
		goto cleanup;
	}
	err = statediff_vfs_bpf__attach(skel);
	if (err) {
		fprintf(stderr, "Failed to attach BPF programs: %d\n", err);
		goto cleanup;
	}

	rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, &rt, NULL);
	if (!rb) {
		err = -1;
		fprintf(stderr, "Failed to create ring buffer\n");
		goto cleanup;
	}
	if (socket_path &&
	    initialize_capture_socket(&capture_socket, socket_path) < 0) {
		err = -1;
		fprintf(stderr, "Failed to listen on capture socket %s: %s\n",
			socket_path, strerror(errno));
		goto cleanup;
	}

	/*
	 * One wait set for every source that must be able to wake the loop.
	 * ring_buffer__epoll_fd() is libbpf's own epoll fd, and an epoll fd is
	 * itself pollable and reports readable whenever any ring inside it has
	 * records, so nesting it here is the supported way to fold a ring
	 * buffer into an external event loop.
	 */
	epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (epoll_fd < 0) {
		err = -1;
		fprintf(stderr, "Failed to create epoll set: %s\n",
			strerror(errno));
		goto cleanup;
	}
	rb_epoll_fd = ring_buffer__epoll_fd(rb);
	if (rb_epoll_fd < 0 || epoll_watch(epoll_fd, rb_epoll_fd) < 0) {
		err = -1;
		fprintf(stderr, "Failed to watch ring buffer: %s\n",
			strerror(errno));
		goto cleanup;
	}
	if (socket_path) {
		capture_socket.epoll_fd = epoll_fd;
		if (epoll_watch(epoll_fd, capture_socket.listen_fd) < 0) {
			err = -1;
			fprintf(stderr, "Failed to watch capture socket: %s\n",
				strerror(errno));
			goto cleanup;
		}
	}

	if (!mmap_snapshot)
		fprintf(stderr,
			"WARNING: mmap snapshot OFF; content written through "
			"writable shared mappings (e.g. SQLite shm/WAL-index) "
			"is NOT captured. Pass --mmap-snapshot to capture it.\n");

	while (!exiting) {
		struct epoll_event evs[8];
		int drain_rb = 0, service_sock = 0;
		int nready, k;

		if (parent_pid && getppid() != parent_pid) {
			fprintf(stderr, "Parent exited; stopping capture daemon\n");
			break;
		}

		/*
		 * Sleep on the ring buffer AND the capture socket at once. A
		 * ring_buffer__poll() would sleep on the ring buffer alone, so a
		 * capture command sitting in the socket receive queue could not
		 * wake this thread. It would wait out the timeout or the next
		 * filesystem event, adding up to 100 ms to every low-load
		 * capture. The timeout below no longer gates responsiveness --
		 * both real event sources wake us directly -- it only paces the
		 * parent-liveness and stats checks.
		 */
		nready = epoll_wait(epoll_fd, evs, (int)ARRAY_SIZE(evs), 100);
		if (nready < 0) {
			if (errno == EINTR)
				continue;
			err = -errno;
			fprintf(stderr, "Error waiting for events: %s\n",
				strerror(errno));
			break;
		}

		for (k = 0; k < nready; k++) {
			if (evs[k].data.fd == rb_epoll_fd)
				drain_rb = 1;
			else
				service_sock = 1;
		}

		if (drain_rb) {
			int consumed = ring_buffer__consume(rb);

			if (consumed < 0) {
				err = consumed;
				fprintf(stderr,
					"Error consuming ring buffer: %d\n",
					consumed);
				break;
			}
		}
		// capture_and_send drains the ring buffer before sending a batch.
		if (socket_path && (service_sock || nready == 0) &&
		    service_capture_socket(&capture_socket, skel, rb, &rt) < 0) {
			err = -EIO;
			break;
		}
	}

cleanup:
	// The client must be deregistered before epoll is closed.
	close_capture_socket(&capture_socket);
	capture_socket.epoll_fd = -1;
	if (epoll_fd >= 0)
		close(epoll_fd);
	if (rb && !socket_path) {
		int mmap_fd = (mmap_snapshot && skel) ?
			bpf_map__fd(skel->maps.mmap_files) : -1;

		// Drain events before requesting mmap writeback.
		ring_buffer__consume(rb);
		if (mmap_fd >= 0) {
			snapshot_mmap_files(&rt, mmap_fd);
			// Drain the writeback ranges appended by the BPF program.
			ring_buffer__consume(rb);
		}
		if (check_capture_loss(skel) < 0)
			rt.batch_error = 1;
		if (rt.batch_error) {
			fprintf(stderr,
				"Not writing %s because capture became incomplete\n",
				rt.output_path);
			if (err == 0)
				err = -1;
		} else if (write_batch_file(&rt.batch, rt.output_path) < 0) {
			fprintf(stderr, "Failed to write %s: %s\n",
				rt.output_path, strerror(errno));
			if (err == 0)
				err = -1;
		}
	}
	if (socket_path && rt.batch.count)
		fprintf(stderr, "Discarding %zu unsent VFS record(s) on exit\n",
			rt.batch.count);
	ring_buffer__free(rb);
	statediff_vfs_bpf__destroy(skel);
	if (rt.dios.count)
		fprintf(stderr,
			"Discarding %zu O_DIRECT write(s) still uncompleted at exit\n",
			rt.dios.count);
	dio_free_all(&rt.dios);
	free_event_batch(&rt.batch);
	free_dir_table(&rt.dirs);
	free_dir_table(&rt.files);
	return err < 0 ? -err : err;
}
