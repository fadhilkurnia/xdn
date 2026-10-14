// SPDX-License-Identifier: GPL-2.0 OR BSD-3-Clause
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "statediff_vfs.h"

#define S_IFMT 00170000
#define S_IFDIR 0040000
#define S_IFREG 0100000

// From include/linux/fs.h, set on file->f_mode iff the open path created it.
#define FMODE_CREATED 0x100000

// From uapi/linux/aio_abi.h, the libaio opcodes. We capture writes only.
#define IOCB_CMD_PWRITE 1
#define IOCB_CMD_PWRITEV 8

// From include/linux/fs.h, aio_prep_rw() marks write kiocbs with this bit.
#define IOCB_WRITE (1 << 18)

// mmap(2) prot/flags bits we care about (uapi/asm-generic/mman-common.h).
#define PROT_WRITE 0x2
#define MAP_SHARED 0x01
#define MAP_SHARED_VALIDATE 0x03
#define MAP_TYPE 0x0f

// From uapi/linux/falloc.h, modes whose logical effects we classify below.
#define FALLOC_FL_KEEP_SIZE 0x01
#define FALLOC_FL_PUNCH_HOLE 0x02
#define FALLOC_FL_ZERO_RANGE 0x10

/*
 * Page-cache geometry for the writeback hook. PAGE_SHIFT 12 means 4 KiB base
 * pages, which is correct on x86-64 (base pages are always 4 KiB, even when
 * large folios bundle several) but NOT portable -- arm64 and ppc64 can be 16K
 * or 64K. PG_head marks a multi-page "large" folio, whose page count then lives
 * in _folio_nr_pages. Note that these two are hardcoded literals read off this
 * kernel's BTF by hand -- only the struct-field *accesses* below are
 * CO-RE-relocated, not these numbers. To go cross-arch, feed page_shift from
 * userspace sysconf(_SC_PAGESIZE) via rodata and read PG_head with
 * bpf_core_enum_value(enum pageflags, PG_head).
 */
#define PAGE_SHIFT 12
#define PG_head 6

// In address_space->flags, these low bits mark an anon or movable mapping,
// which is not a file.
#define PAGE_MAPPING_FLAGS 0x3

char LICENSE[] SEC("license") = "Dual BSD/GPL";

/*
 * Copy `len` bytes of user memory into the staging buffer's data area, with the
 * clamp the verifier needs to accept a variable length.
 *
 * The barrier_var() is the load-bearing part. Without it the compiler is free to
 * re-materialize `len` after the range check -- it can sink the bound test, or
 * keep the checked value in one register and pass an unchecked recomputation to
 * the helper -- and the verifier then sees an unbounded length against a fixed
 * buffer and rejects the program with "invalid access to map value". The empty
 * asm with a "+r" constraint makes the checked value opaque, so the compiler
 * must commit it to a register there and hand the helper that same register,
 * so the bound the verifier proved is the bound the copy uses.
 *
 * Callers must have already established that len is within
 * STATEDIFF_VFS_MAX_WRITE_DATA_LEN. The re-check here is what the barrier
 * attaches to, not a substitute for the caller's own clamp.
 */
static __always_inline long read_user_chunk(struct statediff_vfs_event_storage *storage,
					    unsigned long long len,
					    const void *src)
{
	long chunk = (long)len;

	if (chunk <= 0 || chunk > STATEDIFF_VFS_MAX_WRITE_DATA_LEN)
		return -1;

	// Keep the compiler from undoing the check above.
	barrier_var(chunk);

	return bpf_probe_read_user(storage->data, chunk, src);
}

/*
 * Upper bound on iovec segments walked per writev/pwritev. bpf_loop() iterates
 * the user iovec array and emits one ring-buffer event per segment, so this is
 * only the loop trip cap, not the size of any staging array. 1024 is UIO_MAXIOV,
 * the kernel's own ceiling on iovec count, so a valid writev never exceeds it.
 * Anything past it leaves the tail uncaptured and is tallied as dropped.
 */
#define STATEDIFF_VFS_MAX_IOV_SEGS 1024

// Arguments retained between entry and return hooks.
struct statediff_vfs_pending {
	unsigned int op;
	unsigned int mode;
	unsigned int flags;
	unsigned long long offset;
	unsigned long long size;
	const char *buf;
	unsigned int is_dir;
	struct statediff_vfs_inode_key parent;
	struct statediff_vfs_inode_key object;
	struct statediff_vfs_inode_key new_parent;
	struct statediff_vfs_inode_key new_object;
	char name[STATEDIFF_VFS_NAME_LEN];
	char new_name[STATEDIFF_VFS_NAME_LEN];
	unsigned long long iov_ptr;
	unsigned long long pos_ptr;
	unsigned int iov_segs;
	// Carried from entry: the original arguments are not visible to
	// kretprobe/vfs_writev, where i_nlink is re-checked.
	unsigned long long file_ptr;
};

enum statediff_vfs_error {
	SD_VFS_ERROR_RINGBUF_DROP,
	SD_VFS_ERROR_WRITE_BYTES_DROPPED,
	SD_VFS_ERROR_PATH_READ,
	SD_VFS_ERROR_PAYLOAD_READ,
	SD_VFS_ERROR_DIO_IOCB_READ,
	SD_VFS_ERROR_DIO_IOVEC_READ,
	SD_VFS_ERROR_DIO_PAYLOAD_READ,
	SD_VFS_ERROR_FALLOCATE_UNSUPPORTED,
	SD_VFS_ERROR_INTERNAL,
};

// These maps deliver events and report whether a batch is complete.
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 64 * 1024 * 1024);
} rb SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, unsigned int);
	__type(value, struct statediff_vfs_stats);
} stats SEC(".maps");

// These maps limit capture to the requested directory tree and mapped files.
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__type(key, struct statediff_vfs_inode_key);
	__type(value, unsigned char);
} tracked_dirs SEC(".maps");

/*
 * Inodes of regular files under the tree that hold a writable shared mapping.
 * The mmap hook adds them, and the writeback hook checks membership to decide
 * whether a flushed folio belongs to a file we must snapshot. This is the file-
 * level analogue of tracked_dirs and the in-kernel half of "which files can be
 * written invisibly via mmap".
 */
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__type(key, struct statediff_vfs_inode_key);
	__type(value, unsigned char);
} mmap_files SEC(".maps");

// Per-thread maps pair operation arguments with their return values.
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_mkdir SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_write SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_writev SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_truncate SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_fallocate SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_unlink SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_rmdir SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, struct statediff_vfs_pending);
} pending_rename SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, 1);
	__type(key, unsigned int);
	__type(value, struct statediff_vfs_pending);
} scratch SEC(".maps");

/*
 * Per-CPU staging buffer for variable-length write events. This deliberately is
 * NOT a BPF_MAP_TYPE_PERCPU_ARRAY. The per-CPU allocator caps a single element
 * at PCPU_MIN_UNIT_SIZE (32 KiB), but statediff_vfs_event_storage is ~4 MiB, so
 * a per-CPU array fails to create with -ENOMEM (the whole skeleton then fails to
 * load). Use a plain array indexed by CPU id instead, and userspace sizes
 * max_entries to the number of possible CPUs before load.
 */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, unsigned int);
	__type(value, struct statediff_vfs_event_storage);
} event_scratch SEC(".maps");

/*
 * Candidate libaio cookies (user iocb pointers), registered before
 * io_submit_one touches user memory. At completion, ki_flags distinguishes
 * writes from reads without dereferencing the user IOCB. fexit/io_submit_one
 * removes non-writes and failed submissions, while successful asynchronous
 * writes remain until aio_complete_rw removes them. A synchronous completion may
 * remove its cookie before fexit stages the payload, which is intentional.
 */
struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 8192);
	__type(key, unsigned long long);
	__type(value, unsigned char);
} dio_inflight SEC(".maps");

// PID of the capturer itself, so its own filesystem activity is skipped.
const volatile unsigned int ignored_pid = 0;

static __always_inline void count_error(enum statediff_vfs_error error,
					unsigned long long value)
{
	unsigned int key = 0;
	struct statediff_vfs_stats *s;

	s = bpf_map_lookup_elem(&stats, &key);
	if (!s)
		return;

	if (error == SD_VFS_ERROR_RINGBUF_DROP)
		__sync_fetch_and_add(&s->ringbuf_drops, value);
	else if (error == SD_VFS_ERROR_WRITE_BYTES_DROPPED)
		__sync_fetch_and_add(&s->write_bytes_dropped, value);
	else if (error == SD_VFS_ERROR_PATH_READ)
		__sync_fetch_and_add(&s->path_read_failures, value);
	else if (error == SD_VFS_ERROR_PAYLOAD_READ)
		__sync_fetch_and_add(&s->payload_read_failures, value);
	else if (error == SD_VFS_ERROR_DIO_IOCB_READ)
		__sync_fetch_and_add(&s->dio_iocb_read_failures, value);
	else if (error == SD_VFS_ERROR_DIO_IOVEC_READ)
		__sync_fetch_and_add(&s->dio_iovec_read_failures, value);
	else if (error == SD_VFS_ERROR_DIO_PAYLOAD_READ)
		__sync_fetch_and_add(&s->dio_payload_read_failures, value);
	else if (error == SD_VFS_ERROR_FALLOCATE_UNSUPPORTED)
		__sync_fetch_and_add(&s->fallocate_unsupported, value);
	else if (error == SD_VFS_ERROR_INTERNAL)
		__sync_fetch_and_add(&s->internal_failures, value);
}

static __always_inline unsigned long long current_key(void)
{
	return bpf_get_current_pid_tgid();
}

/*
 * Scope is enforced entirely by tracked_dirs (an event fires only when its
 * parent directory's inode is tracked). The only blanket filter here is the
 * capturer's own PID, so its setup and scan activity never lands in the batch.
 */
static __always_inline int filtered_out(void)
{
	unsigned long long pid_tgid = bpf_get_current_pid_tgid();

	if (ignored_pid && (unsigned int)(pid_tgid >> 32) == ignored_pid)
		return 1;
	return 0;
}

static __always_inline void init_pending(struct statediff_vfs_pending *p,
					 unsigned int op)
{
	p->op = op;
	p->mode = 0;
	p->flags = 0;
	p->offset = 0;
	p->size = 0;
	p->buf = 0;
	p->is_dir = 0;
	p->iov_ptr = 0;
	p->pos_ptr = 0;
	p->iov_segs = 0;
	p->parent.dev = 0;
	p->parent.ino = 0;
	p->object.dev = 0;
	p->object.ino = 0;
	p->new_parent.dev = 0;
	p->new_parent.ino = 0;
	p->new_object.dev = 0;
	p->new_object.ino = 0;
	p->name[0] = '\0';
	p->new_name[0] = '\0';
}

static __always_inline struct statediff_vfs_pending *get_scratch(unsigned int op)
{
	unsigned int key = 0;
	struct statediff_vfs_pending *p;

	p = bpf_map_lookup_elem(&scratch, &key);
	if (!p) {
		count_error(SD_VFS_ERROR_INTERNAL, 1);
		return 0;
	}
	init_pending(p, op);
	return p;
}

static __always_inline int inode_to_key(struct inode *inode,
					struct statediff_vfs_inode_key *key)
{
	struct super_block *sb;

	if (!inode)
		return 0;

	sb = BPF_CORE_READ(inode, i_sb);
	if (!sb)
		return 0;

	key->ino = BPF_CORE_READ(inode, i_ino);
	key->dev = BPF_CORE_READ(sb, s_dev);
	return key->ino != 0;
}

/*
 * A write may complete against an inode whose last link has already been
 * removed by another thread. Such bytes are unreachable once the descriptor
 * is closed, so they cannot appear in the final tree, and the name they would
 * be recorded under no longer refers to this inode -- replaying it would
 * recreate a file that must not exist. These writes are therefore dropped,
 * and are not counted as capture loss.
 */
static __always_inline int write_target_unlinked(struct file *file)
{
	struct inode *inode;

	if (!file)
		return 0;
	inode = BPF_CORE_READ(file, f_inode);
	if (!inode)
		return 0;
	return BPF_CORE_READ(inode, i_nlink) == 0;
}

static __always_inline int dentry_to_key(struct dentry *dentry,
					 struct statediff_vfs_inode_key *key)
{
	struct inode *inode;

	if (!dentry)
		return 0;
	inode = BPF_CORE_READ(dentry, d_inode);
	return inode_to_key(inode, key);
}

static __always_inline int dentry_parent_key(struct dentry *dentry,
					     struct statediff_vfs_inode_key *key)
{
	struct dentry *parent;

	if (!dentry)
		return 0;
	parent = BPF_CORE_READ(dentry, d_parent);
	if (!parent)
		return 0;
	return dentry_to_key(parent, key);
}

static __always_inline int inode_is_dir(struct inode *inode)
{
	umode_t mode;

	if (!inode)
		return 0;
	mode = BPF_CORE_READ(inode, i_mode);
	return (mode & S_IFMT) == S_IFDIR;
}

static __always_inline int dentry_is_dir(struct dentry *dentry)
{
	struct inode *inode;

	if (!dentry)
		return 0;
	inode = BPF_CORE_READ(dentry, d_inode);
	return inode_is_dir(inode);
}

static __always_inline int dir_is_tracked(const struct statediff_vfs_inode_key *key)
{
	void *tracked;

	tracked = bpf_map_lookup_elem(&tracked_dirs, key);
	if (tracked)
		return 1;
	return 0;
}

static __always_inline int read_dentry_name(struct dentry *dentry, char *name)
{
	struct qstr q = {};
	long n;

	if (!dentry)
		return 0;

	BPF_CORE_READ_INTO(&q, dentry, d_name);
	if (!q.name || !q.len || q.len >= STATEDIFF_VFS_NAME_LEN)
		return 0;

	n = bpf_probe_read_kernel_str(name, STATEDIFF_VFS_NAME_LEN, q.name);
	return n > 1 && n <= STATEDIFF_VFS_NAME_LEN;
}

/*
 * The name and parent that a write is recorded under are resolved at
 * completion rather than at entry. The file may be renamed by another thread
 * in between, after which the entry-time name no longer refers to it: the
 * write would then be replayed against the old path, recreating a file there
 * with O_CREAT and breaking every later operation on that name.
 *
 * The tracked check is repeated for the same reason -- a file renamed out of
 * the tree mid-write is no longer part of the captured state.
 */
static __always_inline int refresh_write_target(struct file *file,
						struct statediff_vfs_pending *p)
{
	struct dentry *dentry;

	if (!file)
		return 0;
	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}
	return 1;
}

static __always_inline void fill_common(struct statediff_vfs_event *e,
					const struct statediff_vfs_pending *p,
					long long ret)
{
	e->op = p->op;
	e->ret = ret;
	e->mode = p->mode;
	e->flags = p->flags;
	e->offset = p->offset;
	e->size = p->size;
	e->cookie = 0;
	e->is_dir = p->is_dir;
	e->data_len = 0;
	e->parent = p->parent;
	e->object = p->object;
	e->new_parent = p->new_parent;
	e->new_object = p->new_object;
	__builtin_memcpy(e->name, p->name, sizeof(e->name));
	__builtin_memcpy(e->new_name, p->new_name, sizeof(e->new_name));
}

static __always_inline struct statediff_vfs_event *reserve_empty_event(void)
{
	return bpf_ringbuf_reserve(&rb, STATEDIFF_VFS_EVENT_HEADER_LEN, 0);
}

static __always_inline struct statediff_vfs_event_storage *get_event_scratch(void)
{
	unsigned int key = bpf_get_smp_processor_id();

	return bpf_map_lookup_elem(&event_scratch, &key);
}

// Shared return-hook path. Publish the staged record only when the operation
// actually succeeded, so a failed syscall leaves nothing in the batch.
static __always_inline int emit_pending_event(void *map, long long ret)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event *e;

	p = bpf_map_lookup_elem(map, &key);
	if (!p)
		return 0;

	if (ret < 0) {
		bpf_map_delete_elem(map, &key);
		return 0;
	}

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		bpf_map_delete_elem(map, &key);
		return 0;
	}

	fill_common(e, p, ret);
	bpf_map_delete_elem(map, &key);
	bpf_ringbuf_submit(e, 0);
	return 0;
}

// Shared entry-hook path for operations named by (parent directory, child
// name). Scope is decided by the parent, which is what the tracked-dir map
// holds.
static __always_inline int save_child_op(void *map, unsigned int op,
					 struct inode *dir,
					 struct dentry *dentry,
					 unsigned int mode)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;

	if (filtered_out())
		return 0;

	p = get_scratch(op);
	if (!p)
		return 0;
	if (!inode_to_key(dir, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	p->mode = mode;
	dentry_to_key(dentry, &p->object);
	p->is_dir = op == SD_VFS_OP_MKDIR || op == SD_VFS_OP_RMDIR ||
		dentry_is_dir(dentry);

	if (bpf_map_update_elem(map, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

/*
 * File creation via open(O_CREAT) does NOT go through vfs_create() on ext4 (and
 * most local filesystems), because lookup_open() calls dir->i_op->create()
 * directly,
 * bypassing the vfs_create() wrapper. Hooking vfs_create only catches
 * mknod-style creation.
 */
SEC("fexit/do_filp_open")
int BPF_PROG(handle_do_filp_open, int dfd, void *pathname, void *op,
	     struct file *file)
{
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event *e;
	struct dentry *dentry;
	struct inode *inode;
	unsigned int fmode;
	unsigned long fp = (unsigned long)file;

	// do_filp_open() returns an ERR_PTR (last page) on failure.
	if (fp == 0 || fp >= (unsigned long)-4095L)
		return 0;

	fmode = BPF_CORE_READ(file, f_mode);
	if (!(fmode & FMODE_CREATED))
		return 0;

	if (filtered_out())
		return 0;

	p = get_scratch(SD_VFS_OP_CREATE);
	if (!p)
		return 0;

	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	inode = BPF_CORE_READ(dentry, d_inode);
	inode_to_key(inode, &p->object);
	p->mode = BPF_CORE_READ(inode, i_mode);
	p->is_dir = inode_is_dir(inode);

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return 0;
	}
	fill_common(e, p, 0);
	bpf_ringbuf_submit(e, 0);
	return 0;
}

SEC("fentry/vfs_mkdir")
int BPF_PROG(handle_vfs_mkdir, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry, umode_t mode)
{
	return save_child_op(&pending_mkdir, SD_VFS_OP_MKDIR, dir, dentry, mode);
}

static __always_inline int mkdir_ret_common(struct dentry *created, int ok)
{
	struct statediff_vfs_pending *p;
	struct inode *inode;
	unsigned long long key = current_key();
	unsigned char tracked = 1;

	p = bpf_map_lookup_elem(&pending_mkdir, &key);
	if (p && ok) {
		/*
		 * The mode argument is the requested mode, not the created one:
		 * current_umask() is applied by do_mkdirat() only when
		 * !IS_POSIXACL, so on ext4 it is applied later, inside
		 * posix_acl_create(). The created inode's i_mode is therefore
		 * the only authority, as is already relied on for CREATE.
		 */
		inode = BPF_CORE_READ(created, d_inode);
		if (inode)
			p->mode = BPF_CORE_READ(inode, i_mode);
		if (dentry_to_key(created, &p->object) &&
		    bpf_map_update_elem(&tracked_dirs, &p->object, &tracked,
					BPF_ANY) < 0)
			count_error(SD_VFS_ERROR_INTERNAL, 1);
	}
	return emit_pending_event(&pending_mkdir, ok ? 0 : -1);
}

/*
 * vfs_mkdir() returned int until it was changed to return the created dentry.
 * An fexit program's argument list is matched against the target's BTF, so one
 * program can only load on one kernel generation. Both shapes are provided and
 * the loader enables whichever matches, in select_mkdir_program().
 */
SEC("fexit/vfs_mkdir")
int BPF_PROG(handle_vfs_mkdir_ret, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry, umode_t mode, int ret)
{
	return mkdir_ret_common(dentry, ret >= 0);
}

SEC("fexit/vfs_mkdir")
int BPF_PROG(handle_vfs_mkdir_ret_dentry, struct mnt_idmap *idmap,
	     struct inode *dir, struct dentry *dentry, umode_t mode,
	     struct dentry *ret)
{
	unsigned long dp = (unsigned long)ret;
	// Failure is an ERR_PTR in the last page, as with do_filp_open().
	int ok = dp != 0 && dp < (unsigned long)-4095L;

	/*
	 * The returned dentry is the one the directory was created on, which
	 * can differ from the one passed in, so the mode is taken from it.
	 */
	return mkdir_ret_common(ok ? ret : dentry, ok);
}

// Buffered writes. The entry hook only records where the user buffer is, and
// the bytes are copied at exit once the return value says how many were written.
SEC("fentry/vfs_write")
int BPF_PROG(handle_vfs_write, struct file *file, const char *buf,
	     size_t count, loff_t *pos)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct dentry *dentry;
	loff_t offset = 0;

	if (filtered_out() || !file)
		return 0;

	p = get_scratch(SD_VFS_OP_WRITE);
	if (!p)
		return 0;

	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!dentry_to_key(dentry, &p->object))
		return 0;
	// The name is deliberately not read here; see refresh_write_target().

	if (pos)
		bpf_probe_read_kernel(&offset, sizeof(offset), pos);

	p->offset = offset;
	p->size = count;
	p->buf = buf;
	if (bpf_map_update_elem(&pending_write, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

SEC("fexit/vfs_write")
int BPF_PROG(handle_vfs_write_ret, struct file *file, const char *buf,
	     size_t count, loff_t *pos, ssize_t ret)
{
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event_storage *storage;
	unsigned long long key = current_key();
	struct statediff_vfs_event *e;
	unsigned long long written;
	unsigned long long emitted = 0;
	unsigned long long next_emitted;
	unsigned long long start_offset;
	loff_t end_offset = 0;
	unsigned int data_len;
	int i;

	p = bpf_map_lookup_elem(&pending_write, &key);
	if (!p)
		return 0;
	if (ret <= 0) {
		bpf_map_delete_elem(&pending_write, &key);
		return 0;
	}
	if (write_target_unlinked(file) || !refresh_write_target(file, p)) {
		bpf_map_delete_elem(&pending_write, &key);
		return 0;
	}

	written = (unsigned long long)ret;
	start_offset = p->offset;
	if (pos &&
	    bpf_probe_read_kernel(&end_offset, sizeof(end_offset), pos) == 0 &&
	    end_offset >= ret)
		start_offset = (unsigned long long)(end_offset - ret);

	for (i = 0; i < STATEDIFF_VFS_MAX_WRITE_CHUNKS; i++) {
		if (emitted >= written)
			break;

		if (written - emitted > STATEDIFF_VFS_MAX_WRITE_DATA_LEN)
			data_len = STATEDIFF_VFS_MAX_WRITE_DATA_LEN;
		else
			data_len = (unsigned int)(written - emitted);

		if (!data_len ||
		    data_len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN) {
			count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED,
				    written - emitted);
			count_error(SD_VFS_ERROR_INTERNAL, 1);
			bpf_map_delete_elem(&pending_write, &key);
			return 0;
		}

		storage = get_event_scratch();
		if (!storage) {
			count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED,
				    written - emitted);
			count_error(SD_VFS_ERROR_INTERNAL, 1);
			bpf_map_delete_elem(&pending_write, &key);
			return 0;
		}
		e = (struct statediff_vfs_event *)storage;

		if (read_user_chunk(storage, data_len, p->buf + emitted) < 0) {
			count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED, data_len);
			count_error(SD_VFS_ERROR_PAYLOAD_READ, 1);
			bpf_map_delete_elem(&pending_write, &key);
			return 0;
		}

		fill_common(e, p, data_len);
		e->offset = start_offset + emitted;
		e->size = data_len;
		e->data_len = data_len;
		next_emitted = emitted + data_len;
		if (next_emitted < written &&
		    i == STATEDIFF_VFS_MAX_WRITE_CHUNKS - 1)
			e->flags |= SD_VFS_EVENT_F_TRUNCATED;

		if (bpf_ringbuf_output(&rb, storage,
				       STATEDIFF_VFS_EVENT_HEADER_LEN + data_len,
				       0) < 0) {
			count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
			count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED,
				    written - emitted);
			bpf_map_delete_elem(&pending_write, &key);
			return 0;
		}
		emitted = next_emitted;
	}

	if (emitted < written) {
		count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED, written - emitted);
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	}
	bpf_map_delete_elem(&pending_write, &key);
	return 0;
}

/*
 * writev(2) and pwritev(2) never pass through vfs_write. They reach the file's
 * ->write_iter via do_writev() -> vfs_writev() -> do_iter_write(), all of which
 * are static (inlined) on this kernel and so invisible to fentry. vfs_writev()
 * is the one symbol that still resolves in kallsyms, and it carries the raw
 * user iovec, so attach a k(ret)probe there -- otherwise vectored writes (e.g.
 * PostgreSQL flushing multiple WAL pages with pwritev) are silently missed and
 * replay leaves those files empty. The entry probe only stashes the iovec
 * pointer, the file-position pointer, and the segment count, and the exit probe
 * re-reads the final position and walks the segments with bpf_loop().
 */
SEC("kprobe/vfs_writev")
int BPF_KPROBE(handle_vfs_writev, struct file *file, const struct iovec *vec,
	       unsigned long vlen, loff_t *pos)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct dentry *dentry;
	loff_t offset = 0;

	if (filtered_out() || !file || !vec)
		return 0;

	p = get_scratch(SD_VFS_OP_WRITE);
	if (!p)
		return 0;

	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!dentry_to_key(dentry, &p->object))
		return 0;
	// The name is deliberately not read here; see refresh_write_target().

	if (pos)
		bpf_probe_read_kernel(&offset, sizeof(offset), pos);
	p->offset = offset;
	p->pos_ptr = (unsigned long long)pos;
	p->iov_ptr = (unsigned long long)vec;
	p->iov_segs = vlen;
	p->file_ptr = (unsigned long long)file;

	if (bpf_map_update_elem(&pending_writev, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

/*
 * Per-segment emit. The kretprobe sees only the return value, so the entry
 * probe stashed the user iovec pointer and segment count in pending_writev, and
 * bpf_loop() drives this callback once per segment. Each iovec segment is an
 * independent write whose file offset is the start offset plus the bytes
 * already emitted (segments lie down contiguously), so emitting it as its own
 * WRITE event at storage->data[0] reproduces the bytes without coalescing --
 * userspace merges contiguous segments back together when it builds the batch
 * (append_batch_record() in statediff_vfs.c), where plain C ints do not need to
 * survive the BPF verifier.
 *
 * Verifier note. `emitted` only ever feeds e->offset (a stored value), never a
 * branch or a copy length, so it stays "safety-irrelevant" and the loop body
 * verifies once instead of being simulated segment by segment.
 */
struct writev_emit_ctx {
	unsigned long long key;
	unsigned long long emitted;
};

static long writev_emit_seg(unsigned int i, void *data)
{
	struct writev_emit_ctx *c = data;
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event_storage *storage;
	struct statediff_vfs_event *e;
	const struct iovec *vec;
	struct iovec seg;
	unsigned long long base;
	unsigned long long seg_len;
	unsigned int chunk;
	int truncated;

	p = bpf_map_lookup_elem(&pending_writev, &c->key);
	if (!p)
		return 1;

	vec = (const struct iovec *)p->iov_ptr;
	if (bpf_probe_read_user(&seg, sizeof(seg), &vec[i]) < 0) {
		count_error(SD_VFS_ERROR_PAYLOAD_READ, 1);
		return 1;
	}

	base = (unsigned long long)seg.iov_base;
	seg_len = (unsigned long long)seg.iov_len;
	// Empty segments are valid.
	if (seg_len == 0)
		return 0;

	truncated = seg_len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN;
	chunk = truncated ? STATEDIFF_VFS_MAX_WRITE_DATA_LEN :
		(unsigned int)seg_len;
	// This explicit check keeps the verifier's bound tight.
	if (!chunk || chunk > STATEDIFF_VFS_MAX_WRITE_DATA_LEN)
		return 1;

	storage = get_event_scratch();
	if (!storage) {
		count_error(SD_VFS_ERROR_INTERNAL, 1);
		return 1;
	}
	e = (struct statediff_vfs_event *)storage;

	if (read_user_chunk(storage, chunk, (const void *)base) < 0) {
		count_error(SD_VFS_ERROR_PAYLOAD_READ, 1);
		return 1;
	}

	fill_common(e, p, chunk);
	e->offset = p->offset + c->emitted;
	e->size = chunk;
	e->data_len = chunk;
	if (truncated)
		e->flags |= SD_VFS_EVENT_F_TRUNCATED;

	if (bpf_ringbuf_output(&rb, storage,
				       STATEDIFF_VFS_EVENT_HEADER_LEN + chunk, 0) < 0) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return 1;
	}
	c->emitted += chunk;

	return truncated ? 1 : 0;
}

SEC("kretprobe/vfs_writev")
int BPF_KRETPROBE(handle_vfs_writev_ret, ssize_t ret)
{
	unsigned long long key = current_key();
	unsigned long long written;
	struct statediff_vfs_pending *p;
	struct writev_emit_ctx emit = {};
	unsigned int segs;

	p = bpf_map_lookup_elem(&pending_writev, &key);
	if (!p)
		return 0;
	if (ret <= 0) {
		bpf_map_delete_elem(&pending_writev, &key);
		return 0;
	}
	if (write_target_unlinked((struct file *)p->file_ptr) ||
	    !refresh_write_target((struct file *)p->file_ptr, p)) {
		bpf_map_delete_elem(&pending_writev, &key);
		return 0;
	}

	written = (unsigned long long)ret;
	segs = p->iov_segs;
	if (segs > STATEDIFF_VFS_MAX_IOV_SEGS)
		segs = STATEDIFF_VFS_MAX_IOV_SEGS;

	/*
	 * Correct the start offset before splitting into segments. p->offset is
	 * *pos read at entry, but for an O_APPEND write that is the pre-write
	 * f_pos (typically 0), not where the bytes land -- the kernel only
	 * advances *pos to end-of-write during the write. Re-read it via the
	 * stashed pointer (a kretprobe has no args of its own) and back out the
	 * true start as end - written. For a normal write, end - written is just
	 * the entry offset, so this is always safe. Mirrors handle_vfs_write_ret.
	 */
	if (p->pos_ptr) {
		long long end_offset = 0;

		if (bpf_probe_read_kernel(&end_offset, sizeof(end_offset),
					  (void *)p->pos_ptr) == 0 &&
		    (unsigned long long)end_offset >= written)
			p->offset = (unsigned long long)end_offset - written;
	}

	emit.key = key;
	emit.emitted = 0;

	bpf_loop(segs, writev_emit_seg, &emit, 0);

	/*
	 * emitted is the total bytes captured across all segments, and a faithful
	 * full writev makes that equal the return value. Any mismatch is data we
	 * could not capture cleanly -- an oversized segment truncated above, a
	 * vlen past the loop cap, a short write (over-capture), or a mid-loop
	 * read/reserve failure -- so tally it as loss and let userspace refuse
	 * the partial batch.
	 */
	if (emit.emitted != written) {
		unsigned long long lost = emit.emitted > written ?
			emit.emitted - written : written - emit.emitted;

		count_error(SD_VFS_ERROR_WRITE_BYTES_DROPPED, lost);
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	}

	bpf_map_delete_elem(&pending_writev, &key);
	return 0;
}

/*
 * vfs_truncate() only covers the truncate(2) syscall. ftruncate(2) and
 * open(O_TRUNC) on an existing file reach do_truncate() without going through
 * vfs_truncate (and vfs_truncate itself calls do_truncate), so do_truncate is
 * the single chokepoint that captures all three. For an O_CREAT'd file the
 * kernel skips the truncate, so this never double-fires with the CREATE event.
 */
SEC("fentry/do_truncate")
int BPF_PROG(handle_do_truncate, struct mnt_idmap *idmap, struct dentry *dentry,
	     loff_t length, unsigned int time_attrs, struct file *filp)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;

	if (filtered_out() || !dentry)
		return 0;

	p = get_scratch(SD_VFS_OP_TRUNCATE);
	if (!p)
		return 0;
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!dentry_to_key(dentry, &p->object))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	p->size = length;
	if (bpf_map_update_elem(&pending_truncate, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

SEC("fexit/do_truncate")
int BPF_PROG(handle_do_truncate_ret, struct mnt_idmap *idmap,
	     struct dentry *dentry, loff_t length, unsigned int time_attrs,
	     struct file *filp, int ret)
{
	return emit_pending_event(&pending_truncate, ret);
}

/*
 * fallocate(fd, 0, offset, len) can extend i_size without writing any bytes or
 * passing through do_truncate(), so its final size is serialized as TRUNCATE.
 * ZERO_RANGE and PUNCH_HOLE|KEEP_SIZE both make their logical byte range read
 * as zero, so serialize those as ZERO_RANGE without reproducing physical extent
 * allocation.
 *
 * Pure KEEP_SIZE preallocation has no visible effect and is ignored. Other
 * modes can remove, insert, or shift data, so record those as fatal unsupported
 * operations rather than silently emitting an incorrect batch.
 */
SEC("fentry/vfs_fallocate")
int BPF_PROG(handle_vfs_fallocate, struct file *file, int mode, loff_t offset,
	     loff_t len)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct dentry *dentry;

	if (filtered_out() || !file || mode == FALLOC_FL_KEEP_SIZE)
		return 0;

	p = get_scratch(SD_VFS_OP_TRUNCATE);
	if (!p)
		return 0;
	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;
	if (!dentry_to_key(dentry, &p->object))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	p->mode = (unsigned int)mode;
	p->offset = offset;
	p->size = len;
	if (bpf_map_update_elem(&pending_fallocate, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

SEC("fexit/vfs_fallocate")
int BPF_PROG(handle_vfs_fallocate_ret, struct file *file, int mode,
	     loff_t offset, loff_t len, int ret)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct inode *inode;

	p = bpf_map_lookup_elem(&pending_fallocate, &key);
	if (!p)
		return 0;
	if (ret < 0) {
		bpf_map_delete_elem(&pending_fallocate, &key);
		return 0;
	}
	if (p->mode == FALLOC_FL_ZERO_RANGE ||
	    p->mode == (FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE) ||
	    p->mode == (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)) {
		p->op = SD_VFS_OP_ZERO_RANGE;
		p->flags = p->mode & FALLOC_FL_KEEP_SIZE ?
			SD_VFS_ZERO_RANGE_F_KEEP_SIZE : 0;
		p->mode = 0;
		return emit_pending_event(&pending_fallocate, ret);
	}
	if (p->mode != 0) {
		count_error(SD_VFS_ERROR_FALLOCATE_UNSUPPORTED, 1);
		bpf_map_delete_elem(&pending_fallocate, &key);
		return 0;
	}

	inode = BPF_CORE_READ(file, f_inode);
	if (!inode) {
		count_error(SD_VFS_ERROR_INTERNAL, 1);
		bpf_map_delete_elem(&pending_fallocate, &key);
		return 0;
	}
	p->mode = 0;
	p->offset = 0;
	p->size = BPF_CORE_READ(inode, i_size);
	return emit_pending_event(&pending_fallocate, ret);
}

// Directory-entry removals. Both stage through save_child_op.
SEC("fentry/vfs_unlink")
int BPF_PROG(handle_vfs_unlink, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry, struct inode **delegated_inode)
{
	return save_child_op(&pending_unlink, SD_VFS_OP_UNLINK, dir, dentry, 0);
}

SEC("fexit/vfs_unlink")
int BPF_PROG(handle_vfs_unlink_ret, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry, struct inode **delegated_inode, int ret)
{
	return emit_pending_event(&pending_unlink, ret);
}

SEC("fentry/vfs_rmdir")
int BPF_PROG(handle_vfs_rmdir, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry)
{
	return save_child_op(&pending_rmdir, SD_VFS_OP_RMDIR, dir, dentry, 0);
}

SEC("fexit/vfs_rmdir")
int BPF_PROG(handle_vfs_rmdir_ret, struct mnt_idmap *idmap, struct inode *dir,
	     struct dentry *dentry, int ret)
{
	struct statediff_vfs_pending *p;
	unsigned long long key = current_key();

	p = bpf_map_lookup_elem(&pending_rmdir, &key);
	/*
	 * The parent is already known to be tracked, so the directory itself
	 * should have been tracked by its own mkdir. A miss therefore means
	 * that mkdir was never observed and the subtree's contents were never
	 * captured, which is capture loss and is counted as such.
	 *
	 * The map is written only from here, fexit/vfs_mkdir, fexit/vfs_rename
	 * and the startup seed, so no benign miss remains: a directory moved
	 * into the tree from outside is failed closed separately, by the
	 * userspace RENAME handler.
	 */
	if (p && ret >= 0 &&
	    bpf_map_delete_elem(&tracked_dirs, &p->object) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return emit_pending_event(&pending_rmdir, ret);
}

// A rename is captured when either side is in scope. Moving a file out of the
// tree is a deletion and moving one in is a creation, so checking only the
// source would miss half of them.
SEC("fentry/vfs_rename")
int BPF_PROG(handle_vfs_rename, struct renamedata *rd)
{
	unsigned long long key = current_key();
	struct statediff_vfs_pending *p;
	struct dentry *old_dentry;
	struct dentry *new_dentry;
	int old_tracked;
	int new_tracked;

	if (filtered_out() || !rd)
		return 0;

	p = get_scratch(SD_VFS_OP_RENAME);
	if (!p)
		return 0;

	old_dentry = BPF_CORE_READ(rd, old_dentry);
	new_dentry = BPF_CORE_READ(rd, new_dentry);

	/*
	 * The parent directories are derived from the dentries rather than read
	 * out of renamedata. Its old_dir/new_dir inode fields were replaced by
	 * old_parent/new_parent dentries in later kernels, so naming either one
	 * directly only compiles against a single kernel generation. d_parent is
	 * present in both and yields the same inode.
	 */
	if (!dentry_parent_key(old_dentry, &p->parent))
		return 0;
	if (!dentry_parent_key(new_dentry, &p->new_parent))
		return 0;

	old_tracked = dir_is_tracked(&p->parent);
	if (!old_tracked) {
		new_tracked = dir_is_tracked(&p->new_parent);
		if (!new_tracked)
			return 0;
	}

	if (!dentry_to_key(old_dentry, &p->object))
		return 0;
	p->new_object = p->object;
	p->is_dir = dentry_is_dir(old_dentry);
	p->flags = BPF_CORE_READ(rd, flags);
	if (!read_dentry_name(old_dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}
	if (!read_dentry_name(new_dentry, p->new_name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	if (bpf_map_update_elem(&pending_rename, &key, p, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

SEC("fexit/vfs_rename")
int BPF_PROG(handle_vfs_rename_ret, struct renamedata *rd, int ret)
{
	struct statediff_vfs_pending *p;
	unsigned long long key = current_key();
	unsigned char tracked = 1;
	int old_tracked;
	int new_tracked;

	p = bpf_map_lookup_elem(&pending_rename, &key);
	if (!p)
		return 0;
	if (ret >= 0 && p->is_dir) {
		new_tracked = dir_is_tracked(&p->new_parent);
		if (new_tracked) {
			if (bpf_map_update_elem(&tracked_dirs, &p->object, &tracked,
						BPF_ANY) < 0)
				count_error(SD_VFS_ERROR_INTERNAL, 1);
		} else {
			old_tracked = dir_is_tracked(&p->parent);
			// A miss means the directory was never tracked; see
			// fexit/vfs_rmdir.
			if (old_tracked &&
			    bpf_map_delete_elem(&tracked_dirs, &p->object) < 0)
				count_error(SD_VFS_ERROR_INTERNAL, 1);
		}
	}
	return emit_pending_event(&pending_rename, ret);
}

/*
 * A writable shared mmap is the doorway for content that never passes through a
 * write syscall. The app stores straight into the mapped page-cache pages, and
 * no VFS hook ever fires. Record the file's inode in mmap_files (the writeback
 * hook keys off it) and teach userspace the inode->path via an MMAP event so it
 * can pread() those bytes at capture time. security_mmap_file() is the LSM
 * chokepoint every mmap(2) passes through, carrying the file plus prot/flags.
 */
SEC("fentry/security_mmap_file")
int BPF_PROG(handle_security_mmap_file, struct file *file, unsigned long prot,
	     unsigned long flags)
{
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event *e;
	struct dentry *dentry;
	struct inode *inode;
	unsigned long mtype = flags & MAP_TYPE;
	unsigned char tracked = 1;
	umode_t mode;

	if (!file || !(prot & PROT_WRITE))
		return 0;
	if (mtype != MAP_SHARED && mtype != MAP_SHARED_VALIDATE)
		return 0;
	if (filtered_out())
		return 0;

	p = get_scratch(SD_VFS_OP_MMAP);
	if (!p)
		return 0;

	dentry = BPF_CORE_READ(file, f_path.dentry);
	if (!dentry_parent_key(dentry, &p->parent))
		return 0;
	if (!dir_is_tracked(&p->parent))
		return 0;

	inode = BPF_CORE_READ(dentry, d_inode);
	if (!inode)
		return 0;
	mode = BPF_CORE_READ(inode, i_mode);
	if ((mode & S_IFMT) != S_IFREG)
		return 0;
	if (!inode_to_key(inode, &p->object))
		return 0;
	if (!read_dentry_name(dentry, p->name)) {
		count_error(SD_VFS_ERROR_PATH_READ, 1);
		return 0;
	}

	if (bpf_map_update_elem(&mmap_files, &p->object, &tracked, BPF_ANY) < 0) {
		count_error(SD_VFS_ERROR_INTERNAL, 1);
		return 0;
	}

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return 0;
	}
	fill_common(e, p, 0);
	bpf_ringbuf_submit(e, 0);
	return 0;
}

/*
 * Where a large folio records its extent moved between kernel versions, so it
 * is probed through CO-RE flavors instead of being named directly. Declaring
 * both shapes locally keeps the program compilable on either kernel, and
 * bpf_core_field_exists() resolves against the running one at load time.
 */
struct folio___nrpages {
	unsigned int _folio_nr_pages;
} __attribute__((preserve_access_index));

struct folio___flags1 {
	unsigned long _flags_1;
} __attribute__((preserve_access_index));

/*
 * folio_size() in BPF. PG_head marks a multi-page large folio, whose extent is
 * held either as a page count or as an order in the low byte of _flags_1. An
 * order-0 folio is a single PAGE_SIZE page.
 */
static __always_inline unsigned long long folio_size_bytes(struct folio *folio)
{
	struct folio___nrpages *by_count = (void *)folio;
	struct folio___flags1 *by_order = (void *)folio;
	unsigned long flags = BPF_CORE_READ(folio, flags);

	if (!(flags & (1UL << PG_head)))
		return 1ULL << PAGE_SHIFT;

	if (bpf_core_field_exists(by_count->_folio_nr_pages))
		return (unsigned long long)BPF_CORE_READ(by_count,
							_folio_nr_pages)
			<< PAGE_SHIFT;

	if (bpf_core_field_exists(by_order->_flags_1))
		return 1ULL << (PAGE_SHIFT +
				(BPF_CORE_READ(by_order, _flags_1) & 0xff));

	/*
	 * The extent of this folio cannot be determined on this kernel.
	 * Reporting a single page would silently truncate the snapshot to the
	 * first PAGE_SIZE bytes of a larger dirty range, so the batch is failed
	 * instead. Returning zero also suppresses the read in snapshot_range().
	 */
	count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

/*
 * The convergence point for content written by mmap stores (and io_uring/splice
 * buffered writes). Every dirty page-cache folio transitions through here on its
 * way to the block layer. We emit only a {inode, offset, length} notice -- never
 * the bytes -- and let userspace pread() the page cache, which already holds the
 * mmap-written content. Scope is mmap_files membership (by inode), NOT pid,
 * because userspace forces the flush via sync_file_range(). This runs in the
 * capturer's own context and a pid filter would drop exactly these events.
 */
SEC("fentry/__folio_start_writeback")
int BPF_PROG(handle_folio_start_writeback, struct folio *folio, bool keep_write)
{
	struct statediff_vfs_pending *p;
	struct statediff_vfs_event *e;
	struct address_space *mapping;
	struct inode *host;
	struct statediff_vfs_inode_key key = {};

	if (!folio)
		return 0;
	mapping = BPF_CORE_READ(folio, mapping);
	if (!mapping || ((unsigned long)mapping & PAGE_MAPPING_FLAGS))
		return 0;

	host = BPF_CORE_READ(mapping, host);
	if (!inode_to_key(host, &key))
		return 0;
	if (!bpf_map_lookup_elem(&mmap_files, &key))
		return 0;

	p = get_scratch(SD_VFS_OP_WRITEBACK);
	if (!p)
		return 0;
	p->object = key;
	p->offset = (unsigned long long)BPF_CORE_READ(folio, index) << PAGE_SHIFT;
	p->size = folio_size_bytes(folio);

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return 0;
	}
	fill_common(e, p, 0);
	bpf_ringbuf_submit(e, 0);
	return 0;
}

/*
 * ---------------------------------------------------------------------------
 * libaio write capture via io_submit_one (submit) and aio_complete_rw (done)
 * ---------------------------------------------------------------------------
 *
 * InnoDB (and any libaio writer) submits data-file page writes via
 * io_submit(IOCB_CMD_PWRITE), which runs io_submit_one -> __io_submit_one ->
 * aio_write -> call_write_iter -> <fs>_file_write_iter. That path never touches
 * vfs_write/vfs_writev, so without a hook here the in-place page overwrites are
 * invisible and replay keeps stale bytes (matching file size but wrong content
 * -- the subtle failure mode).
 *
 * The obvious hook, aio_write (the libaio write entry point), does not work. It
 * is static with a single caller and the compiler inlines it into
 * __io_submit_one.constprop.0, so its out-of-line symbol -- where an fexit
 * trampoline attaches -- is never executed. We hook io_submit_one instead. It is
 * filesystem-agnostic (above the VFS/fs split), libaio-only (io_uring uses a
 * different path), and a live out-of-line symbol. It costs us two things
 * aio_write gave for free, both resolved at completion.
 *   - No struct file* (only a raw fd). The path is resolved from kiocb->ki_filp
 *     in aio_complete_rw, where it is still valid.
 *   - No kiocb yet (req is allocated later). The cookie is the *user iocb
 *     pointer*, which __io_submit_one stores into aio_kiocb.ki_res.obj before
 *     issuing the op, so aio_complete_rw recovers it from the kiocb.
 *
 * Entry only registers the user-iocb cookie. Exit runs after the kernel has
 * imported the IOCB/iovec and faulted or pinned the write payload, so that is
 * where the non-faulting BPF reads and DIO_SUBMIT events happen. Completion may
 * run before exit for a synchronous write, so DIO_SUBMIT_DONE explicitly ends
 * staging and userspace waits for both DONE and COMPLETE in either order.
 */

/*
 * Stage one DIO segment by copying [base, base+len) from the submitter's buffer
 * into the per-CPU event_scratch and ship it as a DIO_SUBMIT event tagged with
 * the kiocb cookie and absolute file offset. Returns bytes emitted, 0 on any
 * failure (tallied as loss so userspace refuses a partial batch). Mirrors the
 * vfs_write staging path, including the > STATEDIFF_VFS_MAX_WRITE_DATA_LEN clamp.
 */
static __always_inline unsigned int dio_emit_one(struct statediff_vfs_pending *p,
						 unsigned long long cookie,
						 unsigned long long offset,
						 const void *base,
						 unsigned long long len)
{
	struct statediff_vfs_event_storage *storage;
	struct statediff_vfs_event *e;
	unsigned int chunk;
	int truncated;

	if (len == 0)
		return 0;
	truncated = len > STATEDIFF_VFS_MAX_WRITE_DATA_LEN;
	chunk = truncated ? STATEDIFF_VFS_MAX_WRITE_DATA_LEN : (unsigned int)len;
	if (!chunk || chunk > STATEDIFF_VFS_MAX_WRITE_DATA_LEN)
		return 0;

	storage = get_event_scratch();
	if (!storage) {
		count_error(SD_VFS_ERROR_INTERNAL, 1);
		return 0;
	}
	e = (struct statediff_vfs_event *)storage;

	if (read_user_chunk(storage, chunk, base) < 0) {
		count_error(SD_VFS_ERROR_DIO_PAYLOAD_READ, 1);
		return 0;
	}

	fill_common(e, p, chunk);
	e->cookie = cookie;
	e->offset = offset;
	e->size = chunk;
	e->data_len = chunk;
	if (truncated)
		e->flags |= SD_VFS_EVENT_F_TRUNCATED;

	if (bpf_ringbuf_output(&rb, storage,
				       STATEDIFF_VFS_EVENT_HEADER_LEN + chunk, 0) < 0) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return 0;
	}
	return chunk;
}

/*
 * bpf_loop() body for a vectored libaio write (IOCB_CMD_PWRITEV). It emits one
 * DIO_SUBMIT per user iovec segment, each at its own contiguous file offset.
 * Like writev_emit_seg, c->emitted only ever feeds a stored file offset (never
 * a copy length or a branch), so it stays verifier-"safety-irrelevant" and the
 * loop body verifies once instead of being simulated 4 MiB at a time.
 */
struct dio_seg_ctx {
	struct statediff_vfs_pending *p;
	const struct iovec *iov;
	unsigned long long cookie;
	unsigned long long pos;
	unsigned long long emitted;
	unsigned int failed;
};

static long dio_emit_seg(unsigned int i, void *data)
{
	struct dio_seg_ctx *c = data;
	struct iovec seg;
	unsigned long long seg_len;
	unsigned int got;

	if (bpf_probe_read_user(&seg, sizeof(seg), &c->iov[i]) < 0) {
		count_error(SD_VFS_ERROR_DIO_IOVEC_READ, 1);
		c->failed = 1;
		return 1;
	}
	seg_len = (unsigned long long)seg.iov_len;
	if (seg_len == 0)
		return 0;
	got = dio_emit_one(c->p, c->cookie, c->pos + c->emitted,
			   (const void *)seg.iov_base, seg_len);
	if (!got || (unsigned long long)got != seg_len) {
		c->failed = 1;
		return 1;
	}
	c->emitted += got;
	return 0;
}

// Emit a payload-free state transition for one userspace cookie.
static __always_inline int dio_emit_control(unsigned int op,
					    unsigned long long cookie,
					    long long ret,
					    unsigned long long size)
{
	struct statediff_vfs_event *e;

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return -1;
	}
	__builtin_memset(e, 0, STATEDIFF_VFS_EVENT_HEADER_LEN);
	e->op = op;
	e->ret = ret;
	e->cookie = cookie;
	e->size = size;
	bpf_ringbuf_submit(e, 0);
	return 0;
}

/*
 * Emit a DIO_COMPLETE for a registered write, resolving the target path from the
 * kiocb's file at completion time. ki_filp is still valid here (fput runs after
 * ki_complete), so we turn it into a (parent inode key, dentry name) pair -- the
 * same currency every other op speaks -- and userspace resolves it to a tree-
 * relative path. An untracked parent leaves the name empty, telling userspace to
 * drop the staged bytes. The event is built directly in the ring buffer, NOT via
 * the shared per-CPU `scratch` map, because this hook can run in softirq (async
 * O_DIRECT) and must not race a task-context program mid-scratch. Only kernel
 * reads and a hash lookup happen here, all softirq-safe.
 */
static __always_inline void dio_emit_complete(struct kiocb *kiocb,
					      unsigned long long cookie,
					      long long res)
{
	struct statediff_vfs_inode_key parent = {};
	struct statediff_vfs_event *e;
	struct file *file;
	struct dentry *dentry;
	char name[STATEDIFF_VFS_NAME_LEN] = {};
	int have_name = 0;

	file = BPF_CORE_READ(kiocb, ki_filp);
	dentry = file ? BPF_CORE_READ(file, f_path.dentry) : 0;
	if (dentry && dentry_parent_key(dentry, &parent) &&
	    dir_is_tracked(&parent)) {
		have_name = read_dentry_name(dentry, name);
		if (!have_name)
			count_error(SD_VFS_ERROR_PATH_READ, 1);
	}

	e = reserve_empty_event();
	if (!e) {
		count_error(SD_VFS_ERROR_RINGBUF_DROP, 1);
		return;
	}
	__builtin_memset(e, 0, STATEDIFF_VFS_EVENT_HEADER_LEN);
	e->op = SD_VFS_OP_DIO_COMPLETE;
	e->ret = res;
	e->cookie = cookie;
	if (have_name) {
		e->parent = parent;
		__builtin_memcpy(e->name, name, sizeof(name));
	}
	bpf_ringbuf_submit(e, 0);
}

/*
 * Entry side. Register the user-iocb pointer before the kernel touches its user
 * memory. We intentionally do not inspect the IOCB here, because
 * bpf_probe_read_user() cannot fault and this is the point at which its page may
 * still be absent. aio_complete_rw filters candidates with IOCB_WRITE from the
 * kernel kiocb, while fexit filters them from the now-faulted user IOCB. compat (32-bit) IOCBs have
 * a different layout and are skipped.
 */
SEC("fentry/io_submit_one")
int BPF_PROG(handle_io_submit_one, struct kioctx *ioctx, struct iocb *user_iocb,
	     bool compat)
{
	unsigned char one = 1;
	unsigned long long cookie;

	if (filtered_out() || !user_iocb || compat)
		return 0;

	cookie = (unsigned long long)user_iocb;
	if (bpf_map_update_elem(&dio_inflight, &cookie, &one, BPF_ANY) < 0)
		count_error(SD_VFS_ERROR_INTERNAL, 1);
	return 0;
}

/*
 * Exit side. A successful native-AIO write has already passed the kernel's
 * copy/import and, for ext4 iomap direct I/O, GUP. Read and emit its payload now.
 * A synchronous completion may already have removed dio_inflight, so staging
 * must not depend on map membership. DIO_SUBMIT_DONE is emitted only after
 * every segment was captured. Any IOCB, iovec, or payload failure emits
 * DIO_ABORT, removes a still-pending completion gate, and makes userspace reject
 * the batch.
 */
SEC("fexit/io_submit_one")
int BPF_PROG(handle_io_submit_one_exit, struct kioctx *ioctx,
	     struct iocb *user_iocb, bool compat, int ret)
{
	struct statediff_vfs_pending *p;
	struct iocb ib;
	unsigned long long cookie = (unsigned long long)user_iocb;
	unsigned long long emitted = 0;

	if (filtered_out() || !user_iocb || compat)
		return 0;
	if (ret < 0) {
		bpf_map_delete_elem(&dio_inflight, &cookie);
		return 0;
	}

	if (bpf_probe_read_user(&ib, sizeof(ib), user_iocb) < 0) {
		count_error(SD_VFS_ERROR_DIO_IOCB_READ, 1);
		goto abort;
	}
	if (ib.aio_lio_opcode != IOCB_CMD_PWRITE &&
	    ib.aio_lio_opcode != IOCB_CMD_PWRITEV) {
		bpf_map_delete_elem(&dio_inflight, &cookie);
		return 0;
	}

	p = get_scratch(SD_VFS_OP_DIO_SUBMIT);
	if (!p)
		goto abort;
	p->offset = ib.aio_offset;

	if (ib.aio_lio_opcode == IOCB_CMD_PWRITE) {
		unsigned int got = 0;

		if (ib.aio_nbytes)
			got = dio_emit_one(p, cookie, ib.aio_offset,
					   (const void *)ib.aio_buf,
					   ib.aio_nbytes);
		if ((unsigned long long)got != ib.aio_nbytes)
			goto abort;
		emitted = got;
	} else {
		struct dio_seg_ctx c = {};
		unsigned int segs;
		long loop_ret;

		if (ib.aio_nbytes > STATEDIFF_VFS_MAX_IOV_SEGS) {
			count_error(SD_VFS_ERROR_INTERNAL, 1);
			goto abort;
		}
		segs = (unsigned int)ib.aio_nbytes;
		c.p = p;
		c.iov = (const struct iovec *)ib.aio_buf;
		c.cookie = cookie;
		c.pos = ib.aio_offset;
		loop_ret = bpf_loop(segs, dio_emit_seg, &c, 0);
		if (loop_ret < 0 || c.failed) {
			if (loop_ret < 0)
				count_error(SD_VFS_ERROR_INTERNAL, 1);
			goto abort;
		}
		emitted = c.emitted;
	}

	if (dio_emit_control(SD_VFS_OP_DIO_SUBMIT_DONE, cookie, 0,
			     emitted) < 0)
		bpf_map_delete_elem(&dio_inflight, &cookie);
	return 0;

abort:
	bpf_map_delete_elem(&dio_inflight, &cookie);
	dio_emit_control(SD_VFS_OP_DIO_ABORT, cookie, -1, emitted);
	return 0;
}

/*
 * Completion side. aio_prep_rw() installs aio_complete_rw as ki_complete for
 * native-AIO reads and writes. fentry registered both because it could not
 * safely inspect user memory, so use the kernel-owned IOCB_WRITE flag to discard
 * reads. A buffered write may complete synchronously before io_submit_one exits,
 * while an O_DIRECT write normally completes later from softirq. In either case the
 * cookie comes from aio_kiocb.ki_res.obj and userspace pairs the events.
 */
SEC("fentry/aio_complete_rw")
int BPF_PROG(handle_aio_complete_rw, struct kiocb *kiocb, long res)
{
	unsigned long long cookie;

	if (!kiocb)
		return 0;
	cookie = BPF_CORE_READ((struct aio_kiocb *)kiocb, ki_res.obj);
	if (!bpf_map_lookup_elem(&dio_inflight, &cookie))
		return 0;
	if (!(BPF_CORE_READ(kiocb, ki_flags) & IOCB_WRITE)) {
		bpf_map_delete_elem(&dio_inflight, &cookie);
		return 0;
	}
	bpf_map_delete_elem(&dio_inflight, &cookie);

	if (write_target_unlinked(BPF_CORE_READ(kiocb, ki_filp)))
		return 0;

	dio_emit_complete(kiocb, cookie, res);
	return 0;
}
