// fuselog_internal.h - pure compute helpers extracted from fuselogv2.cpp.
// Anything that touches FUSE, libc syscalls, or global state stays in
// fuselogv2.cpp; only deterministic input -> output transformations live
// here so they can be unit tested without mounting a filesystem.
#ifndef FUSELOG_INTERNAL_H
#define FUSELOG_INTERNAL_H

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define FUSELOG_HAVE_AVX2 1
#endif

// Per-action serialization overhead on the wire:
//   1 (type) + 8 (fid) + 8 (count) + 8 (offset) = 25 bytes.
// When merging adjacent chunks, bridging a small gap of unchanged bytes is
// cheaper than emitting a second action whose header would cost this much.
static const uint32_t COALESCE_ACTION_OVERHEAD = 25;

// Bump-allocator arena: one big heap allocation that backs many chunks.
// Per compute_diff call we allocate one arena (sized at 2 × write_size for
// safety against the merge expansion); each chunk takes a slice with no
// further allocations. shared_ptr<chunk_arena> in the chunk and downstream
// statediff_action keeps the arena alive until the last referencing
// action is destroyed.
//
// Why shared_ptr: ownership is shared across N chunks per call and then
// across N actions through the harvest pipeline. shared_ptr's move is
// refcount-free; copy is a single atomic inc — much cheaper than the
// per-chunk malloc this replaces.
struct chunk_arena {
  std::unique_ptr<unsigned char[]> buf;
  size_t                           cap;
  size_t                           pos = 0;

  explicit chunk_arena(size_t c) : buf(new unsigned char[c]), cap(c) {}

  // Bump-allocate `n` bytes. Returns nullptr if the arena is full so the
  // caller can fall back to a per-chunk allocation (rare in practice).
  unsigned char* alloc(size_t n) {
    if (pos + n > cap) return nullptr;
    unsigned char* p = buf.get() + pos;
    pos += n;
    return p;
  }
};

struct statediff_write_unit {
  uint64_t                     offset;
  std::shared_ptr<chunk_arena> arena;          // shared owner; may be null
  size_t                       arena_offset = 0;
  size_t                       size = 0;

  statediff_write_unit() = default;
  // shared_ptr is movable and copyable; default move/copy are correct here
  // (move transfers ownership without an atomic op).

  // Pointer to this chunk's bytes inside the arena.
  unsigned char* data() {
    return arena ? arena->buf.get() + arena_offset : nullptr;
  }
  const unsigned char* data() const {
    return arena ? arena->buf.get() + arena_offset : nullptr;
  }

  // operator[] preserves the previous unique_ptr<unsigned char[]> interface
  // so existing tests keep working unchanged.
  unsigned char& operator[](size_t i) { return data()[i]; }
  unsigned char  operator[](size_t i) const { return data()[i]; }

  // Test-mode standalone assign: allocates a fresh, single-slot arena.
  // For production paths compute_diff calls `place_in_arena` directly.
  void assign(const unsigned char* src, size_t n) {
    if (n == 0) { arena.reset(); arena_offset = 0; size = 0; return; }
    arena = std::make_shared<chunk_arena>(n);
    arena_offset = 0;
    arena->pos = n;
    std::memcpy(arena->buf.get(), src, n);
    size = n;
  }
  void assign(std::initializer_list<unsigned char> il) {
    assign(il.begin(), il.size());
  }

  // Place `n` bytes from `src` into an existing arena. Falls back to a
  // private one-slot arena if the arena is full.
  void place_in_arena(const std::shared_ptr<chunk_arena>& a,
                       const unsigned char* src, size_t n) {
    if (n == 0) { arena.reset(); arena_offset = 0; size = 0; return; }
    unsigned char* slot = a->alloc(n);
    if (slot != nullptr) {
      arena = a;
      arena_offset = static_cast<size_t>(slot - a->buf.get());
      size = n;
      std::memcpy(slot, src, n);
    } else {
      assign(src, n);  // arena full; standalone
    }
  }

  bool empty() const { return size == 0; }

  bool operator==(const statediff_write_unit& o) const {
    return offset == o.offset && size == o.size &&
           (size == 0 || std::memcmp(data(), o.data(), size) == 0);
  }
  bool operator!=(const statediff_write_unit& o) const { return !(*this == o); }
};

// =============================================================================
// SIMD primitives for byte-level scanning.
//
// find_first_diff(a, b, n)  -> first i in [0, n) with a[i] != b[i], else n.
// find_first_match(a, b, n) -> first i in [0, n) with a[i] == b[i], else n.
//
// Both have an AVX2 fast path (32 bytes per iteration) and a scalar tail.
// AVX2 is runtime-detected and cached in a static — the branch is highly
// predictable so per-call overhead is negligible.
// =============================================================================

inline size_t find_first_diff_scalar(const unsigned char* a,
                                      const unsigned char* b, size_t n) {
  for (size_t i = 0; i < n; i++) {
    if (a[i] != b[i]) return i;
  }
  return n;
}

inline size_t find_first_match_scalar(const unsigned char* a,
                                       const unsigned char* b, size_t n) {
  for (size_t i = 0; i < n; i++) {
    if (a[i] == b[i]) return i;
  }
  return n;
}

#ifdef FUSELOG_HAVE_AVX2
__attribute__((target("avx2")))
inline size_t find_first_diff_avx2(const unsigned char* a,
                                    const unsigned char* b, size_t n) {
  size_t i = 0;
  for (; i + 32 <= n; i += 32) {
    __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
    __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
    __m256i cmp = _mm256_cmpeq_epi8(va, vb);
    uint32_t mask = (uint32_t) _mm256_movemask_epi8(cmp);
    if (mask != 0xFFFFFFFFu) {
      return i + __builtin_ctz(~mask);
    }
  }
  return i + find_first_diff_scalar(a + i, b + i, n - i);
}

__attribute__((target("avx2")))
inline size_t find_first_match_avx2(const unsigned char* a,
                                     const unsigned char* b, size_t n) {
  size_t i = 0;
  for (; i + 32 <= n; i += 32) {
    __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
    __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
    __m256i cmp = _mm256_cmpeq_epi8(va, vb);
    uint32_t mask = (uint32_t) _mm256_movemask_epi8(cmp);
    if (mask != 0) {
      return i + __builtin_ctz(mask);
    }
  }
  return i + find_first_match_scalar(a + i, b + i, n - i);
}

// AVX-512 variants — 64 bytes per iteration. `_mm512_cmpeq_epi8_mask`
// returns a 64-bit mask directly (no movemask step needed), so the inner
// loop is slightly tighter than AVX2 in addition to processing 2× the
// bytes per iteration. Requires AVX-512F + AVX-512BW.
__attribute__((target("avx512f,avx512bw")))
inline size_t find_first_diff_avx512(const unsigned char* a,
                                      const unsigned char* b, size_t n) {
  size_t i = 0;
  for (; i + 64 <= n; i += 64) {
    __m512i va = _mm512_loadu_si512((const void*)(a + i));
    __m512i vb = _mm512_loadu_si512((const void*)(b + i));
    __mmask64 eq = _mm512_cmpeq_epi8_mask(va, vb);
    if (eq != ~(__mmask64) 0) {
      return i + __builtin_ctzll(~eq);
    }
  }
  // Tail: fall through to AVX2 if there's enough room, else scalar.
  if (i + 32 <= n) {
    size_t off = find_first_diff_avx2(a + i, b + i, n - i);
    return i + off;
  }
  return i + find_first_diff_scalar(a + i, b + i, n - i);
}

__attribute__((target("avx512f,avx512bw")))
inline size_t find_first_match_avx512(const unsigned char* a,
                                       const unsigned char* b, size_t n) {
  size_t i = 0;
  for (; i + 64 <= n; i += 64) {
    __m512i va = _mm512_loadu_si512((const void*)(a + i));
    __m512i vb = _mm512_loadu_si512((const void*)(b + i));
    __mmask64 eq = _mm512_cmpeq_epi8_mask(va, vb);
    if (eq != 0) {
      return i + __builtin_ctzll(eq);
    }
  }
  if (i + 32 <= n) {
    size_t off = find_first_match_avx2(a + i, b + i, n - i);
    return i + off;
  }
  return i + find_first_match_scalar(a + i, b + i, n - i);
}
#endif  // FUSELOG_HAVE_AVX2

inline bool fuselog_has_avx2() {
#ifdef FUSELOG_HAVE_AVX2
  static const bool support = ([]() -> bool {
    // Env-var override lets us A/B the SIMD path against the scalar
    // baseline on the same binary; set FUSELOG_DISABLE_SIMD=1 in the
    // fuselog process's environment to force the scalar path.
    if (const char* v = std::getenv("FUSELOG_DISABLE_SIMD")) {
      if (*v != '\0' && *v != '0') return false;
    }
    return __builtin_cpu_supports("avx2");
  })();
  return support;
#else
  return false;
#endif
}

inline bool fuselog_has_avx512() {
#ifdef FUSELOG_HAVE_AVX2
  static const bool support = ([]() -> bool {
    // FUSELOG_DISABLE_SIMD disables everything; FUSELOG_DISABLE_AVX512
    // forces the dispatch back to AVX2 (or scalar if also disabled).
    if (const char* v = std::getenv("FUSELOG_DISABLE_SIMD")) {
      if (*v != '\0' && *v != '0') return false;
    }
    if (const char* v = std::getenv("FUSELOG_DISABLE_AVX512")) {
      if (*v != '\0' && *v != '0') return false;
    }
    return __builtin_cpu_supports("avx512f") &&
           __builtin_cpu_supports("avx512bw");
  })();
  return support;
#else
  return false;
#endif
}

inline const char* fuselog_simd_name() {
#ifdef FUSELOG_HAVE_AVX2
  if (fuselog_has_avx512()) return "avx512";
  if (fuselog_has_avx2()) return "avx2";
#endif
  return "scalar";
}

inline size_t find_first_diff(const unsigned char* a,
                               const unsigned char* b, size_t n) {
#ifdef FUSELOG_HAVE_AVX2
  if (fuselog_has_avx512()) return find_first_diff_avx512(a, b, n);
  if (fuselog_has_avx2()) return find_first_diff_avx2(a, b, n);
#endif
  return find_first_diff_scalar(a, b, n);
}

inline size_t find_first_match(const unsigned char* a,
                                const unsigned char* b, size_t n) {
#ifdef FUSELOG_HAVE_AVX2
  if (fuselog_has_avx512()) return find_first_match_avx512(a, b, n);
  if (fuselog_has_avx2()) return find_first_match_avx2(a, b, n);
#endif
  return find_first_match_scalar(a, b, n);
}

// =============================================================================
// compute_diff: scalar and SIMD implementations + dispatched entry point.
//
// Both implementations emit one statediff_write_unit per contiguous differing
// run with offsets in absolute file coordinates (base = `offset` arg). If
// write_size > rd_size, the trailing bytes of new_buf form an additional chunk
// (folded into the prior chunk if it abuts rd_size).
//
// `min_width_coalesce` keeps extending the current diff chunk through equal
// bytes until at least this many bytes have been captured (0 disables). The
// SIMD path only handles min_width_coalesce == 0; non-zero falls back to
// scalar.
// =============================================================================

// Shared helper for the tail-extension path: if write_size > rd_size,
// the trailing new bytes form an extra chunk (or get folded into a
// terminal diff chunk). Identical between scalar and SIMD.
inline void compute_diff_tail(
    std::vector<statediff_write_unit>& chunks,
    const std::shared_ptr<chunk_arena>& arena,
    const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset) {
  if (rd_size >= write_size) return;

  // Possibly fold the tail into the prior chunk if it abuts. NOTE: the
  // check compares last.offset+size against rd_size (read-local), not
  // offset+rd_size (absolute), so when offset > 0 the fold doesn't
  // fire. This is a missed-merge optimization quirk, not a correctness
  // issue. See test_fuselog.cpp `TailFoldOnlyAtZeroOffset`.
  size_t tail_start = rd_size;
  if (!chunks.empty()) {
    statediff_write_unit& last = chunks.back();
    if (last.offset + last.size == rd_size) {
      tail_start = last.offset;
      // Grow last to absorb the tail. Place merged content in the arena.
      size_t new_size = last.size + (write_size - rd_size);
      statediff_write_unit grown;
      grown.offset = last.offset;
      // Allocate the new slot first, then copy from `last` and the tail.
      unsigned char* slot = arena ? arena->alloc(new_size) : nullptr;
      if (slot != nullptr) {
        std::memcpy(slot, last.data(), last.size);
        std::memcpy(slot + last.size, new_buf + rd_size, write_size - rd_size);
        grown.arena = arena;
        grown.arena_offset = static_cast<size_t>(slot - arena->buf.get());
        grown.size = new_size;
      } else {
        // Arena spilled — fall back to standalone allocation.
        std::unique_ptr<unsigned char[]> nb(new unsigned char[new_size]);
        std::memcpy(nb.get(), last.data(), last.size);
        std::memcpy(nb.get() + last.size, new_buf + rd_size,
                    write_size - rd_size);
        grown.assign(nb.get(), new_size);
      }
      chunks.back() = std::move(grown);
      return;
    }
  }
  // Fresh tail chunk.
  statediff_write_unit cur;
  cur.offset = offset + tail_start;
  if (arena) cur.place_in_arena(arena, new_buf + tail_start,
                                  write_size - tail_start);
  else       cur.assign(new_buf + tail_start, write_size - tail_start);
  chunks.push_back(std::move(cur));
}

// Internal compute_diff using parameterized primitives. Both scalar
// and SIMD versions share this body; only the find_first_* dispatch
// differs.
template <typename FindDiff, typename FindMatch>
inline std::vector<statediff_write_unit> compute_diff_impl(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    FindDiff find_diff, FindMatch find_match) {
  // One arena per compute_diff call backs all chunks + any merge expansion
  // downstream. Upper bound: ~2x write_size (raw chunks ≤ write_size + tail
  // extension; merge can add gap bytes ≤ 24 each but is bounded above by
  // the original total). 0 bytes input → no arena needed.
  std::vector<statediff_write_unit> chunks;
  size_t arena_cap = (write_size > rd_size ? write_size : rd_size) * 2 + 64;
  std::shared_ptr<chunk_arena> arena = (arena_cap > 0)
      ? std::make_shared<chunk_arena>(arena_cap)
      : nullptr;

  size_t i = 0;
  while (i < rd_size) {
    size_t diff_off = find_diff(old_buf + i, new_buf + i, rd_size - i);
    if (diff_off >= rd_size - i) {
      i = rd_size;
      break;
    }
    size_t diff_start = i + diff_off;
    size_t match_off = find_match(old_buf + diff_start,
                                   new_buf + diff_start,
                                   rd_size - diff_start);
    size_t diff_end = diff_start + match_off;  // == rd_size if no match

    statediff_write_unit cur;
    cur.offset = offset + diff_start;
    cur.place_in_arena(arena, new_buf + diff_start, diff_end - diff_start);
    chunks.push_back(std::move(cur));

    i = diff_end;
  }
  compute_diff_tail(chunks, arena, new_buf, rd_size, write_size, offset);
  return chunks;
}

// Push-per-byte legacy scalar (with min_width_coalesce support). Only
// reachable when min_width_coalesce != 0, which the current production
// configuration never sets. Kept for completeness; not arena-optimized.
inline std::vector<statediff_write_unit> compute_diff_pushback(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  std::vector<statediff_write_unit> chunks;
  std::vector<unsigned char> tmp;

  auto flush = [&](uint64_t off) {
    statediff_write_unit cur;
    cur.offset = off;
    cur.assign(tmp.data(), tmp.size());
    chunks.push_back(std::move(cur));
    tmp.clear();
  };

  size_t i = 0;
  while (i < rd_size) {
    if (old_buf[i] == new_buf[i]) { i++; continue; }
    uint64_t cur_off = offset + i;
    while (i < rd_size &&
           (old_buf[i] != new_buf[i] || tmp.size() < min_width_coalesce)) {
      tmp.push_back(new_buf[i]);
      i++;
    }
    flush(cur_off);
  }
  if (i < write_size) {
    uint64_t cur_off = offset + i;
    if (!chunks.empty()) {
      statediff_write_unit& last = chunks.back();
      if (last.offset + last.size == rd_size) {
        cur_off = last.offset;
        tmp.assign(last.data(), last.data() + last.size);
        chunks.pop_back();
      }
    }
    while (i < write_size) { tmp.push_back(new_buf[i]); i++; }
    flush(cur_off);
  }
  return chunks;
}

inline std::vector<statediff_write_unit> compute_diff_scalar(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  if (min_width_coalesce != 0) {
    return compute_diff_pushback(old_buf, new_buf, rd_size, write_size,
                                  offset, min_width_coalesce);
  }
  return compute_diff_impl(
      old_buf, new_buf, rd_size, write_size, offset,
      find_first_diff_scalar, find_first_match_scalar);
}

inline std::vector<statediff_write_unit> compute_diff_simd(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  if (min_width_coalesce != 0) {
    return compute_diff_pushback(old_buf, new_buf, rd_size, write_size,
                                  offset, min_width_coalesce);
  }
#ifdef FUSELOG_HAVE_AVX2
  return compute_diff_impl(
      old_buf, new_buf, rd_size, write_size, offset,
      find_first_diff_avx2, find_first_match_avx2);
#else
  return compute_diff_scalar(old_buf, new_buf, rd_size, write_size,
                              offset, min_width_coalesce);
#endif
}

inline std::vector<statediff_write_unit> compute_diff_avx512(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  if (min_width_coalesce != 0) {
    return compute_diff_pushback(old_buf, new_buf, rd_size, write_size,
                                  offset, min_width_coalesce);
  }
#ifdef FUSELOG_HAVE_AVX2
  return compute_diff_impl(
      old_buf, new_buf, rd_size, write_size, offset,
      find_first_diff_avx512, find_first_match_avx512);
#else
  return compute_diff_scalar(old_buf, new_buf, rd_size, write_size,
                              offset, min_width_coalesce);
#endif
}

// Dispatched entry point. Production code calls this; the SIMD path is
// used when the host CPU supports AVX2.
inline std::vector<statediff_write_unit> compute_diff(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  if (min_width_coalesce == 0) {
    if (fuselog_has_avx512()) {
      return compute_diff_avx512(old_buf, new_buf, rd_size, write_size,
                                  offset, min_width_coalesce);
    }
    if (fuselog_has_avx2()) {
      return compute_diff_simd(old_buf, new_buf, rd_size, write_size,
                               offset, min_width_coalesce);
    }
  }
  return compute_diff_scalar(old_buf, new_buf, rd_size, write_size,
                             offset, min_width_coalesce);
}

// merge_adjacent_chunks fuses neighbouring chunks whose gap of unchanged
// bytes is strictly less than `overhead`. Bridging bytes are pulled from
// `new_buf` using `base_offset` as the origin. The merged buffer is
// placed in the same arena as the input chunks when possible (one
// shared_ptr copy, no malloc); falls back to a standalone allocation
// only if the arena is full.
inline std::vector<statediff_write_unit> merge_adjacent_chunks(
    std::vector<statediff_write_unit> chunks,
    const unsigned char* new_buf, uint64_t base_offset,
    uint32_t overhead) {
  if (chunks.empty()) return chunks;

  std::vector<statediff_write_unit> merged;
  merged.push_back(std::move(chunks[0]));
  for (size_t ci = 1; ci < chunks.size(); ci++) {
    statediff_write_unit& prev = merged.back();
    statediff_write_unit& cur  = chunks[ci];
    uint64_t prev_end = prev.offset + prev.size;
    uint64_t gap = (cur.offset > prev_end) ? (cur.offset - prev_end) : 0;
    if (gap < overhead) {
      size_t new_size = prev.size + gap + cur.size;
      // Prefer the arena both chunks already share.
      std::shared_ptr<chunk_arena> a = prev.arena ? prev.arena : cur.arena;
      unsigned char* slot = a ? a->alloc(new_size) : nullptr;
      if (slot != nullptr) {
        std::memcpy(slot, prev.data(), prev.size);
        if (gap > 0) {
          std::memcpy(slot + prev.size,
                      new_buf + (prev_end - base_offset),
                      gap);
        }
        if (cur.size > 0) {
          std::memcpy(slot + prev.size + gap, cur.data(), cur.size);
        }
        prev.arena = a;
        prev.arena_offset = static_cast<size_t>(slot - a->buf.get());
        prev.size = new_size;
      } else {
        // Arena spilled: standalone allocation for the merged chunk.
        statediff_write_unit grown;
        grown.offset = prev.offset;
        grown.arena = std::make_shared<chunk_arena>(new_size);
        grown.arena->pos = new_size;
        grown.arena_offset = 0;
        grown.size = new_size;
        unsigned char* dst = grown.arena->buf.get();
        std::memcpy(dst, prev.data(), prev.size);
        if (gap > 0) {
          std::memcpy(dst + prev.size,
                      new_buf + (prev_end - base_offset),
                      gap);
        }
        if (cur.size > 0) {
          std::memcpy(dst + prev.size + gap, cur.data(), cur.size);
        }
        merged.back() = std::move(grown);
      }
    } else {
      merged.push_back(std::move(cur));
    }
  }
  return merged;
}

#endif  // FUSELOG_INTERNAL_H
