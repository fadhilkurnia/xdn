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

// Flat owning buffer: one heap allocation per chunk, no capacity metadata,
// no per-byte push machinery. The previous design used std::vector which
// forces push_back-per-byte in the compute_diff inner loops (slow at high
// diff density) and carries 24 B of bookkeeping per chunk. This shape is
// 16 B and the inner loops can bulk-memcpy once the diff-run length is
// known.
struct statediff_write_unit {
  uint64_t                          offset;
  std::unique_ptr<unsigned char[]>  buffer;
  size_t                            size = 0;

  statediff_write_unit() = default;
  statediff_write_unit(statediff_write_unit&&) noexcept = default;
  statediff_write_unit& operator=(statediff_write_unit&&) noexcept = default;
  // Copy is intentionally disabled — chunks are always moved.
  statediff_write_unit(const statediff_write_unit&) = delete;
  statediff_write_unit& operator=(const statediff_write_unit&) = delete;

  // Allocate `n` bytes and copy from `src`. Replaces any existing buffer.
  void assign(const unsigned char* src, size_t n) {
    if (n == 0) { buffer.reset(); size = 0; return; }
    buffer.reset(new unsigned char[n]);
    std::memcpy(buffer.get(), src, n);
    size = n;
  }
  // Ergonomic overload for tests: `unit.assign({'A','B','C'})`.
  void assign(std::initializer_list<unsigned char> il) {
    assign(il.begin(), il.size());
  }
  bool empty() const { return size == 0; }

  // Equality on content (used by tests).
  bool operator==(const statediff_write_unit& o) const {
    return offset == o.offset && size == o.size &&
           (size == 0 || std::memcmp(buffer.get(), o.buffer.get(), size) == 0);
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

inline size_t find_first_diff(const unsigned char* a,
                               const unsigned char* b, size_t n) {
#ifdef FUSELOG_HAVE_AVX2
  if (fuselog_has_avx2()) return find_first_diff_avx2(a, b, n);
#endif
  return find_first_diff_scalar(a, b, n);
}

inline size_t find_first_match(const unsigned char* a,
                                const unsigned char* b, size_t n) {
#ifdef FUSELOG_HAVE_AVX2
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
      // Grow last to absorb the tail in one allocation.
      size_t new_size = last.size + (write_size - rd_size);
      std::unique_ptr<unsigned char[]> nb(new unsigned char[new_size]);
      std::memcpy(nb.get(), last.buffer.get(), last.size);
      std::memcpy(nb.get() + last.size, new_buf + rd_size,
                  write_size - rd_size);
      last.buffer = std::move(nb);
      last.size = new_size;
      return;
    }
  }
  // Fresh tail chunk.
  statediff_write_unit cur;
  cur.offset = offset + tail_start;
  cur.assign(new_buf + tail_start, write_size - tail_start);
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
  std::vector<statediff_write_unit> chunks;
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
    cur.assign(new_buf + diff_start, diff_end - diff_start);
    chunks.push_back(std::move(cur));

    i = diff_end;  // next iteration skips the equal byte (or exits)
  }
  compute_diff_tail(chunks, new_buf, rd_size, write_size, offset);
  return chunks;
}

// Push-per-byte legacy scalar (with min_width_coalesce support). Only
// reachable when min_width_coalesce != 0, which the current production
// configuration never sets. Kept for completeness and for the min-width
// path of compute_diff_scalar.
inline std::vector<statediff_write_unit> compute_diff_pushback(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  std::vector<statediff_write_unit> chunks;
  std::vector<unsigned char> tmp;  // growable scratch; converted at flush.

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
  // Tail extension (mirrors compute_diff_tail logic, but using tmp+vector
  // because min_width_coalesce paths are rare and not perf-critical).
  if (i < write_size) {
    uint64_t cur_off = offset + i;
    if (!chunks.empty()) {
      statediff_write_unit& last = chunks.back();
      if (last.offset + last.size == rd_size) {
        cur_off = last.offset;
        tmp.assign(last.buffer.get(), last.buffer.get() + last.size);
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

// Dispatched entry point. Production code calls this; the SIMD path is
// used when the host CPU supports AVX2.
inline std::vector<statediff_write_unit> compute_diff(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  if (fuselog_has_avx2() && min_width_coalesce == 0) {
    return compute_diff_simd(old_buf, new_buf, rd_size, write_size,
                             offset, min_width_coalesce);
  }
  return compute_diff_scalar(old_buf, new_buf, rd_size, write_size,
                             offset, min_width_coalesce);
}

// merge_adjacent_chunks fuses neighbouring chunks whose gap of unchanged
// bytes is strictly less than `overhead`. Bridging bytes are pulled from
// `new_buf` using `base_offset` as the origin (so new_buf[chunk_off -
// base_offset] is the corresponding relative byte). When a merge fires
// we allocate the resulting buffer once and memcpy the three regions
// (prev, gap, cur) into it — no per-byte push_back.
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
      std::unique_ptr<unsigned char[]> nb(new unsigned char[new_size]);
      std::memcpy(nb.get(), prev.buffer.get(), prev.size);
      if (gap > 0) {
        std::memcpy(nb.get() + prev.size,
                    new_buf + (prev_end - base_offset),
                    gap);
      }
      if (cur.size > 0) {
        std::memcpy(nb.get() + prev.size + gap,
                    cur.buffer.get(), cur.size);
      }
      prev.buffer = std::move(nb);
      prev.size = new_size;
    } else {
      merged.push_back(std::move(cur));
    }
  }
  return merged;
}

#endif  // FUSELOG_INTERNAL_H
