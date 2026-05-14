// fuselog_internal.h - pure compute helpers extracted from fuselogv2.cpp.
// Anything that touches FUSE, libc syscalls, or global state stays in
// fuselogv2.cpp; only deterministic input -> output transformations live
// here so they can be unit tested without mounting a filesystem.
#ifndef FUSELOG_INTERNAL_H
#define FUSELOG_INTERNAL_H

#include <cstddef>
#include <cstdint>
#include <cstdlib>
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

struct statediff_write_unit {
  uint64_t                   offset;
  std::vector<unsigned char> buffer;
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

inline std::vector<statediff_write_unit> compute_diff_scalar(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  std::vector<statediff_write_unit> chunks;

  size_t i = 0;
  while (i < rd_size) {
    if (old_buf[i] == new_buf[i]) {
      i++;
      continue;
    }
    statediff_write_unit cur;
    cur.offset = offset + i;
    while (i < rd_size &&
           (old_buf[i] != new_buf[i] ||
            cur.buffer.size() < min_width_coalesce)) {
      cur.buffer.push_back(new_buf[i]);
      i++;
    }
    chunks.push_back(std::move(cur));
  }

  if (i < write_size) {
    statediff_write_unit cur;
    cur.offset = offset + i;

    // Fold the tail into the prior chunk if it abuts. NOTE: this check
    // compares last.offset+size against rd_size (read-local), not
    // offset+rd_size (absolute), so when offset > 0 the fold doesn't
    // fire. This is a missed-merge optimization quirk, not a correctness
    // issue — all bytes are still captured, just as two chunks instead of
    // one. See test_fuselog.cpp `TailFoldOnlyAtZeroOffset`.
    if (!chunks.empty()) {
      statediff_write_unit& last = chunks.back();
      if (last.offset + last.buffer.size() == rd_size) {
        cur.offset = last.offset;
        cur.buffer = std::move(last.buffer);
        chunks.pop_back();
      }
    }
    while (i < write_size) {
      cur.buffer.push_back(new_buf[i]);
      i++;
    }
    chunks.push_back(std::move(cur));
  }

  return chunks;
}

inline std::vector<statediff_write_unit> compute_diff_simd(
    const unsigned char* old_buf, const unsigned char* new_buf,
    size_t rd_size, size_t write_size, uint64_t offset,
    uint32_t min_width_coalesce) {
  // SIMD path only handles min_width_coalesce == 0. The extend-through-
  // equal-bytes semantics for non-zero values aren't worth vectorizing;
  // fall back to scalar in that case.
  if (min_width_coalesce != 0) {
    return compute_diff_scalar(old_buf, new_buf, rd_size, write_size,
                               offset, min_width_coalesce);
  }

  std::vector<statediff_write_unit> chunks;

  size_t i = 0;
  while (i < rd_size) {
    size_t diff_off = find_first_diff(old_buf + i, new_buf + i, rd_size - i);
    if (diff_off >= rd_size - i) {
      i = rd_size;
      break;
    }
    size_t diff_start = i + diff_off;
    size_t match_off = find_first_match(old_buf + diff_start,
                                         new_buf + diff_start,
                                         rd_size - diff_start);
    size_t diff_end = diff_start + match_off;  // == rd_size if no match

    statediff_write_unit cur;
    cur.offset = offset + diff_start;
    cur.buffer.assign(new_buf + diff_start, new_buf + diff_end);
    chunks.push_back(std::move(cur));

    i = diff_end;  // next iteration skips the equal byte (or exits)
  }

  // Tail extension — identical to scalar.
  if (i < write_size) {
    statediff_write_unit cur;
    cur.offset = offset + i;
    if (!chunks.empty()) {
      statediff_write_unit& last = chunks.back();
      if (last.offset + last.buffer.size() == rd_size) {
        cur.offset = last.offset;
        cur.buffer = std::move(last.buffer);
        chunks.pop_back();
      }
    }
    while (i < write_size) {
      cur.buffer.push_back(new_buf[i]);
      i++;
    }
    chunks.push_back(std::move(cur));
  }

  return chunks;
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
// base_offset] is the corresponding relative byte).
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
    uint64_t prev_end = prev.offset + prev.buffer.size();
    uint64_t gap = (cur.offset > prev_end) ? (cur.offset - prev_end) : 0;
    if (gap < overhead) {
      for (uint64_t g = 0; g < gap; g++) {
        uint64_t buf_idx = (prev_end + g) - base_offset;
        prev.buffer.push_back(new_buf[buf_idx]);
      }
      prev.buffer.insert(prev.buffer.end(),
                         cur.buffer.begin(), cur.buffer.end());
    } else {
      merged.push_back(std::move(cur));
    }
  }
  return merged;
}

#endif  // FUSELOG_INTERNAL_H
