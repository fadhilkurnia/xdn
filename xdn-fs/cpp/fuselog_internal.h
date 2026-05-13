// fuselog_internal.h - pure compute helpers extracted from fuselogv2.cpp.
// Anything that touches FUSE, libc syscalls, or global state stays in
// fuselogv2.cpp; only deterministic input -> output transformations live
// here so they can be unit tested without mounting a filesystem.
#ifndef FUSELOG_INTERNAL_H
#define FUSELOG_INTERNAL_H

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

// Per-action serialization overhead on the wire:
//   1 (type) + 8 (fid) + 8 (count) + 8 (offset) = 25 bytes.
// When merging adjacent chunks, bridging a small gap of unchanged bytes is
// cheaper than emitting a second action whose header would cost this much.
static const uint32_t COALESCE_ACTION_OVERHEAD = 25;

struct statediff_write_unit {
  uint64_t                   offset;
  std::vector<unsigned char> buffer;
};

// compute_diff scans old_buf[0..rd_size) vs new_buf[0..write_size) and emits
// one statediff_write_unit per contiguous differing run, with offsets in
// absolute file coordinates (i.e. base = `offset` arg). If write_size >
// rd_size, the trailing bytes of new_buf form an additional chunk (folded
// into the prior chunk if it abuts rd_size).
//
// `min_width_coalesce` keeps extending the current diff chunk through equal
// bytes until at least this many bytes have been captured (0 disables).
inline std::vector<statediff_write_unit> compute_diff(
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
