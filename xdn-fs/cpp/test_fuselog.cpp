// Unit tests for the pure compute helpers in fuselog_internal.h.
// Build via: ./bin/build_xdn_fuselog.sh test
//
// These tests exercise compute_diff() and merge_adjacent_chunks() in
// isolation — no FUSE mount, no syscalls — so they are the safety net
// for upcoming SIMD rewrites of the hot path.

#include <gtest/gtest.h>

#include <cstdlib>
#include <vector>

#include "fuselog_internal.h"

namespace {

// apply_chunks reconstructs new_buf by overlaying each chunk onto a copy of
// old_buf at the chunk's absolute offset (minus base_offset). Used as a
// reference oracle: compute_diff(old, new) -> chunks; apply(old, chunks)
// must equal new.
std::vector<unsigned char> apply_chunks(
    const std::vector<unsigned char>& old_buf,
    const std::vector<statediff_write_unit>& chunks,
    uint64_t base_offset) {
  std::vector<unsigned char> out = old_buf;
  for (const auto& c : chunks) {
    uint64_t rel = c.offset - base_offset;
    if (rel + c.buffer.size() > out.size()) {
      out.resize(rel + c.buffer.size());
    }
    for (size_t i = 0; i < c.buffer.size(); i++) out[rel + i] = c.buffer[i];
  }
  return out;
}

}  // namespace

// -------- compute_diff --------

TEST(ComputeDiff, EmptyBuffers) {
  auto chunks = compute_diff(nullptr, nullptr, 0, 0, 0, 0);
  EXPECT_TRUE(chunks.empty());
}

TEST(ComputeDiff, AllEqualEmitsNoChunks) {
  std::vector<unsigned char> a = {'a','b','c','d','e','f','g','h'};
  std::vector<unsigned char> b = a;
  auto chunks = compute_diff(a.data(), b.data(), a.size(), b.size(), 0, 0);
  EXPECT_TRUE(chunks.empty());
}

TEST(ComputeDiff, AllDifferentIsOneChunk) {
  std::vector<unsigned char> a(64, 'a');
  std::vector<unsigned char> b(64, 'b');
  auto chunks = compute_diff(a.data(), b.data(), 64, 64, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 0u);
  EXPECT_EQ(chunks[0].buffer.size(), 64u);
}

TEST(ComputeDiff, SingleByteDiffAtStart) {
  std::vector<unsigned char> a = {'a','b','c','d'};
  std::vector<unsigned char> b = {'X','b','c','d'};
  auto chunks = compute_diff(a.data(), b.data(), 4, 4, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 0u);
  ASSERT_EQ(chunks[0].buffer.size(), 1u);
  EXPECT_EQ(chunks[0].buffer[0], 'X');
}

TEST(ComputeDiff, SingleByteDiffAtEnd) {
  std::vector<unsigned char> a = {'a','b','c','d'};
  std::vector<unsigned char> b = {'a','b','c','X'};
  auto chunks = compute_diff(a.data(), b.data(), 4, 4, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 3u);
  ASSERT_EQ(chunks[0].buffer.size(), 1u);
  EXPECT_EQ(chunks[0].buffer[0], 'X');
}

TEST(ComputeDiff, SingleByteDiffInMiddle) {
  std::vector<unsigned char> a = {'a','b','c','d','e'};
  std::vector<unsigned char> b = {'a','b','X','d','e'};
  auto chunks = compute_diff(a.data(), b.data(), 5, 5, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 2u);
  ASSERT_EQ(chunks[0].buffer.size(), 1u);
  EXPECT_EQ(chunks[0].buffer[0], 'X');
}

TEST(ComputeDiff, TwoSeparateDiffsProduceTwoChunks) {
  std::vector<unsigned char> a = {'a','b','c','d','e','f','g','h'};
  std::vector<unsigned char> b = {'X','b','c','d','e','f','g','Y'};
  auto chunks = compute_diff(a.data(), b.data(), 8, 8, 0, 0);
  ASSERT_EQ(chunks.size(), 2u);
  EXPECT_EQ(chunks[0].offset, 0u);
  EXPECT_EQ(chunks[0].buffer.size(), 1u);
  EXPECT_EQ(chunks[1].offset, 7u);
  EXPECT_EQ(chunks[1].buffer.size(), 1u);
}

TEST(ComputeDiff, WriteExtendsBeyondOldFile) {
  std::vector<unsigned char> a = {'a','b','c'};
  std::vector<unsigned char> b = {'a','b','c','d','e','f'};
  auto chunks = compute_diff(a.data(), b.data(), 3, 6, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 3u);
  ASSERT_EQ(chunks[0].buffer.size(), 3u);
  EXPECT_EQ(chunks[0].buffer[0], 'd');
  EXPECT_EQ(chunks[0].buffer[1], 'e');
  EXPECT_EQ(chunks[0].buffer[2], 'f');
}

TEST(ComputeDiff, TailFoldOnlyAtZeroOffset) {
  // The tail-fold check compares last.offset+size against rd_size (read-
  // local) instead of offset+rd_size (absolute). With offset=0 the fold
  // fires; with offset>0 it doesn't, producing two chunks instead of one
  // merged chunk. This is a missed-merge quirk only — all bytes are still
  // captured. Pinning current behavior; can be a follow-up cleanup.
  std::vector<unsigned char> a = {'a','b','c'};
  std::vector<unsigned char> b = {'a','X','Y','Z','W'};

  auto z = compute_diff(a.data(), b.data(), 3, 5, 0, 0);
  ASSERT_EQ(z.size(), 1u);
  EXPECT_EQ(z[0].offset, 1u);
  ASSERT_EQ(z[0].buffer.size(), 4u);
  EXPECT_EQ(z[0].buffer[0], 'X');
  EXPECT_EQ(z[0].buffer[1], 'Y');
  EXPECT_EQ(z[0].buffer[2], 'Z');
  EXPECT_EQ(z[0].buffer[3], 'W');

  auto nz = compute_diff(a.data(), b.data(), 3, 5, 100, 0);
  ASSERT_EQ(nz.size(), 2u);
  EXPECT_EQ(nz[0].offset, 101u);
  EXPECT_EQ(nz[0].buffer.size(), 2u);
  EXPECT_EQ(nz[1].offset, 103u);
  ASSERT_EQ(nz[1].buffer.size(), 2u);
  EXPECT_EQ(nz[1].buffer[0], 'Z');
  EXPECT_EQ(nz[1].buffer[1], 'W');
}

TEST(ComputeDiff, OffsetAppearsInAbsoluteCoordinates) {
  std::vector<unsigned char> a = {'a','b','c'};
  std::vector<unsigned char> b = {'X','b','c'};
  auto chunks = compute_diff(a.data(), b.data(), 3, 3, 1000, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 1000u);
}

TEST(ComputeDiff, AlternatingDiffBytesProduceManyChunks) {
  std::vector<unsigned char> a(8, 'a');
  std::vector<unsigned char> b = {'X','a','X','a','X','a','X','a'};
  auto chunks = compute_diff(a.data(), b.data(), 8, 8, 0, 0);
  EXPECT_EQ(chunks.size(), 4u);
}

TEST(ComputeDiff, DiffAtThirtyTwoByteBoundary) {
  // SIMD lane is typically 16 or 32 bytes. A diff straddling those
  // boundaries is a likely failure mode of a SIMD rewrite.
  std::vector<unsigned char> a(64, 'a');
  std::vector<unsigned char> b = a;
  b[31] = 'X';
  b[32] = 'Y';
  auto chunks = compute_diff(a.data(), b.data(), 64, 64, 0, 0);
  ASSERT_EQ(chunks.size(), 1u);
  EXPECT_EQ(chunks[0].offset, 31u);
  ASSERT_EQ(chunks[0].buffer.size(), 2u);
  EXPECT_EQ(chunks[0].buffer[0], 'X');
  EXPECT_EQ(chunks[0].buffer[1], 'Y');
}

TEST(ComputeDiff, RoundTripRandomized) {
  std::srand(42);
  for (int trial = 0; trial < 200; trial++) {
    size_t n = (std::rand() % 500) + 1;
    std::vector<unsigned char> a(n), b(n);
    for (size_t i = 0; i < n; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      b[i] = (std::rand() & 7) == 0
                 ? (unsigned char)(std::rand() & 0xff)
                 : a[i];
    }
    auto chunks = compute_diff(a.data(), b.data(), n, n, 0, 0);
    auto applied = apply_chunks(a, chunks, 0);
    ASSERT_EQ(applied, b) << "trial " << trial << " n=" << n;
  }
}

TEST(ComputeDiff, RoundTripRandomizedWithExtension) {
  std::srand(7);
  for (int trial = 0; trial < 100; trial++) {
    size_t rd = (std::rand() % 200) + 1;
    size_t wr = rd + (std::rand() % 100);
    std::vector<unsigned char> a(rd), b(wr);
    for (size_t i = 0; i < rd; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      b[i] = (std::rand() & 7) == 0
                 ? (unsigned char)(std::rand() & 0xff)
                 : a[i];
    }
    for (size_t i = rd; i < wr; i++) {
      b[i] = (unsigned char)(std::rand() & 0xff);
    }
    auto chunks = compute_diff(a.data(), b.data(), rd, wr, 0, 0);
    auto applied = apply_chunks(a, chunks, 0);
    ASSERT_EQ(applied, b) << "trial " << trial << " rd=" << rd << " wr=" << wr;
  }
}

// -------- merge_adjacent_chunks --------

TEST(MergeChunks, EmptyInputReturnsEmpty) {
  std::vector<statediff_write_unit> in;
  std::vector<unsigned char> dummy;
  auto out = merge_adjacent_chunks(std::move(in), dummy.data(), 0, 25);
  EXPECT_TRUE(out.empty());
}

TEST(MergeChunks, GapBelowOverheadMerges) {
  std::vector<unsigned char> buf(50, 'z');
  std::vector<statediff_write_unit> in(2);
  in[0].offset = 0;  in[0].buffer = {'A','A','A','A','A'};   // [0..5)
  in[1].offset = 10; in[1].buffer = {'B','B','B','B','B'};   // gap = 5 < 25
  auto out = merge_adjacent_chunks(std::move(in), buf.data(), 0, 25);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].offset, 0u);
  ASSERT_EQ(out[0].buffer.size(), 15u);
  for (size_t i = 5; i < 10; i++) EXPECT_EQ(out[0].buffer[i], 'z');
  EXPECT_EQ(out[0].buffer[10], 'B');
}

TEST(MergeChunks, GapAboveOverheadStaysSeparate) {
  std::vector<unsigned char> buf(200, 'z');
  std::vector<statediff_write_unit> in(2);
  in[0].offset = 0;   in[0].buffer = {'A','A','A','A','A'};
  in[1].offset = 100; in[1].buffer = {'B','B'};               // gap = 95
  auto out = merge_adjacent_chunks(std::move(in), buf.data(), 0, 25);
  EXPECT_EQ(out.size(), 2u);
}

TEST(MergeChunks, GapExactlyOverheadStaysSeparate) {
  // overhead is the cost of a *new* action header; gap < overhead merges,
  // gap == overhead does not (break-even goes to separate).
  std::vector<unsigned char> buf(100, 'z');
  std::vector<statediff_write_unit> in(2);
  in[0].offset = 0;  in[0].buffer = {'A'};
  in[1].offset = 26; in[1].buffer = {'B'};   // gap = 25 == overhead
  auto out = merge_adjacent_chunks(std::move(in), buf.data(), 0, 25);
  EXPECT_EQ(out.size(), 2u);
}

TEST(MergeChunks, ChainedSmallGapsAllMerge) {
  std::vector<unsigned char> buf(50, 'z');
  std::vector<statediff_write_unit> in(3);
  in[0].offset = 0;  in[0].buffer = {'A'};
  in[1].offset = 5;  in[1].buffer = {'B'};   // gap = 4
  in[2].offset = 10; in[2].buffer = {'C'};   // gap = 4 from the merged chunk
  auto out = merge_adjacent_chunks(std::move(in), buf.data(), 0, 25);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].buffer.size(), 11u);
}

TEST(MergeChunks, BridgeBytesPulledFromNewBufAtBaseOffset) {
  // base_offset = 1000: new_buf[0] corresponds to file offset 1000.
  std::vector<unsigned char> buf = {'P','Q','R','S','T','U','V','W','X','Y','Z'};
  std::vector<statediff_write_unit> in(2);
  in[0].offset = 1000; in[0].buffer = {'A'};            // ends at file 1001
  in[1].offset = 1005; in[1].buffer = {'B'};            // gap [1001..1005) = 4
  auto out = merge_adjacent_chunks(std::move(in), buf.data(), 1000, 25);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].buffer.size(), 6u);
  EXPECT_EQ(out[0].buffer[0], 'A');
  EXPECT_EQ(out[0].buffer[1], 'Q');  // buf[1001-1000]
  EXPECT_EQ(out[0].buffer[2], 'R');
  EXPECT_EQ(out[0].buffer[3], 'S');
  EXPECT_EQ(out[0].buffer[4], 'T');
  EXPECT_EQ(out[0].buffer[5], 'B');
}

// -------- SIMD equivalence: scalar vs simd produce identical output --------

namespace {

// True if two chunks are bytewise identical.
bool chunks_equal(const std::vector<statediff_write_unit>& a,
                  const std::vector<statediff_write_unit>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].offset != b[i].offset) return false;
    if (a[i].buffer != b[i].buffer) return false;
  }
  return true;
}

}  // namespace

TEST(ComputeDiffSimd, MatchesScalarOnAllExistingCases) {
  // Exercise the SIMD path on the same inputs the scalar tests use; the
  // outputs must be byte-identical.
  struct Case {
    std::vector<unsigned char> a, b;
    size_t rd_size, write_size;
    uint64_t offset;
  };
  std::vector<Case> cases = {
    {{}, {}, 0, 0, 0},
    {{'a','b','c','d','e','f','g','h'},
     {'a','b','c','d','e','f','g','h'}, 8, 8, 0},
    {std::vector<unsigned char>(64, 'a'),
     std::vector<unsigned char>(64, 'b'), 64, 64, 0},
    {{'a','b','c','d'}, {'X','b','c','d'}, 4, 4, 0},
    {{'a','b','c','d'}, {'a','b','c','X'}, 4, 4, 0},
    {{'a','b','c','d','e'}, {'a','b','X','d','e'}, 5, 5, 0},
    {{'a','b','c','d','e','f','g','h'},
     {'X','b','c','d','e','f','g','Y'}, 8, 8, 0},
    {{'a','b','c'}, {'a','b','c','d','e','f'}, 3, 6, 0},
    {{'a','b','c'}, {'X','b','c'}, 3, 3, 1000},
    {std::vector<unsigned char>(8, 'a'),
     {'X','a','X','a','X','a','X','a'}, 8, 8, 0},
  };
  for (size_t k = 0; k < cases.size(); k++) {
    auto& c = cases[k];
    auto s = compute_diff_scalar(c.a.data(), c.b.data(),
                                  c.rd_size, c.write_size, c.offset, 0);
    auto v = compute_diff_simd(c.a.data(), c.b.data(),
                                c.rd_size, c.write_size, c.offset, 0);
    EXPECT_TRUE(chunks_equal(s, v)) << "mismatch on case " << k;
  }
}

TEST(ComputeDiffSimd, MatchesScalarOn32ByteBoundary) {
  // SIMD lane is 32 bytes; cases straddling that boundary are the most
  // likely failure mode.
  std::vector<unsigned char> a(96, 'a');
  for (size_t pos : {0, 1, 15, 16, 30, 31, 32, 33, 47, 48, 62, 63, 64, 65, 95}) {
    std::vector<unsigned char> b = a;
    b[pos] = 'X';
    auto s = compute_diff_scalar(a.data(), b.data(), 96, 96, 0, 0);
    auto v = compute_diff_simd(a.data(), b.data(), 96, 96, 0, 0);
    EXPECT_TRUE(chunks_equal(s, v)) << "mismatch on single-byte diff at " << pos;
  }
}

TEST(ComputeDiffSimd, MatchesScalarOnRandomized) {
  std::srand(42);
  for (int trial = 0; trial < 300; trial++) {
    size_t n = (std::rand() % 4096) + 1;
    std::vector<unsigned char> a(n), b(n);
    for (size_t i = 0; i < n; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      // Vary diff density across trials (1/N chance of differing).
      int p = (trial % 10) + 2;
      b[i] = (std::rand() % p == 0)
                 ? (unsigned char)(std::rand() & 0xff)
                 : a[i];
    }
    auto s = compute_diff_scalar(a.data(), b.data(), n, n, 0, 0);
    auto v = compute_diff_simd(a.data(), b.data(), n, n, 0, 0);
    ASSERT_TRUE(chunks_equal(s, v))
        << "mismatch on trial " << trial << " n=" << n;
  }
}

TEST(ComputeDiffSimd, MatchesScalarOnExtendingWrites) {
  std::srand(7);
  for (int trial = 0; trial < 100; trial++) {
    size_t rd = (std::rand() % 1024) + 1;
    size_t wr = rd + (std::rand() % 2048);
    std::vector<unsigned char> a(rd), b(wr);
    for (size_t i = 0; i < rd; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      b[i] = (std::rand() & 7) == 0
                 ? (unsigned char)(std::rand() & 0xff)
                 : a[i];
    }
    for (size_t i = rd; i < wr; i++) {
      b[i] = (unsigned char)(std::rand() & 0xff);
    }
    auto s = compute_diff_scalar(a.data(), b.data(), rd, wr, 0, 0);
    auto v = compute_diff_simd(a.data(), b.data(), rd, wr, 0, 0);
    ASSERT_TRUE(chunks_equal(s, v))
        << "mismatch on trial " << trial << " rd=" << rd << " wr=" << wr;
  }
}

TEST(FindFirstDiff, MatchesScalar) {
  std::srand(1);
  for (int trial = 0; trial < 200; trial++) {
    size_t n = (std::rand() % 1024) + 1;
    std::vector<unsigned char> a(n), b(n);
    for (size_t i = 0; i < n; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      b[i] = (std::rand() & 7) == 0
                 ? (unsigned char)(std::rand() & 0xff)
                 : a[i];
    }
    EXPECT_EQ(find_first_diff_scalar(a.data(), b.data(), n),
              find_first_diff(a.data(), b.data(), n))
        << "trial " << trial << " n=" << n;
  }
}

TEST(FindFirstMatch, MatchesScalar) {
  std::srand(2);
  for (int trial = 0; trial < 200; trial++) {
    size_t n = (std::rand() % 1024) + 1;
    std::vector<unsigned char> a(n), b(n);
    for (size_t i = 0; i < n; i++) {
      a[i] = (unsigned char)(std::rand() & 0xff);
      // Mostly differing, with occasional matches.
      b[i] = (std::rand() & 15) == 0
                 ? a[i]
                 : (unsigned char)((a[i] ^ ((std::rand() & 0xff) | 1)));
    }
    EXPECT_EQ(find_first_match_scalar(a.data(), b.data(), n),
              find_first_match(a.data(), b.data(), n))
        << "trial " << trial << " n=" << n;
  }
}
