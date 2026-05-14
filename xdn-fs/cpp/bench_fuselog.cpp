// Microbenchmark for compute_diff: scalar vs SIMD across diff densities.
// Build via: ./bin/build_xdn_fuselog.sh bench
//
// We vary buffer size and the fraction of differing bytes; for each combo
// we time both implementations and print MB/s + speedup.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "fuselog_internal.h"

using clk = std::chrono::steady_clock;

static void fill_random(std::vector<unsigned char>& v, unsigned seed) {
  std::srand(seed);
  for (auto& x : v) x = (unsigned char)(std::rand() & 0xff);
}

static void make_diff_pair(std::vector<unsigned char>& a,
                            std::vector<unsigned char>& b,
                            size_t n, double diff_fraction,
                            unsigned seed) {
  a.resize(n);
  b.resize(n);
  fill_random(a, seed);
  b = a;
  std::srand(seed + 1);
  size_t target = (size_t)(n * diff_fraction);
  for (size_t k = 0; k < target; k++) {
    size_t pos = (size_t)std::rand() % n;
    b[pos] = (unsigned char)((a[pos] ^ ((std::rand() & 0xff) | 1)));
  }
}

template <typename Fn>
static double time_iters(Fn fn, int iters) {
  auto t0 = clk::now();
  for (int i = 0; i < iters; i++) fn();
  auto t1 = clk::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

static void bench_one(size_t n, double diff_fraction) {
  std::vector<unsigned char> a, b;
  make_diff_pair(a, b, n, diff_fraction, /*seed=*/123);

  // Warm-up.
  auto _ = compute_diff_scalar(a.data(), b.data(), n, n, 0, 0);
  (void)_;

  int iters = std::max(1, (int)(1024 * 1024 * 64 / n));
  double scalar_s = time_iters(
      [&]() {
        auto r = compute_diff_scalar(a.data(), b.data(), n, n, 0, 0);
        asm volatile("" :: "g"(&r) : "memory");
      }, iters);
  double avx2_s = time_iters(
      [&]() {
        auto r = compute_diff_simd(a.data(), b.data(), n, n, 0, 0);
        asm volatile("" :: "g"(&r) : "memory");
      }, iters);
  double avx512_s = fuselog_has_avx512()
      ? time_iters(
          [&]() {
            auto r = compute_diff_avx512(a.data(), b.data(), n, n, 0, 0);
            asm volatile("" :: "g"(&r) : "memory");
          }, iters)
      : 0.0;

  double bytes = (double)n * iters;
  double scalar_mbps = bytes / scalar_s / (1024.0 * 1024.0);
  double avx2_mbps   = bytes / avx2_s   / (1024.0 * 1024.0);
  double avx512_mbps = (avx512_s > 0)
      ? bytes / avx512_s / (1024.0 * 1024.0)
      : 0.0;

  if (avx512_s > 0) {
    printf("  n=%8zu  diff=%5.2f%%   scalar=%8.0f   avx2=%8.0f (%4.1fx)   "
           "avx512=%8.0f (%4.1fx vs avx2, %4.1fx vs scalar)  MiB/s\n",
           n, diff_fraction * 100.0,
           scalar_mbps,
           avx2_mbps, scalar_s / avx2_s,
           avx512_mbps, avx2_s / avx512_s, scalar_s / avx512_s);
  } else {
    printf("  n=%8zu  diff=%5.2f%%   scalar=%8.0f   avx2=%8.0f (%4.1fx)   "
           "avx512=n/a  MiB/s\n",
           n, diff_fraction * 100.0,
           scalar_mbps, avx2_mbps, scalar_s / avx2_s);
  }
}

int main() {
  printf("AVX2 supported:   %s\n", fuselog_has_avx2() ? "yes" : "no");
  printf("AVX-512 supported: %s\n\n",
         fuselog_has_avx512() ? "yes" : "no");

  printf("compute_diff scalar vs AVX2 vs AVX-512\n");
  printf("--------------------------------------\n");
  for (size_t n : {(size_t)4096, (size_t)65536, (size_t)262144,
                    (size_t)1 << 20}) {
    for (double f : {0.001, 0.01, 0.10, 0.50}) {
      bench_one(n, f);
    }
    printf("\n");
  }
  return 0;
}
