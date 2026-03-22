# Benchmark Results (rqlite 3-node, write-only)

Date: 2026-02-09

## Environment
- Host OS: macOS 14.0 (Darwin 23.0.0), arm64
- CPU: Apple M1
- Memory: 16 GB
- rqlite: 3-node cluster via `docker-compose.rqlite.yaml`
- Load generator: k6 on same host
- Java runtime (Maven): OpenJDK 23.0.2
- Node runtime: v23.9.0

## k6 settings
- BASE_URL: `http://127.0.0.1:8080`
- RATE: 200 iters/s
- WARMUP: 1m
- DURATION: 3m
- ITEMS: 1000
- PRE_VUS: 50
- MAX_VUS: 200
- Trend stats: `avg,med,p(90),p(95),p(99),min,max`

## Results (write-only 50% POST / 50% DELETE)
All runs had 0% request failures and 100% checks passing.

| Service | p50 (ms) | p95 (ms) | p99 (ms) | RPS | Error Rate |
|---|---:|---:|---:|---:|---:|
| Rust | 2.379 | 3.956 | 6.558 | 201.77 | 0.00% |
| Java (Spring Boot) | 2.587 | 4.279 | 7.142 | 200.81 | 0.00% |
| JS (Fastify) | 2.447 | 4.040 | 5.970 | 201.56 | 0.00% |

## Notes
- Results reflect local, single-machine runs with rqlite 3-way replication.
- If you want sustained throughput testing at higher RPS, rerun with `RATE` increased.
