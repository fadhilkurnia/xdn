# restkv - simple key-value web service

restkv with simple read-only and write-only request
```
GET     /                     read-only
GET     /view                 read-only
GET     /api/kv/:key          read-only
PUT     /api/kv/:key          write-only
POST    /api/kv/:key          write-only
DELETE  /api/kv/:key          write-only
```

## Backend selection (RocksDB or TiKV)

The webkv service can run on a local RocksDB instance or a remote TiKV raw-KV cluster based on environment variables:

- `DB_TYPE`: `rocksdb` (default) or `tikv`.
- `DB_HOST`: when `DB_TYPE=rocksdb`, this is the RocksDB path (default `data`); when `DB_TYPE=tikv`, this must be the PD endpoint(s) (comma-separated `host:port`).

Examples:
```bash
# Local RocksDB (default path)
DB_TYPE=rocksdb DB_HOST=data ./run_webkv.sh

# TiKV via PD at 10.0.0.5:2379
DB_TYPE=tikv DB_HOST=10.0.0.5:2379 ./run_webkv.sh
```
