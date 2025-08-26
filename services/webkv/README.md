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