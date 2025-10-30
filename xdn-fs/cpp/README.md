# Fuselog - capturing statediff via user-level filesystem

Mounting the filesystem:
```
sudo ./fuselog -f -o allow_other <mount_directory>
```
The `allow_other` option ensures other users can use the filesystem, to enable
that option, uncomment the `user_allow_other` in `/etc/fuse.conf` file.
```
# /etc/fuse.conf 
...
user_allow_other
...
```

## Do we need to fsync() for each statediff capture? No
Generally, the answer is No. The next POSIX read() syscall will always read 
the latest write() from any process. So, read from the coordinator will read the
last write by the Fuselog.
Source: https://stackoverflow.com/questions/64093553/according-to-posix-when-will-my-writes-be-visible-to-other-processes

Additionally, correct datastore implementation should call fsync() when required.


## Statediff format

Statediff consists of multiple statediff units, each representing a durable disk IO event (e.g. write or unlink).

Version 1, no header:
```
statediff_size uint64_t
statediff_type uint8_t  // 0 == write
path_len       uint64_t
path           variable length (based on path_len)
offset         uint64_t
size           uint64_t
data           variable length (based on size)

statediff_size uint64_t
statediff_type uint8_t  // 1 == unlink
path_len       uint64_t
path           variable length (based on path_len)
```
Limitation: redundant path in each statediff unit.


(WIP) Version 2, using off-the-shelf serialization lib
```
header: mapping between fid to filename
list of statediff unit:
  fid    uint64_t // refer the pathname in the header
  offset uint64_t
  count  uint64_t
  data   variable length (based on count)
```

TODO:
- reserve SD_TYPE_NULL == 0 to prevent bug, bump up others
- add new attributes in statediff unit for to_fid and size in rename and truncate.