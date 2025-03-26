# Simple state writer service

To run the service:
```
go run main.go
```

You can configure the following parameters via environment variable:
- `PORT`: port where the service listen to incoming request (default: `8000`).
- `STATEDIR`: directory to store the state (default: `data`).
- `INITSIZE`: the initial size of the state, in MB (default: `100`).

The service only has one operation that write to a file (`state.bin`).
Use curl to send an http request:
```
curl -X POST http://localhost:8000 -d "size=100"
```
If size is not provided, the service will write 8 bytes by default.

## Building into docker image

To build the service inside docker image, use this command:
```
docker build --target build-release-stage -t simple-writer:latest .
```
It should produce ~14.3MB docker image. Then run the service as a container 
with:
```
docker run -d -p 8000:8000 simple-writer:latest
```

## Miscellaneous
### Alpine Linux small urandom size
In Alpine Linux the size of /dev/urandom is only 32M.
Thus to generate 100M initial state we need to do:
```
dd if=/dev/urandom of=data/state.bin bs=1M count=100
```
and not
```
dd if=/dev/urandom of=data/state.bin bs=1M count=100
```