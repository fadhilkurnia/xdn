# noop - the minimal XDN service

A no-operation web service: it accepts **any** HTTP request and immediately
returns `200 ok` without touching disk or doing any work. Use it as a baseline
to measure XDN's coordination/replication overhead independent of application
execution time.

Tech: Go (no dependencies, no state).

## Endpoints

Any method and path returns `200` with the body `ok`.

```
curl http://localhost/        # -> ok
curl -X POST http://localhost/anything   # -> ok
```

## Run locally

```
docker run -p 80:80 fadhilkurnia/xdn-noop
```

## Build a container image

```
docker build --tag=xdn-noop .
```

To build for multiple platforms:

```
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-noop .
```
