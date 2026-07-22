# Bandwidth probe sidecar for XDN cluster-mode services.
#
# XDN attaches this container to a cluster member's network namespace
# (--network=container:<member>) and reads per-connection TCP byte counters
# with `docker exec <probe> ss -tinH`, plus overlay peer addresses with
# `docker exec <probe> getent hosts replica-0 ...`. The container itself just
# sleeps; iproute2 provides `ss` with tcp_info support (busybox's does not).
#
# Build (tests build this locally as xdn-bw-probe:test):
#   docker build -t xdn-bw-probe:test services/bw-probe/
#
# Publish (multi-arch, required — CI runners are amd64, dev machines arm64):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -t fadhilkurnia/xdn-bw-probe:latest --push services/bw-probe/
FROM alpine:3.20
RUN apk add --no-cache iproute2-ss
ENTRYPOINT ["sleep", "infinity"]
