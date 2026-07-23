# HTTP KV frontend sidecar for the antidote cluster reference service.
# Based on the same antidote image so the release's erts/escript and the
# shared dist cookie are available; only the escript shim runs here.
# Build: docker build -t xdn-antidote-http:test services/antidote-http/
FROM antidotedb/antidote:latest
COPY httpkv.escript /httpkv.escript
RUN chmod +x /httpkv.escript
ENTRYPOINT ["/bin/sh", "-c", "exec /antidote/erts-*/bin/escript /httpkv.escript"]
