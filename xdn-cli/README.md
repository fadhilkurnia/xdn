# `xdn` - the core command line interface for xdn app developer

## Launching with env vars

- Single-component via flags: repeat `--env KEY=VALUE`.

```bash
xdn launch my-service \
  --image example/app:latest \
  --state /var/data/ \
  --port 8080 \
  --env FOO=bar \
  --env DEBUG=true
```

- Multi-component via YAML: put `environments` (or `env`) under each component in the service file and launch with `-f your-service.yaml`.
