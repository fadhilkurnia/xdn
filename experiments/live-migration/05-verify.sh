#!/usr/bin/env bash
# Status + data-integrity check. Shows where the VM runs and proves postgres
# state survives migration: writes a uniquely-tagged book, reads it back.
#
#   ./05-verify.sh            # status + read current books
#   ./05-verify.sh --write    # also insert a tagged book first
set -euo pipefail
source "$(dirname "$0")/config.sh"

say "where is '${VM_NAME}'?"
for h in "$HOST0_IP:$HOST0_DNS" "$HOST1_IP:$HOST1_DNS"; do
  ip="${h%%:*}"; dns="${h##*:}"
  state="$(sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
            "root@${ip}" "virsh domstate ${VM_NAME} 2>/dev/null" 2>/dev/null || echo "-")"
  printf '  %-12s (%s): %s\n' "$dns" "$ip" "${state:-not present}"
done

if [ "${1:-}" = "--write" ]; then
  tag="migrated-$(date +%H%M%S)"
  say "inserting a book tagged '${tag}'"
  curl -fsS -X POST "http://${VM_IP}:${SERVICE_PORT}${SERVICE_PATH}" \
    -H 'Content-Type: application/json' \
    -d "{\"title\":\"${tag}\",\"author\":\"live-migration\",\"year\":2026}" && echo
fi

say "current books at ${SERVICE_URL}"
curl -fsS --max-time 5 "$SERVICE_URL" || die "service unreachable at ${SERVICE_URL}"
echo
