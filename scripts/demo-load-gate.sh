#!/usr/bin/env bash
# Waits until the machine is quiet enough for a demo run whose latency figures will be published.
# It is not part of `make demo`: run it first, and start `make demo` only if it exits 0.
#
# Quiet means the host's one minute load average is below HOST_LOAD_LIMIT and the Docker VM's,
# read from /proc/loadavg inside a probe container, is below VM_LOAD_LIMIT, on two consecutive
# readings INTERVAL_SECONDS apart. On Linux without a VM both readings come from the same kernel.
# Exits 0 when the gate opens, 1 if it has not opened after TIMEOUT_SECONDS, 2 if it cannot read.
set -euo pipefail

HOST_LOAD_LIMIT="${HOST_LOAD_LIMIT:-8}"
VM_LOAD_LIMIT="${VM_LOAD_LIMIT:-3}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-30}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-2700}"
PROBE_IMAGE="${PROBE_IMAGE:-redis:7-alpine}"
PROBE="dispatchgrid-load-gate-$$"
REQUIRED_PASSES=2

log() { echo "[$(date -u +%H:%M:%SZ)] $*"; }

# The probe is removed however the script ends: gate open, timeout, error or signal.
remove_probe() { docker rm -f "$PROBE" > /dev/null 2>&1 || true; }
trap remove_probe EXIT
trap 'exit 130' INT TERM

host_load() {
  if [ -r /proc/loadavg ]; then
    cut -d' ' -f1 /proc/loadavg
  else
    sysctl -n vm.loadavg | tr -d '{}' | awk '{print $1}'
  fi
}

vm_load() {
  docker exec "$PROBE" cat /proc/loadavg 2> /dev/null | awk '{print $1}'
}

below() {
  awk -v value="$1" -v limit="$2" 'BEGIN { exit !(value != "" && value + 0 < limit + 0) }'
}

for value in "$HOST_LOAD_LIMIT" "$VM_LOAD_LIMIT" "$INTERVAL_SECONDS" "$TIMEOUT_SECONDS"; do
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "limits and seconds must be numbers"; exit 2; }
done
command -v docker > /dev/null || { echo "missing tool: docker"; exit 2; }

docker run -d --rm --name "$PROBE" --entrypoint sleep "$PROBE_IMAGE" \
  "$((${TIMEOUT_SECONDS%.*} + 600))" > /dev/null \
  || { echo "could not start the probe container from $PROBE_IMAGE"; exit 2; }

deadline=$(($(date +%s) + ${TIMEOUT_SECONDS%.*}))
passes=0
while :; do
  host="$(host_load)"
  vm="$(vm_load || true)"
  if below "$host" "$HOST_LOAD_LIMIT" && below "$vm" "$VM_LOAD_LIMIT"; then
    passes=$((passes + 1))
  else
    passes=0
  fi
  log "host load ${host:-unreadable} (limit $HOST_LOAD_LIMIT), Docker VM load ${vm:-unreadable}" \
    "(limit $VM_LOAD_LIMIT): $passes of $REQUIRED_PASSES consecutive passing readings"
  if [ "$passes" -ge "$REQUIRED_PASSES" ]; then
    log "OPEN: start make demo now"
    exit 0
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    log "CLOSED: the gate did not open within ${TIMEOUT_SECONDS}s; do not publish a run started now"
    exit 1
  fi
  # In the background so a signal interrupts the wait at once instead of after it.
  sleep "$INTERVAL_SECONDS" &
  wait $!
done
