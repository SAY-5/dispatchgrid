#!/usr/bin/env bash
# The compose demo, recorded: bring the stack up, run the load generator once, and write the
# summary plus the host-side facts the generator cannot see into DEMO_DIR, so the numbers can be
# quoted later with the commit, clock, machine and load average they were measured under.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/docker-compose.yml)
DEMO_DIR="${DEMO_DIR:-demo-out}"
SUMMARY_FILE="$DEMO_DIR/loadgen-summary.json"
RUN_FILE="$DEMO_DIR/demo-run.json"
LOG_FILE="$DEMO_DIR/demo.log"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# One minute average from the machine that hosts the Docker VM, not from inside it: the VM's CPUs
# are scheduled by this kernel, so its contention moves the latency numbers.
host_load_average() {
  if [ -r /proc/loadavg ]; then
    cut -d' ' -f1 /proc/loadavg
  else
    sysctl -n vm.loadavg | tr -d '{}' | awk '{print $1}'
  fi
}

host_cpus() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown
}

for tool in docker git; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool"; exit 1; }
done

mkdir -p "$DEMO_DIR"
rm -f "$SUMMARY_FILE" "$RUN_FILE"

COMMIT="$(git rev-parse --short HEAD)"
git diff --quiet HEAD || COMMIT="$COMMIT-dirty"
DOCKER_VM="$(docker info --format 'Docker VM {{.NCPU}} CPU / {{.MemTotal}} B')"
HOST="$(uname -s) $(uname -m) $(host_cpus) CPU"
MACHINE="$HOST, $DOCKER_VM"
LOAD_BEFORE="$(host_load_average)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

export RUN_COMMIT="$COMMIT"
export RUN_MACHINE="$MACHINE"
log "commit $COMMIT on $MACHINE"
log "host load average before the run: $LOAD_BEFORE"

"${COMPOSE[@]}" up -d --build --wait redpanda mysql-shard-0 mysql-shard-1 redis
"${COMPOSE[@]}" up -d --build rider-request-service driver-location-service matching-service

set +e
"${COMPOSE[@]}" --profile loadgen run --rm --build -T loadgen 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOAD_AFTER="$(host_load_average)"
log "host load average after the run: $LOAD_AFTER"

# The generator writes its summary inside the container, so take it from the line it prints for
# exactly this reason, the way the kind run does.
if grep -q '^SUMMARY_JSON ' "$LOG_FILE"; then
  grep '^SUMMARY_JSON ' "$LOG_FILE" | tail -1 | sed 's/^SUMMARY_JSON //' > "$SUMMARY_FILE"
fi

printf '{\n' > "$RUN_FILE"
printf '  "command": "make demo",\n' >> "$RUN_FILE"
printf '  "commit": "%s",\n' "$COMMIT" >> "$RUN_FILE"
printf '  "machine": "%s",\n' "$MACHINE" >> "$RUN_FILE"
printf '  "startedAt": "%s",\n' "$STARTED_AT" >> "$RUN_FILE"
printf '  "finishedAt": "%s",\n' "$FINISHED_AT" >> "$RUN_FILE"
printf '  "hostLoadAverageBefore": "%s",\n' "$LOAD_BEFORE" >> "$RUN_FILE"
printf '  "hostLoadAverageAfter": "%s",\n' "$LOAD_AFTER" >> "$RUN_FILE"
printf '  "loadgenExitCode": %s\n' "$EXIT_CODE" >> "$RUN_FILE"
printf '}\n' >> "$RUN_FILE"

if [ "$EXIT_CODE" -ne 0 ]; then
  log "load generator exited $EXIT_CODE; $RUN_FILE records the failure"
  exit "$EXIT_CODE"
fi
[ -s "$SUMMARY_FILE" ] || { log "no SUMMARY_JSON line in the run output"; exit 1; }
log "wrote $SUMMARY_FILE and $RUN_FILE"
