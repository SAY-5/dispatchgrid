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
# This attempt writes here and only moves the pair into place once it has a summary, so a run that
# dies halfway leaves its own failure record without destroying the last pair that did complete.
NEW_SUMMARY="$SUMMARY_FILE.partial"
NEW_RUN="$RUN_FILE.partial"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# One minute average from the machine the demo was launched on. On a Linux host that is the kernel
# the containers share; on macOS it is the kernel scheduling the Docker VM's CPUs. Either way its
# contention moves the latency numbers, and the generator records the container kernel separately.
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
rm -f "$NEW_SUMMARY" "$NEW_RUN"

COMMIT="$(git rev-parse --short HEAD)"
git diff --quiet HEAD || COMMIT="$COMMIT-dirty"
DOCKER_VM="$(docker info --format '{{.NCPU}} {{.MemTotal}}' \
  | awk '{printf "Docker VM %d CPU / %.1f GiB", $1, $2 / 1073741824}')"
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
  grep '^SUMMARY_JSON ' "$LOG_FILE" | tail -1 | sed 's/^SUMMARY_JSON //' > "$NEW_SUMMARY"
fi

printf '{\n' > "$NEW_RUN"
printf '  "command": "make demo",\n' >> "$NEW_RUN"
printf '  "commit": "%s",\n' "$COMMIT" >> "$NEW_RUN"
printf '  "machine": "%s",\n' "$MACHINE" >> "$NEW_RUN"
printf '  "startedAt": "%s",\n' "$STARTED_AT" >> "$NEW_RUN"
printf '  "finishedAt": "%s",\n' "$FINISHED_AT" >> "$NEW_RUN"
printf '  "hostLoadAverageBefore": "%s",\n' "$LOAD_BEFORE" >> "$NEW_RUN"
printf '  "hostLoadAverageAfter": "%s",\n' "$LOAD_AFTER" >> "$NEW_RUN"
printf '  "loadgenExitCode": %s\n' "$EXIT_CODE" >> "$NEW_RUN"
printf '}\n' >> "$NEW_RUN"

if [ "$EXIT_CODE" -ne 0 ] || [ ! -s "$NEW_SUMMARY" ]; then
  log "no summary from this attempt (load generator exit $EXIT_CODE); $NEW_RUN records it and any"
  log "pair already in $DEMO_DIR is from an earlier run, not this one"
  exit "$((EXIT_CODE == 0 ? 1 : EXIT_CODE))"
fi
mv "$NEW_SUMMARY" "$SUMMARY_FILE"
mv "$NEW_RUN" "$RUN_FILE"
log "wrote $SUMMARY_FILE and $RUN_FILE"
