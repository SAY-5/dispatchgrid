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
MEMORY_FILE="$DEMO_DIR/stack-memory.partial"
SAMPLER_PID=""

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

# Memory in use across every container of the compose project, loadgen included, as docker stats
# reports it, so the size of VM the demo needs is a measured figure rather than an estimate.
stack_memory() {
  local ids
  ids="$(docker ps -q --filter label=com.docker.compose.project=dispatchgrid)"
  [ -n "$ids" ] || return 1
  # shellcheck disable=SC2086
  docker stats --no-stream --format '{{.MemUsage}}' $ids | awk '
    {
      value = $1 + 0
      unit = $1
      sub(/^[0-9.]+/, "", unit)
      if (unit == "B") factor = 1 / 1048576
      else if (unit == "KiB") factor = 1 / 1024
      else if (unit == "MiB") factor = 1
      else if (unit == "GiB") factor = 1024
      else bad = 1
      total += value * factor
      count++
    }
    END {
      if (bad || count == 0) exit 1
      printf "%.0f MiB across %d containers\n", total, count
    }'
}

# One reading halfway through the load, once the generator says it is submitting.
sample_stack_memory() {
  for _ in $(seq 1 600); do
    if grep -q '^submitting ' "$LOG_FILE"; then
      sleep "$((${DURATION_SECONDS:-60} / 2))"
      stack_memory > "$MEMORY_FILE" || rm -f "$MEMORY_FILE"
      return
    fi
    sleep 1
  done
}

stop_sampler() {
  if [ -n "$SAMPLER_PID" ]; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=""
  fi
}

for tool in docker git; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool"; exit 1; }
done

mkdir -p "$DEMO_DIR"
rm -f "$NEW_SUMMARY" "$NEW_RUN" "$MEMORY_FILE"
trap stop_sampler EXIT

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

# Emptied first so the sampler cannot find the previous attempt's "submitting" line.
: > "$LOG_FILE"
sample_stack_memory &
SAMPLER_PID=$!

set +e
"${COMPOSE[@]}" --profile loadgen run --rm --build -T loadgen 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOAD_AFTER="$(host_load_average)"
log "host load average after the run: $LOAD_AFTER"
stop_sampler
STACK_MEMORY="$(cat "$MEMORY_FILE" 2>/dev/null || true)"
STACK_MEMORY="${STACK_MEMORY:-unavailable}"
rm -f "$MEMORY_FILE"
log "stack memory halfway through the load: $STACK_MEMORY"

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
printf '  "stackMemory": "%s",\n' "$STACK_MEMORY" >> "$NEW_RUN"
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
