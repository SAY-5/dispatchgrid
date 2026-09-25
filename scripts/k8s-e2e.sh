#!/usr/bin/env bash
# End-to-end proof on a local kind cluster: build images, deploy, run the load generator
# in-cluster, perform a rolling update of every service while load is running, and assert
# that the generator saw zero HTTP errors.
set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER="${KIND_CLUSTER:-dispatchgrid}"
TAG="${TAG:-dev}"
NS=dispatchgrid
DURATION="${DURATION_SECONDS:-60}"
ROLL_AFTER="${ROLL_AFTER_SECONDS:-10}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
MODULES="rider-request-service driver-location-service matching-service loadgen"
SERVICES="rider-request-service driver-location-service matching-service"
SUMMARY_FILE="${SUMMARY_FILE:-loadgen-summary.json}"
# The job controller deletes the pod when the job fails, so kubectl logs has nothing left to
# read by the time diagnostics run. Capture the output while the generator is alive.
LOADGEN_LOG="${LOADGEN_LOG:-loadgen.log}"
LOADGEN_LOG_PID=""

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; dump; exit 1; }

dump() {
  log "--- diagnostics ---"
  kubectl -n "$NS" get pods -o wide || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -40 || true
  for d in $SERVICES; do
    kubectl -n "$NS" logs deploy/"$d" --tail=60 --all-containers || true
  done
  if [ -s "$LOADGEN_LOG" ]; then
    log "--- load generator output captured during the run ---"
    tail -80 "$LOADGEN_LOG" || true
  else
    kubectl -n "$NS" logs job/loadgen --tail=80 || true
  fi
}

cleanup() {
  if [ -n "$LOADGEN_LOG_PID" ]; then kill "$LOADGEN_LOG_PID" 2>/dev/null || true; fi
  if [ "$KEEP_CLUSTER" = "1" ]; then
    log "keeping cluster $CLUSTER (KEEP_CLUSTER=1)"
  else
    log "deleting kind cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}

for tool in docker kind kubectl python3; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool"; exit 1; }
done

if [ "$SKIP_BUILD" != "1" ]; then
  for m in $MODULES; do
    log "building dispatchgrid/$m:$TAG"
    docker build --build-arg MODULE="$m" -t "dispatchgrid/$m:$TAG" . >/dev/null
  done
fi

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "creating kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --wait 120s
fi
trap cleanup EXIT
kubectl config use-context "kind-$CLUSTER" >/dev/null

for m in $MODULES; do
  log "loading dispatchgrid/$m:$TAG into kind"
  kind load docker-image "dispatchgrid/$m:$TAG" --name "$CLUSTER" >/dev/null
done

log "applying manifests"
kubectl apply -k deploy/k8s >/dev/null
kubectl -n "$NS" delete job loadgen --ignore-not-found >/dev/null

log "waiting for infrastructure"
kubectl -n "$NS" rollout status deploy/redpanda --timeout=300s
kubectl -n "$NS" rollout status deploy/redis --timeout=120s
kubectl -n "$NS" rollout status statefulset/mysql-shard-0 --timeout=300s
kubectl -n "$NS" rollout status statefulset/mysql-shard-1 --timeout=300s

log "waiting for services"
for d in $SERVICES; do
  kubectl -n "$NS" rollout status deploy/"$d" --timeout=420s
done
kubectl -n "$NS" get pods

log "starting in-cluster load generator (${DURATION}s at 10 rides/s, 300 drivers per city)"
sed -e "s/TAG_PLACEHOLDER/$TAG/" -e "s/DURATION_PLACEHOLDER/$DURATION/" deploy/k8s/loadgen-job.yaml \
  | kubectl apply -f - >/dev/null

for _ in $(seq 1 120); do
  if kubectl -n "$NS" logs job/loadgen 2>/dev/null | grep -q "submitting"; then break; fi
  sleep 2
done
kubectl -n "$NS" logs job/loadgen 2>/dev/null | grep -q "submitting" || fail "load generator never started submitting"

kubectl -n "$NS" logs -f job/loadgen > "$LOADGEN_LOG" 2>&1 &
LOADGEN_LOG_PID=$!

log "load is running; sleeping ${ROLL_AFTER}s before the rolling update"
sleep "$ROLL_AFTER"

MARKER="rollout-$(date +%s)"
ROLL_START=$(date +%s)
log "rolling update: ROLLOUT_MARKER=$MARKER on $SERVICES (maxUnavailable=0, maxSurge=1)"
# One deployment at a time. Replacing all three at once puts nine service pods on the node
# (two replicas plus one surge each), which on a small local VM starves them: pods restart and
# the rollout never converges. Staged replacement keeps maxUnavailable=0 and maxSurge=1 per
# deployment, keeps load flowing throughout, and only ever adds one extra pod.
for d in $SERVICES; do
  kubectl -n "$NS" set env deploy/"$d" ROLLOUT_MARKER="$MARKER" >/dev/null
  kubectl -n "$NS" rollout status deploy/"$d" --timeout=420s \
    || fail "rollout of $d did not converge within 420s"
done
ROLL_END=$(date +%s)
log "rolling update finished in $((ROLL_END - ROLL_START))s"
kubectl -n "$NS" get pods -l 'app in (rider-request-service,driver-location-service,matching-service)'

log "waiting for the load generator to finish"
LOADGEN_DEADLINE=$((SECONDS + 720))
while :; do
  CONDS=$(kubectl -n "$NS" get job loadgen -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  case "$CONDS" in
    *Complete=True*) break ;;
    *Failed=True*)
      TERM_REASON=$(kubectl -n "$NS" get pods -l app=loadgen \
        -o jsonpath='{.items[*].status.containerStatuses[*].state.terminated.reason}' 2>/dev/null || true)
      fail "load generator job failed ($CONDS terminated=${TERM_REASON:-unknown})"
      ;;
  esac
  [ "$SECONDS" -lt "$LOADGEN_DEADLINE" ] || fail "load generator job did not finish within 720s"
  sleep 5
done

kubectl -n "$NS" logs job/loadgen | sed -n '/== dispatchgrid load summary ==/,$p'
SUMMARY=$(kubectl -n "$NS" logs job/loadgen | grep '^SUMMARY_JSON ' | tail -1 | sed 's/^SUMMARY_JSON //')
[ -n "$SUMMARY" ] || fail "no SUMMARY_JSON line in load generator output"
echo "$SUMMARY" > "$SUMMARY_FILE"

python3 - "$SUMMARY" "$((ROLL_END - ROLL_START))" <<'PY'
import json, sys
s = json.loads(sys.argv[1])
roll = int(sys.argv[2])
problems = []
if s["rideErrors"] != 0:
    problems.append(f"ride http errors during rollout: {s['rideErrors']}")
if s["pingErrors"] != 0:
    problems.append(f"driver ping http errors during rollout: {s['pingErrors']}")
if s["ridesSubmitted"] == 0:
    problems.append("no rides submitted")
# The counters behind /matching/stats are in process and per pod, so a rolling update resets them
# and one read sees only the pod that answered. Assert decisions from the trip rows in the city
# shards, which survive pod replacement.
if s["durableDecided"] < s["ridesSubmitted"]:
    problems.append(f"undecided rides: {s['ridesSubmitted'] - s['durableDecided']} of {s['ridesSubmitted']}")
if s["durableMatched"] < 0.9 * s["ridesSubmitted"]:
    problems.append(f"match rate too low: {s['durableMatched']}/{s['ridesSubmitted']}")
shards = s["tripsByShard"]
for shard, cities in shards.items():
    if len(cities) != 1:
        problems.append(f"{shard} holds trips from cities {sorted(cities)}; expected exactly one city per shard")
print()
print("== rolling update evidence ==")
print(f"rollout duration        {roll}s, overlapping the {s['durationSeconds']}s load run")
print(f"ride requests           {s['ridesSubmitted']} submitted, {s['rideErrors']} http errors")
print(f"driver position pings   {s['pingsOk']} ok, {s['pingErrors']} http errors")
print(f"driver ping retries     {s.get('pingRetries', 0)} (idempotent upsert retried once on transport failure)")
print(f"sends skipped           {s.get('ridesSkipped', 0)} rides, {s.get('pingsSkipped', 0)} driver pings (in-flight bound reached; skipped and counted, not queued, not http errors)")
print(f"rides decided           {s['durableDecided']} of {s['durableTrips']} trip rows, {s['durableMatched']} matched, {s['durableRequested']} still requested")
print(f"matching counters       {s['matched']} matched / {s['unmatched']} unmatched (in process, per pod, reset by the rolling update)")
print(f"matches per minute      {s['matchesPerMinuteRun']} (run), {s['matchesPerMinuteWindow']} (trailing window, answering pod only)")
print(f"match latency           p50={s['p50LatencyMs']}ms p95={s['p95LatencyMs']}ms p99={s['p99LatencyMs']}ms (answering pod reservoir)")
print(f"trips by shard          {json.dumps(shards)}")
if problems:
    print("RESULT: FAIL")
    for p in problems:
        print(" - " + p)
    sys.exit(1)
print("RESULT: PASS (zero request errors across the rolling update)")
PY
