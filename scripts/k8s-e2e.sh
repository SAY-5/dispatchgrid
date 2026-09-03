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

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; dump; exit 1; }

dump() {
  log "--- diagnostics ---"
  kubectl -n "$NS" get pods -o wide || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -40 || true
  for d in $SERVICES; do
    kubectl -n "$NS" logs deploy/"$d" --tail=60 --all-containers || true
  done
  kubectl -n "$NS" logs job/loadgen --tail=80 || true
}

cleanup() {
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

log "load is running; sleeping ${ROLL_AFTER}s before the rolling update"
sleep "$ROLL_AFTER"

MARKER="rollout-$(date +%s)"
ROLL_START=$(date +%s)
log "rolling update: ROLLOUT_MARKER=$MARKER on $SERVICES (maxUnavailable=0, maxSurge=1)"
for d in $SERVICES; do
  kubectl -n "$NS" set env deploy/"$d" ROLLOUT_MARKER="$MARKER" >/dev/null
done
for d in $SERVICES; do
  kubectl -n "$NS" rollout status deploy/"$d" --timeout=420s
done
ROLL_END=$(date +%s)
log "rolling update finished in $((ROLL_END - ROLL_START))s"
kubectl -n "$NS" get pods -l 'app in (rider-request-service,driver-location-service,matching-service)'

log "waiting for the load generator to finish"
if ! kubectl -n "$NS" wait --for=condition=complete job/loadgen --timeout=420s; then
  fail "load generator job did not complete"
fi

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
if s["matched"] + s["unmatched"] < s["ridesSubmitted"]:
    problems.append(f"undecided rides: {s['ridesSubmitted'] - s['matched'] - s['unmatched']}")
if s["matched"] < 0.9 * s["ridesSubmitted"]:
    problems.append(f"match rate too low: {s['matched']}/{s['ridesSubmitted']}")
shards = s["tripsByShard"]
for shard, cities in shards.items():
    if len(cities) != 1:
        problems.append(f"{shard} holds trips from cities {sorted(cities)}; expected exactly one city per shard")
print()
print("== rolling update evidence ==")
print(f"rollout duration        {roll}s, overlapping the {s['durationSeconds']}s load run")
print(f"ride requests           {s['ridesSubmitted']} submitted, {s['rideErrors']} http errors")
print(f"driver position pings   {s['pingsOk']} ok, {s['pingErrors']} http errors")
print(f"matched / unmatched     {s['matched']} / {s['unmatched']}")
print(f"matches per minute      {s['matchesPerMinuteRun']} (run), {s['matchesPerMinuteWindow']} (trailing window)")
print(f"match latency           p50={s['p50LatencyMs']}ms p95={s['p95LatencyMs']}ms p99={s['p99LatencyMs']}ms")
print(f"trips by shard          {json.dumps(shards)}")
if problems:
    print("RESULT: FAIL")
    for p in problems:
        print(" - " + p)
    sys.exit(1)
print("RESULT: PASS (zero request errors across the rolling update)")
PY
