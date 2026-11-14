#!/usr/bin/env bash
# Scale scenarios for cluster-gateway <-> cluster-agent with many DataPlane CRs
# sharing one planeID. Probes run from an in-cluster pod (probe.yaml) so calls
# are load-balanced across gateway replicas like the controller's are.
#
# Usage: measure.sh <scenario>   scenario: connect | newauth | delete | latency | all
# Output: one "RESULT key=value ..." line per scenario (grep-able), details on stderr.
set -euo pipefail

KCTX="${KCTX:-k3d-oc-scale}"
[[ "$KCTX" == "k3d-oc-scale" || "${ALLOW_ANY_CONTEXT:-0}" == "1" ]] || { echo "refusing context $KCTX" >&2; exit 1; }
CP=openchoreo-control-plane
DP=openchoreo-data-plane
AGENT_DEPLOY=cluster-agent-dataplane
PLANE_ID="${PLANE_ID:-shared}"
HERE="$(cd "$(dirname "$0")" && pwd)"
k() { kubectl --context "$KCTX" "$@"; }

G=https://cluster-gateway.$CP.svc:8444
CURL="curl -s --max-time 40 --cacert /c/ca.crt --cert /c/tls.crt --key /c/tls.key"
inpod() { k exec -n "$CP" probe -- sh -c "$1"; }
proxy_path() { echo "/api/proxy/dataplane/$PLANE_ID/$1/default/k8s/api/v1/namespaces/default"; }
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
agents() { k get deploy -n "$DP" "$AGENT_DEPLOY" -o jsonpath='{.spec.replicas}'; }
norgs() { "$HERE/orgs.sh" count; }

ensure_probe() {
  k get pod -n "$CP" probe >/dev/null 2>&1 || k apply -f "$HERE/probe.yaml" >/dev/null
  k wait pod/probe -n "$CP" --for=condition=Ready --timeout=120s >/dev/null
}

gw_log_count() { # pattern since-time
  local total=0 p n
  for p in $(k get pods -n "$CP" -l app=cluster-gateway -o name); do
    n=$(k logs -n "$CP" "$p" --since-time="$2" 2>/dev/null | grep -c "$1" || true)
    total=$((total + n))
  done
  echo "$total"
}

# connect: scale agents to 0, then back; time until every replica is connected
# and authorized for a sample org (via fleet-wide CR status on the gateway).
scenario_connect() {
  local reps t0 since deadline st n
  reps=$(agents)
  k scale deploy -n "$DP" "$AGENT_DEPLOY" --replicas=0 >/dev/null
  k wait --for=delete pod -n "$DP" -l app=cluster-agent --timeout=120s >/dev/null 2>&1 || true
  sleep 3
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  t0=$(now)
  k scale deploy -n "$DP" "$AGENT_DEPLOY" --replicas="$reps" >/dev/null
  deadline=$(( $(date +%s) + ${CONNECT_TIMEOUT:-300} ))
  n=0
  while (( $(date +%s) < deadline )); do
    st=$(inpod "$CURL '$G/api/v1/planes/dataplane/$PLANE_ID/status?namespace=${ORG_PREFIX:-org}-0001&name=default'" || true)
    n=$(printf '%s' "$st" | sed -n 's/.*"connectedAgents":\([0-9]*\).*/\1/p')
    [[ "${n:-0}" -ge "$reps" ]] && break
    sleep 1
  done
  local dt; dt=$(python3 -c "print(f'{$(now)-$t0:.1f}')")
  local ok=1; [[ "${n:-0}" -ge "$reps" ]] || ok=0
  local hs; hs=$(k logs -n "$DP" -l app=cluster-agent --since-time="$since" --tail=-1 2>/dev/null | grep -ciE "handshake|failed to connect|bad handshake|i/o timeout" || true)
  echo "RESULT scenario=connect orgs=$(norgs) agents=$reps ok=$ok seconds=$dt agentConnectErrors=$hs gwConnects=$(gw_log_count 'agent connected successfully' "$since") gwSecretReads=$(gw_log_count 'loaded CA certificate from secret' "$since")"
}

# newauth: create one new org; time until a proxy call for it succeeds.
NEW_IDX="${NEW_IDX:-9001}"
scenario_newauth() {
  local org t0 since code="" deadline
  org=$(printf "${ORG_PREFIX:-org}-%04d" "$NEW_IDX")
  WAIT_NS=true "$HERE/orgs.sh" delete "$NEW_IDX" "$NEW_IDX" >/dev/null 2>&1 || true
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  t0=$(now)
  "$HERE/orgs.sh" create "$NEW_IDX" "$NEW_IDX" >/dev/null
  deadline=$(( $(date +%s) + ${NEWAUTH_TIMEOUT:-300} ))
  # Poll inside the pod (no exec overhead per probe); require 5 consecutive
  # 200s so both gateway replicas / all agent connections are authorized.
  code=$(inpod "end=\$((\$(date +%s)+${NEWAUTH_TIMEOUT:-300})); ok=0; while [ \$(date +%s) -lt \$end ]; do c=\$($CURL -o /dev/null -w '%{http_code}' $G$(proxy_path "$org")); if [ \"\$c\" = 200 ]; then ok=\$((ok+1)); [ \$ok -ge 5 ] && echo 200 && exit 0; else ok=0; fi; sleep 0.2; done; echo \$c")
  local dt; dt=$(python3 -c "print(f'{$(now)-$t0:.1f}')")
  echo "RESULT scenario=newauth orgs=$(norgs) ok=$([[ $code == 200 ]] && echo 1 || echo 0) seconds=$dt lastCode=$code gwSecretReads=$(gw_log_count 'loaded CA certificate from secret' "$since")"
}

# delete: probe an unrelated org continuously while one org is deleted.
scenario_delete() {
  local org since out
  # Use a fresh org (not NEW_IDX): waiting for a namespace to finish
  # terminating can take many minutes on a large k3d cluster.
  local idx=$(( NEW_IDX + 50 + RANDOM % 900 ))
  org=$(printf "${ORG_PREFIX:-org}-%04d" "$idx")
  "$HERE/orgs.sh" create "$idx" "$idx" >/dev/null
  sleep 5
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  ( sleep 3; "$HERE/orgs.sh" delete "$idx" "$idx" >/dev/null ) &
  out=$(inpod "end=\$((\$(date +%s)+${DELETE_WINDOW:-45})); while [ \$(date +%s) -lt \$end ]; do $CURL -o /dev/null -w '%{http_code}\n' $G$(proxy_path ${ORG_PREFIX:-org}-0001); sleep 0.25; done")
  wait
  local total bad
  total=$(printf '%s\n' "$out" | grep -c . || true)
  bad=$(printf '%s\n' "$out" | grep -vc '^200$' || true)
  echo "RESULT scenario=delete orgs=$(norgs) probes=$total failedProbesForOtherOrg=$bad gwDisconnectAll=$(gw_log_count 'disconnected all agents for plane' "$since")"
}

# latency: sequential proxy GETs across random orgs.
scenario_latency() {
  local n="${LAT_N:-200}" max out
  max=$(norgs)
  out=$(inpod "i=0; while [ \$i -lt $n ]; do o=\$(printf '${ORG_PREFIX:-org}-%04d' \$(( (\$(od -An -N2 -tu2 /dev/urandom) % $max) + 1 ))); $CURL -o /dev/null -w '%{http_code} %{time_total}\n' $G/api/proxy/dataplane/$PLANE_ID/\$o/default/k8s/api/v1/namespaces/default; i=\$((i+1)); done")
  printf '%s\n' "$out" | python3 -c '
import sys
rows=[l.split() for l in sys.stdin if l.strip()]
t=sorted(float(r[1]) for r in rows); bad=sum(1 for r in rows if r[0]!="200")
from collections import Counter
codes=",".join(f"{k}:{v}" for k,v in sorted(Counter(r[0] for r in rows).items()))
p=lambda q: t[min(len(t)-1,int(q*len(t)))]*1000
print(f"RESULT scenario=latency orgs='"$max"' n={len(rows)} errors={bad} p50ms={p(.5):.0f} p95ms={p(.95):.0f} p99ms={p(.99):.0f} maxms={t[-1]*1000:.0f} codes={codes}")'
}

ensure_probe
case "${1:-}" in
  connect) scenario_connect ;;
  newauth) scenario_newauth ;;
  delete) scenario_delete ;;
  latency) scenario_latency ;;
  all) scenario_latency; scenario_newauth; scenario_delete; scenario_connect ;;
  *) sed -n '2,8p' "$0"; exit 1 ;;
esac
