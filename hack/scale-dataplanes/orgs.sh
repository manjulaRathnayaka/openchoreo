#!/usr/bin/env bash
# Generate N "org" namespaces, each with one DataPlane CR that shares a single
# planeID (one physical data plane, one agent), to scale-test cluster-gateway.
#
# Usage:
#   orgs.sh create <from> <to>     create org-<from>..org-<to>
#   orgs.sh delete <from> <to>     delete those orgs (DataPlane first, then namespace)
#   orgs.sh count                  number of DataPlanes with the shared planeID
#
# Env:
#   KCTX     kube context (default k3d-oc-scale; refuses to run on anything else
#            unless ALLOW_ANY_CONTEXT=1)
#   CA_MODE  secret | inline (default secret) - where each DataPlane gets its client CA
#   PLANE_ID shared planeID (default shared)
#   AGENT_NS namespace of the data-plane agent whose CA is trusted (default openchoreo-data-plane)
set -euo pipefail

KCTX="${KCTX:-k3d-oc-scale}"
CA_MODE="${CA_MODE:-secret}"
PLANE_ID="${PLANE_ID:-shared}"
AGENT_NS="${AGENT_NS:-openchoreo-data-plane}"
CHUNK="${CHUNK:-100}"

if [[ "$KCTX" != "k3d-oc-scale" && "${ALLOW_ANY_CONTEXT:-0}" != "1" ]]; then
  echo "refusing to run against context '$KCTX' (set ALLOW_ANY_CONTEXT=1)" >&2
  exit 1
fi
k() { kubectl --context "$KCTX" "$@"; }

agent_ca() {
  k get secret cluster-agent-tls -n "$AGENT_NS" -o jsonpath='{.data.ca\.crt}' | base64 -d
}

render_org() {
  local i="$1" ca="$2" ns
  ns=$(printf "${ORG_PREFIX:-org}-%04d" "$i")
  cat <<EOF
---
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
  labels:
    openchoreo.dev/control-plane: "true"
    scale-test: "true"
EOF
  if [[ "$CA_MODE" == "secret" ]]; then
    cat <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: dp-agent-ca
  namespace: $ns
  labels:
    scale-test: "true"
type: Opaque
data:
  ca.crt: $(printf '%s' "$ca" | base64 | tr -d '\n')
---
apiVersion: openchoreo.dev/v1alpha1
kind: DataPlane
metadata:
  name: default
  namespace: $ns
  labels:
    scale-test: "true"
spec:
  planeID: $PLANE_ID
  clusterAgent:
    clientCA:
      secretKeyRef:
        name: dp-agent-ca
        key: ca.crt
EOF
  else
    cat <<EOF
---
apiVersion: openchoreo.dev/v1alpha1
kind: DataPlane
metadata:
  name: default
  namespace: $ns
  labels:
    scale-test: "true"
spec:
  planeID: $PLANE_ID
  clusterAgent:
    clientCA:
      value: |
$(printf '%s\n' "$ca" | sed 's/^/        /')
EOF
  fi
}

cmd="${1:-}"
case "$cmd" in
  create)
    from="$2"; to="$3"
    ca="$(agent_ca)"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    for ((start = from; start <= to; start += CHUNK)); do
      end=$((start + CHUNK - 1)); ((end > to)) && end=$to
      f="$tmp/chunk-$start.yaml"
      : >"$f"
      for ((i = start; i <= end; i++)); do render_org "$i" "$ca" >>"$f"; done
      k apply --server-side -f "$f" >/dev/null
      echo "created ${ORG_PREFIX:-org}-$(printf %04d "$start")..${ORG_PREFIX:-org}-$(printf %04d "$end")"
    done
    ;;
  delete)
    from="$2"; to="$3"
    for ((i = from; i <= to; i++)); do
      ns=$(printf "${ORG_PREFIX:-org}-%04d" "$i")
      k delete dataplane default -n "$ns" --ignore-not-found --wait=true >/dev/null
      k delete namespace "$ns" --ignore-not-found --wait="${WAIT_NS:-false}" >/dev/null
    done
    echo "deleted ${ORG_PREFIX:-org}-$(printf %04d "$from")..${ORG_PREFIX:-org}-$(printf %04d "$to")"
    ;;
  count)
    k get dataplanes -A -o jsonpath='{range .items[*]}{.spec.planeID}{"\n"}{end}' | grep -cx "$PLANE_ID" || true
    ;;
  *)
    sed -n '2,17p' "$0"; exit 1 ;;
esac
