#!/usr/bin/env bash
# Controller status-refresh throughput: DataPlane reconciles completed in the
# last WINDOW seconds, scaled to per-minute. Ideal is ~1 per DataPlane per
# minute (the status refresh interval).
set -euo pipefail
KCTX="${KCTX:-k3d-oc-scale}"
WINDOW="${WINDOW:-60}"
HERE="$(cd "$(dirname "$0")" && pwd)"
n=$(kubectl --context "$KCTX" logs -n openchoreo-control-plane deploy/controller-manager --since="${WINDOW}s" \
  | grep -c "populated agent connection status" || true)
orgs=$("$HERE/orgs.sh" count)
conc=$(kubectl --context "$KCTX" get deploy -n openchoreo-control-plane controller-manager \
  -o jsonpath='{.spec.template.spec.containers[0].args}' | grep -o 'max-concurrent-reconciles=[0-9]*' | cut -d= -f2)
python3 -c "n=$n*60/$WINDOW; print(f'RESULT scenario=throughput orgs=$orgs concurrency=${conc:-1} refreshesPerMin={n:.0f} refreshEveryMin={$orgs/max(n,1):.1f}')"
