#!/usr/bin/env bash
# #630 actuator (invoked by the driver's --actuator-command): "upgrade" one node to v2
# by bumping its bind-mounted binary tag in versions.env and force-recreating just it.
# The driver waits for exit 0 (--wait => healthy); it confirms raft re-attach on its next refresh.
set -euo pipefail
# Local colima/lima users can point IC_DOCKER_BIN at their docker-CLI dir; contributors with
# docker already on PATH need nothing.
export PATH="${IC_DOCKER_BIN:+$IC_DOCKER_BIN:}$PATH"
HARNESS="$(cd "$(dirname "$0")" && pwd)"; cd "$HARNESS"
SVC="$1"; N="${SVC##*-}"
grep -v "^IC_VER_${N}=" versions.env > versions.env.tmp 2>/dev/null || true
echo "IC_VER_${N}=v2" >> versions.env.tmp; mv versions.env.tmp versions.env
docker compose --env-file versions.env -f docker-compose.smoke.yml up -d --force-recreate --wait "$SVC" >/dev/null 2>&1
echo "actuator: recreated $SVC on v2"
