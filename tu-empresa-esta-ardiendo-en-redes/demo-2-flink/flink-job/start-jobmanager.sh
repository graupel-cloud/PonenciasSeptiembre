#!/usr/bin/env bash
# Arranca el JobManager en modo aplicación. Si hay un checkpoint completo del job, lo restaura.
#
# Sin alta disponibilidad (que exigiría ZooKeeper o Kubernetes), un JobManager que se reinicia
# lanzaría el job desde cero y perdería el estado. Para evitarlo:
#   - el job id es fijo (FLINK_JOB_ID), así sus checkpoints siempre están en la misma carpeta;
#   - los checkpoints se conservan (RETAIN_ON_CANCELLATION) en un volumen compartido;
#   - aquí se busca el último chk-N con _metadata y se arranca con --fromSavepoint.
set -euo pipefail

JOB_ID="${FLINK_JOB_ID:-00000000000000000000000000000001}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/opt/flink/checkpoints}"
JOB_DIR="${CHECKPOINT_ROOT}/${JOB_ID}"

latest=""
if [ -d "$JOB_DIR" ]; then
  latest=$(find "$JOB_DIR" -maxdepth 2 -path "*/chk-*/_metadata" -printf '%h\n' 2>/dev/null \
    | awk -F'chk-' '{print $NF" "$0}' | sort -n | tail -1 | cut -d' ' -f2-)
fi

args=(standalone-job --job-classname demo.reputacion.ReputationJob --job-id "$JOB_ID")
if [ -n "$latest" ]; then
  echo "event=restore_from_checkpoint job_id=$JOB_ID path=$latest"
  args+=(--fromSavepoint "$latest" --allowNonRestoredState)
else
  echo "event=fresh_start job_id=$JOB_ID msg=\"no hay checkpoints previos\""
fi

exec /docker-entrypoint.sh "${args[@]}"
