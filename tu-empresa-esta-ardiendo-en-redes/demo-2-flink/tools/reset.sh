#!/usr/bin/env bash
# Deja la demo como recién instalada entre tomas de la grabación:
#   - vacía los topics de Kafka (borra su volumen),
#   - borra el estado y los checkpoints de Flink,
#   - borra el fichero de alertas ya enviadas del notificador,
# y vuelve a arrancar todo esperando a que el job esté en marcha.
# No toca los certificados HTTPS de Caddy (volumen caddy-data).
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT=martes-en-x
SERVICES=(ingest notifier panel taskmanager jobmanager kafka-init kafka)

echo "==> Parando servicios"
docker compose stop "${SERVICES[@]}"
docker compose rm -f "${SERVICES[@]}"

echo "==> Borrando topics, estado de Flink y alertas enviadas"
for v in kafka-data flink-checkpoints notifier-data; do
  docker volume rm -f "${PROJECT}_${v}" >/dev/null && echo "    volumen ${PROJECT}_${v} borrado"
done

echo "==> Arrancando"
docker compose up -d

echo -n "==> Esperando a que el job de Flink esté RUNNING "
for _ in $(seq 1 90); do
  if curl -fs http://127.0.0.1:8081/jobs/overview 2>/dev/null | grep -q '"state":"RUNNING"'; then
    echo " listo."
    docker compose ps --format "table {{.Service}}\t{{.Status}}"
    exit 0
  fi
  echo -n "."
  sleep 2
done
echo " el job no ha arrancado en 3 minutos: revisa 'docker compose logs jobmanager taskmanager'"
exit 1
