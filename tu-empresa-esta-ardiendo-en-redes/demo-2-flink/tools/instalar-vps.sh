#!/usr/bin/env bash
# Se ejecuta EN el VPS (Ubuntu/Debian): instala Docker si falta, comprueba NTP, arranca todo
# y espera a que el job de Flink esté en marcha. Con --prueba lanza además el escenario "escena".
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> VPS: $(nproc) vCPU, $(free -g | awk '/Mem/{print $2}') GB RAM, $(. /etc/os-release; echo "$PRETTY_NAME")"

if ! command -v docker >/dev/null 2>&1; then
  echo "==> Instalando Docker (script oficial get.docker.com)"
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER" >/dev/null 2>&1 || true
DOCKER="docker"; docker info >/dev/null 2>&1 || DOCKER="sudo docker"
$DOCKER compose version

if timedatectl show -p NTPSynchronized --value 2>/dev/null | grep -q yes; then
  echo "==> Reloj sincronizado por NTP"
else
  echo "==> Activando NTP"; sudo timedatectl set-ntp true || true
fi

chmod 600 .env
echo "==> Construyendo y arrancando (la primera vez tarda unos minutos)"
$DOCKER compose up -d --build

echo -n "==> Esperando a que el job de Flink esté RUNNING "
for _ in $(seq 1 120); do
  if curl -fs http://127.0.0.1:8081/jobs/overview 2>/dev/null | grep -q '"state":"RUNNING"'; then echo " listo."; break; fi
  echo -n "."; sleep 3
done
$DOCKER compose ps --format "table {{.Service}}\t{{.Status}}"
echo "==> Ingest (X):"; $DOCKER compose logs --no-log-prefix --tail 6 ingest | grep -oE "event=[a-z_0-9]+.*" || true

if [ "${1:-}" = "--prueba" ]; then
  echo "==> Prueba: escenario 'escena' (debe llegar 1 correo)"
  $DOCKER compose run --rm -T simulate escena
fi
echo "==> FIN"
