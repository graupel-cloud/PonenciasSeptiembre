#!/bin/bash
# Demo "ventas de ayer": genera el día sintético de las 400 tiendas y ejecuta el ETL en YARN.
# Uso (en node1, como el usuario de la VM):  ~/demo/run-demo.sh [YYYY-MM-DD]     (por defecto: ayer)
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
cd "$(dirname "$0")"
FECHA=${1:-$(TZ=Europe/Madrid date -d yesterday +%F)}
PROGRESO='Running job|map +[0-9]+% reduce|completed successfully|failed'

paso() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }

paso "Cluster: nodos de HDFS y YARN"
sudo -u hdfs hdfs dfsadmin -report 2>/dev/null | grep -E "^Live datanodes|^Name:" || true
yarn node -list 2>/dev/null | tail -n +2

if [ "${SOLO_ETL:-0}" != "1" ]; then
  paso "1/3  Cajas registradoras -> HDFS: ventas sintéticas de $FECHA (400 tiendas)"
  hadoop jar retail-etl.jar generate --date "$FECHA" 2>&1 | grep --line-buffered -E "$PROGRESO|Generados"
fi

paso "2/3  ETL en YARN: limpieza + ventas tienda x producto + resumen"
hadoop jar retail-etl.jar etl --date "$FECHA" 2> >(grep --line-buffered -E "$PROGRESO" >&2)

paso "3/3  Resultado en HDFS"
hdfs dfs -du -s -h "/retail/raw/sales/dt=$FECHA" "/retail/curated/daily_sales/dt=$FECHA" "/retail/reports/dt=$FECHA"
echo
echo "Detalle de una tienda:  ~/demo/tienda.sh T042 $FECHA"
