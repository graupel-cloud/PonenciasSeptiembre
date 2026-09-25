#!/bin/bash
# ¿Qué vendió ayer una tienda, y de qué productos?   Uso: ~/demo/tienda.sh T042 [YYYY-MM-DD]
set -euo pipefail
TIENDA=${1:?Uso: tienda.sh T042 [YYYY-MM-DD]}
FECHA=${2:-$(TZ=Europe/Madrid date -d yesterday +%F)}

# columnas: dt store_id store_name city region format sku product category units returned lines tickets gross net
FILAS=$(hdfs dfs -cat "/retail/curated/daily_sales/dt=$FECHA/part-*" 2>/dev/null | awk -F'\t' -v t="$TIENDA" '$2 == t')
if [ -z "$FILAS" ]; then
  echo "Sin ventas para $TIENDA el $FECHA"
  exit 1
fi

awk -F'\t' '{ n++; net += $15; uds += $10; tk += $13; name = $3 " (" $4 ", " $5 ")" }
  END {
    printf "\n%s  %s  -  %s\n", $2, name, $1
    printf "Ventas netas: %.2f EUR | Unidades: %d | Tickets: %d | Ticket medio: %.2f EUR | Referencias: %d\n\n",
           net, uds, tk, net / tk, n
    printf "%-7s %-42s %-20s %7s %12s\n", "SKU", "Producto (top 15 por ventas)", "Categoria", "Uds", "Ventas EUR"
  }' <<< "$FILAS"
{ sort -t$'\t' -k15,15 -gr <<< "$FILAS" || true; } | head -15 |
  awk -F'\t' '{ printf "%-7s %-42s %-20s %7d %12.2f\n", $7, substr($8, 1, 42), $9, $10, $15 }'
