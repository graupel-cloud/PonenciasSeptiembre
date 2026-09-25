-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 7 · Explorar los datos (para enseñar en la demo)
-- MAGIC Ejecuta las celdas una a una. Todas usan el **último día cargado** salvo que se indique otra cosa.
-- MAGIC
-- MAGIC `landing (ficheros)` → `ventas_bronze` → `ventas_lineas` → `ventas_tienda_producto` / `resumen_*`

-- COMMAND ----------

USE CATALOG databrick_graupel_demo;
USE SCHEMA retail_demo;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md ## 1. Lo que llega de las cajas: ficheros en el volume `landing`

-- COMMAND ----------

LIST '/Volumes/databrick_graupel_demo/retail_demo/landing/ventas/';

-- COMMAND ----------

-- Primeras líneas en crudo de un día (formato ticket;timestamp;tienda;línea;sku;cantidad;precio;dto;pago)
SELECT value AS linea_en_crudo
FROM text.`/Volumes/databrick_graupel_demo/retail_demo/landing/ventas/`
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 2. Bronze: las mismas líneas, ya en Delta, con su fichero de origen

-- COMMAND ----------

SELECT dt, count(*) AS lineas, count(DISTINCT fichero) AS ficheros, max(ingesta_ts) AS ultima_ingesta
FROM ventas_bronze GROUP BY dt ORDER BY dt;

-- COMMAND ----------

-- Las líneas "sucias" que el ETL tiene que descartar
SELECT estado, linea
FROM ventas_bronze_validada
WHERE dt = (SELECT max(dt) FROM ventas_bronze_validada) AND estado <> 'Válida'
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 3. Silver: líneas válidas, tipadas y con importes

-- COMMAND ----------

SELECT * FROM ventas_lineas WHERE dt = (SELECT max(dt) FROM ventas_lineas) LIMIT 20;

-- COMMAND ----------

-- Un ticket completo
SELECT l.ticket_id, l.ts, l.linea_no, p.producto, l.cantidad, l.precio_u, l.dto, l.importe_neto, l.pago
FROM ventas_lineas l JOIN dim_productos p USING (sku)
WHERE l.ticket_id = (SELECT min(ticket_id) FROM ventas_lineas WHERE dt = (SELECT max(dt) FROM ventas_lineas)
                     AND tienda_id = 'T042')
ORDER BY l.linea_no;

-- COMMAND ----------

-- MAGIC %md ## 4. Gold: ¿cuánto vendió ayer cada tienda y de qué productos?

-- COMMAND ----------

SELECT tienda_id, tienda, region, formato, ventas_netas, tickets, ticket_medio, top3_productos
FROM resumen_tienda WHERE dt = (SELECT max(dt) FROM resumen_tienda)
ORDER BY ventas_netas DESC;

-- COMMAND ----------

-- Detalle de una tienda: qué vendió ayer la T042
SELECT producto, categoria, unidades, tickets, ventas_netas
FROM ventas_tienda_producto
WHERE dt = (SELECT max(dt) FROM ventas_tienda_producto) AND tienda_id = 'T042'
ORDER BY ventas_netas DESC LIMIT 20;

-- COMMAND ----------

-- Comparativa entre días cargados
SELECT dt, date_format(dt, 'EEEE') AS dia, round(sum(ventas_netas), 2) AS ventas_netas, sum(tickets) AS tickets,
       round(sum(ventas_netas) / sum(tickets), 2) AS ticket_medio
FROM resumen_tienda GROUP BY dt ORDER BY dt;

-- COMMAND ----------

SELECT dt, metrica, valor FROM calidad_datos ORDER BY dt DESC, valor DESC;

-- COMMAND ----------

-- MAGIC %md ## 5. Delta Lake: historial de versiones (cada ejecución del ETL queda registrada)

-- COMMAND ----------

DESCRIBE HISTORY resumen_tienda;
