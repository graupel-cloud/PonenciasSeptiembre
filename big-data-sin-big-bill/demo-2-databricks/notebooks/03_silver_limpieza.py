# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Silver: validación, tipado y limpieza
# MAGIC Cada línea de bronze se clasifica en **Válida** o en un motivo de descarte (registro corrupto, fuera de fecha,
# MAGIC producto desconocido). Las válidas se tipan, se calculan importes y se guardan en `ventas_lineas`; la
# MAGIC clasificación completa queda en `ventas_bronze_validada` para el control de calidad.

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

bronze = spark.table(f"{SCHEMA}.ventas_bronze").where(F.col("dt") == DIA)
skus = spark.table(f"{SCHEMA}.dim_productos").select("sku", F.lit(True).alias("sku_ok"))


def campo(i, tipo=None):
    """Campo i (base 0), tolerante: NULL si la línea tiene menos campos o no se puede convertir (modo ANSI)."""
    valor = f"try_element_at(split(linea, ';', -1), {i + 1})"
    return F.expr(f"try_cast({valor} as {tipo})" if tipo else valor)


validada = guardar_dia(
    bronze.select(
        "dt", "linea",
        F.size(F.split("linea", ";", -1)).alias("n_campos"),
        campo(0).alias("ticket_id"), campo(1).alias("ts_txt"), campo(2).alias("tienda_id"),
        campo(3, "int").alias("linea_no"), campo(4).alias("sku"), campo(5, "int").alias("cantidad"),
        campo(6, "double").alias("precio_u"), campo(7, "int").alias("dto"), campo(8).alias("pago"))
    .join(F.broadcast(skus), "sku", "left")
    .withColumn("estado",
                F.when(F.col("linea").startswith("#") | (F.col("n_campos") != 9)
                       | F.col("cantidad").isNull() | F.col("precio_u").isNull(), "Descartada: registro corrupto")
                 .when(~F.col("ts_txt").startswith(DIA), "Descartada: fuera de fecha")
                 .when(F.col("sku_ok").isNull(), "Descartada: producto desconocido")
                 .otherwise("Válida"))
    .drop("sku_ok", "n_campos"),
    "ventas_bronze_validada",
    "Líneas de bronze parseadas y clasificadas (Válida / motivo de descarte)")

lineas = guardar_dia(
    validada.where(F.col("estado") == "Válida").select(
        "dt", "ticket_id", F.to_timestamp("ts_txt").alias("ts"), "tienda_id", "linea_no", "sku", "cantidad",
        "precio_u", "dto", "pago",
        F.round(F.col("cantidad") * F.col("precio_u"), 2).alias("importe_bruto"),
        F.round(F.col("cantidad") * F.col("precio_u") * (100 - F.col("dto")) / 100, 2).alias("importe_neto")),
    "ventas_lineas",
    "Silver: líneas de venta válidas del día, tipadas y con importe bruto y neto")

display(validada.groupBy("estado").count().orderBy(F.desc("count")))

# COMMAND ----------

n = lineas.count()
dbutils.notebook.exit(f"Silver {DIA}: {num(n)} líneas válidas en {time.time() - T0:.1f} s")
