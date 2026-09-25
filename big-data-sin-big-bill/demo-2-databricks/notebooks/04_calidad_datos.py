# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Control de calidad de datos
# MAGIC Contadores del día (líneas leídas, descartadas por motivo, válidas, devoluciones) en `calidad_datos`.
# MAGIC Se ejecuta **en paralelo** con los agregados gold.

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

validada = spark.table(f"{SCHEMA}.ventas_bronze_validada").where(F.col("dt") == DIA)
lineas = spark.table(f"{SCHEMA}.ventas_lineas").where(F.col("dt") == DIA)

metricas = (
    validada.groupBy(F.col("estado").alias("metrica")).agg(F.count("*").alias("valor"))
    .unionByName(validada.agg(F.count("*").alias("valor")).select(F.lit("Líneas leídas").alias("metrica"), "valor"))
    .unionByName(lineas.where("cantidad < 0").agg(F.count("*").alias("valor"))
                 .select(F.lit("Líneas de devolución").alias("metrica"), "valor"))
)
calidad = guardar_dia(
    metricas.select(F.lit(DIA).cast("date").alias("dt"), "metrica", "valor"),
    "calidad_datos",
    "Control: contadores de calidad de datos por día")

filas = calidad.orderBy(F.desc("valor")).collect()
leidas = next(f.valor for f in filas if f.metrica == "Líneas leídas")
descartadas = sum(f.valor for f in filas if f.metrica.startswith("Descartada"))
display(calidad.orderBy(F.desc("valor")))

# COMMAND ----------

dbutils.notebook.exit(f"Calidad {DIA}: {num(descartadas)} de {num(leidas)} líneas descartadas "
                      f"({100 * descartadas / leidas:.2f} %)")
