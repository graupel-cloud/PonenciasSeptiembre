# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Bronze: ingesta en crudo
# MAGIC Carga las líneas **tal cual** llegan de las cajas (ficheros del volume `landing`) en la tabla Delta
# MAGIC `ventas_bronze`, guardando de qué fichero viene cada línea y cuándo se ingirió.

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

origen = f"{LANDING}/dt={DIA}"
ficheros = [f for f in dbutils.fs.ls(origen) if f.name.startswith("part-")]
print(f"{len(ficheros)} ficheros en {origen} ({sum(f.size for f in ficheros) / 1e6:,.0f} MB)")

bronze = guardar_dia(
    spark.read.text(origen).select(
        F.lit(DIA).cast("date").alias("dt"),
        F.col("value").alias("linea"),
        F.col("_metadata.file_name").alias("fichero"),
        F.current_timestamp().alias("ingesta_ts")),
    "ventas_bronze",
    "Bronze: líneas de venta en crudo tal y como llegan de las cajas (una fila por línea de fichero)")

n = bronze.count()
display(bronze.limit(20))

# COMMAND ----------

dbutils.notebook.exit(f"Bronze {DIA}: {num(n)} líneas de {len(ficheros)} ficheros en {time.time() - T0:.1f} s")
