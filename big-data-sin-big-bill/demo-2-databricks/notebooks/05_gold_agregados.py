# Databricks notebook source
# MAGIC %md
# MAGIC # 5 · Gold: la respuesta a la pregunta de la mañana
# MAGIC - `ventas_tienda_producto`: cuánto vendió cada tienda de cada producto (enriquecido con los maestros)
# MAGIC - `resumen_tienda`: ventas, tickets, ticket medio y top 3 de productos por tienda
# MAGIC - `resumen_region`, `resumen_categoria`

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

from pyspark.sql.window import Window

lineas = spark.table(f"{SCHEMA}.ventas_lineas").where(F.col("dt") == DIA)
tiendas = spark.table(f"{SCHEMA}.dim_tiendas").select("tienda_id", "tienda", "ciudad", "region", "formato")
productos = spark.table(f"{SCHEMA}.dim_productos").select("sku", "producto", "categoria")

tienda_producto = guardar_dia(
    lineas.groupBy("dt", "tienda_id", "sku").agg(
        F.sum("cantidad").alias("unidades"),
        F.sum(F.when(F.col("cantidad") < 0, -F.col("cantidad")).otherwise(0)).alias("unidades_devueltas"),
        F.count("*").alias("lineas"),
        F.countDistinct("ticket_id").alias("tickets"),
        F.round(F.sum("importe_bruto"), 2).alias("ventas_brutas"),
        F.round(F.sum("importe_neto"), 2).alias("ventas_netas"))
    .join(F.broadcast(tiendas), "tienda_id")
    .join(F.broadcast(productos), "sku")
    .select("dt", "tienda_id", "tienda", "ciudad", "region", "formato", "sku", "producto", "categoria",
            "unidades", "unidades_devueltas", "lineas", "tickets", "ventas_brutas", "ventas_netas"),
    "ventas_tienda_producto",
    "Gold: ventas del día por tienda y producto (unidades, devoluciones, tickets, ventas brutas y netas)")

top3 = (tienda_producto
        .withColumn("rk", F.row_number().over(Window.partitionBy("tienda_id").orderBy(F.desc("ventas_netas"))))
        .where("rk <= 3")
        .groupBy("tienda_id")
        .agg(F.concat_ws(" | ", F.transform(F.array_sort(F.collect_list(F.struct("rk", "producto"))),
                                            lambda s: s["producto"])).alias("top3_productos")))

resumen_tienda = guardar_dia(
    lineas.groupBy("dt", "tienda_id").agg(
        F.round(F.sum("importe_neto"), 2).alias("ventas_netas"),
        F.sum("cantidad").alias("unidades"),
        F.countDistinct("ticket_id").alias("tickets"))
    .withColumn("ticket_medio", F.round(F.col("ventas_netas") / F.col("tickets"), 2))
    .join(F.broadcast(tiendas), "tienda_id")
    .join(top3, "tienda_id")
    .select("dt", "tienda_id", "tienda", "ciudad", "region", "formato", "ventas_netas", "unidades", "tickets",
            "ticket_medio", "top3_productos"),
    "resumen_tienda",
    "Gold: resumen del día por tienda con su top 3 de productos")

guardar_dia(
    resumen_tienda.groupBy("dt", "region").agg(
        F.round(F.sum("ventas_netas"), 2).alias("ventas_netas"), F.sum("tickets").alias("tickets"),
        F.count("*").alias("tiendas")),
    "resumen_region",
    "Gold: ventas del día por región")

guardar_dia(
    tienda_producto.groupBy("dt", "categoria").agg(
        F.round(F.sum("ventas_netas"), 2).alias("ventas_netas"), F.sum("unidades").alias("unidades")),
    "resumen_categoria",
    "Gold: ventas del día por categoría de producto")

display(resumen_tienda.orderBy(F.desc("ventas_netas")))

# COMMAND ----------

tot = resumen_tienda.agg(F.sum("ventas_netas").alias("v"), F.count("*").alias("n")).first()
dbutils.notebook.exit(f"Gold {DIA}: {eur(tot.v)} en {tot.n} tiendas en {time.time() - T0:.1f} s")
