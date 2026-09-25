# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Común: parámetros y utilidades
# MAGIC Lo incluyen el resto de notebooks con `%run ./00_comun`. Parámetro `fecha` (YYYY-MM-DD); vacío = **ayer**.

# COMMAND ----------

dbutils.widgets.text("fecha", "", "Fecha (YYYY-MM-DD, vacío = ayer)")
dbutils.widgets.text("catalogo", "databrick_graupel_demo", "Catálogo")

# COMMAND ----------

import datetime
import time
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

_fecha = dbutils.widgets.get("fecha").strip()
FECHA = (datetime.date.fromisoformat(_fecha) if _fecha
         else datetime.datetime.now(ZoneInfo("Europe/Madrid")).date() - datetime.timedelta(days=1))
DIA = FECHA.isoformat()
CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA = f"{CATALOGO}.retail_demo"
LANDING = f"/Volumes/{CATALOGO}/retail_demo/landing/ventas"
DIAS_SEMANA = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
T0 = time.time()


def guardar_dia(df, tabla, comentario=None):
    """Escribe el día en una tabla Delta sustituyendo solo las filas de ese dt (idempotente)."""
    nombre = f"{SCHEMA}.{tabla}"
    if spark.catalog.tableExists(nombre):
        df.write.mode("overwrite").option("replaceWhere", f"dt = '{DIA}'").saveAsTable(nombre)
    else:
        df.write.saveAsTable(nombre)
    if comentario:
        spark.sql(f"COMMENT ON TABLE {nombre} IS '{comentario}'")
    return spark.table(nombre).where(F.col("dt") == DIA)


def eur(x):
    return f"{x:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def num(x):
    return f"{x:,}".replace(",", ".")


print(f"Fecha de proceso: {DIA} ({DIAS_SEMANA[FECHA.weekday()]})  ·  esquema {SCHEMA}")
