# Databricks notebook source
# MAGIC %md
# MAGIC # 6 · Informe de la mañana
# MAGIC **¿Cuánto vendió ayer cada tienda y de qué productos?** (a partir de las tablas gold y de calidad).
# MAGIC El mismo informe, en visual, está en el dashboard *Retail · Informe de ventas diario*.

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

def dia(tabla):
    return spark.table(f"{SCHEMA}.{tabla}").where(F.col("dt") == DIA)


resumen_tienda, resumen_region = dia("resumen_tienda"), dia("resumen_region")
resumen_categoria, calidad = dia("resumen_categoria"), dia("calidad_datos")

tot = resumen_tienda.agg(F.sum("ventas_netas").alias("v"), F.sum("unidades").alias("u"),
                         F.sum("tickets").alias("t"), F.count("*").alias("n")).first()
r = ["=" * 100, f"  INFORME DE VENTAS  -  {DIA} ({DIAS_SEMANA[FECHA.weekday()]})", "=" * 100,
     f"  Ventas netas de la cadena: {eur(tot.v)}  |  Tiendas: {tot.n}  |  Unidades: {num(tot.u)}",
     f"  Tickets: {num(tot.t)}  |  Ticket medio: {eur(tot.v / tot.t)}", "", "  TOP 10 TIENDAS"]
for f in resumen_tienda.orderBy(F.desc("ventas_netas")).limit(10).collect():
    r.append(f"  {f.tienda_id:<5} {f.tienda[:32]:<32} {f.region[:18]:<18} {eur(f.ventas_netas):>15} "
             f"{num(f.tickets):>7}  {f.top3_productos[:60]}")
r += ["", "  LAS 5 TIENDAS QUE MENOS VENDIERON"]
for f in resumen_tienda.orderBy("ventas_netas").limit(5).collect():
    r.append(f"  {f.tienda_id:<5} {f.tienda[:32]:<32} {f.region[:18]:<18} {eur(f.ventas_netas):>15} {num(f.tickets):>7}")
r += ["", "  VENTAS POR REGIÓN"]
regiones = resumen_region.orderBy(F.desc("ventas_netas")).collect()
for f in regiones:
    r.append(f"  {f.region:<20} {eur(f.ventas_netas):>16} {100 * f.ventas_netas / tot.v:5.1f}%  "
             + "#" * round(30 * f.ventas_netas / regiones[0].ventas_netas))
r += ["", "  VENTAS POR CATEGORÍA"]
for f in resumen_categoria.orderBy(F.desc("ventas_netas")).collect():
    r.append(f"  {f.categoria:<20} {eur(f.ventas_netas):>16} {100 * f.ventas_netas / tot.v:5.1f}%  {num(f.unidades):>10} uds")
r += ["", "  CALIDAD DE DATOS"]
for f in calidad.orderBy(F.desc("valor")).collect():
    r.append(f"  {f.metrica:<40} {num(f.valor):>12}")
r.append("=" * 100)
print("\n".join(r))

# COMMAND ----------

display(resumen_tienda.orderBy(F.desc("ventas_netas")).select(
    "tienda_id", "tienda", "region", "formato", "ventas_netas", "tickets", "ticket_medio", "top3_productos"))

# COMMAND ----------

dbutils.notebook.exit(f"{DIA}: {eur(tot.v)} en {tot.n} tiendas, {num(tot.t)} tickets, ticket medio {eur(tot.v / tot.t)}")
