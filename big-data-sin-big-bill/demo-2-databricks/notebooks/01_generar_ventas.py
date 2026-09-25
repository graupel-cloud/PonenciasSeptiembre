# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Cajas registradoras → landing
# MAGIC Genera las ventas sintéticas de **un día** de la cadena (400 tiendas, ~550 productos) y las deja como ficheros
# MAGIC CSV en el volume `landing`, tal y como llegarían de las cajas: con algunos registros **corruptos**, líneas de
# MAGIC **otro día** (subidas tarde) y **devoluciones**, para que el ETL tenga algo que limpiar.
# MAGIC
# MAGIC Formato de línea: `ticket_id;timestamp;tienda_id;linea_no;sku;cantidad;precio_unitario;descuento_pct;pago`

# COMMAND ----------

# MAGIC %run ./00_comun

# COMMAND ----------

dbutils.widgets.text("escala", "1.0", "Escala de volumen")

import random

ESCALA = float(dbutils.widgets.get("escala") or "1.0")
t0 = T0

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.landing")
print(f"Generando ventas de {DIA} en {LANDING}/dt={DIA}")

# COMMAND ----------

# MAGIC %md ## Maestros: tiendas y productos (deterministas, siempre la misma cadena)

# COMMAND ----------

CIUDADES = [
    ("Madrid", "Madrid"), ("Alcalá de Henares", "Madrid"), ("Getafe", "Madrid"), ("Móstoles", "Madrid"),
    ("Barcelona", "Cataluña"), ("L'Hospitalet", "Cataluña"), ("Badalona", "Cataluña"), ("Tarragona", "Cataluña"),
    ("Girona", "Cataluña"), ("Lleida", "Cataluña"), ("Valencia", "C. Valenciana"), ("Alicante", "C. Valenciana"),
    ("Elche", "C. Valenciana"), ("Castellón", "C. Valenciana"), ("Sevilla", "Andalucía"), ("Málaga", "Andalucía"),
    ("Córdoba", "Andalucía"), ("Granada", "Andalucía"), ("Almería", "Andalucía"), ("Cádiz", "Andalucía"),
    ("Bilbao", "País Vasco"), ("Vitoria", "País Vasco"), ("San Sebastián", "País Vasco"),
    ("Zaragoza", "Aragón"), ("Huesca", "Aragón"), ("Valladolid", "Castilla y León"), ("Burgos", "Castilla y León"),
    ("León", "Castilla y León"), ("Salamanca", "Castilla y León"), ("Toledo", "Castilla-La Mancha"),
    ("Albacete", "Castilla-La Mancha"), ("A Coruña", "Galicia"), ("Vigo", "Galicia"), ("Santiago", "Galicia"),
    ("Oviedo", "Asturias"), ("Gijón", "Asturias"), ("Santander", "Cantabria"), ("Pamplona", "Navarra"),
    ("Logroño", "La Rioja"), ("Murcia", "Murcia"), ("Cartagena", "Murcia"), ("Palma", "Baleares"),
    ("Las Palmas", "Canarias"), ("Santa Cruz de Tenerife", "Canarias"), ("Badajoz", "Extremadura"),
]
CATEGORIAS = [  # categoría, precio mín, precio máx, artículos
    ("Frutas y verduras", 0.60, 4.50, ["Manzana Golden", "Plátano de Canarias", "Tomate rama", "Lechuga iceberg",
                                       "Naranja zumo", "Patata lavada", "Cebolla", "Pimiento rojo", "Aguacate", "Fresón"]),
    ("Carne y charcutería", 2.50, 18.00, ["Pechuga de pollo", "Filete de ternera", "Lomo de cerdo", "Jamón serrano",
                                          "Chorizo ibérico", "Pavo en lonchas", "Hamburguesa mixta", "Salchichas frescas"]),
    ("Pescadería", 3.00, 22.00, ["Salmón fresco", "Merluza", "Gambas", "Atún en aceite", "Bacalao desalado",
                                 "Mejillones", "Sardinas"]),
    ("Lácteos y huevos", 0.70, 6.50, ["Leche entera", "Leche semidesnatada", "Yogur natural", "Queso curado",
                                      "Queso fresco", "Mantequilla", "Huevos camperos", "Nata para cocinar", "Kéfir"]),
    ("Panadería", 0.40, 4.00, ["Barra de pan", "Pan de molde", "Croissant", "Magdalenas", "Pan integral", "Baguette"]),
    ("Despensa", 0.50, 7.00, ["Aceite de oliva virgen extra", "Arroz redondo", "Macarrones", "Lentejas", "Garbanzos",
                              "Tomate frito", "Café molido", "Galletas María", "Cereales", "Azúcar", "Harina"]),
    ("Bebidas", 0.35, 9.00, ["Agua mineral", "Refresco de cola", "Cerveza", "Zumo de naranja", "Vino tinto",
                             "Vino blanco", "Bebida isotónica", "Refresco de limón"]),
    ("Congelados", 1.20, 8.50, ["Pizza congelada", "Guisantes", "Helado de vainilla", "Croquetas",
                                "Varitas de merluza", "Verduras para salteado"]),
    ("Droguería", 0.90, 12.00, ["Detergente líquido", "Suavizante", "Lavavajillas", "Lejía", "Papel higiénico",
                                "Rollo de cocina", "Limpiahogar"]),
    ("Higiene personal", 1.00, 10.00, ["Champú", "Gel de ducha", "Pasta de dientes", "Desodorante",
                                       "Crema hidratante", "Cepillo de dientes"]),
    ("Mascotas", 1.50, 25.00, ["Pienso perro", "Pienso gato", "Arena para gatos", "Snacks perro"]),
]
VARIANTES = ["Marca propia", "Premium", "Eco", "Pack ahorro", "Formato familiar", "Clásico", "Origen España", "Selección"]


def construir_tiendas():
    r = random.Random(20260401)
    tiendas, por_ciudad = [], {}
    for i in range(1, 401):
        ciudad, region = CIUDADES[min(len(CIUDADES) - 1, int(r.random() ** 1.6 * len(CIUDADES)))]
        p = r.random()
        if p < 0.10:
            formato, trafico = "Hipermercado", 2.8 + r.random() * 0.8
        elif p < 0.70:
            formato, trafico = "Supermercado", 0.8 + r.random() * 0.5
        else:
            formato, trafico = "Express", 0.35 + r.random() * 0.25
        por_ciudad[ciudad] = por_ciudad.get(ciudad, 0) + 1
        tiendas.append((f"T{i:03d}", f"{formato} {ciudad} {por_ciudad[ciudad]}", ciudad, region, formato, round(trafico, 3)))
    return tiendas


def construir_productos():
    r = random.Random(20260402)
    productos, n = [], 0
    for categoria, pmin, pmax, articulos in CATEGORIAS:
        for articulo in articulos:
            for variante in VARIANTES:
                if r.random() < 0.25:
                    continue  # no todos los artículos existen en todas las variantes
                n += 1
                precio = round(pmin + r.random() ** 1.5 * (pmax - pmin), 2)
                popularidad = 1.0 / (1 + r.randrange(400)) ** 0.6  # pocos productos venden mucho
                if categoria in ("Panadería", "Lácteos y huevos", "Frutas y verduras"):
                    popularidad *= 2.5  # compra diaria
                productos.append((f"P{n:05d}", f"{articulo} {variante}", categoria, precio, popularidad))
    return productos


tiendas = construir_tiendas()
productos = construir_productos()
(spark.createDataFrame(tiendas, "tienda_id string, tienda string, ciudad string, region string, formato string, trafico double")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.dim_tiendas"))
spark.sql(f"COMMENT ON TABLE {SCHEMA}.dim_tiendas IS 'Maestro de las 400 tiendas de la cadena (formato, ciudad, región)'")
(spark.createDataFrame(productos, "sku string, producto string, categoria string, precio double, popularidad double")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.dim_productos"))
spark.sql(f"COMMENT ON TABLE {SCHEMA}.dim_productos IS 'Maestro de productos (SKU, categoría, precio de tarifa)'")
print(f"Maestros: {len(tiendas)} tiendas, {len(productos)} productos")

# COMMAND ----------

# MAGIC %md ## Gustos regionales y volumen de tickets por tienda

# COMMAND ----------

# Cada región tiene sus preferencias: un peso por categoría y algunos "favoritos locales".
# Se materializan como SLOTS huecos por región; elegir un hueco al azar = elegir producto con esa probabilidad.
SLOTS = 5000
huecos = []
for region in sorted({t[3] for t in tiendas}):
    r = random.Random(f"region-{region}")
    peso_cat, pesos = {}, []
    for sku, _, categoria, _, popularidad in productos:
        cf = peso_cat.setdefault(categoria, 0.6 + r.random() * 1.2)
        local = 3 + r.random() * 5 if r.random() < 0.05 else 0.5 + r.random()
        pesos.append(popularidad * cf * local)
    total, acc, j = sum(pesos), 0.0, 0
    acumulado = []
    for w in pesos:
        acc += w
        acumulado.append(acc / total)
    for k in range(SLOTS):
        x = (k + 0.5) / SLOTS
        while acumulado[j] < x:
            j += 1
        huecos.append((region, k, productos[j][0], productos[j][3]))
huecos_df = spark.createDataFrame(huecos, "region string, slot int, sku string, precio double")

# Tickets del día por tienda: tráfico de la tienda x día de la semana x "suerte" del día
factor_dia = {5: 1.2, 6: 1.35, 7: 0.7}.get(FECHA.isoweekday(), 1.0)
volumen = []
for tienda_id, _, _, region, _, trafico in tiendas:
    suerte = 0.85 + random.Random(f"{DIA}-{tienda_id}").random() * 0.3
    volumen.append((tienda_id, region, int(2500 * trafico * factor_dia * suerte * ESCALA)))
volumen_df = spark.createDataFrame(volumen, "tienda_id string, region string, n_tickets int")
total_tickets = sum(v[2] for v in volumen)
print(f"Tickets a generar: {total_tickets:,}")

# COMMAND ----------

# MAGIC %md ## Tickets y líneas de venta (en paralelo, sobre Spark)

# COMMAND ----------

dia_anterior = (FECHA - datetime.timedelta(days=1)).isoformat()
compacto = DIA.replace("-", "")

tickets = (
    volumen_df.repartition(40, "tienda_id")
    .withColumn("t", F.explode(F.sequence(F.lit(1), "n_tickets")))
    # horario 08:00-22:00 con picos a mediodía y por la tarde
    .withColumn("x", F.rand())
    .withColumn("m", F.when(F.col("x") < 0.35, 300 + F.randn() * 80)
                      .when(F.col("x") < 0.75, 660 + F.randn() * 80)
                      .otherwise(F.rand() * 840))
    .withColumn("m", F.greatest(F.lit(0), F.least(F.lit(839), F.floor("m"))).cast("int"))
    .withColumn("ts", F.when(F.rand() < 0.002,  # subida tardía: línea del día anterior
                             F.format_string("%sT21:%02d:%02d", F.lit(dia_anterior),
                                             (F.rand() * 60).cast("int"), (F.rand() * 60).cast("int")))
                       .otherwise(F.format_string("%sT%02d:%02d:%02d", F.lit(DIA), (F.col("m") / 60).cast("int") + 8,
                                                  F.col("m") % 60, (F.rand() * 60).cast("int"))))
    .withColumn("ticket_id", F.format_string("%s-%s-%06d", F.col("tienda_id"), F.lit(compacto), F.col("t")))
    .withColumn("pago", F.when(F.rand() < 0.72, "TARJETA").when(F.rand() < 0.6, "EFECTIVO").otherwise("APP"))
    .withColumn("n_lineas", (1 + F.least(F.lit(24), F.floor(-F.log(1 - F.rand()) * 5.5))).cast("int"))
)

lineas = (
    tickets
    .withColumn("linea_no", F.explode(F.sequence(F.lit(1), "n_lineas")))
    .withColumn("slot", (F.rand() * SLOTS).cast("int"))
    .join(F.broadcast(huecos_df), ["region", "slot"])
    .withColumn("cantidad", 1 + F.when(F.rand() < 0.25, (F.rand() * 4).cast("int")).otherwise(0))
    .withColumn("cantidad", F.when(F.rand() < 0.004, -F.col("cantidad")).otherwise(F.col("cantidad")))  # devolución
    .withColumn("precio_u", F.col("precio") * (0.97 + F.rand() * 0.06))
    .withColumn("d", F.rand())
    .withColumn("dto", F.when(F.col("d") < 0.80, 0).when(F.col("d") < 0.92, 10).when(F.col("d") < 0.98, 20).otherwise(30))
    .withColumn("linea", F.when(F.rand() < 0.0005,  # registro corrupto de una caja
                                F.concat(F.lit("#ERR#"), F.col("ticket_id"), F.lit(";;"),
                                         (F.rand() * 1000).cast("int").cast("string"), F.lit(";NaN")))
                          .otherwise(F.concat_ws(";", "ticket_id", "ts", "tienda_id", F.col("linea_no").cast("string"),
                                                 "sku", F.col("cantidad").cast("string"),
                                                 F.format_string("%.2f", "precio_u"), F.col("dto").cast("string"), "pago")))
    .select("linea")
)

destino = f"{LANDING}/dt={DIA}"
lineas.write.mode("overwrite").text(destino)

# COMMAND ----------

n_lineas = spark.read.text(destino).count()
n_ficheros = len([f for f in dbutils.fs.ls(destino) if f.name.startswith("part-")])
resumen = (f"Generados {total_tickets:,} tickets y {n_lineas:,} líneas de venta de {DIA} "
           f"en {n_ficheros} ficheros ({destino}) en {time.time() - t0:.1f} s")
print(resumen)
dbutils.notebook.exit(resumen)
