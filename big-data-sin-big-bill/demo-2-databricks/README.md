# Retail · informe diario de ventas en Databricks

El mismo caso de uso que la demo de Ambari: una cadena de 400 tiendas y la pregunta de cada mañana,
*¿cuánto vendió ayer cada tienda y de qué productos?* Usa cómputo serverless y Unity Catalog.

| Ruta | Contenido |
|---|---|
| `notebooks/` | Notebooks en formato *source* de Databricks (se abren como notebooks en una Git folder) |
| `dashboards/Retail_informe_ventas_diario.lvdash.json` | Dashboard AI/BI exportado (importable desde el workspace) |
| `dashboard.py` | Definición del dashboard como código (la usa `deploy.py`) |
| `deploy.py` | Sube notebooks, crea/actualiza los 3 jobs y publica el dashboard |

Notebooks: `00_comun` (parámetros) · `01_generar_ventas` (cajas → landing) · `02_bronze_ingesta` ·
`03_silver_limpieza` · `04_calidad_datos` · `05_gold_agregados` · `06_informe` · `07_explorar_datos` (SQL, para la demo).

Despliegue:
```bash
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<token personal>      # no lo guardes en el repo
python3 deploy.py                                                  # notebooks + jobs + dashboard
python3 deploy.py --run "Retail · Proceso diario completo" 2026-09-23   # y procesa una fecha
```
Los notebooks usan el catálogo `databrick_graupel_demo` por defecto (widget `catalogo`); los datasets del dashboard
lo llevan fijo en `dashboard.py`. Guía de la demo: [`GUIA-DEMO.md`](GUIA-DEMO.md).

Capturas en `media/`:
[`databricks_creacion.png`](media/databricks_creacion.png) (creación del workspace) e
[`informe_databricks.png`](media/informe_databricks.png) (dashboard).
