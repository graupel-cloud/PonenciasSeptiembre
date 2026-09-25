# Demo Databricks: el mismo caso de uso ("¿cuánto vendió ayer cada tienda?")

Workspace: https://<workspace>.azuredatabricks.net (todo con **serverless** + **Unity Catalog**).

## Qué hay montado

| Qué | Dónde |
|---|---|
| Notebooks | *Workspace → Users → <tu-usuario> → retail_demo* |
| Job **Retail · Proceso diario completo** (orquesta los dos siguientes; programado a las 07:00, en pausa) | https://<workspace>.azuredatabricks.net/jobs/<job_id> |
| Job **Retail 1 · Generar ventas del día** (cajas → landing) | https://<workspace>.azuredatabricks.net/jobs/<job_id> |
| Job **Retail 2 · ETL diario** (bronze → silver → calidad ‖ gold → informe) | https://<workspace>.azuredatabricks.net/jobs/<job_id> |
| Dashboard **Retail · Informe de ventas diario** | https://<workspace>.azuredatabricks.net/dashboardsv3/<dashboard_id>/published |
| Datos | *Catalog → databrick_graupel_demo → retail_demo* (tablas + volume `landing`) |

Todos los jobs tienen el parámetro **`fecha`** (YYYY-MM-DD). **Vacío = ayer.**

**Estado actual:** ya está procesado **D-2 (23/09/2026)**: 6,5 M líneas → 33.002.840,72 € en 400 tiendas y 1.092.579 tickets.
**D-1 (24/09/2026) queda sin procesar a propósito**, para lanzarlo en directo.

### El proceso (medallion)

```
01 Generar ventas ─► landing (ficheros CSV)  /Volumes/.../landing/ventas/dt=AAAA-MM-DD
02 Bronze        ─► ventas_bronze            líneas en crudo + fichero de origen
03 Silver        ─► ventas_lineas            válidas y tipadas   (+ ventas_bronze_validada)
04 Calidad  ┐    ─► calidad_datos            corruptas, fuera de fecha, devoluciones…
05 Gold     ┘    ─► ventas_tienda_producto, resumen_tienda, resumen_region, resumen_categoria
06 Informe       ─► informe de la mañana (salida del notebook)
```
Cada ejecución reemplaza solo su día (`replaceWhere` en Delta), así que se puede relanzar las veces que haga falta.

## Guion de la demo (~15 min)

1. **Punto de partida** (Catalog Explorer): tablas de `retail_demo` con datos del 23/09 y volume `landing` con la
   carpeta `dt=2026-09-23`. En el **dashboard**, la página *Ayer* muestra aún el 23/09.
2. **Lanza D-1 en directo**: job **Retail · Proceso diario completo → Run now** (fecha vacía = ayer = 24/09).
   Tarda ~3,5 min.
   - En la vista del run se ven las 2 tareas (generar → ETL). Entra en la tarea `etl_diario` para ver el **DAG del ETL**,
     con *calidad* y *gold* ejecutándose **en paralelo**.
   - Pincha en cada tarea para ver la salida del notebook. En `informe` está el informe de la mañana completo.
   - Alternativa "paso a paso": lanza a mano **Retail 1** y, cuando termine, **Retail 2**.
   - Otra fecha: *Run now with different parameters* → `fecha = 2026-09-20` (un domingo vende menos).
3. **Mientras corre**, abre el notebook **07_explorar_datos** y ejecuta las celdas: ficheros en landing, líneas en
   crudo, líneas sucias descartadas, un ticket completo, qué vendió la T042…
4. **Al terminar**, recarga el **dashboard**:
   - *Ayer* pasa a mostrar el 24/09.
   - *Detalle por tienda*: elige una tienda (por ejemplo la T042) → qué vendió y de qué productos.
   - *Histórico*: ahora hay 2 días para comparar.
5. **Cierre**: en el notebook 07, la celda **Comparativa entre días** y `DESCRIBE HISTORY` (versiones de Delta de
   cada ejecución).

## Antes de la demo

- [ ] Abre el dashboard 5 minutos antes: arranca el SQL Warehouse serverless y ahorra la espera de ~1 min en directo.
- [ ] El primer run de un job serverless tarda ~1 min más en arrancar. Cuenta con ello o lanza antes cualquier fecha
      antigua como calentamiento.
- [ ] Revoca el token personal de acceso cuando acabes (*Settings → Developer → Access tokens*).

## Redesplegar desde cero

```bash
cd big-data-sin-big-bill/demo-2-databricks
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<token personal>   # nunca en el repo
python3 deploy.py                                                  # notebooks + jobs + dashboard
python3 deploy.py --run "Retail · Proceso diario completo" 2026-09-23   # y además procesa una fecha
```
