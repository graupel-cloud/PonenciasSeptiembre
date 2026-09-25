# Big Data sin Big Bill

Un mismo caso de uso resuelto con dos plataformas para comparar esfuerzo, operación y coste:

> **¿Cuánto vendió ayer cada tienda y de qué productos?**

Una cadena de **400 tiendas** en 17 regiones y unas **550 referencias** en 11 categorías. Cada noche se generan ~1,1 M de tickets y ~6,5–6,9 M de líneas sintéticas, con datos "sucios" a propósito (registros corruptos, líneas de otro día, devoluciones). El proceso limpia, agrega y deja listo el informe de la mañana.

| | [Demo 1 · Ambari](demo-1-ambari/) | [Demo 2 · Databricks](demo-2-databricks/) |
|---|---|---|
| Plataforma | Apache Ambari 3.0.0 + Bigtop 3.3.0 (Hadoop 3.3.6) en 2 VMs propias | Azure Databricks, serverless + Unity Catalog |
| Procesamiento | MapReduce en Java sobre YARN | PySpark / SQL en notebooks, tablas Delta |
| Orquestación | Script `run-demo.sh` | Jobs de Databricks (DAG con tareas en paralelo) |
| Almacenamiento | HDFS (réplica 2) | Volume `landing` + tablas bronze / silver / gold |
| Informe | `informe.txt` en HDFS y por consola | Dashboard AI/BI + salida del notebook |
| Tiempo del proceso | ~100 s | ~3,5 min (arranque serverless incluido) |
| Coste | VMs encendidas (se pueden desasignar) | Pago por uso |

## Contenido

```
demo-1-ambari/       retail-etl/ (Maven, Java 8) + GUIA-DEMO.md + media/
demo-2-databricks/   notebooks/, dashboards/, deploy.py + GUIA-DEMO.md + media/
```
