# Tu empresa está ardiendo en redes

> Y tu batch corre mañana.

Una marca publica un cambio de receta a las 12:00. A las 12:03 llega el primer comentario negativo y a las 13:30 ya se está compartiendo. Si te enteras a las 15:00, por casualidad, llegas tarde.

La charla enseña cómo detectar ese momento **en minutos** escuchando X, clasificando el sentimiento de cada post con un modelo de lenguaje y avisando por correo cuando se juntan varios negativos. Se resuelve dos veces, con dos enfoques:

| | [Demo 1 · n8n](demo-1-n8n/) | [Demo 2 · Flink](demo-2-flink/) |
|---|---|---|
| Enfoque | Low-code, *micro-batch* cada 15 min | Streaming, evento a evento |
| Fuente | API de búsqueda reciente de X | Stream filtrado de la API v2 de X |
| Clasificación | Ollama Cloud (HTTP Request) | Ollama Cloud con Async I/O en Flink |
| Estado | Data Table de n8n | Estado de Flink con checkpoints cada 30 s |
| Alerta | ≥ 2 negativos en 60 min → Gmail | ≥ 2 negativos *nuevos* en 60 min, cooldown 5 min → SMTP |
| Latencia | Hasta 15 min | Segundos |
| Infra | Una instancia de n8n | Docker Compose en un VPS de 4 vCPU / 8 GB |

## Contenido

```
presentacion/tu-empresa-esta-ardiendo-en-redes.pdf
demo-1-n8n/      workflow-demo-arde-redes.json + media/
demo-2-flink/    código completo (ver su README) + media/
```
