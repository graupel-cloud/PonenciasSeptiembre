# Ponencias · septiembre 2026

Material de dos charlas técnicas: presentación, código de las demos, guiones y grabaciones.

| Ponencia | Demo 1 | Demo 2 |
|---|---|---|
| [**Tu empresa está ardiendo en redes**](tu-empresa-esta-ardiendo-en-redes/) — detectar a tiempo una crisis de reputación en X | [n8n](tu-empresa-esta-ardiendo-en-redes/demo-1-n8n/): workflow low-code cada 15 min | [Flink](tu-empresa-esta-ardiendo-en-redes/demo-2-flink/): streaming en tiempo real con Kafka + Flink |
| [**Big Data sin Big Bill**](big-data-sin-big-bill/) — el informe diario de ventas de 400 tiendas, con dos modelos de coste | [Ambari](big-data-sin-big-bill/demo-1-ambari/): clúster Hadoop propio (MapReduce en YARN) | [Databricks](big-data-sin-big-bill/demo-2-databricks/): serverless + Unity Catalog (medallion) |

## Estructura

```
.
├── tu-empresa-esta-ardiendo-en-redes/
│   ├── presentacion/                 PDF de la charla
│   ├── demo-1-n8n/                   workflow de n8n (JSON importable) + capturas
│   └── demo-2-flink/                 Docker Compose: ingest X → Kafka → Flink → correo + panel web
└── big-data-sin-big-bill/
    ├── demo-1-ambari/                job MapReduce (Java) + scripts de la demo + guía
    └── demo-2-databricks/            notebooks, dashboard y script de despliegue + guía
```

Cada demo tiene una carpeta `media/` con capturas. Las grabaciones en vídeo no están en el repo porque muestran datos del entorno real (URLs, IPs, cuentas).

## Credenciales

Este repositorio **no contiene secretos**. Todo lo que hace falta se configura fuera:

| Demo | Dónde van los secretos |
|---|---|
| n8n | Credenciales de n8n (*Header Auth* para X y Ollama, *Gmail OAuth2*), tras importar el workflow |
| Flink | Fichero `.env` a partir de `.env.example` (ignorado por Git) |
| Ambari | Contraseña de Ambari / auth básica, entregada aparte |
| Databricks | Variables de entorno `DATABRICKS_HOST` y `DATABRICKS_TOKEN` |

Las IPs, URLs del workspace e identificadores del entorno real se han sustituido por marcadores como `<IP_NODE1>` o `<workspace>`.
