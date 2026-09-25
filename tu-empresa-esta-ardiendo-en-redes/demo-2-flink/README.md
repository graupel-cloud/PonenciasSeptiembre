# El martes en X: detectar a tiempo una crisis de reputación con Flink

Demo para una charla técnica. Publicas en X posts con un hashtag y el sistema:

1. recibe cada post en segundos por el **stream filtrado de la API v2 de X**;
2. **Flink** lo clasifica como `positive`, `negative` o `neutral` con un modelo de **Ollama Cloud**;
3. si en los últimos 60 minutos (por hora de publicación) hay **2 negativos nuevos**, emite una alerta;
4. un **notificador** redacta el correo con el modelo de lenguaje y lo envía por **SMTP**;
5. un **panel web** muestra en directo cada post, su clasificación, la hora de cada paso y las alertas.

Guion que tiene que funcionar:

| Paso | Qué pasa |
|---|---|
| Primer post negativo | Nada |
| Segundo negativo dentro de la misma hora | Llega un correo |
| Tercer negativo poco después | No llega otro: solo hay 1 negativo nuevo |
| Otros 2 negativos nuevos | Otro correo, pero nunca más de uno cada 5 minutos |

Es una demo de un solo nodo: todo corre con Docker Compose en un VPS de 4 vCPU y 8 GB.

## Arquitectura

```
X (stream filtrado)
   │
   ▼
[ingest]  Python ── escribe ──►  Kafka: x.posts.raw
                                        │
                                        ▼
                                 [flink-job]  Java 17 / Flink 1.20
                                   1. lee x.posts.raw
                                   2. clasifica con Ollama Cloud (Async I/O)
                                   3. lógica: ≥2 negativos nuevos en 60 min
                                        │                    │
                          Kafka: x.posts.scored        Kafka: alerts
                                        │                    │
                                        │                    ▼
                                        │             [notifier]  Python
                                        │               1. Ollama redacta el correo
                                        │               2. envía por SMTP
                                        │               3. escribe en Kafka: notifications
                                        ▼                    │
                                 [panel]  Python  ◄──────────┘
                                   web con los posts, horas y alertas en directo (SSE)
```

| Servicio | Qué es |
|---|---|
| `kafka` | Kafka 4.3.1 en un solo nodo, modo KRaft (sin ZooKeeper) |
| `kafka-init` | Crea los 4 topics al arrancar y termina |
| `jobmanager` | Flink 1.20.5 en modo aplicación: arranca el job solo (y lo restaura del último checkpoint) |
| `taskmanager` | Un TaskManager de Flink |
| `ingest` | Conexión al stream filtrado de X y escritura en Kafka |
| `notifier` | Redacción del correo con Ollama y envío por SMTP |
| `panel` | Web del panel (FastAPI + Server-Sent Events + un solo HTML) |
| `caddy` | Opcional (perfil `public`): panel por HTTPS con contraseña |
| `simulate` | Herramienta (perfil `tools`): posts falsos para probar sin X |

Topics (1 partición cada uno): `x.posts.raw`, `x.posts.scored`, `alerts`, `notifications`.

### Estructura del repositorio

```
.
├── docker-compose.yml
├── .env.example            ← copia a .env y rellena
├── flink-job/              ← job de Flink (Maven, compilado dentro del Dockerfile)
│   ├── src/main/java/demo/reputacion/
│   │   ├── ReputationJob.java               fuente, Async I/O, sinks
│   │   ├── OllamaClassifyFunction.java      llamada asíncrona a Ollama
│   │   ├── SentimentParser.java             validación de la respuesta
│   │   └── NegativeBurstAlertFunction.java  lógica de alerta con estado
│   ├── src/main/resources/classify_prompt.txt
│   ├── src/test/java/...                    pruebas con el harness de Flink
│   └── start-jobmanager.sh                  restaura el último checkpoint
├── ingest/                 ← stream filtrado de X → Kafka
├── notifier/               ← alerts → Ollama → SMTP → notifications
│   └── email_prompt.txt
├── panel/                  ← FastAPI + static/index.html
├── caddy/Caddyfile         ← panel público opcional
└── tools/
    ├── simulate.py         ← escenarios de prueba sin X
    ├── instalar-vps.sh     ← instalación y arranque en un VPS
    └── reset.sh            ← reinicio limpio entre tomas
```

## Requisitos del VPS

- Linux con **4 vCPU y 8 GB de RAM**. Los límites de memoria de Compose suman unos 6,3 GB: Kafka con 1 GB de heap, JobManager con 1 GB, TaskManager con 2 GB y el resto pequeño.
- **Docker Engine 24 o superior** con el plugin **Docker Compose v2** (`docker compose version`).
- **Reloj sincronizado por NTP.** La ventana de 60 minutos usa la hora de publicación de X, y las horas del panel comparan relojes. Compruébalo con:
  ```bash
  timedatectl status   # "System clock synchronized: yes"
  ```
  Si pone `no`: `sudo timedatectl set-ntp true`.
- Salida a Internet hacia `api.x.com`, `ollama.com` y tu servidor SMTP (puerto 587 o 465).
- Puertos de entrada: solo **22** (SSH). Abre **80 y 443** únicamente si publicas el panel con el perfil `public`.

## Configuración

```bash
cp .env.example .env
nano .env
```

| Variable | Qué poner |
|---|---|
| `X_BEARER_TOKEN` | Bearer token de la app de X (developer.x.com → proyecto → *Keys and tokens*). Tal cual, aunque tenga `%2F`. |
| `X_RULE_VALUE` | La regla, p. ej. `'#MiHashtagDemo -is:retweet'` (entre comillas simples). |
| `X_RULE_TAG` | Etiqueta de la regla (`demo`). El ingest no toca reglas con otras etiquetas. |
| `X_STALL_TIMEOUT_SECONDS` | Segundos sin recibir nada antes de reconectar (30). |
| `X_INGEST_ENABLED` | `false` para no conectarse a X y probar solo con el simulador. |
| `OLLAMA_API_KEY` | Clave de https://ollama.com/settings/keys |
| `OLLAMA_MODEL` | Modelo de Ollama Cloud (`deepseek-v4.1-flash`). Lista: `curl -s https://ollama.com/api/tags` |
| `BRAND_NAME` | Nombre de la marca, sale en los prompts, el correo y el panel. |
| `NEGATIVE_THRESHOLD` / `WINDOW_MINUTES` / `COOLDOWN_MINUTES` | 2 / 60 / 5 |
| `SMTP_*`, `ALERT_EMAIL_TO` | Servidor de correo. `ALERT_EMAIL_TO` admite varias direcciones separadas por comas. |
| `TZ` | Zona horaria del panel y de los correos (`Europe/Madrid`). |

Los valores con `$`, `#`, espacios u otros símbolos van entre **comillas simples**, por ejemplo `SMTP_PASSWORD='a$b#c'`.

Los secretos solo están en `.env`, que no entra en Git. En los logs las claves salen enmascaradas (`abcd****`).

## Arrancar y parar

### Instalación en un VPS nuevo (Ubuntu/Debian)

Copia la carpeta al VPS (con tu `.env` ya relleno) y ejecuta:

```bash
./tools/instalar-vps.sh            # instala Docker si falta, activa NTP, arranca y espera al job
./tools/instalar-vps.sh --prueba   # además lanza el escenario "escena" (envía 1 correo real)
```

### Día a día

```bash
docker compose up -d --build          # la primera vez tarda unos minutos (compila el job y pasa las pruebas)
docker compose ps                      # todo "Up"; kafka-init "Exited (0)" es lo normal
docker compose logs -f ingest          # debe aparecer event=stream_connected
```

El job aparece como **RUNNING** en la interfaz de Flink sin ningún paso manual.

```bash
docker compose stop                    # parar conservando datos y estado
docker compose start                   # volver a arrancar (el job se restaura del último checkpoint)
docker compose down                    # parar y borrar contenedores (los volúmenes se conservan)
```

## Abrir el panel y la interfaz de Flink (túnel SSH)

El panel (8080) y Flink (8081) solo escuchan en `127.0.0.1` del VPS. Desde tu portátil:

```bash
ssh -N -L 8080:127.0.0.1:8080 -L 8081:127.0.0.1:8081 usuario@IP_DEL_VPS
```

Luego abre:

- Panel: http://localhost:8080
- Flink: http://localhost:8081

El panel está pensado para grabarlo: tema oscuro, letra grande, reloj con segundos y, en cada post, la hora de publicación en X, de llegada, de clasificación y de alerta, con el retraso de cada paso.

### Ver lo que llega en la interfaz de Flink

El job no encadena operadores (`disableOperatorChaining`), así que en **Jobs → Running Jobs → reputacion-x** cada paso sale como una caja propia: `kafka-x.posts.raw` → `parse-raw` → `classify-ollama` → `scored-to-json` → `kafka-x.posts.scored` y `alert-logic` → `kafka-alerts`. En la tabla de abajo, **Records Received / Records Sent** suben con cada post.

Métricas propias, en cada caja → pestaña **Metrics** → *Add metric*:

| Caja | Métricas |
|---|---|
| `classify-ollama` | `posts_negative`, `posts_positive`, `posts_neutral`, `posts_unknown`, `ollama_last_latency_ms` |
| `alert-logic` | `alerts_emitted`, `alerts_deferred`, `duplicates_ignored` |

Otras pestañas útiles: **Checkpoints** (uno cada 30 s, con el tamaño del estado) y **Task Managers → Logs** (las líneas `event=post_classified`, `event=alert_emitted`…).

### Opcional: panel público por HTTPS

Si el VPS tiene los puertos 80 y 443 abiertos y un dominio apunta a su IP, puedes servir el panel en `https://tu-dominio` con usuario y contraseña. Caddy saca el certificado de Let's Encrypt solo.

Si no tienes dominio, `sslip.io` sirve: para la IP `20.1.2.3`, usa `20-1-2-3.sslip.io`.

```bash
docker run --rm caddy:2.11.4-alpine caddy hash-password --plaintext 'una-contraseña-larga'
```

En `.env`:

```bash
COMPOSE_PROFILES=public
PANEL_DOMAIN=20-1-2-3.sslip.io
PANEL_USER=demo
PANEL_PASSWORD_HASH='$2a$14$...'     # comillas simples: el hash lleva $
FLINK_DOMAIN=flink.20-1-2-3.sslip.io  # opcional: interfaz de Flink en solo lectura
```

Después, `docker compose up -d`.

Con `FLINK_DOMAIN`, la interfaz de Flink se publica **en solo lectura** y con el mismo usuario y contraseña. Tiene dos protecciones:

- En Flink, subir jars, cancelar y reescalar jobs desde la web está desactivado (`web.submit.enable`, `web.cancel.enable` y `web.rescale.enable` a `false`).
- Caddy solo deja pasar peticiones `GET` y `HEAD`. Cualquier otra petición a la API REST recibe un 403.

Aun así, si no la necesitas en público, lo más seguro es verla por el túnel SSH.

## Guion de la demo en directo

Lo que se enseña en la charla, en unos 5 minutos:

**Antes de salir al escenario**

1. `./tools/reset.sh`: topics vacíos, sin estado y sin alertas enviadas.
2. Comprueba que el ingest dice `event=stream_connected`.
3. Abre en pestañas:
   - el panel;
   - Flink, en *Jobs → reputacion-x*;
   - la bandeja de entrada del correo de alertas.
4. Ten preparados en el móvil tres posts negativos con el hashtag, desde la cuenta de prueba.

**En directo**

| Paso | Qué haces | Qué se ve |
|---|---|---|
| 1 | Enseñas la arquitectura y el panel vacío | Reloj en marcha y «Negativos en la última hora: 0 / 2» |
| 2 | Publicas el **primer post negativo** | En unos segundos aparece como `NEGATIVO`, con sus horas: publicado, llegada (+2–5 s) y clasificado (+1 s). En Flink, *Records Received* sube en cada caja. **No pasa nada más**: 1 / 2. |
| 3 | Publicas el **segundo negativo** | El contador pasa a 2 / 2, sale la **tarjeta roja de alerta** y, un par de segundos después, la **tarjeta verde del correo** con el asunto que ha redactado el modelo. |
| 4 | Abres el correo | Correo en texto plano: qué pasa, los dos comentarios citados con su enlace y las dos acciones. |
| 5 | Publicas el **tercer negativo** | Se clasifica, pero **no llega otro correo**: solo hay 1 negativo nuevo desde la alerta. Nada de spam de avisos. |
| 6 (opcional) | Reinicias Flink en directo: `docker compose restart jobmanager taskmanager` | El job vuelve en unos segundos desde el último checkpoint, conserva el recuento y no repite el correo. |

**Mensajes para contar**

- Flink evalúa cada post al llegar, no espera a que se cierre una ventana. Con poco volumen, las marcas de agua no avanzarían y la alerta llegaría tarde o nunca.
- El modelo de lenguaje se llama con **Async I/O**: el job no se bloquea mientras espera a Ollama.
- Si Ollama no responde, los posts salen como `unknown` y el job sigue en marcha.
- El texto de terceros va delimitado y nunca como instrucción.
- Estado con **checkpoints cada 30 s** y `alert_id` determinista: un reinicio no pierde el recuento ni duplica correos.
- Todo cabe en un VPS de 8 GB con Docker Compose.

Si X falla el día de la charla, el plan B es el simulador: `docker compose run --rm simulate escena --tag demo` hace el mismo guion con posts falsos.

## Simulador (sin gastar crédito de X)

`tools/simulate.py` escribe posts falsos directamente en `x.posts.raw`, con la hora de publicación que pide cada escenario. Los posts pasan por la clasificación real con Ollama, la lógica de Flink y el correo real.

Al terminar, el simulador comprueba cuántas alertas y correos han salido y dice `OK` o `FALLO`.

```bash
docker compose run --rm simulate escena
docker compose run --rm simulate separados
docker compose run --rm simulate duplicado
docker compose run --rm simulate mixto
docker compose run --rm simulate rafaga
```

| Escenario | Posts | Correos esperados |
|---|---|---|
| `escena` | negativo (−23 min), negativo (−15 min) y un tercer negativo 15 min después (ahora) | **1** |
| `separados` | dos negativos con 61 minutos de diferencia | **0** |
| `duplicado` | el mismo `post_id` dos veces (cuenta una sola vez) | **0** |
| `mixto` | 2 positivos, 2 neutros y 2 negativos en la última hora | **1** |
| `rafaga` | 4 negativos seguidos | **2**: uno al momento y otro 5 min después |

Cada ejecución usa su propia etiqueta (`sim-escena-HHMMSS`), así los escenarios no se afectan entre sí aunque no resetees. Opciones:

- `--tag demo`: usa la misma etiqueta que el stream real.
- `--pause 6`: segundos entre posts.
- `--no-watch`: no espera a comprobar el resultado.

> Los posts simulados tienen enlaces `https://x.com/sim_user/status/...` que no existen. En el panel salen marcados como `(sim)`.

## Resetear entre tomas de la grabación

```bash
./tools/reset.sh
```

El script para los servicios y borra los volúmenes de Kafka (topics vacíos), de Flink (estado y checkpoints) y del notificador (fichero de alertas enviadas). Luego arranca todo y espera a que el job esté en `RUNNING`. Tarda un minuto más o menos. No toca los certificados HTTPS.

Hazlo **con el panel cerrado o recárgalo después**: el panel guarda en memoria lo que ya ha visto.

## Prueba final con X real

1. `docker compose logs -f ingest` y espera `event=stream_connected`. La primera vez verás también `event=rule_created`.
2. Abre el panel.
3. Desde la cuenta de prueba publica un post negativo con el hashtag. Aparece en el panel en unos segundos como `NEGATIVO`.
4. Publica un segundo post negativo. En el panel sale la tarjeta de **alerta** y enseguida la de **correo**.
5. Publica un tercero: no llega otro correo, porque solo hay 1 negativo nuevo.

## Cómo funciona la lógica de alerta

Está en `flink-job/.../NegativeBurstAlertFunction.java` (una `KeyedProcessFunction` por `rule_tag`):

- **Se evalúa al llegar cada post, no con ventanas de event time.** Con tan pocos posts, las marcas de agua no avanzarían y la alerta tardaría en salir o no saldría nunca.
- La **ventana** se guarda en `ListState`. Se conservan los posts con `published_at` dentro de los últimos `WINDOW_MINUTES`, contados desde el post publicado más reciente.
- Un `post_id` repetido se ignora. Pasa, por ejemplo, con una reconexión.
- Solo cuentan los `negative`. Los `unknown` (Ollama no respondió) no cuentan.
- Un negativo que ya se usó en una alerta **no vuelve a contar**: para otra alerta hacen falta 2 negativos nuevos.
- Entre alertas pasan al menos `COOLDOWN_MINUTES`, medidos con el reloj del servidor. Si durante la espera se juntan 2 negativos nuevos, un temporizador lanza la alerta en cuanto termina la espera.
- El estado tiene **TTL** (3 h como mínimo) para que no crezca sin límite.

### Reinicios sin perder estado ni duplicar correos

- **Checkpoint cada 30 s.** Se guardan la ventana, la hora de la última alerta, los temporizadores pendientes y los offsets de Kafka. Los checkpoints se conservan en el volumen `flink-checkpoints`.
- **Sin ZooKeeper.** El job tiene un id fijo, y `start-jobmanager.sh` busca el último `chk-N` completo y arranca el job desde ahí. Da igual reiniciar el jobmanager, el taskmanager o los dos.
- **`alert_id` determinista.** Es un UUID calculado a partir del `rule_tag` y de los `post_id` que disparan la alerta. Si Flink reprocesa posts tras un reinicio, la alerta repetida tiene el mismo `alert_id`.
- **El notificador guarda los `alert_id` enviados** en `/data/sent_alerts.txt`, en un volumen, y no manda dos veces el mismo correo.
- Por eso los sinks de Kafka son *at-least-once*: cada alerta sale en cuanto se produce. Con transacciones esperaría al siguiente checkpoint, hasta 30 s más, y en el vídeo se notaría.

### Si Ollama no responde

- **Clasificación:** timeout de 20 s y 10 peticiones en vuelo. Con timeout, error HTTP o respuesta no válida, el post sale como `unknown` y el job sigue.
- **Correo:** si Ollama falla o tarda más de 30 s, se usa una plantilla fija y la notificación lleva `used_fallback_template: true`. En el panel, la tarjeta del correo sale en ámbar.

### Sobre los prompts

Los prompts están en ficheros, no en el código:

- clasificación: `flink-job/src/main/resources/classify_prompt.txt`
- correo: `notifier/email_prompt.txt`

El texto del post se recorta a 1.000 caracteres. Se pasa solo como dato, entre `<comentario>` y `</comentario>`, y se neutralizan esas etiquetas si aparecen dentro del texto.

Se envía `format` con un esquema JSON, pero algunos modelos de Ollama Cloud (entre ellos `deepseek-v4.1-flash`) no lo aplican. Por eso el prompt también describe la forma exacta del JSON, y la respuesta **siempre se valida**: si no es JSON o el valor no está en la lista, el resultado es `unknown`.

## Contratos de datos (JSON en UTF-8)

**`x.posts.raw`** (clave: `post_id`):

```json
{
  "post_id": "1790000000000000000",
  "text": "Habéis arruinado el sabor #MiHashtagDemo",
  "author_username": "cuenta_demo",
  "url": "https://x.com/cuenta_demo/status/1790000000000000000",
  "published_at": "2026-10-06T12:11:04Z",
  "ingested_at": "2026-10-06T12:11:08.412Z",
  "rule_tag": "demo",
  "source": "x"
}
```

**`x.posts.scored`** lleva lo mismo más:

- `sentiment`: `positive`, `negative`, `neutral` o `unknown`
- `classified_at`
- `model`

**`alerts`** (clave: `alert_id`):

- `alert_id`, `rule_tag`, `brand`
- `window_minutes`, `negative_threshold`, `cooldown_minutes`
- `negatives_in_window`: todos los negativos de la ventana
- `new_negatives`: los que disparan esta alerta
- `counts_in_window`
- `negative_posts`: los posts nuevos que disparan la alerta
- `first_published_at`, `triggered_by_post_id`, `alert_at`

**`notifications`** (clave: `alert_id`):

- `alert_id`, `rule_tag`, `subject`, `sent_at`, `to`
- `status`: `sent` o `failed`
- `error`: solo si falla
- `used_fallback_template`

## Pruebas

Las pruebas del job se ejecutan en cada `docker compose build`: si alguna falla, la imagen no se construye. Para lanzarlas a mano:

```bash
docker build --target build flink-job/
```

`NegativeBurstAlertFunctionTest` usa el harness de Flink para funciones con estado (`KeyedOneInputStreamOperatorTestHarness`) y comprueba que:

- 1 negativo no alerta;
- 2 negativos en 60 minutos alertan;
- 2 negativos separados 61 minutos no alertan;
- un post que llega tarde fuera de la ventana no cuenta;
- un duplicado no cuenta;
- un `unknown` no cuenta;
- un tercer negativo después de una alerta no genera otra;
- 2 negativos nuevos después de la espera generan una segunda alerta;
- la espera retrasa la segunda alerta y el temporizador la lanza al terminar;
- las etiquetas son independientes;
- el `alert_id` es determinista;
- tras restaurar un checkpoint no se duplica la alerta;
- el temporizador pendiente sobrevive a un checkpoint.

`SentimentParserTest` valida las respuestas del modelo, comprueba que los delimitadores del texto se neutralizan y que los secretos se enmascaran.

## Seguir un post de punta a punta

Cada servicio escribe una línea por evento con el `post_id` o el `alert_id`:

```bash
docker compose logs --no-log-prefix ingest taskmanager notifier panel | grep 1790000000000000000
docker compose logs --no-log-prefix taskmanager notifier | grep <alert_id>
```

Eventos útiles:

| Servicio | Eventos |
|---|---|
| ingest | `stream_connected`, `post_received`, `kafka_written`, `stream_429` |
| taskmanager | `raw_received`, `post_classified`, `window_evaluated`, `alert_emitted`, `alert_deferred`, `duplicate_ignored`, `ollama_*` |
| notifier | `alert_received`, `email_drafted`, `email_fallback_template`, `email_sent`, `smtp_error`, `alert_already_sent` |

## Problemas típicos

**`event=stream_429` en el ingest (Too Many Requests / TooManyConnections).**

El plan de pago por uso de X solo permite **una conexión al stream por proyecto**. Suele pasar por una de estas causas:

- hay otro ingest conectado con el mismo token (otra máquina, tu portátil, una toma anterior que no se cerró);
- X aún no ha liberado la conexión anterior tras un reinicio.

El ingest no reintenta en bucle: espera 60 s, 120 s, 240 s… (hasta 15 min) y deja el motivo en el log. Apaga cualquier otro consumidor del stream. Si hace falta, en el portal de X (*Stream connections*) puedes cerrar las conexiones abiertas.

**`event=ollama_http_error status=404` o `model not found`.**

El nombre de `OLLAMA_MODEL` no existe en Ollama Cloud. Compruébalo:

```bash
curl -s https://ollama.com/api/tags | python3 -m json.tool | grep name
```

Usa el nombre exacto, por ejemplo `deepseek-v4.1-flash` o `gemma4:31b`. Con `status=401`, la `OLLAMA_API_KEY` no es válida. En los dos casos los posts salen como `unknown` y no hay alertas.

**Todos los posts salen como `SIN CLASIFICAR`.**

Mira `docker compose logs taskmanager | grep ollama_`. Puede ser:

- `ollama_timeout`: Ollama tarda más de 20 s;
- `ollama_invalid_response`: el modelo no devolvió el JSON pedido. Prueba otro modelo.

**Fallo de SMTP (`event=smtp_error`).**

El notificador reintenta 3 veces (0, 5 y 15 s). Si no lo consigue, escribe `status: failed` en `notifications` y el panel muestra la tarjeta del correo en rojo. Causas habituales:

- `SMTPAuthenticationError`: usuario o contraseña. Revisa las comillas simples si la contraseña tiene símbolos.
- `SSL`/`STARTTLS`: con el puerto 587 usa `SMTP_STARTTLS=true`; con 465 usa `SMTP_PORT=465`.
- `timed out`: el proveedor del VPS bloquea la salida SMTP. Algunos (Azure, por ejemplo) bloquean el puerto 25, pero no el 587.

Un correo fallido no se reintenta más tarde: corrige la configuración, `docker compose up -d notifier` y repite el escenario.

**El job no aparece como RUNNING.**

```bash
docker compose logs jobmanager | grep -E "event=|ERROR"
```

Si el restore de un checkpoint falla (por ejemplo, porque cambiaste el código del job de forma incompatible), borra el estado con `./tools/reset.sh`.

**No llega nada al panel.**

- Comprueba que el ingest dice `stream_connected` y que la regla es la correcta (`event=rule_ok`).
- Recuerda que `-is:retweet` descarta los retuits.
- Los posts de cuentas protegidas no salen en el stream.

## Versiones (comprobadas en septiembre de 2026)

| Pieza | Versión |
|---|---|
| Flink | `flink:1.20.5-java17`, `flink-streaming-java` 1.20.5 |
| Conector Kafka | `flink-connector-kafka` 3.4.0-1.20 |
| Kafka | `apache/kafka:4.3.1` (KRaft) |
| Build | `maven:3.9.16-eclipse-temurin-17`, JUnit 5.14.4, Jackson 2.18.11 |
| Python | `python:3.12-slim`, confluent-kafka 2.15.1, requests 2.34.2, httpx 0.28.1, fastapi 0.141.1, uvicorn 0.53.0 |
| Caddy | `caddy:2.11.4-alpine` (opcional) |
| Modelo | `deepseek-v4.1-flash` en Ollama Cloud (configurable) |

## Capturas

- [`media/flink-pipeline.png`](media/flink-pipeline.png): el job en la interfaz de Flink.
