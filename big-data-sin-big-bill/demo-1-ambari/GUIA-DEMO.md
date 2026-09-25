# Demo Ambari 3.0: "¿cuánto vendió ayer cada tienda y de qué productos?"

## Accesos (HTTPS con Let's Encrypt vía sslip.io, puerto 443 de VM1)

> Las IPs y URLs reales del entorno de la charla se han sustituido por marcadores (`<IP_NODE1>`, `<IP-NODE1>` = la misma IP con guiones para sslip.io).

| Qué | URL | Usuario |
|---|---|---|
| Ambari 3.0.0 | https://ambari.<IP-NODE1>.sslip.io | `admin` |
| NameNode (HDFS) | https://hdfs.<IP-NODE1>.sslip.io | `admin` (auth básica) |
| YARN ResourceManager | https://yarn.<IP-NODE1>.sslip.io | `admin` (auth básica) |
| MapReduce JobHistory | https://historial.<IP-NODE1>.sslip.io | `admin` (auth básica) |

La contraseña es la misma en las cuatro URLs; se entrega aparte, no está en este fichero.
SSH: `ssh <usuario>@<IP_NODE1>` (node1) y `ssh <usuario>@<IP_NODE2>` (node2).

## Qué hay montado

- **2 VMs Ubuntu 24.04** (4 vCPU / 16 GB). Están en VNets distintas y el NSG solo abre el 22 y el 443, así que los nodos
  se comunican por un **túnel SSH L3** (`ambari-tunnel.service` en node1): `node1.demo.lan`=10.99.0.1,
  `node2.demo.lan`=10.99.0.2.
- **Apache Ambari 3.0.0** (la última release) **compilado desde el código fuente** y empaquetado como `.deb`.
  Ambari no publica binarios para Ubuntu y el repositorio comunitario de binarios estaba caído.
- **Stack BIGTOP 3.3.0**: Hadoop 3.3.6 y ZooKeeper 3.8.4. Son los paquetes oficiales de Apache Bigtop para Ubuntu 24.04,
  reempaquetados con la estructura versionada que espera Ambari (`/usr/bigtop/3.3.0`) y servidos desde un repo APT
  firmado en node1 (`:8081`).
- **Cluster `retail_demo`**: HDFS (NameNode en node1 y SecondaryNameNode en node2), YARN (ResourceManager y
  Timeline 1.5), MapReduce2 (JobHistory) y ZooKeeper. Hay DataNode y NodeManager en los dos nodos.
- Auto-recuperación activada: tras reiniciar cualquier VM, todo vuelve solo en unos 2-3 minutos (probado con las dos).

## El caso de uso (jar `retail-etl.jar`, MapReduce en Java)

Una cadena de **400 tiendas** (hipermercados, supermercados y express repartidos por 17 regiones) y unas **550
referencias** en 11 categorías, con datos sintéticos.

1. `generate`: un job map-only con 20 tareas simula las cajas de ayer, con ~1,16 M de tickets y **~6,9 M de líneas
   (≈480 MB)** en `/retail/raw/sales/dt=AAAA-MM-DD`. Incluye datos "sucios" a propósito: registros corruptos, líneas
   de otro día y devoluciones. Tarda **~35 s**.
2. `etl`: dos jobs encadenados, en **~60 s**:
   - **Job 1**: limpieza (con contadores de calidad), agregación tienda × producto con *combiner* y enriquecimiento
     con los maestros de tiendas y productos. Salida en `/retail/curated/daily_sales/dt=…`.
   - **Job 2**: resumen por tienda (con su top 3 de productos), por región, por categoría y total de la cadena.
     Salida en `/retail/reports/dt=…`, junto con `informe.txt`.

Código fuente: [`retail-etl/`](retail-etl/) (compila con `mvn package`, Java 8 y Hadoop 3.3.6).

## Guion de la demo (~15 min)

1. **Ambari** (https://ambari.<IP-NODE1>.sslip.io):
   - Dashboard: los 4 servicios en verde y los 2 hosts.
   - *Hosts*: node1/node2 con sus componentes.
   - *Services → HDFS → Configs*: configuración centralizada con versiones.
   - *Stack and Versions*: BIGTOP 3.3.0, Hadoop 3.3.6.
2. **Lanza la demo** en una terminal:
   ```bash
   ssh <usuario>@<IP_NODE1>
   ~/demo/run-demo.sh            # genera "ayer" y lanza el ETL (~100 s en total)
   ```
   - Mientras corre, abre **YARN** (https://yarn.<IP-NODE1>.sslip.io) y enseña las aplicaciones *RUNNING*, los
     contenedores repartidos entre node1 y node2 y la cola.
   - Al terminar sale el **informe de la mañana**: ventas totales, top 10 tiendas, las 5 peores, regiones, categorías
     y calidad de datos.
3. **Pregunta concreta**: "¿Qué vendió ayer la tienda T042?"
   ```bash
   ~/demo/tienda.sh T042
   ```
4. **JobHistory** (https://historial.<IP-NODE1>.sslip.io): los 3 jobs, sus tiempos, los *counters* ("Calidad de
   datos") y los mappers/reducers.
5. **HDFS** (https://hdfs.<IP-NODE1>.sslip.io → *Utilities → Browse*): `/retail/raw`, `/retail/curated` y
   `/retail/reports`, con réplica 2.
6. Opcional, **tolerancia a fallos**: en Ambari, *Hosts → node2 → NodeManager → Stop* y relanza
   `SOLO_ETL=1 ~/demo/run-demo.sh`. YARN lo ejecuta todo en node1. Luego vuelve a arrancarlo (o espera a que lo haga
   la auto-recuperación).

Otras opciones:
- `~/demo/run-demo.sh 2026-09-20`: cualquier otra fecha (el fin de semana vende más).
- `SOLO_ETL=1 ~/demo/run-demo.sh`: solo el ETL, sobre datos ya generados.
- `hdfs dfs -cat /retail/reports/dt=AAAA-MM-DD/informe.txt`: el informe guardado.

## Antes de la demo (checklist)

- [ ] Las VMs están encendidas. **Stop/Deallocate no rompe nada**: todo vive en el disco del SO. Lo que había en
      `/data` (disco temporal de Azure) solo eran restos de compilación.
- [ ] Ambari muestra todo en verde. Si algo está parado: *Actions → Start All*.
- [ ] Haz un ensayo con `~/demo/run-demo.sh`.

## Si algo falla

| Síntoma | Solución |
|---|---|
| node2 aparece como *lost heartbeat* | En node1: `sudo systemctl restart ambari-tunnel` y `ping 10.99.0.2` |
| Servicios parados tras reiniciar | Espera 2-3 min (auto-start) o *Actions → Start All* en Ambari |
| Ambari no responde | En node1: `sudo ambari-server restart` |
| La web da error de certificado | En node1: `sudo systemctl restart caddy` (revisa que el 443 siga abierto en el NSG) |
