"""
ingest: stream filtrado de la API v2 de X -> Kafka (x.posts.raw).

- Solo lee de X. Nunca publica nada.
- Al arrancar deja la regla con etiqueta X_RULE_TAG con el valor X_RULE_VALUE, sin tocar reglas de otras etiquetas.
- Una sola conexión al stream (límite del plan de pago por uso). 429 => backoff exponencial, nunca reintento en bucle.
- Si pasan X_STALL_TIMEOUT_SECONDS sin recibir nada (X manda una línea vacía cada 20 s), se cierra y reconecta.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

API = "https://api.x.com/2"
TOPIC_RAW = "x.posts.raw"

BEARER = os.environ.get("X_BEARER_TOKEN", "").strip()
RULE_VALUE = os.environ.get("X_RULE_VALUE", "").strip()
RULE_TAG = os.environ.get("X_RULE_TAG", "demo").strip()
STALL_TIMEOUT = int(os.environ.get("X_STALL_TIMEOUT_SECONDS", "30"))
ENABLED = os.environ.get("X_INGEST_ENABLED", "true").strip().lower() == "true"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

STREAM_PARAMS = {
    "tweet.fields": "created_at,author_id,lang",
    "expansions": "author_id",
    "user.fields": "username",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s level=%(levelname)s svc=ingest %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ingest")
running = True


def mask(secret: str) -> str:
    if not secret:
        return "(vacío)"
    return secret[:4] + "****" if len(secret) > 8 else "****"


def q(s) -> str:
    """Valor entrecomillado y en una sola línea para los logs."""
    return json.dumps(str(s), ensure_ascii=False)


def utc_now_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_created_at(created_at: str) -> str:
    """'2026-10-06T12:11:04.000Z' -> '2026-10-06T12:11:04Z'."""
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def headers(json_body: bool = False) -> dict:
    h = {"Authorization": f"Bearer {BEARER}", "User-Agent": "reputacion-demo-ingest"}
    if json_body:
        h["Content-Type"] = "application/json"
    return h


def api_error_summary(resp: requests.Response) -> str:
    try:
        body = resp.json()
        parts = [body.get("title"), body.get("detail")]
        for e in body.get("errors", []) or []:
            parts.append(e.get("title") or e.get("message"))
        return "; ".join(p for p in parts if p)[:300]
    except ValueError:
        return resp.text[:300].replace("\n", " ")


# ---------------------------------------------------------------- reglas

def sync_rule() -> None:
    """Crea o corrige la regla con etiqueta RULE_TAG. La API no tiene 'editar': se borra por id y se vuelve a crear."""
    resp = requests.get(f"{API}/tweets/search/stream/rules", headers=headers(), timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"GET rules status={resp.status_code} error={api_error_summary(resp)}")
    rules = resp.json().get("data", []) or []
    mine = [r for r in rules if r.get("tag") == RULE_TAG]
    others = len(rules) - len(mine)
    log.info("event=rules_listed total=%d with_tag=%d other_tags=%d", len(rules), len(mine), others)

    if len(mine) == 1 and mine[0].get("value") == RULE_VALUE:
        log.info("event=rule_ok rule_id=%s tag=%s value=%s", mine[0]["id"], RULE_TAG, q(RULE_VALUE))
        return

    if mine:
        ids = [r["id"] for r in mine]
        resp = requests.post(
            f"{API}/tweets/search/stream/rules",
            headers=headers(True),
            json={"delete": {"ids": ids}},
            timeout=20,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"DELETE rule status={resp.status_code} error={api_error_summary(resp)}")
        log.info("event=rule_deleted rule_ids=%s tag=%s reason=value_changed", ",".join(ids), RULE_TAG)

    resp = requests.post(
        f"{API}/tweets/search/stream/rules",
        headers=headers(True),
        json={"add": [{"value": RULE_VALUE, "tag": RULE_TAG}]},
        timeout=20,
    )
    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if resp.status_code not in (200, 201) or not body.get("data"):
        raise RuntimeError(f"ADD rule status={resp.status_code} error={api_error_summary(resp)}")
    log.info("event=rule_created rule_id=%s tag=%s value=%s", body["data"][0]["id"], RULE_TAG, q(RULE_VALUE))


# ---------------------------------------------------------------- kafka

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 0,
        "client.id": "ingest",
    })


def delivery_report(err, msg):
    key = msg.key().decode() if msg.key() else None
    if err is not None:
        log.error("event=kafka_delivery_failed post_id=%s error=%s", key, q(err))
    else:
        log.info("event=kafka_written post_id=%s topic=%s offset=%s", key, msg.topic(), msg.offset())


def handle_line(line: bytes, producer: Producer) -> None:
    try:
        payload = json.loads(line)
    except ValueError:
        log.warning("event=stream_invalid_json bytes=%d", len(line))
        return

    if "data" not in payload:
        # Mensajes operativos de X (p. ej. desconexión forzada) llegan como {"errors": [...]}
        log.warning("event=stream_message_without_data content=%s", q(json.dumps(payload)[:300]))
        return

    data = payload["data"]
    post_id = data.get("id")
    tags = [r.get("tag") for r in payload.get("matching_rules", []) or []]
    if RULE_TAG not in tags:
        log.info("event=post_ignored post_id=%s reason=other_rule tags=%s", post_id, q(",".join(map(str, tags))))
        return

    users = {u.get("id"): u.get("username") for u in (payload.get("includes", {}) or {}).get("users", []) or []}
    username = users.get(data.get("author_id")) or "i"
    created_at = data.get("created_at")
    try:
        published_at = normalize_created_at(created_at)
    except (TypeError, ValueError):
        log.warning("event=post_invalid_created_at post_id=%s created_at=%s", post_id, q(created_at))
        return

    msg = {
        "post_id": post_id,
        "text": data.get("text", ""),
        "author_username": username,
        "url": f"https://x.com/{username}/status/{post_id}",
        "published_at": published_at,
        "ingested_at": utc_now_iso_ms(),
        "rule_tag": RULE_TAG,
        "source": "x",
    }
    log.info("event=post_received post_id=%s author=%s published_at=%s lang=%s",
             post_id, username, published_at, data.get("lang"))
    producer.produce(TOPIC_RAW, key=post_id.encode(), value=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
                     on_delivery=delivery_report)
    producer.flush(10)


# ---------------------------------------------------------------- stream

class Backoff:
    """Esperas recomendadas por X: red -> lineal 250 ms..16 s; HTTP -> exponencial 5 s..320 s; 429 -> exponencial desde 60 s."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.network = 0.0
        self.http = 0.0
        self.rate = 0.0

    def next_network(self) -> float:
        self.network = min(self.network + 0.25, 16.0)
        return self.network

    def next_http(self) -> float:
        self.http = 5.0 if self.http == 0 else min(self.http * 2, 320.0)
        return self.http

    def next_rate_limit(self) -> float:
        self.rate = 60.0 if self.rate == 0 else min(self.rate * 2, 900.0)
        return self.rate


def sleep_interruptible(seconds: float) -> None:
    end = time.monotonic() + seconds
    while running and time.monotonic() < end:
        time.sleep(min(1.0, end - time.monotonic()))


def stream_forever(producer: Producer) -> None:
    backoff = Backoff()
    rule_ready = False
    while running:
        if not rule_ready:
            try:
                sync_rule()
                rule_ready = True
            except (requests.RequestException, RuntimeError) as e:
                wait = backoff.next_http()
                log.error("event=rule_sync_failed error=%s retry_in_s=%.0f", q(e), wait)
                sleep_interruptible(wait)
                continue

        try:
            log.info("event=stream_connecting stall_timeout_s=%d", STALL_TIMEOUT)
            # timeout de lectura = tiempo máximo sin recibir ni un byte (ni las líneas vacías de keep-alive)
            with requests.get(f"{API}/tweets/search/stream", headers=headers(), params=STREAM_PARAMS,
                              stream=True, timeout=(10, STALL_TIMEOUT)) as resp:
                if resp.status_code == 429:
                    wait = backoff.next_rate_limit()
                    reset_at = resp.headers.get("x-rate-limit-reset")
                    log.warning("event=stream_429 reason=%s rate_limit_reset=%s retry_in_s=%.0f "
                                "hint=%s", q(api_error_summary(resp)), reset_at, wait,
                                q("solo se permite 1 conexión por proyecto: ¿hay otro ingest conectado?"))
                    sleep_interruptible(wait)
                    continue
                if resp.status_code in (401, 403):
                    wait = backoff.next_http()
                    log.error("event=stream_auth_error status=%d error=%s retry_in_s=%.0f",
                              resp.status_code, q(api_error_summary(resp)), wait)
                    sleep_interruptible(wait)
                    continue
                if resp.status_code != 200:
                    wait = backoff.next_http()
                    log.error("event=stream_http_error status=%d error=%s retry_in_s=%.0f",
                              resp.status_code, q(api_error_summary(resp)), wait)
                    sleep_interruptible(wait)
                    continue

                log.info("event=stream_connected")
                backoff.reset()
                for line in resp.iter_lines(chunk_size=1):
                    if not running:
                        break
                    if not line or not line.strip():
                        continue  # keep-alive
                    handle_line(line, producer)
                log.warning("event=stream_closed_by_server")
        except requests.exceptions.ConnectionError as e:
            # requests envuelve el timeout de lectura del stream en ConnectionError("... Read timed out")
            wait = backoff.next_network()
            if "Read timed out" in str(e):
                log.warning("event=stream_stalled no_data_s=%d action=reconnect retry_in_s=%.2f", STALL_TIMEOUT, wait)
            else:
                log.warning("event=stream_network_error error=%s retry_in_s=%.2f", q(type(e).__name__), wait)
            sleep_interruptible(wait)
        except requests.exceptions.Timeout:
            wait = backoff.next_network()
            log.warning("event=stream_stalled no_data_s=%d action=reconnect retry_in_s=%.2f", STALL_TIMEOUT, wait)
            sleep_interruptible(wait)
        except requests.RequestException as e:
            wait = backoff.next_network()
            log.warning("event=stream_error error=%s retry_in_s=%.2f", q(type(e).__name__), wait)
            sleep_interruptible(wait)


def stop(signum, _frame):
    global running
    running = False
    log.info("event=shutdown signal=%d", signum)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("event=start enabled=%s rule_tag=%s rule_value=%s token=%s stall_timeout_s=%d",
             ENABLED, RULE_TAG, q(RULE_VALUE), mask(BEARER), STALL_TIMEOUT)
    if not ENABLED or not BEARER or not RULE_VALUE:
        log.warning("event=ingest_disabled reason=%s",
                    q("X_INGEST_ENABLED=false o faltan X_BEARER_TOKEN / X_RULE_VALUE: no se conecta a X"))
        while running:
            time.sleep(1)
        return
    producer = make_producer()
    try:
        stream_forever(producer)
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
