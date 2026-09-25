"""
panel: web mínima para grabar la demo.

Lee x.posts.raw, x.posts.scored, alerts y notifications desde el principio (sin grupo persistente),
guarda en memoria lo último y lo emite en directo al navegador con Server-Sent Events.
"""
import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = ["x.posts.raw", "x.posts.scored", "alerts", "notifications"]
MAX_POSTS = 200
MAX_ALERTS = 50

CONFIG = {
    "brand": os.environ.get("BRAND_NAME", "Marca Demo"),
    "rule_value": os.environ.get("X_RULE_VALUE", ""),
    "rule_tag": os.environ.get("X_RULE_TAG", "demo"),
    "threshold": int(os.environ.get("NEGATIVE_THRESHOLD", "2") or 2),
    "window_minutes": int(os.environ.get("WINDOW_MINUTES", "60") or 60),
    "cooldown_minutes": int(os.environ.get("COOLDOWN_MINUTES", "5") or 5),
    "tz": os.environ.get("TZ", "UTC") or "UTC",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s level=%(levelname)s svc=panel %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("panel")

STATIC = Path(__file__).parent / "static"


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.posts: "OrderedDict[str, dict]" = OrderedDict()
        self.alerts: "OrderedDict[str, dict]" = OrderedDict()
        self.clients: set = set()
        self.loop: asyncio.AbstractEventLoop | None = None

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "type": "snapshot",
                "config": CONFIG,
                "server_now_ms": int(time.time() * 1000),
                "posts": list(self.posts.values()),
                "alerts": list(self.alerts.values()),
            }

    def broadcast(self, event: dict) -> None:
        if self.loop is None:
            return
        for q in list(self.clients):
            self.loop.call_soon_threadsafe(q.put_nowait, event)

    # --- actualización desde Kafka (hilo del consumidor)

    def upsert_post(self, data: dict, stage: str) -> None:
        post_id = data.get("post_id")
        if not post_id:
            return
        with self.lock:
            p = self.posts.get(post_id, {})
            if stage == "raw" and p.get("sentiment"):
                return  # ya estaba clasificado (reproceso)
            p.update({k: v for k, v in data.items() if v is not None})
            self.posts[post_id] = p
            self.posts.move_to_end(post_id)
            while len(self.posts) > MAX_POSTS:
                self.posts.popitem(last=False)
            event = {"type": "post", "post": dict(p)}
        self.broadcast(event)

    def add_alert(self, alert: dict) -> None:
        alert_id = alert.get("alert_id")
        if not alert_id:
            return
        with self.lock:
            if alert_id in self.alerts:
                return  # duplicado at-least-once
            a = dict(alert)
            self.alerts[alert_id] = a
            while len(self.alerts) > MAX_ALERTS:
                self.alerts.popitem(last=False)
            updated = []
            for np in alert.get("negative_posts", []):
                p = self.posts.get(np.get("post_id"))
                if p is not None and not p.get("alert_at"):
                    p["alert_at"] = alert.get("alert_at")
                    p["alert_id"] = alert_id
                    updated.append(dict(p))
        self.broadcast({"type": "alert", "alert": a})
        for p in updated:
            self.broadcast({"type": "post", "post": p})

    def add_notification(self, n: dict) -> None:
        alert_id = n.get("alert_id")
        with self.lock:
            a = self.alerts.get(alert_id)
            if a is None:
                a = {"alert_id": alert_id}
                self.alerts[alert_id] = a
            a["notification"] = n
            event = {"type": "alert", "alert": dict(a)}
        self.broadcast(event)


state = State()


def consume_forever() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"panel-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "panel",
    })
    consumer.subscribe(TOPICS)
    log.info("event=consumer_started topics=%s", ",".join(TOPICS))
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                log.warning("event=kafka_error error=%s", json.dumps(str(msg.error())))
            continue
        try:
            data = json.loads(msg.value())
        except (ValueError, TypeError):
            log.warning("event=invalid_json topic=%s offset=%s", msg.topic(), msg.offset())
            continue
        topic = msg.topic()
        if topic == "x.posts.raw":
            state.upsert_post(data, "raw")
        elif topic == "x.posts.scored":
            state.upsert_post(data, "scored")
            log.info("event=post_shown post_id=%s sentiment=%s", data.get("post_id"), data.get("sentiment"))
        elif topic == "alerts":
            state.add_alert(data)
            log.info("event=alert_shown alert_id=%s", data.get("alert_id"))
        elif topic == "notifications":
            state.add_notification(data)
            log.info("event=notification_shown alert_id=%s status=%s", data.get("alert_id"), data.get("status"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    state.loop = asyncio.get_running_loop()
    threading.Thread(target=consume_forever, name="kafka-consumer", daemon=True).start()
    yield


app = FastAPI(title="Panel demo", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/healthz")
async def healthz():
    return JSONResponse({"ok": True})


@app.get("/events")
async def events(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    state.clients.add(q)

    async def stream():
        try:
            yield f"data: {json.dumps(state.snapshot(), ensure_ascii=False)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    event = {"type": "tick", "server_now_ms": int(time.time() * 1000)}
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            state.clients.discard(q)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
