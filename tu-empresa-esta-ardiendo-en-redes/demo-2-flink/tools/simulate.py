"""
Simulador: escribe posts falsos en x.posts.raw (sin pasar por X) y comprueba cuántas alertas y correos salen.

Uso (desde la carpeta del proyecto):
    docker compose run --rm simulate escena
    docker compose run --rm simulate separados
    docker compose run --rm simulate duplicado
    docker compose run --rm simulate mixto
    docker compose run --rm simulate rafaga
Opciones:
    --tag demo        usar la misma etiqueta que el stream real (por defecto cada ejecución usa una propia)
    --pause 6         segundos entre posts (para que se vean llegar en el panel)
    --no-watch        no esperar a comprobar alertas y correos
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import OFFSET_END, Consumer, KafkaError, Producer, TopicPartition

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
RULE_VALUE = os.environ.get("X_RULE_VALUE", "#MiHashtagDemo")
HASHTAG = next((w for w in RULE_VALUE.split() if w.startswith("#")), "#MiHashtagDemo")
COOLDOWN_MINUTES = int(os.environ.get("COOLDOWN_MINUTES", "5") or 5)

NEG = [
    f"Habéis arruinado el sabor, qué decepción {HASHTAG}",
    f"Genial. Justo lo que nadie pidió. {HASHTAG}",
    f"Tercera vez que el pedido llega mal. No vuelvo {HASHTAG}",
    f"El nuevo envase es un desastre, se rompe al abrirlo {HASHTAG}",
    f"Atención al cliente no contesta desde hace una semana. Vergonzoso {HASHTAG}",
    f"Más caro y peor que antes. Muy mal {HASHTAG}",
]
POS = [
    f"Me encanta la nueva receta, buenísima {HASHTAG}",
    f"Enhorabuena al equipo, el servicio de hoy ha sido de diez {HASHTAG}",
]
NEU = [
    f"¿A qué hora abrís el domingo? {HASHTAG}",
    f"Mañana presentan la nueva gama en Bilbao {HASHTAG}",
]

# (sentimiento esperado, texto, minutos respecto a ahora, id repetido opcional)
SCENARIOS = {
    "escena": {
        "desc": "negativo, negativo 8 min después (correo) y un tercer negativo 15 min después (sin correo)",
        "posts": [("negative", NEG[0], -23), ("negative", NEG[1], -15), ("negative", NEG[2], 0)],
        "expected_emails": 1,
        "watch_s": 45,
    },
    "separados": {
        "desc": "dos negativos con 61 minutos de diferencia (sin correo)",
        "posts": [("negative", NEG[0], -61), ("negative", NEG[1], 0)],
        "expected_emails": 0,
        "watch_s": 40,
    },
    "duplicado": {
        "desc": "el mismo post_id dos veces: cuenta una sola vez (sin correo)",
        "posts": [("negative", NEG[3], -5, "dup"), ("negative", NEG[3], -5, "dup")],
        "expected_emails": 0,
        "watch_s": 40,
    },
    "mixto": {
        "desc": "positivos, neutros y negativos mezclados: 2 negativos en la hora (1 correo)",
        "posts": [("positive", POS[0], -40), ("neutral", NEU[0], -35), ("negative", NEG[4], -30),
                  ("positive", POS[1], -20), ("neutral", NEU[1], -10), ("negative", NEG[5], -5)],
        "expected_emails": 1,
        "watch_s": 45,
    },
    "rafaga": {
        "desc": f"4 negativos seguidos: 1 correo al momento y otro al terminar la espera de {COOLDOWN_MINUTES} min",
        "posts": [("negative", NEG[0], -4), ("negative", NEG[1], -3), ("negative", NEG[2], -2),
                  ("negative", NEG[3], -1)],
        "expected_emails": 2,
        "watch_s": COOLDOWN_MINUTES * 60 + 60,
    },
}


def iso(dt: datetime, ms: bool = False) -> str:
    return dt.isoformat(timespec="milliseconds" if ms else "seconds").replace("+00:00", "Z")


def fake_post_id() -> str:
    # Parecido a un id de X (snowflake): milisegundos desde la época de Twitter << 22 + aleatorio
    return str(((int(time.time() * 1000) - 1288834974657) << 22) | random.getrandbits(22))


def log(event: str, **kw) -> None:
    parts = " ".join(f"{k}={json.dumps(v, ensure_ascii=False) if isinstance(v, str) and ' ' in v else v}"
                     for k, v in kw.items())
    print(f"{datetime.now().strftime('%H:%M:%S')} event={event} {parts}", flush=True)


def watcher():
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"simulate-{fake_post_id()}",
                  "enable.auto.commit": False, "auto.offset.reset": "latest"})
    tps = []
    for topic in ("alerts", "notifications"):
        _low, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        tps.append(TopicPartition(topic, 0, high if high >= 0 else OFFSET_END))
    c.assign(tps)
    return c


def main():
    ap = argparse.ArgumentParser(description="Simulador de posts para la demo")
    ap.add_argument("scenario", choices=sorted(SCENARIOS))
    ap.add_argument("--tag", default=None, help="rule_tag de los posts (por defecto sim-<escenario>-<hora>)")
    ap.add_argument("--pause", type=float, default=6.0, help="segundos entre posts")
    ap.add_argument("--no-watch", action="store_true", help="no esperar a comprobar alertas y correos")
    args = ap.parse_args()

    sc = SCENARIOS[args.scenario]
    tag = args.tag or f"sim-{args.scenario}-{datetime.now().strftime('%H%M%S')}"
    log("scenario_start", scenario=args.scenario, rule_tag=tag, desc=sc["desc"],
        expected_emails=sc["expected_emails"])

    consumer = None if args.no_watch else watcher()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
    dup_ids = {}
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for i, item in enumerate(sc["posts"]):
        expected, text, minutes = item[:3]
        dup_key = item[3] if len(item) > 3 else None
        post_id = dup_ids.setdefault(dup_key, fake_post_id()) if dup_key else fake_post_id()
        published = now + timedelta(minutes=minutes)
        msg = {
            "post_id": post_id,
            "text": text,
            "author_username": "sim_user",
            "url": f"https://x.com/sim_user/status/{post_id}",
            "published_at": iso(published),
            "ingested_at": iso(datetime.now(timezone.utc), ms=True),
            "rule_tag": tag,
            "source": "sim",
        }
        producer.produce("x.posts.raw", key=post_id.encode(), value=json.dumps(msg, ensure_ascii=False).encode())
        producer.flush(10)
        log("post_sent", post_id=post_id, expected_sentiment=expected, published_at=msg["published_at"],
            text=text[:50])
        if i < len(sc["posts"]) - 1:
            time.sleep(args.pause)

    if consumer is None:
        return 0

    deadline = time.time() + sc["watch_s"]
    alerts, emails = set(), {}
    log("watching", seconds=sc["watch_s"])
    while time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            if m is not None and m.error().code() != KafkaError._PARTITION_EOF:
                log("kafka_error", error=str(m.error()))
            continue
        data = json.loads(m.value())
        if data.get("rule_tag") != tag:
            continue
        if m.topic() == "alerts" and data["alert_id"] not in alerts:
            alerts.add(data["alert_id"])
            log("alert_seen", alert_id=data["alert_id"], new_negatives=data.get("new_negatives"),
                triggered_by_post_id=data.get("triggered_by_post_id"))
        elif m.topic() == "notifications":
            emails[data["alert_id"]] = data
            log("notification_seen", alert_id=data["alert_id"], status=data.get("status"),
                used_fallback_template=data.get("used_fallback_template"), subject=data.get("subject", ""))
        if len(emails) > sc["expected_emails"]:
            break
    consumer.close()

    sent = sum(1 for n in emails.values() if n.get("status") == "sent")
    ok = len(alerts) == sc["expected_emails"] and sent == sc["expected_emails"]
    log("scenario_result", scenario=args.scenario, rule_tag=tag, alerts=len(alerts), emails_sent=sent,
        emails_failed=len(emails) - sent, expected=sc["expected_emails"], result="OK" if ok else "FALLO")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
