"""
notifier: alerts -> (Ollama redacta el correo) -> SMTP -> notifications.

- Si Ollama falla o tarda más de 30 s, se usa una plantilla fija (used_fallback_template=true).
- Idempotente: los alert_id ya enviados se guardan en un fichero de un volumen y no se reenvían.
"""
import concurrent.futures
import json
import logging
import os
import re
import signal
import smtplib
import ssl
import sys
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from confluent_kafka import Consumer, KafkaError, Producer

TOPIC_ALERTS = "alerts"
TOPIC_NOTIFICATIONS = "notifications"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY", "").strip()
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "https://ollama.com/api").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v4.1-flash")
OLLAMA_THINK = os.environ.get("OLLAMA_THINK", "false").strip().lower()
OLLAMA_DEADLINE_S = 30

BRAND_NAME = os.environ.get("BRAND_NAME", "Marca Demo")
RULE_VALUE = os.environ.get("X_RULE_VALUE", "")
TZ = ZoneInfo(os.environ.get("TZ", "UTC") or "UTC")

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587") or 587)
SMTP_STARTTLS = os.environ.get("SMTP_STARTTLS", "true").strip().lower() == "true"
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER)
ALERT_EMAIL_TO = [a.strip() for a in os.environ.get("ALERT_EMAIL_TO", "").split(",") if a.strip()]

SENT_FILE = Path(os.environ.get("SENT_ALERTS_FILE", "/data/sent_alerts.txt"))
PROMPT = (Path(__file__).parent / "email_prompt.txt").read_text(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s level=%(levelname)s svc=notifier %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("notifier")
running = True


def q(s) -> str:
    return json.dumps(str(s), ensure_ascii=False)


def mask(secret: str) -> str:
    if not secret:
        return "(vacío)"
    return secret[:4] + "****" if len(secret) > 8 else "****"


def utc_now_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def local_time(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.astimezone(TZ).strftime("%H:%M:%S")
    except (AttributeError, ValueError):
        return str(iso)


def clip(text: str, n: int = 1000) -> str:
    text = text or ""
    return text if len(text) <= n else text[:n] + "…"


# ---------------------------------------------------------------- idempotencia

def load_sent() -> set:
    if not SENT_FILE.exists():
        return set()
    return {line.strip() for line in SENT_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}


def mark_sent(alert_id: str) -> None:
    SENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SENT_FILE.open("a", encoding="utf-8") as f:
        f.write(alert_id + "\n")
        f.flush()
        os.fsync(f.fileno())


# ---------------------------------------------------------------- redacción

def counts_text(counts: dict) -> str:
    return (f"{counts.get('negative', 0)} negativos, {counts.get('positive', 0)} positivos, "
            f"{counts.get('neutral', 0)} neutros, {counts.get('unknown', 0)} sin clasificar")


def posts_text(posts: list) -> str:
    lines = []
    for p in posts:
        lines.append(f'- "{clip(p.get("text", ""))}" — @{p.get("author_username", "?")}, '
                     f'{local_time(p.get("published_at", ""))}, {p.get("url", "")}')
    return "\n".join(lines)


def build_prompt(alert: dict) -> str:
    posts = alert.get("negative_posts", [])
    return (PROMPT
            .replace("{BRAND_NAME}", BRAND_NAME)
            .replace("{RULE_VALUE}", RULE_VALUE or alert.get("rule_tag", ""))
            .replace("{WINDOW_MINUTES}", str(alert.get("window_minutes", 60)))
            .replace("{NEGATIVES}", str(len(posts)))
            .replace("{COUNTS}", counts_text(alert.get("counts_in_window", {})))
            .replace("{NEGATIVE_POSTS}", posts_text(posts)))


def normalize_subject(subject: str) -> str:
    s = " ".join(subject.split())
    if not s.lower().startswith("revisar conversación"):
        s = "Revisar conversación: " + s
    return s if len(s) < 70 else s[:68].rstrip() + "…"


def parse_email_json(content: str):
    s = re.sub(r"^```[a-zA-Z]*\s*|\s*```$", "", (content or "").strip())
    data = json.loads(s)
    if not isinstance(data, dict):
        raise ValueError("la respuesta no es un objeto JSON")
    subject, body = data.get("subject"), data.get("body")
    if not isinstance(subject, str) or not subject.strip() or not isinstance(body, str) or not body.strip():
        raise ValueError("faltan subject o body")
    return normalize_subject(subject), body.strip()


def call_ollama(prompt: str):
    body = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "options": {"temperature": 0.3},
        "format": {
            "type": "object",
            "properties": {"subject": {"type": "string"}, "body": {"type": "string"}},
            "required": ["subject", "body"],
        },
        "messages": [{"role": "user", "content": prompt}],
    }
    if OLLAMA_THINK in ("true", "false"):
        body["think"] = OLLAMA_THINK == "true"
    resp = httpx.post(f"{OLLAMA_BASE_URL}/chat", json=body, timeout=OLLAMA_DEADLINE_S,
                      headers={"Authorization": f"Bearer {OLLAMA_API_KEY}"})
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    content = resp.json().get("message", {}).get("content", "")
    return parse_email_json(content)


def fallback_email(alert: dict):
    posts = alert.get("negative_posts", [])
    n = len(posts)
    subject = normalize_subject(f"Revisar conversación: {n} comentarios negativos sobre {BRAND_NAME}")
    body = (
        f"Hola, equipo:\n\n"
        f"En los últimos {alert.get('window_minutes', 60)} minutos han aparecido {n} comentarios negativos "
        f"sobre {BRAND_NAME} con {RULE_VALUE or alert.get('rule_tag', '')}. Llega este aviso porque se ha "
        f"alcanzado el umbral de {alert.get('negative_threshold', n)} comentarios negativos nuevos.\n\n"
        f"Comentarios:\n{posts_text(posts)}\n\n"
        f"Recuento de la última hora: {counts_text(alert.get('counts_in_window', {}))}.\n\n"
        f"Acciones:\n"
        f"1. Revisar la conversación.\n"
        f"2. Pausar las publicaciones programadas hasta confirmar.\n\n"
        f"(Aviso redactado con la plantilla fija: el modelo de lenguaje no respondió a tiempo.)\n"
    )
    return subject, body


def draft_email(alert: dict, executor):
    alert_id = alert["alert_id"]
    start = time.monotonic()
    future = executor.submit(call_ollama, build_prompt(alert))
    try:
        subject, body = future.result(timeout=OLLAMA_DEADLINE_S)
        log.info("event=email_drafted alert_id=%s source=ollama model=%s latency_ms=%d",
                 alert_id, OLLAMA_MODEL, (time.monotonic() - start) * 1000)
        return subject, body, False
    except concurrent.futures.TimeoutError:
        reason = f"timeout de {OLLAMA_DEADLINE_S} s"
    except Exception as e:  # noqa: BLE001 - cualquier fallo de Ollama => plantilla
        reason = f"{type(e).__name__}: {str(e)[:200]}"
    log.warning("event=email_fallback_template alert_id=%s reason=%s", alert_id, q(reason))
    subject, body = fallback_email(alert)
    return subject, body, True


# ---------------------------------------------------------------- SMTP

def send_email(subject: str, body: str) -> None:
    if not SMTP_HOST or not ALERT_EMAIL_TO:
        raise RuntimeError("SMTP_HOST o ALERT_EMAIL_TO vacíos")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(ALERT_EMAIL_TO)
    msg["Date"] = formatdate(localtime=False)
    domain = SMTP_FROM.split("@")[-1] if "@" in SMTP_FROM else None
    msg["Message-ID"] = make_msgid(domain=domain)
    msg.set_content(body, subtype="plain", charset="utf-8")

    context = ssl.create_default_context()
    if SMTP_PORT == 465:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30, context=context)
    else:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    try:
        server.ehlo()
        if SMTP_STARTTLS and SMTP_PORT != 465:
            server.starttls(context=context)
            server.ehlo()
        if SMTP_USER:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001
            pass


def send_with_retries(alert_id: str, subject: str, body: str):
    last_error = None
    for attempt, wait in enumerate((0, 5, 15), start=1):
        if wait:
            time.sleep(wait)
        try:
            send_email(subject, body)
            log.info("event=email_sent alert_id=%s to=%s attempt=%d subject=%s",
                     alert_id, q(",".join(ALERT_EMAIL_TO)), attempt, q(subject))
            return None
        except Exception as e:  # noqa: BLE001
            last_error = f"{type(e).__name__}: {str(e)[:300]}"
            log.warning("event=smtp_error alert_id=%s attempt=%d error=%s", alert_id, attempt, q(last_error))
    return last_error


# ---------------------------------------------------------------- bucle

def stop(signum, _frame):
    global running
    running = False
    log.info("event=shutdown signal=%d", signum)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    sent = load_sent()
    log.info("event=start model=%s ollama_key=%s smtp=%s:%d starttls=%s smtp_user=%s smtp_password=%s to=%s "
             "already_sent=%d", OLLAMA_MODEL, mask(OLLAMA_API_KEY), SMTP_HOST, SMTP_PORT, SMTP_STARTTLS,
             SMTP_USER, mask(SMTP_PASSWORD), q(",".join(ALERT_EMAIL_TO)), len(sent))

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "notifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "notifier",
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "client.id": "notifier"})
    consumer.subscribe([TOPIC_ALERTS])
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("event=kafka_error error=%s", q(msg.error()))
                continue
            try:
                alert = json.loads(msg.value())
                alert_id = alert["alert_id"]
            except (ValueError, KeyError, TypeError):
                log.error("event=alert_invalid offset=%s", msg.offset())
                consumer.commit(message=msg, asynchronous=False)
                continue

            if alert_id in sent:
                log.info("event=alert_already_sent alert_id=%s action=skip", alert_id)
                consumer.commit(message=msg, asynchronous=False)
                continue

            post_ids = ",".join(p.get("post_id", "?") for p in alert.get("negative_posts", []))
            log.info("event=alert_received alert_id=%s rule_tag=%s negative_post_ids=%s",
                     alert_id, alert.get("rule_tag"), post_ids)
            subject, body, used_fallback = draft_email(alert, executor)
            error = send_with_retries(alert_id, subject, body)

            notification = {
                "alert_id": alert_id,
                "rule_tag": alert.get("rule_tag"),
                "subject": subject,
                "sent_at": utc_now_iso_ms(),
                "to": ALERT_EMAIL_TO,
                "status": "sent" if error is None else "failed",
                "used_fallback_template": used_fallback,
            }
            if error is not None:
                notification["error"] = error
                log.error("event=email_failed alert_id=%s error=%s", alert_id, q(error))
            else:
                mark_sent(alert_id)
                sent.add(alert_id)

            producer.produce(TOPIC_NOTIFICATIONS, key=alert_id.encode(),
                             value=json.dumps(notification, ensure_ascii=False).encode("utf-8"))
            producer.flush(10)
            log.info("event=notification_written alert_id=%s status=%s used_fallback_template=%s",
                     alert_id, notification["status"], used_fallback)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    main()
