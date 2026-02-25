"""
Bitrix24 <> WhatsApp bridge via Evolution API — multi-instance.

Each WhatsApp number (Evolution instance) has its own Open Line in Bitrix24.

Flows:
  1. WhatsApp → Bitrix24
       Evolution fires MESSAGES_UPSERT → POST /webhook/evolution/{instance_name}
       → downloads media via Evolution API → saves to /media/
       → imconnector.send.messages (LINE = matching open line)

  2. Bitrix24 → WhatsApp
       Agent replies in Open Channel → OnImConnectorMessageAdd event
       → POST /webhook/bitrix (form-encoded or JSON)
       → send via the correct Evolution instance
       → imconnector.send.status.delivery (confirm delivery)
"""
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse

import bitrix
import evolution
import storage
from config import (
    get_instance_by_line,
    get_instance_by_name,
    get_instances,
    init_instances,
    settings,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).resolve().parent
MEDIA_DIR = APP_DIR / "media"
STATIC_DIR = APP_DIR / "static"


# ─── Startup ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.init_db()
    init_instances()
    MEDIA_DIR.mkdir(exist_ok=True)
    instances = get_instances()
    logger.info(
        "Loaded %d instance(s): %s",
        len(instances),
        ", ".join(
            f"{i.name} ({i.evolution_instance}→line {i.bitrix24_line_id})"
            for i in instances
        ),
    )
    yield


app = FastAPI(
    title="Bitrix24 <> WhatsApp Bridge (multi-instance)",
    lifespan=lifespan,
)


# ════════════════════════════════════════════════════════════════════════════
# 1.  Evolution API webhook  →  Bitrix24
#     URL: /webhook/evolution/{instance_name}
# ════════════════════════════════════════════════════════════════════════════

@app.post("/webhook/evolution/{instance_name}")
async def evolution_webhook(
    instance_name: str, request: Request, bg: BackgroundTasks,
):
    inst = get_instance_by_name(instance_name)
    if not inst:
        logger.warning("Webhook for unknown instance: %s", instance_name)
        raise HTTPException(404, f"Instance '{instance_name}' not configured")

    payload: dict = await request.json()

    if payload.get("event") != "MESSAGES_UPSERT":
        return {"ok": True}

    data = payload.get("data", {})
    key = data.get("key", {})

    # Ignore own messages and group chats
    if key.get("fromMe"):
        return {"ok": True}
    remote_jid: str = key.get("remoteJid", "")
    if "@g.us" in remote_jid:
        return {"ok": True}

    wa_phone = remote_jid.split("@")[0]
    push_name = data.get("pushName") or wa_phone
    message_id = key.get("id", "")
    message = data.get("message", {})

    text, media_info = _parse_wa_message(message)

    if not text and not media_info:
        logger.warning(
            "[%s] Unsupported WA msg from %s: %s",
            inst.name, wa_phone, list(message.keys()),
        )
        return {"ok": True}

    logger.info(
        "[%s] WA→Bitrix | %s (%s): %s",
        inst.name, wa_phone, push_name,
        (text[:80] if text else "[media]"),
    )
    bg.add_task(
        _forward_to_bitrix,
        inst.name,
        inst.evolution_instance,
        inst.bitrix24_line_id,
        wa_phone,
        push_name,
        message_id,
        text,
        media_info,
        key,  # pass original key for media download
    )
    return {"ok": True}


def _parse_wa_message(msg: dict) -> tuple[str, dict | None]:
    """
    Parse a WhatsApp message from the Evolution webhook.
    Returns (text, media_info) where media_info is None for text-only messages
    or {"type": "image"|"video"|"audio"|"document"|"sticker", "name": "...", "mime": "..."}.
    """
    text = ""
    media = None

    if "conversation" in msg:
        text = msg["conversation"]
    elif "extendedTextMessage" in msg:
        text = msg["extendedTextMessage"].get("text", "")
    elif "imageMessage" in msg:
        text = msg["imageMessage"].get("caption", "")
        media = {
            "type": "image",
            "name": "image.jpg",
            "mime": msg["imageMessage"].get("mimetype", "image/jpeg"),
        }
    elif "videoMessage" in msg:
        text = msg["videoMessage"].get("caption", "")
        media = {
            "type": "video",
            "name": "video.mp4",
            "mime": msg["videoMessage"].get("mimetype", "video/mp4"),
        }
    elif "audioMessage" in msg:
        media = {
            "type": "audio",
            "name": "audio.ogg",
            "mime": msg["audioMessage"].get("mimetype", "audio/ogg"),
        }
    elif "documentMessage" in msg:
        text = msg["documentMessage"].get("caption", "")
        fname = msg["documentMessage"].get("fileName", "document")
        media = {
            "type": "document",
            "name": fname,
            "mime": msg["documentMessage"].get(
                "mimetype", "application/octet-stream"
            ),
        }
    elif "stickerMessage" in msg:
        media = {"type": "sticker", "name": "sticker.webp", "mime": "image/webp"}

    return text, media


async def _forward_to_bitrix(
    instance_name: str,
    evo_instance: str,
    line_id: str,
    wa_phone: str,
    push_name: str,
    message_id: str,
    text: str,
    media_info: dict | None,
    message_key: dict,
):
    """Download media (if any), then forward to Bitrix24."""
    files: list[dict] = []

    # Download media from Evolution and save locally
    if media_info:
        try:
            media_bytes = await evolution.get_media_base64(
                evo_instance, message_key,
            )
            ext = _ext_from_name(media_info["name"])
            filename = f"{uuid4().hex}.{ext}"
            file_path = MEDIA_DIR / filename
            file_path.write_bytes(media_bytes)
            _cleanup_old_media()

            public_url = f"{settings.app_url}/media/{filename}"
            files.append({"link": public_url, "name": media_info["name"]})
            logger.info(
                "[%s] Media saved: %s (%d bytes)",
                instance_name, filename, len(media_bytes),
            )
        except Exception as exc:
            logger.error(
                "[%s] Media download failed (sending text only): %s",
                instance_name, exc,
            )

    try:
        result = await bitrix.send_message(
            line_id=line_id,
            wa_phone=wa_phone,
            push_name=push_name,
            message_id=message_id,
            text=text,
            files=files or None,
        )
        _capture_chat_id(result, wa_phone, instance_name, push_name)

    except Exception as exc:
        logger.error("[%s] WA→Bitrix failed: %s", instance_name, exc)


def _capture_chat_id(
    result: dict, wa_phone: str, instance_name: str, push_name: str,
):
    """
    Extract CHAT_ID from the imconnector.send.messages response.
    The Bitrix24 response structure is:
        result.DATA.CHAT_ID  →  {wa_phone: chat_id}  or  int
    """
    result_data = result.get("result", {})
    if not isinstance(result_data, dict):
        logger.warning(
            "[%s] Unexpected imconnector response type: %s",
            instance_name, type(result_data),
        )
        return

    data_block = result_data.get("DATA", {})
    if not isinstance(data_block, dict):
        logger.warning(
            "[%s] Unexpected DATA block type: %s | full: %s",
            instance_name, type(data_block), result_data,
        )
        return

    chat_id_raw = data_block.get("CHAT_ID")
    chat_id = ""

    if isinstance(chat_id_raw, dict):
        # Map format: {"5511999991111": 123}
        chat_id = str(chat_id_raw.get(wa_phone, ""))
    elif chat_id_raw is not None:
        # Direct format: CHAT_ID = 123
        chat_id = str(chat_id_raw)

    if chat_id:
        storage.save_session(chat_id, wa_phone, instance_name, push_name)
        logger.info(
            "[%s] Mapped chat_id=%s <> WA %s",
            instance_name, chat_id, wa_phone,
        )
    else:
        logger.warning(
            "[%s] Could not extract CHAT_ID. DATA: %s",
            instance_name, data_block,
        )


# ════════════════════════════════════════════════════════════════════════════
# 2.  Bitrix24 event  →  Evolution API (WhatsApp)
#
#     Bitrix24 sends OnImConnectorMessageAdd as form-encoded (PHP arrays)
#     or occasionally JSON.  We handle both.
# ════════════════════════════════════════════════════════════════════════════

@app.post("/webhook/bitrix")
async def bitrix_webhook(request: Request, bg: BackgroundTasks):
    body = await request.body()
    payload = _parse_bitrix_payload(body)

    logger.info(
        "Bitrix24 event received: %s | top-keys: %s",
        payload.get("event", "?"),
        list(payload.keys()),
    )

    # Validate application_token
    app_token = (
        payload.get("auth", {}).get("application_token", "")
        if isinstance(payload.get("auth"), dict)
        else ""
    )
    if (
        settings.bitrix24_application_token
        and app_token != settings.bitrix24_application_token
    ):
        logger.warning("Bitrix24 webhook: invalid application_token")
        raise HTTPException(403, "Invalid application_token")

    event = str(payload.get("event", "")).upper()
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return {"ok": True}

    if event == "ONIMCONNECTORMESSAGEADD":
        connector = data.get("CONNECTOR", "")
        if connector == settings.bitrix24_connector_id:
            bg.add_task(_forward_to_whatsapp, data)

    return {"ok": True}


def _parse_bitrix_payload(body: bytes) -> dict:
    """Parse Bitrix24 webhook payload (JSON or PHP-style form-encoded)."""
    # Try JSON first
    try:
        return json.loads(body)
    except (json.JSONDecodeError, ValueError):
        pass

    # Parse form-encoded with PHP array notation:
    # data[MESSAGES][0][im][chat_id]=123  →  {"data":{"MESSAGES":[{"im":{"chat_id":"123"}}]}}
    flat = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
    result: dict = {}
    for compound_key, values in sorted(flat.items()):
        value = values[0] if len(values) == 1 else values
        keys = re.findall(r"[^\[\]]+", compound_key)
        if keys:
            _nested_set(result, keys, value)
    return result


def _nested_set(d, keys: list[str], value):
    """Set a value in a nested dict/list structure from a key path."""
    for i, key in enumerate(keys[:-1]):
        next_key = keys[i + 1]
        next_is_idx = next_key.isdigit()

        if key.isdigit():
            idx = int(key)
            if isinstance(d, list):
                while len(d) <= idx:
                    d.append([] if next_is_idx else {})
                d = d[idx]
        else:
            if key not in d:
                d[key] = [] if next_is_idx else {}
            d = d[key]

    last = keys[-1]
    if isinstance(d, list):
        idx = int(last)
        while len(d) <= idx:
            d.append(None)
        d[idx] = value
    else:
        d[last] = value


async def _forward_to_whatsapp(data: dict):
    """
    Forward a Bitrix24 agent reply to WhatsApp.
    Handles the OnImConnectorMessageAdd event data structure:
        data.CONNECTOR, data.LINE, data.MESSAGES[n].{im, message, chat}
    """
    try:
        line_id = str(data.get("LINE", ""))
        messages = data.get("MESSAGES", [])

        if not isinstance(messages, list) or not messages:
            logger.warning("Bitrix event has no MESSAGES. Data keys: %s", list(data.keys()))
            return

        for msg_block in messages:
            if not isinstance(msg_block, dict):
                continue

            im = msg_block.get("im", {})
            if not isinstance(im, dict):
                im = {}
            bitrix_chat_id = str(im.get("chat_id", ""))
            bitrix_msg_id = str(im.get("message_id", ""))

            msg_content = msg_block.get("message", {})
            if not isinstance(msg_content, dict):
                msg_content = {}
            text = str(msg_content.get("text", ""))
            files = msg_content.get("files", []) or []
            if not isinstance(files, list):
                files = []

            # wa_chat_id = external chat ID (the wa_phone we set)
            chat_block = msg_block.get("chat", {})
            if not isinstance(chat_block, dict):
                chat_block = {}
            wa_chat_id = str(chat_block.get("id", ""))

            # Resolve WA phone and instance
            wa_phone, instance_name, evo_instance = _resolve_destination(
                bitrix_chat_id, wa_chat_id, line_id,
            )
            if not wa_phone or not evo_instance:
                logger.warning(
                    "Cannot route Bitrix message: chat_id=%s wa_chat=%s line=%s",
                    bitrix_chat_id, wa_chat_id, line_id,
                )
                continue

            logger.info(
                "[%s] Bitrix→WA | chat=%s → %s: %s",
                instance_name, bitrix_chat_id, wa_phone,
                (text[:80] if text else "[media]"),
            )

            # Send text
            if text:
                await evolution.send_text(evo_instance, wa_phone, text)

            # Send files
            for f in files:
                if not isinstance(f, dict):
                    continue
                url = f.get("link") or f.get("url", "")
                name = f.get("name", "file")
                mime = f.get("type", "")
                if not url:
                    continue
                await _send_file_to_wa(evo_instance, wa_phone, url, name, mime)

            # Confirm delivery to Bitrix24
            if line_id and bitrix_msg_id:
                try:
                    await bitrix.delivery_status(
                        line_id,
                        [{"id": bitrix_msg_id, "chat": {"id": wa_chat_id or wa_phone}}],
                    )
                except Exception as exc:
                    logger.warning("delivery_status failed (non-critical): %s", exc)

    except Exception as exc:
        logger.error("Bitrix→WA failed: %s", exc, exc_info=True)


def _resolve_destination(
    bitrix_chat_id: str, wa_chat_id: str, line_id: str,
) -> tuple[str, str, str]:
    """
    Resolve (wa_phone, instance_name, evolution_instance) from available info.
    Priority: session lookup by chat_id > event chat.id + line lookup.
    """
    # Primary: look up by Bitrix chat_id in our saved sessions
    if bitrix_chat_id:
        session = storage.get_session_by_chat(bitrix_chat_id)
        if session:
            inst = get_instance_by_name(session["instance_name"])
            if inst:
                return session["wa_phone"], inst.name, inst.evolution_instance

    # Fallback: use chat.id as wa_phone + resolve instance from LINE
    if wa_chat_id and line_id:
        inst = get_instance_by_line(line_id)
        if inst:
            return wa_chat_id, inst.name, inst.evolution_instance

    return "", "", ""


async def _send_file_to_wa(
    instance: str, phone: str, url: str, name: str, mime: str,
):
    """Route a file to the correct Evolution API method based on MIME or extension."""
    mime_lower = mime.lower()
    ext = _ext_from_name(name)

    if "audio" in mime_lower or ext in {"ogg", "mp3", "wav", "m4a", "aac", "opus"}:
        await evolution.send_audio(instance, phone, url)
    elif "image" in mime_lower or ext in {"jpg", "jpeg", "png", "gif", "webp", "bmp"}:
        await evolution.send_media(instance, phone, "image", url, name)
    elif "video" in mime_lower or ext in {"mp4", "mov", "avi", "mkv", "webm"}:
        await evolution.send_media(instance, phone, "video", url, name)
    else:
        await evolution.send_media(instance, phone, "document", url, name)


# ════════════════════════════════════════════════════════════════════════════
# 3.  Media proxy  (serve downloaded WA media to Bitrix24)
# ════════════════════════════════════════════════════════════════════════════

@app.get("/media/{filename}")
async def serve_media(filename: str):
    """Serve a media file downloaded from WhatsApp."""
    # Sanitize filename to prevent path traversal
    safe = Path(filename).name
    path = MEDIA_DIR / safe
    if not path.exists() or not path.is_file():
        raise HTTPException(404)
    return FileResponse(path)


def _ext_from_name(name: str) -> str:
    return name.rsplit(".", 1)[-1].lower() if "." in name else "bin"


def _cleanup_old_media(max_age_seconds: int = 86400):
    """Delete media files older than max_age_seconds (default 24h)."""
    cutoff = time.time() - max_age_seconds
    try:
        for f in MEDIA_DIR.iterdir():
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# 4.  Bitrix24 Local App install handler
# ════════════════════════════════════════════════════════════════════════════

@app.post("/bitrix/install")
@app.get("/bitrix/install")
async def bitrix_install(request: Request):
    if request.method == "POST":
        raw = await request.form()
        payload = dict(raw)
    else:
        payload = dict(request.query_params)

    logger.info("Bitrix24 install handler. Keys: %s", list(payload.keys()))

    auth = payload.get("auth", {})
    if isinstance(auth, str):
        try:
            auth = json.loads(auth)
        except Exception:
            auth = {}

    access_token  = auth.get("access_token") or payload.get("AUTH_ID", "")
    refresh_token = auth.get("refresh_token") or payload.get("REFRESH_ID", "")
    expires_in    = int(auth.get("expires_in") or payload.get("AUTH_EXPIRES") or 3600)
    # Bitrix24 sends SERVER_ENDPOINT (not DOMAIN) in the install callback
    server_endpoint = (
        payload.get("SERVER_ENDPOINT")
        or auth.get("domain")
        or payload.get("DOMAIN", "")
    )
    member_id = payload.get("member_id") or payload.get("MEMBER_ID", "")
    domain = server_endpoint  # store the REST endpoint as domain for API calls

    if access_token and server_endpoint:
        storage.save_oauth(
            access_token, refresh_token, expires_in, domain,
            member_id=member_id, server_endpoint=server_endpoint,
        )
        app_token = (
            auth.get("application_token") or payload.get("APPLICATION_TOKEN", "")
        )
        if app_token:
            logger.info(
                "application_token: %s — set BITRIX24_APPLICATION_TOKEN in .env",
                app_token,
            )
        logger.info(
            "Install OK: member_id=%s, endpoint=%s, expires_in=%s",
            member_id, server_endpoint, expires_in,
        )
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;padding:40px;text-align:center'>"
            "<h2 style='color:#10b981'>WhatsApp Bridge instalado!</h2>"
            "<p>Abra a <b>UI do conector</b> na Linha Aberta para configurar.</p>"
            "<p style='margin-top:20px;color:#9ca3af;font-size:13px'>"
            "Ou execute <code>POST /setup</code> manualmente.</p>"
            "<script>if(typeof BX24!=='undefined')BX24.init(function(){});</script>"
            "</body></html>"
        )

    logger.warning("Install handler: tokens not found. Keys: %s", list(payload.keys()))
    return HTMLResponse(
        "<html><body style='font-family:sans-serif;padding:40px'>"
        "<h2 style='color:#dc2626'>Tokens nao encontrados</h2>"
        "<p>Verifique os logs do servidor.</p>"
        "</body></html>",
        status_code=400,
    )


# ════════════════════════════════════════════════════════════════════════════
# 5.  Front-end (connector UI inside Bitrix24 iframe)
# ════════════════════════════════════════════════════════════════════════════

@app.get("/bitrix/connector-ui")
async def connector_ui():
    """Serve the admin UI (works inside Bitrix24 iframe and standalone)."""
    html_path = STATIC_DIR / "connector_ui.html"
    if not html_path.exists():
        raise HTTPException(500, "connector_ui.html not found")
    return FileResponse(html_path, media_type="text/html")


# ─── API endpoints for the front-end ────────────────────────────────────────

@app.get("/api/instances")
async def api_list_instances():
    """List all instances with their Evolution connection status."""
    instances = get_instances()
    result = []
    for inst in instances:
        status = "unknown"
        try:
            info = await evolution.get_instance_status(inst.evolution_instance)
            # Evolution returns a list; first item has instance state
            if isinstance(info, list) and info:
                state_info = info[0].get("instance", {})
                status = state_info.get("state", "unknown")
            elif isinstance(info, dict):
                status = info.get("instance", {}).get("state", "unknown")
        except Exception as exc:
            logger.warning(
                "Could not get status for %s: %s", inst.evolution_instance, exc,
            )

        result.append({
            "name": inst.name,
            "label": inst.label,
            "evolution_instance": inst.evolution_instance,
            "line_id": inst.bitrix24_line_id,
            "status": status,
        })
    return result


@app.get("/api/instances/{instance_name}/qr")
async def api_instance_qr(instance_name: str):
    """Get QR code for connecting a WhatsApp instance."""
    inst = get_instance_by_name(instance_name)
    if not inst:
        raise HTTPException(404, f"Instance '{instance_name}' not found")

    try:
        data = await evolution.connect_instance(inst.evolution_instance)

        # If already connected
        if isinstance(data, dict):
            state = data.get("instance", {}).get("state", "")
            if state == "open":
                return {"connected": True, "status": "open"}

            # QR code available
            b64 = data.get("base64", "")
            if b64:
                return {"connected": False, "base64": b64}

        return {"connected": False, "error": "QR code nao disponivel. Verifique a instancia na Evolution API."}

    except Exception as exc:
        logger.error("QR code request failed for %s: %s", instance_name, exc)
        return {"connected": False, "error": str(exc)}


# ════════════════════════════════════════════════════════════════════════════
# 6.  Setup (run once after install)
# ════════════════════════════════════════════════════════════════════════════

@app.post("/setup")
async def run_setup():
    """
    Run once after installing the app in Bitrix24.
    - Registers connector (once)
    - Activates it on EACH open line
    - Binds the event handler (idempotent)
    - Configures Evolution webhook for EACH instance
    """
    results: dict = {}
    instances = get_instances()

    # 1. Register connector
    try:
        results["1_register_connector"] = await bitrix.register_connector()
    except Exception as exc:
        results["1_register_connector"] = {"error": str(exc)}

    # 2. Activate per line
    for inst in instances:
        key = f"2_activate_{inst.name}_line_{inst.bitrix24_line_id}"
        try:
            results[key] = await bitrix.activate_connector(inst.bitrix24_line_id)
        except Exception as exc:
            results[key] = {"error": str(exc)}

    # 3. Bind events (idempotent — checks for existing bindings)
    try:
        results["3_bind_events"] = await bitrix.bind_events()
    except Exception as exc:
        results["3_bind_events"] = {"error": str(exc)}

    # 4. Configure Evolution webhook per instance
    for inst in instances:
        key = f"4_evolution_webhook_{inst.name}"
        webhook_url = f"{settings.app_url}/webhook/evolution/{inst.name}"
        try:
            results[key] = await evolution.set_webhook(
                inst.evolution_instance, webhook_url,
            )
        except Exception as exc:
            results[key] = {"error": str(exc)}

    return results


# ════════════════════════════════════════════════════════════════════════════
# 7.  Health & Debug
# ════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    oauth = storage.get_oauth()
    instances = get_instances()
    return {
        "status": "ok",
        "bitrix_authed": oauth is not None,
        "bitrix_domain": oauth["domain"] if oauth else None,
        "instances": [
            {"name": i.name, "label": i.label, "line_id": i.bitrix24_line_id}
            for i in instances
        ],
    }


@app.post("/debug/bitrix")
async def debug_bitrix(request: Request):
    """Log the raw Bitrix24 event for debugging."""
    body = await request.body()
    payload = _parse_bitrix_payload(body)
    logger.info(
        "DEBUG BITRIX:\n%s",
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
    )
    return {"received": True, "event": payload.get("event")}


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app", host=settings.app_host, port=settings.app_port, reload=False,
    )
