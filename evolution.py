"""
Evolution API v2 client (multi-instance).
Every method receives the `instance` name to route to the correct WhatsApp number.
"""
import base64
import logging

import httpx

from config import settings

logger = logging.getLogger(__name__)

_BASE = settings.evolution_api_url.rstrip("/")
_HDR = {"apikey": settings.evolution_api_key, "Content-Type": "application/json"}


# ─── Mensagens ───────────────────────────────────────────────────────────────

async def send_text(instance: str, number: str, text: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{_BASE}/message/sendText/{instance}",
            headers=_HDR,
            json={"number": number, "text": text},
        )
        r.raise_for_status()
        return r.json()


async def send_media(
    instance: str,
    number: str,
    mediatype: str,
    url: str,
    filename: str,
    caption: str = "",
) -> dict:
    """mediatype: 'image' | 'video' | 'document'"""
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{_BASE}/message/sendMedia/{instance}",
            headers=_HDR,
            json={
                "number":    number,
                "mediatype": mediatype,
                "media":     url,
                "fileName":  filename,
                "caption":   caption,
            },
        )
        r.raise_for_status()
        return r.json()


async def send_audio(instance: str, number: str, url: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{_BASE}/message/sendWhatsAppAudio/{instance}",
            headers=_HDR,
            json={"number": number, "audio": url},
        )
        r.raise_for_status()
        return r.json()


# ─── Mídia download ─────────────────────────────────────────────────────────

async def get_media_base64(instance: str, message_key: dict) -> bytes:
    """
    Download media from a received WhatsApp message via Evolution's API.
    Returns raw bytes of the media file.
    """
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{_BASE}/chat/getBase64FromMediaMessage/{instance}",
            headers=_HDR,
            json={
                "message":      {"key": message_key},
                "convertToMp4": False,
            },
        )
        r.raise_for_status()
        data = r.json()

    b64_str: str = data.get("base64", "")
    if "," in b64_str:
        b64_str = b64_str.split(",", 1)[1]

    return base64.b64decode(b64_str)


# ─── Instância & Webhook ────────────────────────────────────────────────────

async def list_instances() -> list[dict]:
    """
    Fetch ALL instances from Evolution API (no filter).
    Returns a list of raw instance dicts as returned by the API.

    Evolution API v2 response shapes:
      - List: [{"instance": {"instanceName": ..., "connectionStatus": ...}, ...}, ...]
      - Dict: {"instances": [...]}  (some builds)
    """
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{_BASE}/instance/fetchInstances",
            headers=_HDR,
        )
        r.raise_for_status()
        data = r.json()

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("instances", [])
    return []


async def get_instance_status(instance: str) -> dict:
    """Return instance info including connection state."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{_BASE}/instance/fetchInstances",
            headers=_HDR,
            params={"instanceName": instance},
        )
        r.raise_for_status()
        return r.json()


async def connect_instance(instance: str) -> dict:
    """
    Request QR code / pairing code for WhatsApp connection.
    Returns {"base64": "data:image/png;base64,...", "code": "2@..."} or
    {"instance": {"state": "open"}} if already connected.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            f"{_BASE}/instance/connect/{instance}",
            headers=_HDR,
        )
        r.raise_for_status()
        return r.json()


async def set_webhook(instance: str, webhook_url: str) -> dict:
    """
    Set webhook for an Evolution instance.

    Tries two body formats in order:
      1. v2-nested  — {"webhook": {"enabled": true, "url": ..., "events": [...]}}
      2. v1-flat    — {"url": ..., "enabled": true, "events": [...]}

    If the instance doesn't exist (404), raises immediately without retry.
    Logs which format succeeded.
    """
    url = f"{_BASE}/webhook/set/{instance}"
    attempts = [
        (
            "v2-nested",
            {
                "webhook": {
                    "enabled":         True,
                    "url":             webhook_url,
                    "webhookByEvents": False,
                    "webhookBase64":   False,
                    "events":          ["MESSAGES_UPSERT"],
                }
            },
        ),
        (
            "v1-flat",
            {
                "url":             webhook_url,
                "enabled":         True,
                "events":          ["MESSAGES_UPSERT"],
                "webhookByEvents": False,
                "webhookBase64":   False,
            },
        ),
    ]

    last_exc: Exception | None = None
    async with httpx.AsyncClient(timeout=30) as client:
        for label, body in attempts:
            try:
                r = await client.post(url, headers=_HDR, json=body)
                if r.status_code == 404:
                    logger.error(
                        "set_webhook %s: 404 at %s — instance not found in Evolution",
                        instance, url,
                    )
                    r.raise_for_status()
                r.raise_for_status()
                logger.info("set_webhook %s: succeeded with format %s", instance, label)
                return r.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    raise  # instance doesn't exist, no point retrying
                logger.warning(
                    "set_webhook %s [%s] failed (%s): %s",
                    instance, label, exc.response.status_code, exc.response.text[:200],
                )
                last_exc = exc

    raise last_exc  # type: ignore[misc]
