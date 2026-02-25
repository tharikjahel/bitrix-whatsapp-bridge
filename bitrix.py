"""
Bitrix24 REST API client using OAuth 2.0 (Local App tokens).
Handles automatic token refresh and imconnector methods.
Multi-instance: line_id is passed per call (one line per WhatsApp number).
"""
import logging
import time

import httpx

import storage
from config import settings

logger = logging.getLogger(__name__)

_OAUTH_URL = "https://oauth.bitrix.info/oauth/token/"


# ─── OAuth ──────────────────────────────────────────────────────────────────

async def _get_token() -> tuple[str, str]:
    """Return (access_token, domain), refreshing the token if expired."""
    data = storage.get_oauth()
    if not data:
        raise RuntimeError(
            "Bitrix24 not authenticated. "
            "Install the local app first (visit /bitrix/install)."
        )

    if time.time() < data["expires_at"] - 300:
        return data["access_token"], data["domain"]

    logger.info("Refreshing Bitrix24 OAuth token...")
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            _OAUTH_URL,
            params={
                "grant_type":    "refresh_token",
                "client_id":     settings.bitrix24_app_id,
                "client_secret": settings.bitrix24_app_secret,
                "refresh_token": data["refresh_token"],
            },
        )
        r.raise_for_status()
        new = r.json()

    storage.save_oauth(
        access_token=new["access_token"],
        refresh_token=new["refresh_token"],
        expires_in=new["expires_in"],
        domain=data["domain"],
    )
    logger.info("Bitrix24 token refreshed.")
    return new["access_token"], data["domain"]


# ─── Generic REST call ───────────────────────────────────────────────────────

async def call(method: str, params: dict | None = None) -> dict:
    token, domain = await _get_token()
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"https://{domain}/rest/{method}.json",
            headers={"Authorization": f"Bearer {token}"},
            json=params or {},
        )
        r.raise_for_status()
        resp = r.json()

    if "error" in resp:
        raise RuntimeError(
            f"Bitrix24 [{method}] → {resp['error']}: "
            f"{resp.get('error_description', '')}"
        )
    return resp


# ─── imconnector (multi-instance) ───────────────────────────────────────────

async def send_message(
    line_id: str,
    wa_phone: str,
    push_name: str,
    message_id: str,
    text: str = "",
    files: list[dict] | None = None,
) -> dict:
    """
    Forward an incoming WhatsApp message to a specific Bitrix24 Open Line.
    files format: [{"link": "https://...", "name": "photo.jpg"}]
    """
    msg_payload: dict = {
        "id":   message_id,
        "date": int(time.time()),
        "text": text,
    }
    if files:
        msg_payload["files"] = files

    return await call(
        "imconnector.send.messages",
        {
            "CONNECTOR": settings.bitrix24_connector_id,
            "LINE":      line_id,
            "MESSAGES": [
                {
                    "user": {
                        "id":                  wa_phone,
                        "name":                (push_name or wa_phone)[:25],
                        "phone":               wa_phone,
                        "skip_phone_validate": "Y",
                    },
                    "message": msg_payload,
                    "chat":    {"id": wa_phone},
                }
            ],
        },
    )


async def register_connector() -> dict:
    """Register the custom WhatsApp connector in Bitrix24 (once)."""
    return await call(
        "imconnector.register",
        {
            "ID":                settings.bitrix24_connector_id,
            "NAME":              "WhatsApp (Evolution API)",
            "ICON":              {"DATA_IMAGE": "", "COLOR": "#25D366", "CONNECTOR": "Y"},
            "PLACEMENT_HANDLER": f"{settings.app_url}/bitrix/connector-ui",
        },
    )


async def activate_connector(line_id: str) -> dict:
    """Activate the connector on a specific open line."""
    return await call(
        "imconnector.activate",
        {
            "CONNECTOR": settings.bitrix24_connector_id,
            "LINE":      line_id,
            "ACTIVE":    1,
        },
    )


async def bind_events() -> dict:
    """
    Idempotently bind OnImConnectorMessageAdd event.
    Checks for existing bindings to avoid duplicates.
    """
    handler_url = f"{settings.app_url}/webhook/bitrix"

    try:
        existing = await call("event.get")
        for entry in existing.get("result", []):
            evt = entry.get("event", "").upper()
            handler = entry.get("handler", "")
            if evt == "ONIMCONNECTORMESSAGEADD":
                if handler == handler_url:
                    logger.info("event.bind already exists for %s", handler_url)
                    return {"already_bound": True}
                # Different handler URL for same event — remove stale one
                logger.info("Removing stale event handler: %s", handler)
                await call("event.unbind", {
                    "event":   "OnImConnectorMessageAdd",
                    "handler": handler,
                })
    except Exception as exc:
        logger.warning("event.get failed (non-critical): %s", exc)

    return await call(
        "event.bind",
        {
            "event":   "OnImConnectorMessageAdd",
            "handler": handler_url,
        },
    )


async def delivery_status(
    line_id: str,
    messages: list[dict],
) -> dict:
    """
    Confirm message delivery to Bitrix24.
    messages: [{"id": msg_id, "chat": {"id": wa_phone}}]
    """
    return await call(
        "imconnector.send.status.delivery",
        {
            "CONNECTOR": settings.bitrix24_connector_id,
            "LINE":      line_id,
            "MESSAGES":  messages,
        },
    )
