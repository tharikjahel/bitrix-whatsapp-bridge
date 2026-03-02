"""
Bitrix24 REST API client using OAuth 2.0 (Local App tokens).
Handles automatic token refresh and imconnector methods.
Multi-instance: line_id is passed per call (one line per WhatsApp number).
"""
import logging
import re
import time

import httpx

import storage
from config import settings

logger = logging.getLogger(__name__)

_OAUTH_URL = "https://oauth.bitrix.info/oauth/token/"


# ─── OAuth ──────────────────────────────────────────────────────────────────

async def _get_token() -> tuple[str, str]:
    """
    Return (access_token, rest_base_url).

    rest_base_url is the full REST endpoint URL stored as server_endpoint,
    e.g. 'https://oauth.bitrix.info/rest/' or 'https://portal.bitrix24.com/rest/'.

    ROOT-CAUSE NOTE: the old code did  f"https://{domain}/rest/{method}.json"
    where domain already contained a full URL — producing double-https and
    triggering [Errno -3] name resolution failure.  Fixed here by using the
    URL directly via rest_base.rstrip('/') + '/' + method + '.json'.
    """
    data = storage.get_oauth()
    if not data:
        raise RuntimeError(
            "Bitrix24 not authenticated. "
            "Install the local app first (visit /bitrix/install)."
        )

    # Priority: portal_domain → portal-specific REST endpoint (required for
    # imconnector.*, event.*, imopenlines.* methods).
    # server_endpoint (oauth.bitrix.info/rest/) is the OAuth proxy and does NOT
    # support all REST methods — use it only when portal_domain is absent.
    rest_base = (
        (f"https://{data['portal_domain']}/rest/" if data.get("portal_domain") else "")
        or data.get("server_endpoint")
        or data.get("domain", "")
    )
    if not rest_base or not rest_base.startswith("http"):
        raise RuntimeError(
            "No portal_domain stored. Re-install the Bitrix24 app so that "
            "DOMAIN is captured — it will be used as the REST endpoint."
        )

    if time.time() < data["expires_at"] - 300:
        return data["access_token"], rest_base

    logger.info("Refreshing Bitrix24 OAuth token via %s", _OAUTH_URL)
    try:
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
    except httpx.ConnectError as exc:
        logger.error("Token refresh: DNS/connection failure to %s: %s", _OAUTH_URL, exc)
        raise
    except httpx.HTTPError as exc:
        logger.error("Token refresh failed (url=%s): %s", _OAUTH_URL, exc)
        raise

    storage.save_oauth(
        access_token=new["access_token"],
        refresh_token=new["refresh_token"],
        expires_in=new["expires_in"],
        domain=data.get("domain", ""),
        member_id=data.get("member_id", ""),
        server_endpoint=data.get("server_endpoint", ""),
        portal_domain=data.get("portal_domain", ""),
    )
    logger.info("Bitrix24 token refreshed.")
    return new["access_token"], rest_base


# ─── Generic REST call ───────────────────────────────────────────────────────

async def call(method: str, params: dict | None = None) -> dict:
    token, rest_base = await _get_token()
    url = f"{rest_base.rstrip('/')}/{method}.json"
    logger.debug("Bitrix24 REST → POST %s", url)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                url,
                # Bitrix24 standard: access_token as ?auth= query param.
                # The Authorization: Bearer header is less reliable across methods.
                params={"auth": token},
                json=params or {},
            )
            r.raise_for_status()
            resp = r.json()
    except httpx.ConnectError as exc:
        logger.error(
            "DNS/connection failure calling Bitrix24: url=%s error=%s", url, exc
        )
        raise
    except httpx.HTTPStatusError as exc:
        logger.error(
            "HTTP error calling Bitrix24: url=%s status=%s body=%s",
            url, exc.response.status_code, exc.response.text[:300],
        )
        raise

    if "error" in resp:
        raise RuntimeError(
            f"Bitrix24 [{method}] → {resp['error']}: "
            f"{resp.get('error_description', '')}"
        )
    return resp


async def detect_portal_domain() -> str:
    """
    Call app.info through the OAuth proxy to discover the portal domain.
    Returns the portal hostname, e.g. 'motoclube.bitrix24.com.br'.
    Saves the result to storage so subsequent calls use the portal URL.
    """
    data = storage.get_oauth()
    if not data:
        raise RuntimeError("Not authenticated")

    proxy_base = data.get("server_endpoint") or "https://oauth.bitrix.info/rest/"
    url = f"{proxy_base.rstrip('/')}/app.info.json"
    logger.info("Detecting portal domain via %s", url)

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, params={"auth": data["access_token"]}, json={})
        r.raise_for_status()
        resp = r.json()

    logger.info("app.info response: %s", resp)
    result = resp.get("result", {})

    # Bitrix24 app.info returns DOMAIN with the portal hostname
    domain = (
        result.get("DOMAIN")
        or result.get("domain")
        or ""
    ).strip()

    if domain:
        storage.save_oauth(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_in=max(60, int(data["expires_at"] - time.time())),
            domain=data.get("domain", ""),
            member_id=data.get("member_id", ""),
            server_endpoint=data.get("server_endpoint", ""),
            portal_domain=domain,
        )
        logger.info("portal_domain auto-detected and saved: %s", domain)

    return domain


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
                        "name":                re.sub(r'[^A-Za-z\u00C0-\u024F\s\'-]', '', (push_name or '').strip())[:25] or 'WhatsApp',
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
            "ICON":              {"DATA_IMAGE": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAAeklEQVR4nO3PUQkAIBTAwFfGaha2kCH8OITBAtxmnf11wwUNaEEDWtCAFjSgBQ1oQQNa0IAWNKAFDWhBA1rQgBY0oAUNaEEDWtCAFjSgBQ1oQQNa0IAWNKAFDWhBA1rQgBY0oAUNaEEDWtCAFjSgBQ1oQQNa0IAWPHYBDdnhPAgqzLQAAAAASUVORK5CYII=", "COLOR": "#25D366", "CONNECTOR": "Y"},
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


async def create_open_line(name: str) -> str:
    """
    Create a new Open Line in Bitrix24 Contact Center.
    Returns the line ID as a string.
    Raises RuntimeError if Bitrix24 returns no ID.
    """
    result = await call(
        "imopenlines.config.add",
        {
            "PARAMS": {
                "LINE_NAME":   name,
                "LANGUAGE_ID": "pt",
            }
        },
    )
    line_id = result.get("result", "")
    if not line_id:
        raise RuntimeError(
            f"imopenlines.config.add returned no line ID. Full response: {result}"
        )
    return str(line_id)


async def bind_events() -> dict:
    """
    Bind OnImConnectorMessageAdd with auth_type=3 (system/admin context).

    auth_type=3 is required for the event to fire for ALL operator replies,
    regardless of which user sends the message in the Open Channel.
    Without it, Bitrix24 binds the event to a specific user context and
    connector events are never dispatched.

    Always unbinds existing bindings first — the URL-only check in the old
    code would silently skip rebinding even when auth_type was wrong.
    """
    handler_url = f"{settings.app_url}/webhook/bitrix"

    # Unbind ALL existing OnImConnectorMessageAdd bindings (regardless of URL
    # or auth_type) so we start with a clean, correctly configured binding.
    try:
        existing = await call("event.get")
        for entry in existing.get("result", []):
            if entry.get("event", "").upper() == "ONIMCONNECTORMESSAGEADD":
                old_handler   = entry.get("handler", "")
                old_auth_type = entry.get("auth_type", "not set")
                logger.info(
                    "Removing existing event binding: handler=%s auth_type=%s",
                    old_handler, old_auth_type,
                )
                try:
                    await call("event.unbind", {
                        "event":   "OnImConnectorMessageAdd",
                        "handler": old_handler,
                    })
                except Exception as exc:
                    logger.warning("event.unbind failed (continuing): %s", exc)
    except Exception as exc:
        logger.warning("event.get failed (non-critical): %s", exc)

    result = await call(
        "event.bind",
        {
            "event":     "OnImConnectorMessageAdd",
            "handler":   handler_url,
            "auth_type": 3,
        },
    )
    logger.info(
        "event.bind OK: handler=%s auth_type=3 | result=%s",
        handler_url, result,
    )
    return result


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
