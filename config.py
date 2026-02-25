import json
import logging
from dataclasses import dataclass
from pathlib import Path

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

_APP_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    # Evolution API (global)
    evolution_api_url: str
    evolution_api_key: str

    # Bitrix24 OAuth (Local App)
    bitrix24_app_id: str = ""
    bitrix24_app_secret: str = ""
    bitrix24_connector_id: str = "whatsapp_evolution"
    bitrix24_application_token: str = ""

    # App
    app_url: str
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    model_config = {"env_file": ".env"}


settings = Settings()


# ─── Instance config (from instances.json) ──────────────────────────────────

@dataclass
class Instance:
    name: str                # Identificador interno (ex: "vendas")
    evolution_instance: str  # Nome da instância na Evolution API
    bitrix24_line_id: str    # ID da Linha Aberta no Bitrix24
    label: str               # Nome amigável (ex: "WhatsApp Vendas")


def load_instances() -> list[Instance]:
    path = _APP_DIR / "instances.json"
    if not path.exists():
        logger.warning(
            "instances.json not found at %s — starting with 0 instances. "
            "Create the file and restart.",
            path,
        )
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [Instance(**item) for item in data]
    except Exception as exc:
        logger.error("Failed to load instances.json: %s", exc)
        return []


# Dicts para lookup rápido
_instances: list[Instance] = []
_by_name: dict[str, Instance] = {}
_by_line: dict[str, Instance] = {}
_by_evolution: dict[str, Instance] = {}


def init_instances():
    global _instances, _by_name, _by_line, _by_evolution
    _instances = load_instances()
    _by_name = {i.name: i for i in _instances}
    _by_line = {i.bitrix24_line_id: i for i in _instances}
    _by_evolution = {i.evolution_instance: i for i in _instances}


def get_instances() -> list[Instance]:
    return _instances


def get_instance_by_name(name: str) -> Instance | None:
    return _by_name.get(name)


def get_instance_by_line(line_id: str) -> Instance | None:
    return _by_line.get(line_id)


def get_instance_by_evolution(evo_name: str) -> Instance | None:
    return _by_evolution.get(evo_name)
