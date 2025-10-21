from __future__ import annotations

import os
from typing import Final

from dotenv import load_dotenv

# Carrega o .env (não sobrescreve o que já estiver no ambiente)
load_dotenv(override=False)

# -----------------------------
# Configurações globais do app
# -----------------------------
DB_PATH: Final[str] = os.getenv("DB_PATH", "data/srag.sqlite")
UF_DEFAULT: Final[str] = os.getenv("UF_INICIAL", "SP")
INGEST_MODE: Final[str] = os.getenv("INGEST_MODE", "auto").lower()  # auto|local|remote
NEWS_QUERY: Final[str] = os.getenv("NEWS_QUERY", "SRAG Brasil")

OPENAI_API_KEY: Final[str] = os.getenv("OPENAI_API_KEY", "")
OPENAI_SUMMARY_MODEL: Final[str] = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
SERPER_API_KEY: Final[str] = os.getenv("SERPER_API_KEY", "")


# parâmetros de robustez p/ chamadas externas
def _parse_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _parse_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


API_TIMEOUT: Final[int] = _parse_int("API_TIMEOUT", 15)
API_MAX_RETRIES: Final[int] = _parse_int("API_MAX_RETRIES", 2)
API_BACKOFF_BASE: Final[float] = _parse_float("API_BACKOFF_BASE", 0.5)


# SRAG_URLS em lista
def _parse_urls(env_val: str | None) -> list[str]:
    if not env_val:
        return []
    return [u.strip() for u in env_val.split(",") if u.strip()]


SRAG_URLS: Final[list[str]] = _parse_urls(os.getenv("SRAG_URLS", ""))

__all__ = [
    "DB_PATH",
    "UF_DEFAULT",
    "INGEST_MODE",
    "NEWS_QUERY",
    "OPENAI_API_KEY",
    "OPENAI_SUMMARY_MODEL",
    "SERPER_API_KEY",
    "API_TIMEOUT",
    "API_MAX_RETRIES",
    "API_BACKOFF_BASE",
    "SRAG_URLS",
]
