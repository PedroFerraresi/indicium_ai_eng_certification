from __future__ import annotations

import os

# -----------------------------
# Caminhos e modo de ingestão
# -----------------------------
DB_PATH: str = os.getenv("DB_PATH", "data/srag.sqlite")
UF_DEFAULT: str = os.getenv("UF_INICIAL", "SP")
INGEST_MODE: str = os.getenv("INGEST_MODE", "auto").lower()

# Lista default (pode ficar vazia). Funções devem preferir ler do env dinamicamente.
SRAG_URLS: list[str] = [
    u.strip() for u in os.getenv("SRAG_URLS", "").split(",") if u.strip()
]

# -----------------------------
# Chaves / modelos externos
# -----------------------------
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
SERPER_API_KEY: str = os.getenv("SERPER_API_KEY", "")
OPENAI_SUMMARY_MODEL: str = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")

# -----------------------------
# Rede e retries (usado por tools.news)
# -----------------------------
API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "2"))
API_BACKOFF_BASE: float = float(os.getenv("API_BACKOFF_BASE", "0.5"))

__all__ = [
    "DB_PATH",
    "UF_DEFAULT",
    "INGEST_MODE",
    "SRAG_URLS",
    "OPENAI_API_KEY",
    "SERPER_API_KEY",
    "OPENAI_SUMMARY_MODEL",
    "API_TIMEOUT",
    "API_MAX_RETRIES",
    "API_BACKOFF_BASE",
]
