# src/tools/__init__.py
from __future__ import annotations

# importa SOMENTE do pacote pai já inicializado
from src import (
    API_BACKOFF_BASE,
    API_MAX_RETRIES,
    API_TIMEOUT,
    DB_PATH,
    INGEST_MODE,
    NEWS_QUERY,
    OPENAI_API_KEY,
    OPENAI_SUMMARY_MODEL,
    SERPER_API_KEY,
    SRAG_URLS,
    UF_DEFAULT,
)

# Conjunto mínimo de colunas usadas pelos ingesters SRAG (2024/2025)
COLS: list[str] = [
    "DT_SIN_PRI",
    "EVOLUCAO",
    "UTI",
    "VACINA_COV",
    "CLASSI_FIN",
    "SEM_PRI",
    "SG_UF_NOT",
    "SG_UF",
    "SG_UF_RES",
]

__all__ = [
    "DB_PATH",
    "UF_DEFAULT",
    "INGEST_MODE",
    "NEWS_QUERY",
    "OPENAI_API_KEY",
    "OPENAI_SUMMARY_MODEL",
    "SERPER_API_KEY",
    "SRAG_URLS",
    "API_TIMEOUT",
    "API_MAX_RETRIES",
    "API_BACKOFF_BASE",
    "COLS",
]
