from __future__ import annotations
"""
Orquestrador de ingestão (SQLite) + métricas.

Modo de ingestão controlado por .env:
- INGEST_MODE=auto   -> usa local se houver arquivos em data/raw/, senão remoto
- INGEST_MODE=local  -> força ingestão local (ignora URLs)
- INGEST_MODE=remote -> força ingestão remota (ignora arquivos locais)

SRAG_URLS: lista separada por vírgulas com as URLs CSV/ZIP do OpenDATASUS.
Ex.: SRAG_URLS=https://.../INFLUD24.csv,https://.../INFLUD25.csv
"""

import os, glob
from typing import List
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

from src.tools.ingestion_local_sqlite import ingest_local
from src.tools.ingestion_remote_sqlite import ingest_remote

# --- ENV & Config ------------------------------------------------------------
load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/srag.sqlite")
UF_DEFAULT = os.getenv("UF_INICIAL", "SP")
INGEST_MODE = os.getenv("INGEST_MODE", "auto").lower()  # auto | local | remote

# Parse SRAG_URLS do .env (lista separada por vírgulas)
def _parse_urls(env_val: str | None) -> List[str]:
    if not env_val:
        return []
    return [u.strip() for u in env_val.split(",") if u.strip()]

SRAG_URLS: List[str] = _parse_urls(os.getenv("SRAG_URLS", ""))

# Colunas presentes nos CSVs SRAG 2024/2025 (núcleo mínimo que usamos)
COLS: List[str] = [
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

os.makedirs("data", exist_ok=True)


# --- Infra -------------------------------------------------------------------
def _engine():
    """Cria engine SQLAlchemy para o arquivo SQLite."""
    return create_engine(f"sqlite:///{DB_PATH}", future=True)


# --- Orquestração ------------------------------------------------------------
def ingest():
    """
    Decide e executa a ingestão com base em:
      - Conteúdo de data/raw (quando INGEST_MODE=auto)
      - Valor de INGEST_MODE (local/remote)
      - SRAG_URLS (para modo remoto)
    """
    # Detecta presença de arquivos locais
    raw_csvs = glob.glob(os.path.join("data", "raw", "*.csv"))
    raw_zips = glob.glob(os.path.join("data", "raw", "*.zip"))
    has_local = len(raw_csvs) + len(raw_zips) > 0

    # Força modo conforme .env
    if INGEST_MODE == "local":
        has_local = True
        print("⚙️  INGEST_MODE=local → usando ingestão local (data/raw).")
    elif INGEST_MODE == "remote":
        has_local = False
        print("⚙️  INGEST_MODE=remote → usando ingestão remota (SRAG_URLS).")
    else:
        # auto
        print("⚙️  INGEST_MODE=auto → escolhendo automaticamente (local se houver arquivos; senão remoto).")

    if has_local:
        print("📦 Detectados arquivos locais em data/raw/ → ingestão local.")
        ingest_local(engine_fn=_engine, uf_default=UF_DEFAULT, cols=COLS, folder="data/raw")
    else:
        if not SRAG_URLS:
            raise RuntimeError(
                "INGEST_MODE=remote (ou auto sem arquivos locais), mas SRAG_URLS está vazio no .env. "
                "Preencha SRAG_URLS com as URLs CSV/ZIP do OpenDATASUS, separadas por vírgulas."
            )
        print(f"🌐 Ingestão remota com {len(SRAG_URLS)} URL(s).")
        ingest_remote(engine_fn=_engine, uf_default=UF_DEFAULT, cols=COLS, urls=SRAG_URLS)


# --- Métricas ----------------------------------------------------------------
def compute_metrics(uf: str | None = None) -> dict:
    """
    Calcula:
      - increase_rate     : Δ% do mês atual vs mês anterior
      - mortality_rate    : deaths / cases (mês mais recente)
      - icu_rate          : icu_cases / cases (mês mais recente)
      - vaccination_rate  : vaccinated_cases / cases (mês mais recente) [proxy]

    Séries:
      - series_30d : casos diários últimos 30 dias
      - series_12m : casos mensais últimos 12 meses
    """
    uf = uf or UF_DEFAULT
    eng = _engine()

    with eng.begin() as conn:
        last_two = conn.execute(text("""
            SELECT month, cases
            FROM srag_monthly
            WHERE uf = :uf
            ORDER BY month DESC
            LIMIT 2;
        """), {"uf": uf}).fetchall()

    current_cases = prev_cases = None
    if last_two:
        current_cases = last_two[0][1]
        if len(last_two) > 1:
            prev_cases = last_two[1][1]

    increase_rate = (
        (current_cases - prev_cases) / prev_cases
        if (current_cases not in (None, 0) and prev_cases not in (None, 0))
        else None
    )

    def _pair(sql: str):
        with eng.begin() as c:
            r = c.execute(text(sql), {"uf": uf}).one_or_none()
        return (r[0], r[1]) if r else (None, None)

    deaths, cases_m = _pair("SELECT deaths, cases FROM srag_monthly WHERE uf=:uf ORDER BY month DESC LIMIT 1;")
    icu,    cases_i = _pair("SELECT icu_cases, cases FROM srag_monthly WHERE uf=:uf ORDER BY month DESC LIMIT 1;")
    vax,    cases_v = _pair("SELECT vaccinated_cases, cases FROM srag_monthly WHERE uf=:uf ORDER BY month DESC LIMIT 1;")

    mortality_rate   = (deaths / cases_m) if cases_m else None
    icu_rate         = (icu    / cases_i) if cases_i else None
    vaccination_rate = (vax    / cases_v) if cases_v else None

    last_30 = pd.read_sql_query("""
        SELECT day, cases
        FROM srag_daily
        WHERE uf = ? AND day >= date('now','-30 day')
        ORDER BY day;
    """, eng, params=(uf,))

    last_12 = pd.read_sql_query("""
        SELECT month, cases
        FROM srag_monthly
        WHERE uf = ? AND month >= date('now','-12 month')
        ORDER BY month;
    """, eng, params=(uf,))

    return {
        "uf": uf,
        "increase_rate": increase_rate,
        "mortality_rate": mortality_rate,
        "icu_rate": icu_rate,
        "vaccination_rate": vaccination_rate,
        "series_30d": last_30,
        "series_12m": last_12,
        "current_cases": current_cases,
        "prev_cases": prev_cases,
    }
