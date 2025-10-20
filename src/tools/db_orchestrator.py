from __future__ import annotations

import glob
import os
from typing import Any

from dotenv import find_dotenv, load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

from src.tools.ingestion_remote_sqlite import ingest_remote
from src.tools.local_ingestion import ingest_local

# -----------------------------------------------------------------------------
# ENV & Config
# -----------------------------------------------------------------------------
# Carrega .env a partir da raiz do projeto (sem sobrescrever env já setado)
load_dotenv(find_dotenv(usecwd=True), override=False)

DB_PATH = os.getenv("DB_PATH", "data/srag.sqlite")
UF_DEFAULT = os.getenv("UF_INICIAL", "SP")
INGEST_MODE = os.getenv("INGEST_MODE", "auto").lower()  # auto | local | remote


def _parse_urls(env_val: str | None) -> list[str]:
    """Divide SRAG_URLS por vírgula e remove espaços vazios."""
    if not env_val:
        return []
    return [u.strip() for u in env_val.split(",") if u.strip()]


SRAG_URLS: list[str] = _parse_urls(os.getenv("SRAG_URLS", ""))

# Colunas presentes nos CSVs SRAG 2024/2025 (núcleo mínimo que usamos)
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

# Garante diretório do arquivo do banco (se DB_PATH possuir subpastas)
db_dir = os.path.dirname(DB_PATH) or "."
os.makedirs(db_dir, exist_ok=True)


# -----------------------------------------------------------------------------
# Infra
# -----------------------------------------------------------------------------
def _engine():
    """Cria engine SQLAlchemy para o arquivo SQLite."""
    # future=True habilita comportamentos da 2.x
    return create_engine(f"sqlite:///{DB_PATH}", future=True)


# -----------------------------------------------------------------------------
# Orquestração de ingestão
# -----------------------------------------------------------------------------
def ingest() -> None:
    """
    Decide e executa a ingestão com base em:
      - Conteúdo de data/raw (quando INGEST_MODE=auto)
      - Valor de INGEST_MODE (local/remote)
      - SRAG_URLS (para modo remoto)
    """
    # Detecta presença de arquivos locais
    raw_glob = os.path.join("data", "raw", "*")
    raw_csvs = glob.glob(os.path.join("data", "raw", "*.csv"))
    raw_zips = glob.glob(os.path.join("data", "raw", "*.zip"))
    has_local = (len(raw_csvs) + len(raw_zips)) > 0

    # Seleciona modo conforme .env
    if INGEST_MODE == "local":
        has_local = True
        print("⚙️  INGEST_MODE=local → usando ingestão local (data/raw).")
    elif INGEST_MODE == "remote":
        has_local = False
        print("⚙️  INGEST_MODE=remote → usando ingestão remota (SRAG_URLS).")
    else:
        print(
            "⚙️  INGEST_MODE=auto → escolhendo automaticamente (local se houver arquivos; senão remoto)."
        )

    if has_local:
        print(f"📦 Detectados arquivos locais em {raw_glob} → ingestão local.")
        ingest_local(
            engine_fn=_engine, uf_default=UF_DEFAULT, cols=COLS, folder="data/raw"
        )
    else:
        if not SRAG_URLS:
            raise RuntimeError(
                "INGEST_MODE=remote (ou auto sem arquivos locais), mas SRAG_URLS está vazio no .env. "
                "Preencha SRAG_URLS com as URLs CSV/ZIP do OpenDATASUS, separadas por vírgulas."
            )
        print(f"🌐 Ingestão remota com {len(SRAG_URLS)} URL(s).")
        ingest_remote(engine_fn=_engine, uf_default=UF_DEFAULT, cols=COLS, urls=SRAG_URLS)


# -----------------------------------------------------------------------------
# Métricas
# -----------------------------------------------------------------------------
def _fetch_last_two_months(eng, uf: str) -> list[tuple[str, int]]:
    """
    Busca os dois meses mais recentes (month, cases) para a UF.
    Retorna lista possivelmente vazia.
    """
    with eng.begin() as conn:
        return conn.execute(
            text("""
                SELECT month, cases
                FROM srag_monthly
                WHERE uf = :uf
                ORDER BY month DESC
                LIMIT 2
            """),
            {"uf": uf},
        ).fetchall()


def _fetch_single_pair(eng, uf: str, fields: str) -> tuple[int | None, int | None]:
    """
    Helper genérico para obter (x, cases) do mês mais recente em srag_monthly.
    Ex.: fields="deaths, cases"   ou   fields="icu_cases, cases"
    """
    with eng.begin() as conn:
        row = conn.execute(
            text(f"""
                SELECT {fields}
                FROM srag_monthly
                WHERE uf = :uf
                ORDER BY month DESC
                LIMIT 1
            """),
            {"uf": uf},
        ).one_or_none()
    if row:
        return row[0], row[1]
    return None, None


def compute_metrics(uf: str | None = None) -> dict[str, Any]:
    """
    Calcula:
      - increase_rate     : Δ% do mês atual vs mês anterior
      - mortality_rate    : deaths / cases (mês mais recente)
      - icu_rate          : icu_cases / cases (mês mais recente)
      - vaccination_rate  : vaccinated_cases / cases (mês mais recente) [proxy]

    Séries:
      - series_30d : casos diários últimos 30 dias
      - series_12m : casos mensais últimos 12 meses

    Retorna dicionário com KPIs + DataFrames.
    """
    uf = uf or UF_DEFAULT

    eng = _engine()

    # --- A) Taxa de aumento mês a mês ---------------------------------------
    last_two = _fetch_last_two_months(eng, uf)  # [(month, cases), ...] desc
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

    # --- B) Taxas do mês mais recente ---------------------------------------
    deaths, cases_m = _fetch_single_pair(eng, uf, "deaths, cases")
    icu, cases_i = _fetch_single_pair(eng, uf, "icu_cases, cases")
    vax, cases_v = _fetch_single_pair(eng, uf, "vaccinated_cases, cases")

    mortality_rate = (deaths / cases_m) if cases_m else None
    icu_rate = (icu / cases_i) if cases_i else None
    vaccination_rate = (vax / cases_v) if cases_v else None

    # --- C) Séries (últimos 30 dias / 12 meses) ------------------------------
    # Observação: usamos filtros relativos ao "agora" do SQLite; caso deseje
    # filtrar até o último dia/mês disponível no dataset, ajuste aqui.
    last_30 = pd.read_sql_query(
        """
        SELECT day, cases
        FROM srag_daily
        WHERE uf = ? AND day >= date('now','-30 day')
        ORDER BY day
        """,
        eng,
        params=(uf,),
    )

    last_12 = pd.read_sql_query(
        """
        SELECT month, cases
        FROM srag_monthly
        WHERE uf = ? AND month >= date('now','-12 month')
        ORDER BY month
        """,
        eng,
        params=(uf,),
    )

    return {
        "uf": uf,
        "increase_rate": increase_rate,
        "mortality_rate": mortality_rate,
        "icu_rate": icu_rate,
        "vaccination_rate": vaccination_rate,
        "series_30d": last_30,  # DataFrame com colunas: day, cases
        "series_12m": last_12,  # DataFrame com colunas: month, cases
        "current_cases": current_cases,
        "prev_cases": prev_cases,
    }
