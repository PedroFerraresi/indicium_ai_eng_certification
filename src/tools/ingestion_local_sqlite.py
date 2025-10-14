from __future__ import annotations
"""
Ingestão local para SQLite (CSV/ZIP em data/raw/) — schema SRAG 2024/2025.

Ajustes importantes:
- Leitura seletiva (usecols) para só carregar colunas necessárias.
- Construção segura de UF a partir de ['SG_UF_NOT','SG_UF','SG_UF_RES'].
- Datas robustas (detecta ISO YYYY-MM-DD vs DD/MM/YYYY).
- Cria EVOLUCAO/UTI/VACINA_COV com 0 se ausentes.
- Um statement por execute() (requisito do SQLite).
"""

import os, zipfile, glob, io, re
import pandas as pd
from sqlalchemy import text

UF_CANDIDATES = ["SG_UF_NOT", "SG_UF", "SG_UF_RES", "UF"]  # ordem de preferência

def _detect_date_parse(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    is_iso_like = s.str.match(r"\d{4}-\d{2}-\d{2}$").mean() > 0.5
    if is_iso_like:
        return pd.to_datetime(series, errors="coerce")
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def _post_clean(df: pd.DataFrame, uf_default: str) -> pd.DataFrame:
    # DT_SIN_PRI
    if "DT_SIN_PRI" in df.columns:
        df["DT_SIN_PRI"] = _detect_date_parse(df["DT_SIN_PRI"])
    else:
        df["DT_SIN_PRI"] = pd.NaT

    # UF derivada
    uf_series = None
    for c in UF_CANDIDATES:
        if c in df.columns:
            uf_series = df[c].astype(str).str.upper().str[:2]
            break
    df["UF"] = (uf_series if uf_series is not None else uf_default)
    if isinstance(df["UF"], pd.Series):
        df["UF"] = df["UF"].fillna(uf_default)

    # Flags numéricas
    for col in ["EVOLUCAO", "UTI", "VACINA_COV"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    # Seleção final mínima
    return df[["DT_SIN_PRI", "EVOLUCAO", "UTI", "VACINA_COV", "UF"]]

def _read_csv_selective(path: str, wanted_cols: list[str]) -> pd.DataFrame:
    """
    Lê CSV ou ZIP carregando apenas as colunas disponíveis de 'wanted_cols'.
    Faz um pass só de cabeçalho para descobrir as colunas e então carrega com usecols.
    """
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path, "r") as zf:
            name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
            with zf.open(name) as f:
                header = pd.read_csv(f, sep=";", nrows=0).columns.tolist()
            with zf.open(name) as f:
                usecols = [c for c in wanted_cols if c in header]
                return pd.read_csv(f, sep=";", low_memory=False, usecols=usecols)
    else:
        header = pd.read_csv(path, sep=";", nrows=0).columns.tolist()
        usecols = [c for c in wanted_cols if c in header]
        return pd.read_csv(path, sep=";", low_memory=False, usecols=usecols)

def ingest_local(engine_fn, uf_default: str, cols: list[str], folder: str = "data/raw"):
    os.makedirs(folder, exist_ok=True)

    paths = sorted(glob.glob(os.path.join(folder, "*.csv")) + glob.glob(os.path.join(folder, "*.zip")))
    if not paths:
        print(f"⚠️  ingest_local: nenhum CSV/ZIP encontrado em '{folder}'.")
        return

    frames = []
    for path in paths:
        raw = _read_csv_selective(path, cols)
        df = _post_clean(raw, uf_default)
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)

    eng = engine_fn()
    with eng.begin() as conn:
        # staging
        full.to_sql("srag_staging", conn, if_exists="replace", index=False)

        # base
        conn.execute(text("DROP TABLE IF EXISTS srag_base"))
        conn.execute(text("""
            CREATE TABLE srag_base AS
            SELECT
              DT_SIN_PRI AS event_date,
              UF AS uf,
              CASE WHEN EVOLUCAO=2 THEN 1 ELSE 0 END AS death_flag,
              CASE WHEN UTI=1 THEN 1 ELSE 0 END AS icu_flag,
              CASE WHEN VACINA_COV=1 THEN 1 ELSE 0 END AS vaccinated_flag
            FROM srag_staging
            WHERE DT_SIN_PRI IS NOT NULL
        """))

        # diárias
        conn.execute(text("DROP TABLE IF EXISTS srag_daily"))
        conn.execute(text("""
            CREATE TABLE srag_daily AS
            SELECT date(event_date) AS day, uf,
                   COUNT(*) AS cases,
                   SUM(icu_flag) AS icu_cases,
                   SUM(death_flag) AS deaths,
                   SUM(vaccinated_flag) AS vaccinated_cases
            FROM srag_base
            GROUP BY 1,2
        """))

        # mensais
        conn.execute(text("DROP TABLE IF EXISTS srag_monthly"))
        conn.execute(text("""
            CREATE TABLE srag_monthly AS
            SELECT strftime('%Y-%m-01', event_date) AS month, uf,
                   COUNT(*) AS cases,
                   SUM(icu_flag) AS icu_cases,
                   SUM(death_flag) AS deaths,
                   SUM(vaccinated_flag) AS vaccinated_cases
            FROM srag_base
            GROUP BY 1,2
        """))

    print(f"✅ Ingestão local concluída ({len(paths)} arquivo(s) em '{folder}').")
