from __future__ import annotations
"""
Ingestão remota para SQLite (URLs do OpenDATASUS) — schema SRAG 2024/2025.

- Leitura seletiva (usecols) conforme colunas reais dos CSVs.
- UF derivada de ['SG_UF_NOT','SG_UF','SG_UF_RES'].
- Datas robustas e flags numéricas.
- Um statement por execute() (SQLite).
"""

import io, zipfile, requests
import pandas as pd
from sqlalchemy import text

UF_CANDIDATES = ["SG_UF_NOT", "SG_UF", "SG_UF_RES", "UF"]

def _detect_date_parse(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    is_iso_like = s.str.match(r"\d{4}-\d{2}-\d{2}$").mean() > 0.5
    if is_iso_like:
        return pd.to_datetime(series, errors="coerce")
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def _post_clean(df: pd.DataFrame, uf_default: str) -> pd.DataFrame:
    if "DT_SIN_PRI" in df.columns:
        df["DT_SIN_PRI"] = _detect_date_parse(df["DT_SIN_PRI"])
    else:
        df["DT_SIN_PRI"] = pd.NaT

    uf_series = None
    for c in UF_CANDIDATES:
        if c in df.columns:
            uf_series = df[c].astype(str).str.upper().str[:2]
            break
    df["UF"] = (uf_series if uf_series is not None else uf_default)
    if isinstance(df["UF"], pd.Series):
        df["UF"] = df["UF"].fillna(uf_default)

    for col in ["EVOLUCAO", "UTI", "VACINA_COV"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    return df[["DT_SIN_PRI", "EVOLUCAO", "UTI", "VACINA_COV", "UF"]]

def _download_selective(url: str, wanted_cols: list[str]) -> pd.DataFrame:
    r = requests.get(url, timeout=60); r.raise_for_status()
    if url.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
            with zf.open(name) as f:
                header = pd.read_csv(f, sep=";", nrows=0).columns.tolist()
            with zf.open(name) as f:
                usecols = [c for c in wanted_cols if c in header]
                return pd.read_csv(f, sep=";", low_memory=False, usecols=usecols)
    else:
        header = pd.read_csv(io.BytesIO(r.content), sep=";", nrows=0).columns.tolist()
        usecols = [c for c in wanted_cols if c in header]
        return pd.read_csv(io.BytesIO(r.content), sep=";", low_memory=False, usecols=usecols)

def ingest_remote(engine_fn, uf_default: str, cols: list[str], urls: list[str]):
    if not urls:
        print("⚠️  ingest_remote: nenhuma URL informada. Configure SRAG_URLS no orquestrador.")
        return

    frames = []
    for u in urls:
        raw = _download_selective(u, cols)
        df = _post_clean(raw, uf_default)
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)

    eng = engine_fn()
    with eng.begin() as conn:
        full.to_sql("srag_staging", conn, if_exists="replace", index=False)

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

    print(f"✅ Ingestão remota concluída ({len(urls)} URL(s) processada(s)).")
