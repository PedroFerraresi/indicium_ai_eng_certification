from __future__ import annotations

import glob
import io
import os
import zipfile

import pandas as pd
from sqlalchemy import text

from src.utils.validation import VALID_UFS  # conjunto de UFs válidas

# Ordem de preferência para detectar UF nos CSVs
UF_CANDIDATES = ["SG_UF_NOT", "SG_UF", "SG_UF_RES", "UF"]


# ------------------ Helpers ------------------ #
def _detect_date_parse(series: pd.Series) -> pd.Series:
    """Detecta formato ISO (YYYY-MM-DD) vs dd/mm/YYYY e faz parse robusto."""
    s = series.astype(str)
    is_iso_like = s.str.match(r"\d{4}-\d{2}-\d{2}$").mean() > 0.5
    if is_iso_like:
        return pd.to_datetime(series, errors="coerce")
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _normalize_uf(raw: pd.Series | str, uf_default: str) -> pd.Series:
    """
    Normaliza UF para duas letras maiúsculas e valida no conjunto VALID_UFS.
    Se não for série ou se valor inválido, cai para uf_default.
    """
    if isinstance(raw, pd.Series):
        u = raw.astype(str).str.upper().str[:2]
        u = u.where(u.isin(VALID_UFS), other=uf_default)
        return u.fillna(uf_default)
    # string ou vazio
    u = (str(raw).upper()[:2]) if raw else uf_default
    return u if u in VALID_UFS else uf_default


def _post_clean(df: pd.DataFrame, uf_default: str) -> pd.DataFrame:
    """Padroniza colunas, datas e flags para o pipeline."""
    # Data do primeiro sintoma
    if "DT_SIN_PRI" in df.columns:
        df["DT_SIN_PRI"] = _detect_date_parse(df["DT_SIN_PRI"])
    else:
        df["DT_SIN_PRI"] = pd.NaT

    # Deriva UF a partir da primeira coluna candidata existente
    uf_series = None
    for c in UF_CANDIDATES:
        if c in df.columns:
            uf_series = df[c]
            break
    df["UF"] = _normalize_uf(
        uf_series if uf_series is not None else uf_default, uf_default
    )

    # Flags numéricas (coerção defensiva)
    for col in ["EVOLUCAO", "UTI", "VACINA_COV"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int8")
        else:
            df[col] = pd.Series(0, index=df.index, dtype="Int8")

    # Apenas as colunas padronizadas
    return df[["DT_SIN_PRI", "EVOLUCAO", "UTI", "VACINA_COV", "UF"]]


def _read_csv_like(fobj, wanted_cols: list[str]) -> pd.DataFrame:
    """
    Lê CSV a partir de um file-like com tolerância a encoding e linhas ruins.
    Tenta utf-8; se falhar, cai para latin-1. Sempre usa a interseção de colunas.
    """
    # Descobre cabeçalho real
    fobj.seek(0)
    header = pd.read_csv(
        fobj, sep=";", nrows=0, encoding="utf-8", on_bad_lines="skip"
    ).columns.tolist()
    cols = [c for c in wanted_cols if c in header]
    if not cols:
        base = [
            "DT_SIN_PRI",
            "EVOLUCAO",
            "UTI",
            "VACINA_COV",
            "UF",
            "SG_UF_NOT",
            "SG_UF",
            "SG_UF_RES",
        ]
        cols = [c for c in base if c in header]

    # Lê os dados
    fobj.seek(0)
    try:
        return pd.read_csv(
            fobj,
            sep=";",
            low_memory=False,
            usecols=cols,
            encoding="utf-8",
            on_bad_lines="skip",
        )
    except UnicodeDecodeError:
        fobj.seek(0)
        return pd.read_csv(
            fobj,
            sep=";",
            low_memory=False,
            usecols=cols,
            encoding="latin-1",
            on_bad_lines="skip",
        )


def _read_csv_from_zip(path: str, wanted_cols: list[str]) -> pd.DataFrame:
    """
    Abre um ZIP local e escolhe o **maior** .csv para ler (dataset principal).
    """
    with zipfile.ZipFile(path, "r") as zf:
        csv_infos = [zi for zi in zf.infolist() if zi.filename.lower().endswith(".csv")]
        if not csv_infos:
            raise ValueError(f"ZIP sem CSVs: {path}")
        target_info = max(csv_infos, key=lambda z: z.file_size)
        with zf.open(target_info) as f:
            return _read_csv_like(f, wanted_cols)


def _read_csv_selective(path: str, wanted_cols: list[str]) -> pd.DataFrame:
    """
    Lê CSV/ZIP local carregando apenas as colunas disponíveis de 'wanted_cols'.
    """
    if path.lower().endswith(".zip"):
        return _read_csv_from_zip(path, wanted_cols)

    # CSV simples no disco: abre bytes para reaproveitar lógica de encoding
    with open(path, "rb") as fb:
        bio = io.BytesIO(fb.read())
    return _read_csv_like(bio, wanted_cols)


# ------------------ Pipeline ------------------ #
def ingest_local(engine_fn, uf_default: str, cols: list[str], folder: str = "data/raw"):
    """
    Ingestão local:
    - Lê todos os .csv/.zip do diretório informado.
    - Normaliza/valida UF, datas e flags.
    - Materializa srag_staging/base/daily/monthly e cria índices.
    """
    os.makedirs(folder, exist_ok=True)

    paths = sorted(
        glob.glob(os.path.join(folder, "*.csv"))
        + glob.glob(os.path.join(folder, "*.zip"))
    )
    if not paths:
        print(f"⚠️  ingest_local: nenhum CSV/ZIP encontrado em '{folder}'.")
        return

    frames = []
    for path in paths:
        print(f"📄 Lendo: {os.path.basename(path)}")
        raw = _read_csv_selective(path, cols)
        print(f"   → Linhas lidas: {len(raw):,}")
        df = _post_clean(raw, uf_default)
        frames.append(df)

    if not frames:
        raise RuntimeError("Falha na ingestão local: nenhum arquivo foi carregado.")

    full = pd.concat(frames, ignore_index=True)
    print(f"📦 Total consolidado: {len(full):,} linhas")

    eng = engine_fn()
    with eng.begin() as conn:
        # staging
        full.to_sql("srag_staging", conn, if_exists="replace", index=False)

        # base
        conn.execute(text("DROP TABLE IF EXISTS srag_base"))
        conn.execute(
            text("""
            CREATE TABLE srag_base AS
            SELECT
              DT_SIN_PRI AS event_date,
              UF AS uf,
              CASE WHEN EVOLUCAO=2 THEN 1 ELSE 0 END AS death_flag,
              CASE WHEN UTI=1 THEN 1 ELSE 0 END AS icu_flag,
              CASE WHEN VACINA_COV=1 THEN 1 ELSE 0 END AS vaccinated_flag
            FROM srag_staging
            WHERE DT_SIN_PRI IS NOT NULL
        """)
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_srag_base_date_uf ON srag_base (event_date, uf)"
            )
        )

        # diárias
        conn.execute(text("DROP TABLE IF EXISTS srag_daily"))
        conn.execute(
            text("""
            CREATE TABLE srag_daily AS
            SELECT date(event_date) AS day, uf,
                   COUNT(*) AS cases,
                   SUM(icu_flag) AS icu_cases,
                   SUM(death_flag) AS deaths,
                   SUM(vaccinated_flag) AS vaccinated_cases
            FROM srag_base
            GROUP BY 1,2
        """)
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_srag_daily_day_uf ON srag_daily (day, uf)"
            )
        )

        # mensais
        conn.execute(text("DROP TABLE IF EXISTS srag_monthly"))
        conn.execute(
            text("""
            CREATE TABLE srag_monthly AS
            SELECT strftime('%Y-%m-01', event_date) AS month, uf,
                   COUNT(*) AS cases,
                   SUM(icu_flag) AS icu_cases,
                   SUM(death_flag) AS deaths,
                   SUM(vaccinated_flag) AS vaccinated_cases
            FROM srag_base
            GROUP BY 1,2
        """)
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_srag_monthly_month_uf ON srag_monthly (month, uf)"
            )
        )

    print(f"✅ Ingestão local concluída ({len(paths)} arquivo(s) em '{folder}').")
