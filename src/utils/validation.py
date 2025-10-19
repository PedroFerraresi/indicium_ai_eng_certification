from __future__ import annotations

import datetime

import pandas as pd

VALID_UFS = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}


def validate_uf(uf: str) -> str:
    """Normaliza e valida UF; lança ValueError se inválida."""
    if not uf:
        raise ValueError("UF vazia.")
    u = uf.strip().upper()
    if u not in VALID_UFS:
        raise ValueError(f"UF inválida: {uf!r}. Use duas letras, ex.: 'SP'.")
    return u


def clamp_future_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Remove registros cuja data em `col` esteja no futuro (> hoje).
    Retorna um novo DataFrame (cópia), mantendo o schema original.

    Regras:
    - Converte `df[col]` para datetime (coerce NaT para valores inválidos).
    - Se a coluna tiver timezone, remove o timezone (torna naive).
    - Compara com 'hoje' (UTC) sem timezone para evitar erros de comparação.
    """
    if col not in df.columns:
        return df.copy()

    # Converte para datetime; valores inválidos viram NaT
    s = pd.to_datetime(df[col], errors="coerce")

    # Se vier com timezone, remove (naive) para comparação consistente
    # (checamos o dtype da Series, não a Series em si)
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_localize(None)

    # 'Hoje' como Timestamp naive (UTC) para evitar tz mismatch
    today = pd.Timestamp(datetime.datetime.now(datetime.UTC).date())

    # Mantém linhas com data válida e <= hoje
    mask = (s.notna()) & (s <= today)
    out = df.loc[mask].copy()

    # Garante que a coluna no output fica normalizada (datetime naive)
    out[col] = s.loc[mask].values

    return out
