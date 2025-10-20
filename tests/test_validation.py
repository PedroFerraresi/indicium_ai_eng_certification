import datetime

import pandas as pd

from src.utils.validate import clamp_future_dates, validate_uf


def test_validate_uf_ok():
    assert validate_uf("sp") == "SP"
    assert validate_uf("RJ") == "RJ"


def test_validate_uf_fail():
    import pytest

    with pytest.raises(ValueError):
        validate_uf("XX")
    with pytest.raises(ValueError):
        validate_uf("")


def test_clamp_future_dates():
    # Usa a mesma referência de "hoje" que a função: data em UTC (timezone-aware -> date -> Timestamp naive)
    today = pd.Timestamp(datetime.datetime.now(datetime.UTC).date())
    df = pd.DataFrame(
        {
            "day": [today, today + pd.Timedelta(days=1)],
            "cases": [1, 2],
        }
    )
    out = clamp_future_dates(df, "day")

    # Deve remover o registro do futuro e manter o de hoje
    assert len(out) == 1
    assert out.iloc[0]["day"] == today
