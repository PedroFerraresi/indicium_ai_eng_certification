import pandas as pd
import pytest

from src.reports.render import render_html


def test_renderer_blocks_dataframes():
    """Deve barrar DataFrame no contexto para evitar vazamento de dados por linha."""
    ctx = {
        "uf": "SP",
        "increase_rate": 0.1,
        "mortality_rate": 0.02,
        "icu_rate": 0.05,
        "vaccination_rate": 0.8,
        "chart_30d": None,
        "chart_12m": None,
        "news_summary": "Exemplo.",
        "now": "01/01/2025 00:00",
        # Injeção indevida de DataFrame deve ser barrada:
        "series_30d": pd.DataFrame({"x": [1], "y": [2]}),
    }
    with pytest.raises(ValueError) as ei:
        render_html(ctx)
    assert "series_30d" in str(ei.value)


def test_renderer_blocks_series():
    """Deve barrar pandas.Series no contexto (mesmo motivo: evitar dados tabulares)."""
    ctx = {
        "uf": "SP",
        "increase_rate": 0.1,
        "mortality_rate": 0.02,
        "icu_rate": 0.05,
        "vaccination_rate": 0.8,
        "chart_30d": None,
        "chart_12m": None,
        "news_summary": "Exemplo.",
        "now": "01/01/2025 00:00",
        # Injeção indevida de Series deve ser barrada:
        "series_12m": pd.Series([1, 2, 3]),
    }
    with pytest.raises(ValueError) as ei:
        render_html(ctx)
    assert "series_12m" in str(ei.value)
