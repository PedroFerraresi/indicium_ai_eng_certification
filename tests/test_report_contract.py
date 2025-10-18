# tests/test_report_contract.py
import pathlib
import re
from src.reports.renderer import render_html

def _ensure_report():
    """Garante que resources/reports/relatorio.html exista.
    Se não existir, renderiza um HTML mínimo com dados fake.
    """
    p = pathlib.Path("resources/reports/relatorio.html")
    if p.exists():
        return p

    # Contexto mínimo – não precisa de imagens reais; só referências
    ctx = {
        "uf": "SP",
        "increase_rate": 0.12,
        "mortality_rate": 0.034,
        "icu_rate": 0.18,
        "vaccination_rate": 0.77,
        # Qualquer string que contenha o nome do arquivo passa no regex abaixo
        "chart_30d": "charts/casos_30d.png",
        "chart_12m": "charts/casos_12m.png",
        "news_summary": "Resumo fake para testes.",
        "now": "01/01/2025 00:00",
    }
    out = render_html(ctx)  # grava em resources/reports/relatorio.html
    return pathlib.Path(out)

def test_report_contract_exists_and_has_sections():
    p = _ensure_report()
    assert p.exists(), "Falha ao gerar/achar o relatório HTML."
    html = p.read_text(encoding="utf-8")

    # ---- KPIs: checa pelos data-testids adicionados ao template ----
    must_testids = [
        'data-testid="kpi-increase"',
        'data-testid="kpi-mortality"',
        'data-testid="kpi-icu"',
        'data-testid="kpi-vaccination"',
    ]
    missing = [t for t in must_testids if t not in html]
    assert not missing, f"KPI(s) ausente(s) no HTML: {missing}"

    # ---- Gráficos exigidos (30d e 12m) – referencia aos arquivos ----
    assert re.search(r"casos_30d\.png", html), "Gráfico 30d não referenciado no HTML."
    assert re.search(r"casos_12m\.png", html), "Gráfico 12m não referenciado no HTML."
