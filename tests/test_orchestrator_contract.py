# tests/test_orchestrator_contract.py
"""
Valida o CONTRATO de saída de run_pipeline():

run_pipeline(uf) deve SEMPRE retornar um dicionário com as chaves:
  {
    "uf": str,
    "metrics": dict,
    "news_summary": str,
    "chart_30d": Optional[str],
    "chart_12m": Optional[str],
    "html_path": Optional[str],
    "pdf_path": Optional[str],
  }

O teste é auto-contido: não lê CSV, não chama APIs e não exige arquivos reais.
Usa monkeypatch para trocar as funções internas por stubs determinísticos.
"""

import pathlib
import pandas as pd

import src.agents.orchestrator as orch_mod  # importa o módulo para monkeypatch em seu namespace


def test_run_pipeline_contract(monkeypatch, tmp_path):
    # ---- Stubs determinísticos ----

    # 1) ingestão: no-op
    def fake_ingest():
        return None

    # 2) métricas: retorna KPIs e séries mínimas
    def fake_compute_metrics(uf: str):
        return {
            "increase_rate": 0.10,
            "mortality_rate": 0.02,
            "icu_rate": 0.03,
            "vaccination_rate": 0.80,
            "series_30d": pd.DataFrame(
                {"day": [pd.Timestamp("2025-01-01")], "cases": [10]}
            ),
            "series_12m": pd.DataFrame(
                {"month": [pd.Timestamp("2025-01")], "cases": [300]}
            ),
        }

    # 3) plot: não cria arquivo, apenas retorna o caminho recebido
    def fake_plot_series(df, x_col, y_col, title, out_path):
        return out_path

    # 4) render_html: grava um HTML mínimo no tmp e retorna o caminho
    def fake_render_html(ctx: dict) -> str:
        reports_dir = tmp_path / "resources" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        out = reports_dir / "relatorio.html"
        html = f"""
        <html><body>
          <h1 data-testid="title">Relatório SRAG — UF {ctx.get('uf')}</h1>
          <div data-testid="kpi-increase">{ctx.get('increase_rate')}</div>
          <div data-testid="kpi-mortality">{ctx.get('mortality_rate')}</div>
          <div data-testid="kpi-icu">{ctx.get('icu_rate')}</div>
          <div data-testid="kpi-vaccination">{ctx.get('vaccination_rate')}</div>
          <img src="{ctx.get('chart_30d') or ''}">
          <img src="{ctx.get('chart_12m') or ''}">
          <p data-testid="news">{ctx.get('news_summary') or ''}</p>
        </body></html>
        """.strip()
        out.write_text(html, encoding="utf-8")
        return str(out)

    # 5) html_to_pdf: retorna None (simula PDF desativado/indisponível)
    def fake_html_to_pdf(html_path: str):
        return None

    # 6) notícias: não chama rede
    def fake_search_news(q: str, num: int = 5, run_id=None):
        return []

    def fake_summarize_news(items, run_id=None) -> str:
        return "Sem notícias recentes encontradas."

    # ---- Aplica monkeypatch diretamente no namespace do orquestrador ----
    monkeypatch.setattr(orch_mod, "ingest_csvs", fake_ingest, raising=True)
    monkeypatch.setattr(orch_mod, "compute_metrics", fake_compute_metrics, raising=True)
    monkeypatch.setattr(orch_mod, "plot_series", fake_plot_series, raising=True)
    monkeypatch.setattr(orch_mod, "render_html", fake_render_html, raising=True)
    monkeypatch.setattr(orch_mod, "html_to_pdf", fake_html_to_pdf, raising=True)
    monkeypatch.setattr(orch_mod, "search_news", fake_search_news, raising=True)
    monkeypatch.setattr(orch_mod, "summarize_news", fake_summarize_news, raising=True)

    # ---- Executa ----
    out = orch_mod.run_pipeline("SP")

    # ---- Valida o CONTRATO ----
    # chaves obrigatórias
    expected_keys = {
        "uf",
        "metrics",
        "news_summary",
        "chart_30d",
        "chart_12m",
        "html_path",
        "pdf_path",
    }
    assert (
        set(out.keys()) == expected_keys
    ), f"Contrato diferente do esperado: {out.keys()}"

    # tipos/formatos básicos
    assert out["uf"] == "SP"
    assert isinstance(out["metrics"], dict)
    assert isinstance(out["news_summary"], str)

    # charts: como fornecemos séries não vazias, devem estar preenchidos
    assert isinstance(out["chart_30d"], (str, type(None)))
    assert isinstance(out["chart_12m"], (str, type(None)))
    # no caminho default do orquestrador, os nomes são estes:
    # (não exigimos existência física dos arquivos — o plot foi stubado)
    assert out["chart_30d"] == "resources/charts/casos_30d.png"
    assert out["chart_12m"] == "resources/charts/casos_12m.png"

    # HTML sempre deve vir string com caminho; PDF pode ser None
    assert isinstance(out["html_path"], (str, type(None)))
    assert out["pdf_path"] is None

    # O stub do HTML gravou no tmp; garanta que o arquivo citado existe
    assert out["html_path"] is not None
    assert pathlib.Path(out["html_path"]).exists(), "HTML não foi gerado pelo stub."
