# tests/test_pipeline_offline_smoke.py
"""
Smoke test OFFLINE do pipeline:
- Força ausência de chaves (SERPER/OPENAI) e INGEST_MODE=local
- Recarrega módulos que leem .env no import (news e orchestrator)
- Roda a pipeline e valida artefatos + fallback de notícias
"""

import importlib
from pathlib import Path


def test_pipeline_offline_end_to_end(monkeypatch):
    # 1) Força modo "sem rede": sem chaves e ingestão local
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("SERPER_API_KEY", "")
    monkeypatch.setenv("INGEST_MODE", "local")

    # 2) Recarrega módulos que leem .env no import
    import src.tools.news as news

    importlib.reload(news)  # agora SERPER/OPENAI_API_KEY ficam vazios

    import src.agents.orchestrator as orch

    importlib.reload(orch)  # garante que o orchestrator use o 'news' recarregado

    # 3) Executa a pipeline (data/raw já tem arquivos no repo)
    out = orch.run_pipeline("SP")

    # 4) Valida contrato mínimo dos artefatos
    html = out.get("html_path")
    pdf = out.get("pdf_path")

    assert html and Path(html).exists(), "HTML não foi gerado."
    # PDF pode existir (xhtml2pdf instalado) ou não — ambos aceitáveis
    assert (pdf is None) or Path(pdf).exists(), "PDF apontado mas arquivo não existe."

    # 5) Com SERPER vazio, o node_news não chama summarize_news
    #    → summary deve ser o fallback “Sem notícias recentes…”
    summary = out.get("news_summary") or ""
    assert isinstance(summary, str) and summary, "Resumo de notícias inválido."
    assert "Sem notícias recentes" in summary, (
        "Esperava fallback de notícias no modo offline, "
        f"mas recebi: {summary[:180]!r}"
    )
