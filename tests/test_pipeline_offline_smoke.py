from pathlib import Path


def test_pipeline_offline_end_to_end(monkeypatch):
    """
    Smoke test OFFLINE da pipeline:
    - Força INGEST_MODE=local para não tentar baixar nada.
    - Usa o banco SQLite versionado no repositório.
    - Garante que o relatório HTML é gerado e que o resumo de notícias
      entra em modo “offline” (sem chamadas externas).
    """
    # Força modo local ANTES de importar o orquestrador
    monkeypatch.setenv("INGEST_MODE", "local")
    # Chaves vazias no CI são OK (evitam chamadas externas)
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("SERPER_API_KEY", "")
    # Query padrão
    monkeypatch.setenv("NEWS_QUERY", "SRAG Brasil")

    # Banco versionado no repo deve existir
    db = Path("data/srag.sqlite")
    assert db.exists(), "Banco SQLite data/srag.sqlite não encontrado no repo."

    # Importa depois dos monkeypatches
    from src.agents.orchestrator import run_pipeline

    out = run_pipeline("SP")

    # Artefatos gerados
    html = out.get("html_path")
    assert html and Path(html).exists(), "Relatório HTML não foi gerado."
    # PDF é opcional (xhtml2pdf pode não estar disponível no ambiente)
    # if out.get("pdf_path"): assert Path(out["pdf_path"]).exists()

    # Em offline, não haverá busca real de notícias:
    news = out.get("news_summary", "")
    assert (
        "Sem notícias recentes" in news
        or "indispon" in news.lower()  # cobre “indisponível”/“indisponivel”
    ), f"Resumo de notícias não parece offline/fallback: {news[:200]}"
