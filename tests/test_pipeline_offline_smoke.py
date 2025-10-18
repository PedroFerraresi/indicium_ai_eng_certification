import importlib
from pathlib import Path
import sys


def test_pipeline_offline_end_to_end(monkeypatch):
    """
    Smoke test OFFLINE da pipeline:
    - Força INGEST_MODE=local (sem baixar nada).
    - Recarrega os módulos que leem .env no import.
    - Gera o relatório e valida fallback de notícias.
    """
    # 1) Força ambiente OFFLINE antes de importar qualquer módulo do projeto
    monkeypatch.setenv("INGEST_MODE", "local")
    monkeypatch.setenv("OPENAI_API_KEY", "")   # evita chamadas ao LLM
    monkeypatch.setenv("SERPER_API_KEY", "")   # evita chamadas ao Serper
    monkeypatch.setenv("NEWS_QUERY", "SRAG Brasil")

    # 2) Recarrega módulos que já possam ter sido importados por outros testes
    #    (eles capturam env no import)
    if "src.tools.database_orchestrator_sqlite" in sys.modules:
        importlib.reload(sys.modules["src.tools.database_orchestrator_sqlite"])

    # Recarregar o orquestrador garante que o grafo seja recompilado com o módulo acima
    if "src.agents.orchestrator" in sys.modules:
        importlib.reload(sys.modules["src.agents.orchestrator"])

    # 3) Agora importamos com o ambiente correto
    from src.agents.orchestrator import run_pipeline
    import src.tools.database_orchestrator_sqlite as dbmod

    # Sanidade: o módulo de ingestão precisa estar em 'local'
    assert dbmod.INGEST_MODE == "local"

    # 4) Banco versionado no repo deve existir (sem baixar nada)
    db = Path("data/srag.sqlite")
    assert db.exists(), "Banco SQLite data/srag.sqlite não encontrado no repo."

    # 5) Executa a pipeline
    out = run_pipeline("SP")

    # 6) Artefatos
    html = out.get("html_path")
    assert html and Path(html).exists(), "Relatório HTML não foi gerado."
    # PDF é opcional; não exigimos no CI

    # 7) Fallback de notícias (chaves vazias => offline)
    news = out.get("news_summary", "")
    assert ("Sem notícias recentes" in news) or ("indispon" in news.lower()), (
        f"Resumo de notícias não parece offline/fallback: {news[:200]}"
    )
