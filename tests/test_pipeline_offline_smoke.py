import os


def test_pipeline_offline_end_to_end(monkeypatch):
    """
    Smoke test **OFFLINE** da pipeline de ponta a ponta.

    Objetivo
    --------
    Verificar que o orquestrador executa o fluxo completo sem depender de serviços
    externos (download remoto, OpenAI, Serper). Para isso, o teste força
    `INGEST_MODE=local`, o que faz a pipeline usar apenas os artefatos locais
    (banco/CSVs) e seguir até a geração do relatório.

    O que validamos
    ---------------
    - O ambiente foi ajustado para `INGEST_MODE=local`.
    - `run_pipeline("SP")` completa sem erro e retorna o dicionário canônico.
    - O caminho do HTML (`html_path`) está presente (relatório gerado).

    Observações
    -----------
    - Não inspecionamos conteúdo do relatório nem presença de PDF/imagens.
    - Este é um teste de fumaça (sanity) para garantir que a execução offline
      da pipeline não quebre e produz ao menos o HTML final.
    """
    monkeypatch.setenv("INGEST_MODE", "local")
    from src.agents.orchestrator import run_pipeline

    # Confere que o ambiente foi realmente ajustado
    assert os.getenv("INGEST_MODE") == "local"

    out = run_pipeline("SP")

    # Deve existir um caminho para o HTML gerado
    assert out["html_path"]


def test_auto_mode_without_local_files_falls_back(monkeypatch):
    """
    Smoke test do modo de ingestão "auto".

    Este teste força `INGEST_MODE=auto` e remove `SRAG_URLS` do ambiente para
    simular um cenário sem arquivos locais e sem URLs remotas configuradas.
    Ao chamar `ingest()`, aceitamos dois comportamentos válidos:
      1) a função apenas loga a decisão e retorna, ou
      2) levanta `RuntimeError` informando a ausência de dados locais/URLs.

    O objetivo é garantir que o caminho de decisão do modo "auto" seja
    previsível e não quebre de forma inesperada (não validamos logs/STDOUT).
    """
    # Força auto e zera SRAG_URLS para não bater remoto em ambiente dev
    monkeypatch.setenv("INGEST_MODE", "auto")
    monkeypatch.delenv("SRAG_URLS", raising=False)

    # O seu ingest imprime escolha — não vamos assertar stdout,
    # apenas garantir que a função não explode quando chamada indiretamente
    from src.tools.db_orchestrator import ingest

    try:
        ingest()  # com SRAG_URLS vazio e sem arquivos locais, tende a levantar RuntimeError
    except RuntimeError:
        # É o comportamento esperado quando não há dados locais nem URLs remotas
        pass
