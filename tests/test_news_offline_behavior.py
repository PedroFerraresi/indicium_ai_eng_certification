import importlib

import requests


def test_news_offline_behaviour(monkeypatch):
    # 1) Força modo offline: sem chaves
    monkeypatch.setenv("SERPER_API_KEY", "")
    monkeypatch.setenv("OPENAI_API_KEY", "")

    # 2) Recarrega o módulo para que ele leia as vars atualizadas
    import src.tools.news as news

    importlib.reload(news)

    # 3) Bloqueia qualquer tentativa de rede por segurança
    def _blocked(*args, **kwargs):  # se for chamado, falha o teste
        raise AssertionError("requests.post não deve ser chamado em modo offline")

    monkeypatch.setattr(requests, "post", _blocked, raising=True)

    # 4) search_news deve retornar lista vazia quando SERPER_API_KEY está ausente
    items = news.search_news("SRAG Brasil", num=1, run_id="t-offline")
    assert items == [], f"search_news deveria retornar [], mas retornou: {items!r}"

    # 5) summarize_news com lista vazia → fallback 'Sem notícias recentes...'
    summary_empty = news.summarize_news(items, run_id="t-offline")
    assert isinstance(summary_empty, str) and "Sem notícias recentes" in summary_empty

    # 6) summarize_news com itens, mas sem OPENAI_API_KEY → fallback 'indisponível'
    fake_items = [
        {"title": "Qualquer coisa", "source": "Fonte X", "link": "https://exemplo.com"}
    ]
    summary_no_key = news.summarize_news(fake_items, run_id="t-offline")
    assert "indisponível" in summary_no_key.lower(), (
        "Esperava fallback indicando indisponibilidade quando OPENAI_API_KEY está vazia; "
        f"recebi: {summary_no_key!r}"
    )
