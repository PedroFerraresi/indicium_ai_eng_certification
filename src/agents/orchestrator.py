from __future__ import annotations
"""
Agente orquestrador (LangGraph):

Fluxo em nós:
  ingest  -> metrics -> charts -> news -> report -> END

- 'ingest'  : baixa/ingere dados do SRAG e cria tabelas no SQLite.
- 'metrics' : calcula as quatro métricas + séries temporais.
- 'charts'  : plota e salva gráficos (30 dias e 12 meses).
- 'news'    : busca notícias e resume com LLM (explicabilidade).
- 'report'  : renderiza HTML (e PDF, se disponível) e registra logs.

Este grafo mantém a parte "numérica" determinística e delega apenas
a síntese textual das notícias ao LLM, reduzindo riscos de alucinação.
"""

import os
from typing import TypedDict, Optional
from langgraph.graph import StateGraph, END
from datetime import datetime

# Banco de dados (SQLite)
from src.tools.database_orchestrator_sqlite import ingest as ingest_csvs, compute_metrics

# Ferramenta de notícias (Serper + OpenAI)
from src.tools.news import search_news, summarize_news

# Renderização de relatório e gráficos
from src.reports.renderer import plot_series, render_html, html_to_pdf

# Logging estruturado (JSON)
from src.utils.logging import log_event


class AgentState(TypedDict):
    """
    Estado compartilhado do grafo (imutável entre nós).
    """
    uf: str
    metrics: dict
    news_items: list
    news_summary: str
    chart_30d: Optional[str]
    chart_12m: Optional[str]
    html_path: str
    pdf_path: Optional[str]


def node_ingest(state: AgentState):
    """
    Nó 1: Ingestão de dados + materialização das tabelas no SQLite.
    """
    ingest_csvs()
    log_event("ingest_done", {"uf": state["uf"]})
    return state


def node_metrics(state: AgentState):
    """
    Nó 2: Cálculo das métricas e séries temporais.
    """
    m = compute_metrics(uf=state["uf"])
    state["metrics"] = m

    # Loga apenas o tamanho das séries para evitar JSON muito grande
    log_event("metrics", {
        **{k: v for k, v in m.items() if k not in ("series_30d", "series_12m")},
        "series_30d_len": len(m["series_30d"]),
        "series_12m_len": len(m["series_12m"]),
    })
    return state


def node_charts(state: AgentState):
    """
    Nó 3: Geração de gráficos (PNG) a partir das séries calculadas.
    """
    m = state["metrics"]
    os.makedirs("resources/charts", exist_ok=True)

    c30 = "resources/charts/casos_30d.png"
    c12 = "resources/charts/casos_12m.png"

    if len(m["series_30d"]) > 0:
        plot_series(m["series_30d"], "day", "cases", "Casos diários (30d)", c30)
        state["chart_30d"] = c30

    if len(m["series_12m"]) > 0:
        plot_series(m["series_12m"], "month", "cases", "Casos mensais (12m)", c12)
        state["chart_12m"] = c12

    log_event("charts", {"chart_30d": state.get("chart_30d"), "chart_12m": state.get("chart_12m")})
    return state


def node_news(state: AgentState):
    """
    Nó 4: Busca e sumarização de notícias.
    Em qualquer erro de API/cota, gera um texto seguro e continua.
    """
    q = os.getenv("NEWS_QUERY", "SRAG Brasil")
    try:
        items = search_news(q, num=5)
    except Exception as e:
        items = []
    state["news_items"] = items

    try:
        state["news_summary"] = summarize_news(items) if items else "Sem notícias recentes encontradas."
    except Exception as e:
        state["news_summary"] = "Resumo de notícias indisponível no momento."
    log_event("news", {"query": q, "items_len": len(items)})
    return state


def node_report(state: AgentState):
    """
    Nó 5: Renderização do relatório final (HTML e, se possível, PDF).
    Usa caminhos relativos a 'resources/reports' para que as imagens carreguem.
    """
    m = state["metrics"]

    def _rel(p: str | None) -> str | None:
        if not p:
            return None
        base = os.path.join("resources", "reports")
        return os.path.relpath(p, start=base)  # ex.: ../charts/casos_30d.png

    ctx = {
        "uf": state["uf"],
        "increase_rate": m["increase_rate"],
        "mortality_rate": m["mortality_rate"],
        "icu_rate": m["icu_rate"],
        "vaccination_rate": m["vaccination_rate"],
        "chart_30d": _rel(state.get("chart_30d")),
        "chart_12m": _rel(state.get("chart_12m")),
        "news_summary": state.get("news_summary", "Sem notícias recentes encontradas."),
        "now": datetime.now().strftime("%d/%m/%Y %H:%M"),
    }

    html = render_html(ctx)
    state["html_path"] = html
    state["pdf_path"] = html_to_pdf(html)

    log_event("report", {"html": html, "pdf": state["pdf_path"]})
    return state


def build_graph():
    """
    Define o grafo de estados e a ordem de execução dos nós.
    """
    g = StateGraph(AgentState)
    g.add_node("ingest", node_ingest)
    g.add_node("metrics", node_metrics)
    g.add_node("charts", node_charts)
    g.add_node("news", node_news)
    g.add_node("report", node_report)

    g.set_entry_point("ingest")
    g.add_edge("ingest", "metrics")
    g.add_edge("metrics", "charts")
    g.add_edge("charts", "news")
    g.add_edge("news", "report")
    g.add_edge("report", END)
    return g.compile()


def run_pipeline(uf: str):
    """
    Função auxiliar para executar o grafo inteiro com uma UF.
    Retorna o estado final (com caminhos dos arquivos gerados).
    """
    graph = build_graph()
    state: AgentState = {
        "uf": uf,
        "metrics": {},
        "news_items": [],
        "news_summary": "",
        "chart_30d": None,
        "chart_12m": None,
        "html_path": "",
        "pdf_path": None
    }
    return graph.invoke(state)
