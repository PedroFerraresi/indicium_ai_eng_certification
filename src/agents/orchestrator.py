from __future__ import annotations
"""
Orquestrador do pipeline (LangGraph).

Fluxo de nós:
  ingest  -> metrics -> charts -> news -> report -> END

- 'ingest'  : decide local/remoto e materializa tabelas no SQLite.
- 'metrics' : lê o banco e calcula KPIs + séries (30d/12m).
- 'charts'  : gera PNGs com seaborn.
- 'news'    : busca notícias (Serper) e resume (OpenAI) com fallback.
- 'report'  : renderiza HTML (Jinja2) e tenta gerar PDF (xhtml2pdf).

Observabilidade:
- Cada nó é envolvido por `audit_span(...)` que loga início/fim/erro,
  duração (ms) e um `run_id` para rastreabilidade ponta-a-ponta.

Novos guardrails:
- Validação de UF (apenas siglas válidas).
- Remoção de datas futuras nas séries (clamp).
- `run_id` repassado à ferramenta de notícias (para auditoria de retries/latência).
"""

import os
from typing import TypedDict, Optional, Any
from datetime import datetime
from langgraph.graph import StateGraph, END

# Ingestão + métricas
from src.tools.database_orchestrator_sqlite import ingest as ingest_csvs, compute_metrics

# Notícias (busca + sumarização) — news.py já tem timeouts/retries/backoff
from src.tools.news import search_news, summarize_news

# Relatório e gráficos
from src.reports.renderer import plot_series, render_html, html_to_pdf

# Auditoria estruturada
from src.utils.audit import new_run_id, audit_span, log_kv

# Validações e clamp de datas
from src.utils.validation import validate_uf, clamp_future_dates


class AgentState(TypedDict, total=False):
    """
    Estado compartilhado do grafo.
    `total=False` => as chaves podem ser adicionadas conforme o fluxo avança.
    """
    run_id: str                 # id único desta execução (para auditoria)
    uf: str                     # UF a ser analisada
    metrics: dict[str, Any]     # KPIs e DataFrames (series_30d/series_12m)
    news_items: list            # itens de notícia (dicts do Serper)
    news_summary: str           # resumo textual das notícias
    chart_30d: Optional[str]    # caminho do PNG de 30 dias (se existir)
    chart_12m: Optional[str]    # caminho do PNG de 12 meses (se existir)
    html_path: str              # caminho do HTML renderizado
    pdf_path: Optional[str]     # caminho do PDF gerado (ou None)


def node_ingest(state: AgentState):
    """
    Nó 1: Ingestão e preparação do banco.
    - Respeita INGEST_MODE=auto|local|remote
    - Cria/atualiza srag_staging/base/daily/monthly no SQLite
    """
    run_id = state["run_id"]
    mode = os.getenv("INGEST_MODE", "auto")
    with audit_span("ingest", run_id, node="ingest", ingest_mode=mode):
        ingest_csvs()
        log_kv(run_id, "ingest.output", db=os.getenv("DB_PATH"))
    return state


def node_metrics(state: AgentState):
    """
    Nó 2: Cálculo de métricas e séries temporais (determinístico).
    - Lê o SQLite e retorna KPIs + DataFrames (series_30d/series_12m)
    - Valida UF e remove datas futuras das séries (clamp)
    - Loga um resumo leve das métricas
    """
    run_id = state["run_id"]
    # Validação de UF (ex.: 'sp' -> 'SP'; lança ValueError se inválida)
    uf = validate_uf(state["uf"])

    with audit_span("metrics", run_id, node="metrics", uf=uf):
        m = compute_metrics(uf)

        # Guardrail: não permitir datas no futuro nas séries
        # (colunas esperadas: 'day' para série diária, 'month' para mensal)
        m["series_30d"] = clamp_future_dates(m["series_30d"], "day")
        m["series_12m"] = clamp_future_dates(m["series_12m"], "month")

        # Resumo ‘leve’ das métricas para auditoria
        summary = {
            "increase_rate": m["increase_rate"],
            "mortality_rate": m["mortality_rate"],
            "icu_rate": m["icu_rate"],
            "vaccination_rate": m["vaccination_rate"],
            "rows_30d": int(m["series_30d"].shape[0]),
            "rows_12m": int(m["series_12m"].shape[0]),
        }
        log_kv(run_id, "metrics.summary", **summary)
        state["metrics"] = m
        state["uf"] = uf  # normaliza UF no estado
    return state


def node_charts(state: AgentState):
    """
    Nó 3: Geração de gráficos (PNG) com seaborn.
    - Salva arquivos em resources/charts/
    - Guarda os caminhos no estado para o relatório usar
    """
    run_id = state["run_id"]
    with audit_span("charts", run_id, node="charts"):
        m = state["metrics"]
        os.makedirs("resources/charts", exist_ok=True)

        c30 = "resources/charts/casos_30d.png"
        c12 = "resources/charts/casos_12m.png"

        # Só plota se houver dados suficientes
        if len(m["series_30d"]) > 0:
            plot_series(m["series_30d"], "day", "cases", "Casos diários (30d)", c30)
            state["chart_30d"] = c30

        if len(m["series_12m"]) > 0:
            plot_series(m["series_12m"], "month", "cases", "Casos mensais (12m)", c12)
            state["chart_12m"] = c12

        log_kv(run_id, "charts.output",
               chart_30d=state.get("chart_30d"),
               chart_12m=state.get("chart_12m"))
    return state


def node_news(state: AgentState):
    """
    Nó 4: Busca e sumarização de notícias.
    - Busca itens no Serper (NewsFetcherTool) com timeout/retries/backoff
    - Sumariza com LLM (OpenAI). Em caso de erro/quota, usa fallback.
    - Nunca quebra o pipeline (try/except).
    """
    run_id = state["run_id"]
    q = os.getenv("NEWS_QUERY", "SRAG Brasil")

    with audit_span("news", run_id, node="news", query=q):
        # Busca com fallback (rede/servidor pode falhar)
        try:
            items = search_news(q, num=5, run_id=run_id)  # repassa run_id p/ auditar retries
        except Exception:
            items = []
        log_kv(run_id, "news.items", count=len(items))

        # Sumarização com fallback (quota 429, etc.)
        try:
            summary = summarize_news(items, run_id=run_id) if items else "Sem notícias recentes encontradas."
        except Exception:
            summary = "Resumo de notícias indisponível no momento."

        log_kv(run_id, "news.summary", length=len(summary))
        state["news_items"] = items
        state["news_summary"] = summary
    return state


def node_report(state: AgentState):
    """
    Nó 5: Renderização do relatório final.
    - Monta o contexto para o template Jinja2 (KPIs + caminhos das imagens)
    - Gera HTML e tenta converter para PDF (xhtml2pdf)
    - Caminhos relativos garantem que as imagens carreguem no HTML
    """
    run_id = state["run_id"]
    with audit_span("report", run_id, node="report"):
        # >>> Ajuste solicitado: arredondar KPIs antes de enviar ao template
        kpis = ["increase_rate", "mortality_rate", "icu_rate", "vaccination_rate"]
        m = state["metrics"].copy()
        for k in kpis:
            try:
                m[k] = round(float(m[k]), 4)
            except Exception:
                # Se não for numérico, mantém como veio (defensivo)
                pass

        ctx = {
            "uf": state["uf"],
            # KPIs numéricos usados como indicadores na capa (agora arredondados)
            **{k: m[k] for k in kpis},
            # as imagens são referenciadas relativamente ao diretório do HTML
            "chart_30d": os.path.relpath(state.get("chart_30d"), start="resources/reports") if state.get("chart_30d") else None,
            "chart_12m": os.path.relpath(state.get("chart_12m"), start="resources/reports") if state.get("chart_12m") else None,
            "news_summary": state.get("news_summary", "Sem notícias recentes encontradas."),
            "now": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }
        html = render_html(ctx)       # renderiza resources/reports/relatorio.html
        pdf = html_to_pdf(html)       # tenta gerar resources/reports/relatorio.pdf (pode ser None)

        log_kv(run_id, "report.output", html=html, pdf=pdf)
        state["html_path"] = html
        state["pdf_path"] = pdf
    return state


def build_graph():
    """
    Define o grafo de estados (nós + arestas) e compila.
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


# Compila o grafo uma única vez ao importar o módulo
graph = build_graph()


def run_pipeline(uf: str):
    """
    Executa o grafo para a UF informada e retorna o estado final.
    - Normaliza/valida a UF antes de iniciar.
    - Gera um `run_id` único para auditar a execução inteira.
    """
    run_id = new_run_id()
    uf = validate_uf(uf)  # valida o parâmetro de entrada o quanto antes
    state: AgentState = {"uf": uf, "run_id": run_id}
    with audit_span("run", run_id, node="orchestrator", uf=uf):
        return graph.invoke(state)
