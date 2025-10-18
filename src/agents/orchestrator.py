from __future__ import annotations

import os
from pathlib import Path
from typing import TypedDict, Optional, Any, Dict
from datetime import datetime
from langgraph.graph import StateGraph, END

# Ingestão + métricas
from src.tools.database_orchestrator_sqlite import (
    ingest as ingest_csvs,
    compute_metrics,
)

# Notícias (busca + sumarização) — news.py já tem timeouts/retries/backoff
from src.tools.news import search_news, summarize_news

# Relatório e gráficos
from src.reports.renderer import plot_series, render_html, html_to_pdf

# Auditoria estruturada
from src.utils.audit import new_run_id, audit_span, log_kv

# Validações e clamp de datas
from src.utils.validation import validate_uf, clamp_future_dates

"""
Orquestrador do pipeline (LangGraph).

Fluxo de nós:
  ingest  -> metrics -> charts -> news -> report -> END

Observabilidade:
- Cada nó é envolvido por `audit_span(...)` que loga início/fim/erro,
  duração (ms) e um `run_id` para rastreabilidade ponta-a-ponta.

Guardrails:
- Validação de UF (apenas siglas válidas).
- Remoção de datas futuras nas séries (clamp).
- `run_id` repassado à ferramenta de notícias (para auditoria de retries/latência).

Contrato de saída:
- `run_pipeline(uf)` SEMPRE retorna:
  {
    "uf", "metrics", "news_summary",
    "chart_30d", "chart_12m",
    "html_path", "pdf_path"
  }
"""


class AgentState(TypedDict, total=False):
    """Estado compartilhado do grafo (chaves adicionadas ao longo do fluxo)."""

    run_id: str
    uf: str
    metrics: dict[str, Any]
    news_items: list
    news_summary: str
    chart_30d: Optional[str]
    chart_12m: Optional[str]
    html_path: str
    pdf_path: Optional[str]


def node_ingest(state: AgentState):
    run_id = state["run_id"]
    mode = os.getenv("INGEST_MODE", "auto")
    with audit_span("ingest", run_id, node="ingest", ingest_mode=mode):
        ingest_csvs()
        log_kv(run_id, "ingest.output", db=os.getenv("DB_PATH"))
    return state


def node_metrics(state: AgentState):
    run_id = state["run_id"]
    uf = validate_uf(state["uf"])  # normaliza/valida
    with audit_span("metrics", run_id, node="metrics", uf=uf):
        m = compute_metrics(uf)
        # clamp de datas futuras
        m["series_30d"] = clamp_future_dates(m["series_30d"], "day")
        m["series_12m"] = clamp_future_dates(m["series_12m"], "month")
        # resumo leve
        log_kv(
            run_id,
            "metrics.summary",
            increase_rate=m["increase_rate"],
            mortality_rate=m["mortality_rate"],
            icu_rate=m["icu_rate"],
            vaccination_rate=m["vaccination_rate"],
            rows_30d=int(m["series_30d"].shape[0]),
            rows_12m=int(m["series_12m"].shape[0]),
        )
        state["metrics"] = m
        state["uf"] = uf
    return state


def node_charts(state: AgentState):
    run_id = state["run_id"]
    with audit_span("charts", run_id, node="charts"):
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
        log_kv(
            run_id,
            "charts.output",
            chart_30d=state.get("chart_30d"),
            chart_12m=state.get("chart_12m"),
        )
    return state


def node_news(state: AgentState):
    run_id = state["run_id"]
    q = os.getenv("NEWS_QUERY", "SRAG Brasil")
    with audit_span("news", run_id, node="news", query=q):
        try:
            items = search_news(q, num=5, run_id=run_id)
        except Exception:
            items = []
        log_kv(run_id, "news.items", count=len(items))
        try:
            summary = (
                summarize_news(items, run_id=run_id)
                if items
                else "Sem notícias recentes encontradas."
            )
        except Exception:
            summary = "Resumo de notícias indisponível no momento."
        log_kv(run_id, "news.summary", length=len(summary))
        state["news_items"] = items
        state["news_summary"] = summary
    return state


def node_report(state: AgentState):
    run_id = state["run_id"]
    with audit_span("report", run_id, node="report"):
        # Arredonda KPIs para estabilidade de apresentação
        kpis = ["increase_rate", "mortality_rate", "icu_rate", "vaccination_rate"]
        m = state["metrics"].copy()
        for k in kpis:
            try:
                m[k] = round(float(m[k]), 4)
            except Exception:
                pass

        # --- Caminhos relativos em formato POSIX ('/') independentemente do SO
        reports_dir = Path("resources/reports")

        def _rel_posix(p: Optional[str]) -> Optional[str]:
            if not p:
                return None
            # os.path.relpath calcula a relatividade correta; Path(...).as_posix() normaliza para '/'
            return Path(os.path.relpath(p, start=reports_dir)).as_posix()

        rel30 = _rel_posix(state.get("chart_30d"))
        rel12 = _rel_posix(state.get("chart_12m"))

        ctx = {
            "uf": state["uf"],
            **{k: m[k] for k in kpis},
            "chart_30d": rel30,
            "chart_12m": rel12,
            "news_summary": state.get(
                "news_summary", "Sem notícias recentes encontradas."
            ),
            "now": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }
        html = render_html(ctx)
        pdf = html_to_pdf(html)
        log_kv(run_id, "report.output", html=html, pdf=pdf)
        state["html_path"] = html
        state["pdf_path"] = pdf
    return state


def build_graph():
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


def run_pipeline(uf: str) -> Dict[str, Any]:
    """
    Executa o grafo para a UF informada e retorna um dicionário CANÔNICO.
    """
    run_id = new_run_id()
    uf = validate_uf(uf)  # valida a entrada antes de iniciar
    initial_state: AgentState = {"uf": uf, "run_id": run_id}

    with audit_span("run", run_id, node="orchestrator", uf=uf):
        final_state: AgentState = graph.invoke(initial_state)

    # Normaliza a saída para respeitar o contrato
    canonical_out: Dict[str, Any] = {
        "uf": final_state.get("uf", uf),
        "metrics": final_state.get("metrics", {}),
        "news_summary": final_state.get(
            "news_summary", "Sem notícias recentes encontradas."
        ),
        "chart_30d": final_state.get("chart_30d"),
        "chart_12m": final_state.get("chart_12m"),
        "html_path": final_state.get("html_path"),
        "pdf_path": final_state.get("pdf_path"),
    }
    return canonical_out
