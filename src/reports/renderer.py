from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from jinja2 import Environment, FileSystemLoader, select_autoescape

"""
renderer.py
-----------
Responsável por:
1) Gerar gráficos (PNG) com seaborn
2) Renderizar o relatório HTML via Jinja2
3) Converter HTML -> PDF com xhtml2pdf (pure-Python; sem wkhtmltopdf)

Guardrails adicionados:
- PRIVACY GUARD: `render_html` rejeita DataFrames no contexto do template,
  impedindo vazamento de dados em nível de linha (somente agregados são permitidos).

Notas:
- Os gráficos são salvos em resources/charts/ e referenciados no HTML por caminhos
  RELATIVOS a resources/reports/ (o orquestrador já calcula os paths relativos).
- Se xhtml2pdf não estiver instalado, seguimos apenas com HTML (retorna None no PDF).
"""

# Conversão HTML -> PDF sem binários externos (wkhtmltopdf não é necessário)
try:
    from xhtml2pdf import pisa
except Exception:
    pisa = None  # se indisponível, html_to_pdf retornará None com segurança

# === Diretórios padrão (mantidos fixos para compatibilidade com o projeto) ===
TEMPLATES_DIR = Path("src/reports/templates")  # onde está report.html.j2
REPORTS_DIR = Path("resources/reports")  # onde salvamos o HTML/PDF
CHARTS_DIR = Path("resources/charts")  # onde salvamos os PNGs

# Garante existência dos diretórios de artefatos
for _p in (REPORTS_DIR, CHARTS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


# === 1) GRÁFICOS COM SEABORN =================================================
def plot_series(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    out_path: str,
) -> str:
    """
    Gera um gráfico de linhas + pontos com seaborn a partir do DataFrame `df`
    e salva a figura em `out_path` (PNG).
    """
    # Checagens de segurança
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"plot_series: DataFrame não contém {x_col} e/ou {y_col}.")
    if df.empty:
        raise ValueError("plot_series: DataFrame vazio — nada para plotar.")

    # Cópia defensiva
    data = df[[x_col, y_col]].copy()

    # Converte X para datetime (se possível) e ordena
    try:
        data[x_col] = pd.to_datetime(data[x_col])
    except Exception:
        pass
    data = data.sort_values(by=x_col)

    sns.set_theme(context="talk", style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.lineplot(data=data, x=x_col, y=y_col, ax=ax)
    sns.scatterplot(data=data, x=x_col, y=y_col, ax=ax)
    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    fig.tight_layout()

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return out_path


# === 2) RENDERIZAÇÃO DO HTML (Jinja2) ========================================
def _jinja_env() -> Environment:
    """Ambiente Jinja2 apontando para src/reports/templates."""
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(enabled_extensions=("html", "j2")),
    )


def render_html(
    context: Dict[str, Any],
    template_name: str = "report.html.j2",
    out_name: str = "relatorio.html",
) -> str:
    """
    Renderiza o template Jinja2 com o `context` e grava o HTML final.
    Bloqueia DataFrames/Series no contexto por privacidade.
    """
    # PRIVACY GUARD
    for k, v in context.items():
        if isinstance(v, (pd.DataFrame, pd.Series)):
            raise ValueError(f"Contexto contém dados tabulares não permitidos: {k}")

    # Normaliza caminhos de gráficos para POSIX ('/') — necessário no Windows
    for key in ("chart_30d", "chart_12m"):
        if isinstance(context.get(key), str):
            context[key] = Path(context[key]).as_posix()

    env = _jinja_env()
    template = env.get_template(template_name)
    html_str = template.render(**context)

    out_path = REPORTS_DIR / out_name
    out_path.write_text(html_str, encoding="utf-8")
    return str(out_path)


# === 3) HTML -> PDF (xhtml2pdf) ==============================================
def html_to_pdf(html_path: str) -> Optional[str]:
    """
    Converte o HTML em PDF usando xhtml2pdf (pure-Python).
    Retorna o caminho do PDF ou None (se indisponível/erro).
    """
    if pisa is None:
        return None

    pdf_path = html_path.replace(".html", ".pdf")
    base_dir = Path(html_path).parent

    # Resolve URIs relativos (../charts/xyz.png) para caminhos absolutos.
    # Faz normalização de barras para suportar HTML gerado no Windows.
    def link_callback(uri: str, rel: str) -> str:
        # Normaliza barras invertidas -> POSIX
        uri = (uri or "").replace("\\", "/")

        # Mantém http(s) e file:// como estão
        if uri.startswith(("http://", "https://", "file://")):
            return uri

        p = Path(uri)
        # Se for absoluto (inclusive "C:/..."), resolve direto
        if p.is_absolute():
            return str(p.resolve())
        # Caso contrário, resolve relativo ao diretório do HTML
        return str((base_dir / p).resolve())

    try:
        html = Path(html_path).read_text(encoding="utf-8")
        with open(pdf_path, "wb") as out:
            result = pisa.CreatePDF(
                src=html,
                dest=out,
                link_callback=link_callback,
                encoding="utf-8",
            )

        print(pdf_path)
        return pdf_path if not result.err else None
    except Exception:
        return None
