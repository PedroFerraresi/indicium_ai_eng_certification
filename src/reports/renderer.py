from __future__ import annotations
"""
renderer.py
-----------
Responsável por:
1) Gerar gráficos (PNG) com seaborn
2) Renderizar o relatório HTML via Jinja2
3) Converter HTML -> PDF com xhtml2pdf (pure-Python; sem wkhtmltopdf)

Notas:
- Os gráficos são salvos em resources/charts.
- O HTML é gravado em resources/reports/relatorio.html.
- O orquestrador passa caminhos RELATIVOS para as imagens, então o
  template consegue encontrar ../charts/*.png mesmo fora do navegador.
"""

import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from jinja2 import Environment, FileSystemLoader, select_autoescape

# Conversão HTML -> PDF sem binários externos (wkhtmltopdf não necessário)
try:
    from xhtml2pdf import pisa
except Exception:
    pisa = None  # Se não estiver instalado, html_to_pdf retorna None com segurança.


# === Diretórios padrão ===
TEMPLATES_DIR = Path("src/reports/templates")      # onde está report.html.j2
REPORTS_DIR = Path("resources/reports")            # onde salvamos o HTML/PDF
CHARTS_DIR = Path("resources/charts")              # onde salvamos os PNGs

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

    Parâmetros:
      - df: DataFrame com colunas x_col e y_col
      - x_col: eixo X (ex.: 'day' ou 'month')
      - y_col: eixo Y (ex.: 'cases')
      - title: título do gráfico
      - out_path: caminho de saída do PNG (ex.: resources/charts/casos_30d.png)

    Retorno:
      - Caminho salvo (string)
    """
    # Checagens de segurança (evita KeyError silencioso)
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"plot_series: DataFrame não contém {x_col} e/ou {y_col}.")

    # Cópia defensiva (não altera o df original)
    data = df[[x_col, y_col]].copy()

    # Tenta converter X para datetime (se já for datetime, permanece)
    try:
        data[x_col] = pd.to_datetime(data[x_col])
    except Exception:
        pass

    # Ordena X para evitar “zig-zag” visual
    data = data.sort_values(by=x_col)

    # Estilo claro e legível
    sns.set_theme(context="talk", style="whitegrid")

    # Cria figura explicitamente para controlar salvamento/fechamento
    fig, ax = plt.subplots(figsize=(10, 4))

    # Linha principal
    sns.lineplot(data=data, x=x_col, y=y_col, ax=ax)
    # Pontos por cima da linha (ajuda a ver datas individuais)
    sns.scatterplot(data=data, x=x_col, y=y_col, ax=ax)

    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    fig.tight_layout()

    # Garante diretório de saída
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    # Salva em 300 DPI (boa qualidade para PDF)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    # Libera recursos
    plt.close(fig)

    return out_path


# === 2) RENDERIZAÇÃO DO HTML (Jinja2) ========================================

def _jinja_env() -> Environment:
    """
    Instancia um ambiente Jinja2 apontando para src/reports/templates.
    O autoescape é habilitado para .html/.j2.
    """
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

    O orquestrador monta o `context` com:
      - KPIs: increase_rate, mortality_rate, icu_rate, vaccination_rate
      - chart_30d/chart_12m: caminhos RELATIVOS (ou None)
      - news_summary
      - now: timestamp legível

    Retorna:
      - Caminho absoluto do HTML salvo (string)
    """
    env = _jinja_env()
    template = env.get_template(template_name)
    html_str = template.render(**context)

    out_path = REPORTS_DIR / out_name
    out_path.write_text(html_str, encoding="utf-8")
    return str(out_path)


# === 3) HTML -> PDF (xhtml2pdf) ==============================================

def html_to_pdf(html_path: str) -> str | None:
    """
    Converte o HTML em PDF usando xhtml2pdf (pure-Python).
    - Retorna o caminho do PDF ou None (se xhtml2pdf não estiver disponível
      ou se ocorrer erro). O pipeline não deve quebrar.

    O xhtml2pdf precisa que links/URIs de imagens sejam resolvidos para
    caminhos absolutos; `link_callback` faz essa resolução.
    """
    if pisa is None:
        # Biblioteca não instalada → seguimos apenas com HTML
        return None

    pdf_path = html_path.replace(".html", ".pdf")
    base_dir = Path(html_path).parent

    def link_callback(uri: str, rel: str) -> str:
        # Permite http/https (se um dia usarmos)
        if uri.startswith(("http://", "https://")):
            return uri
        # Resolve caminho relativo ao diretório do HTML
        return str((base_dir / uri).resolve())

    try:
        html = Path(html_path).read_text(encoding="utf-8")
        with open(pdf_path, "wb") as out:
            result = pisa.CreatePDF(
                src=html,
                dest=out,
                link_callback=link_callback,
                encoding="utf-8",
            )
        # result.err == 0 indica sucesso
        return pdf_path if not result.err else None
    except Exception:
        # Qualquer erro aqui não deve impedir o resto do pipeline
        return None
