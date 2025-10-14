from __future__ import annotations
"""
Renderização do relatório:
- Gera e salva gráficos (PNG) a partir de DataFrames.
- Renderiza um HTML via template Jinja2.
- Opcionalmente converte HTML em PDF usando wkhtmltopdf (via pdfkit).

Observações:
- Evitamos uso de seaborn para reduzir dependências e facilitar ambientes.
- Um gráfico por figura (mais simples de compor no HTML).
"""

import os
import matplotlib.pyplot as plt
from jinja2 import Environment, FileSystemLoader
import pdfkit
from xhtml2pdf import pisa

# Pasta onde ficam os templates Jinja2 (report.html.j2)
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def plot_series(df, xcol, ycol, title, outpath):
    """
    Gera um gráfico simples de linha a partir de um DataFrame.
    - df: DataFrame com colunas xcol e ycol
    - outpath: caminho do arquivo .png a ser salvo
    """
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    plt.figure()
    plt.plot(df[xcol], df[ycol])  # linha padrão (sem cores específicas)
    plt.title(title)
    plt.xlabel(xcol)
    plt.ylabel(ycol)
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def render_html(context: dict, template_name="report.html.j2") -> str:
    """
    Preenche o template Jinja2 com as métricas, caminhos dos gráficos e texto
    de notícias. Salva o HTML final em resources/reports/relatorio.html.
    """
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    html = env.get_template(template_name).render(**context)

    out_html = os.path.join("resources", "reports", "relatorio.html")
    os.makedirs(os.path.dirname(out_html), exist_ok=True)
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    return out_html


def html_to_pdf(html_path: str) -> str | None:
    """
    Converte o HTML em PDF usando xhtml2pdf (pure-Python).
    - Suporta nosso CSS inline e imagens PNG.
    - Não depende do wkhtmltopdf.
    """
    pdf_path = html_path.replace(".html", ".pdf")
    base_dir = os.path.dirname(html_path)

    # Resolve caminhos relativos de imagens (ex.: ../charts/casos_30d.png)
    def link_callback(uri, rel):
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        return os.path.abspath(os.path.join(base_dir, uri))

    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        with open(pdf_path, "wb") as out:
            result = pisa.CreatePDF(
                src=html,
                dest=out,
                link_callback=link_callback,
                encoding="utf-8"
            )
        return pdf_path if not result.err else None
    except Exception:
        return None

