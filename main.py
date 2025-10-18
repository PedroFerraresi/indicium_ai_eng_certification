"""
Ponto de entrada do pipeline.

Recursos:
- Carrega variáveis do .env
- Oferece CLI (em src/utils/cli.py) para sobrescrever parâmetros
- Executa o grafo e imprime caminhos dos artefatos

Exemplos:
  python main.py
  python main.py --uf RJ
  python main.py --ingest-mode remote --news-query "SRAG Brasil" --no-news --no-pdf
"""

from __future__ import annotations

import os
import sys
from dotenv import load_dotenv

from src.utils.cli import parse_args
from src.agents.orchestrator import run_pipeline


def main() -> int:
    # 1) Carrega .env primeiro (para que parse_args use esses defaults)
    load_dotenv()

    # 2) Lê flags (sobrescrevem os valores do .env para esta execução)
    args = parse_args()

    # 3) Propaga flags para o ambiente (consumido pelos nós do pipeline)
    os.environ["INGEST_MODE"] = args.ingest_mode
    os.environ["NEWS_QUERY"] = args.news_query

    if args.no_news:
        os.environ["DISABLE_NEWS"] = "1"
    if args.no_pdf:
        os.environ["DISABLE_PDF"] = "1"

    # 4) Executa pipeline
    try:
        result = run_pipeline(args.uf)
    except Exception as e:
        print(f"[ERRO] Falha ao executar pipeline: {e}", file=sys.stderr)
        return 1

    # 5) Exibe artefatos gerados
    html = result.get("html_path")
    pdf = result.get("pdf_path")
    print("Relatório HTML:", html or "não gerado")
    print("Relatório PDF :", pdf or "não gerado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
