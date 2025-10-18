from __future__ import annotations

import os
import argparse

"""
Utilitários de CLI para o projeto.

- parse_args(): constrói o ArgumentParser e lê flags da linha de comando,
  usando valores do ambiente (ex.: carregados do .env) como defaults.

Obs.: NÃO chama load_dotenv aqui; o main.py deve chamar antes para
popular os defaults corretamente.
"""


def parse_args() -> argparse.Namespace:
    """Retorna os argumentos de execução do pipeline."""
    parser = argparse.ArgumentParser(description="Executa o pipeline SRAG.")
    parser.add_argument(
        "--uf",
        default=os.getenv("UF_INICIAL", "SP"),
        help="Sigla da UF (ex.: SP, RJ). Padrão: UF_INICIAL no .env ou 'SP'.",
    )
    parser.add_argument(
        "--ingest-mode",
        choices=["auto", "local", "remote"],
        default=os.getenv("INGEST_MODE", "auto"),
        help="Modo de ingestão: auto | local | remote. Padrão: INGEST_MODE no .env.",
    )
    parser.add_argument(
        "--news-query",
        default=os.getenv("NEWS_QUERY", "SRAG Brasil"),
        help="Consulta para busca de notícias. Padrão: NEWS_QUERY do .env.",
    )
    parser.add_argument(
        "--no-news",
        action="store_true",
        help="Desativa a etapa de notícias (ignora Serper/OpenAI).",
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="Desativa a geração de PDF (gera apenas HTML).",
    )
    return parser.parse_args()
