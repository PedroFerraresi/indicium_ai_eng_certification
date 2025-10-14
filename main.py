"""
Ponto de entrada simples para rodar a pipeline:
- Carrega .env
- Lê UF_INICIAL
- Executa o grafo do agente
- Imprime os caminhos do relatório gerado (HTML/PDF)
"""

from dotenv import load_dotenv
import os
from src.agents.orchestrator import run_pipeline

if __name__ == "__main__":
    # Carrega variáveis de ambiente do .env
    load_dotenv()

    # UF padrão para o relatório (pode ser alterado no .env)
    uf = os.getenv("UF_INICIAL", "SP")

    # Executa a pipeline completa
    result = run_pipeline(uf)

    # Mostra onde encontrar os artefatos gerados
    print("Relatório HTML:", result.get("html_path"))
    print("Relatório PDF:", result.get("pdf_path") or "PDF não gerado (wkhtmltopdf ausente)")

