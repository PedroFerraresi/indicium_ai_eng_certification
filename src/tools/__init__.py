from __future__ import annotations

# Colunas mínimas que selecionamos nos CSVs SRAG (2024/2025)
COLS: list[str] = [
    "DT_SIN_PRI",
    "EVOLUCAO",
    "UTI",
    "VACINA_COV",
    "CLASSI_FIN",
    "SEM_PRI",
    "SG_UF_NOT",
    "SG_UF",
    "SG_UF_RES",
]

# Ordem de preferência para derivar a UF no pré-processamento
UF_CANDIDATES: list[str] = ["SG_UF_NOT", "SG_UF", "SG_UF_RES", "UF"]

# Pasta padrão de arquivos locais (mantido aqui p/ evitar strings “mágicas”)
RAW_FOLDER: str = "data/raw"

__all__ = ["COLS", "UF_CANDIDATES", "RAW_FOLDER"]
