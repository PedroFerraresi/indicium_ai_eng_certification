from __future__ import annotations

import json
import os
import time
from typing import Any


def log_event(name: str, payload: dict[str, Any], folder: str = "resources/json") -> str:
    """
    Salva um JSON com timestamp (ms) e nome do evento.
    Retorna o caminho do arquivo salvo.
    """
    os.makedirs(folder, exist_ok=True)
    ts = int(time.time() * 1000)
    path = os.path.join(folder, f"{ts}_{name}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return path
