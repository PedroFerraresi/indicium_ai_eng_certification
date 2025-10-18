from __future__ import annotations
"""
Utilitários de auditoria/observabilidade.

- write_event(): grava um evento JSONL (1 linha por evento).
- audit_span(): contexto que loga início/fim/erro + duração.
- log_kv(): atalho para eventos simples (chave-valor).
- new_run_id(): gera um id único por execução.

Configuração por .env (com defaults seguros):
- LOG_DIR       (default: resources/json)
- LOG_FILE      (default: <LOG_DIR>/events.jsonl)
- LOG_LEVEL     (default: INFO)  [INFO | DEBUG]
- LOG_SANITIZE  (default: 1)     [1=liga sanitização, 0=desliga]
"""

import os
import json
import time
import uuid
import hashlib
import traceback
from contextlib import contextmanager
import datetime
from typing import Any, Dict, Optional  # <- para Optional[str]

# === Config via .env ===
LOG_DIR = os.getenv("LOG_DIR", "resources/json")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "events.jsonl"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # INFO | DEBUG
SANITIZE = os.getenv("LOG_SANITIZE", "1") == "1"     # 1 = mascara prompts/segredos

# Garante diretório
os.makedirs(LOG_DIR, exist_ok=True)


def _now() -> str:
    """Timestamp ISO8601 com milissegundos (UTC)."""
    # Usa horário consciente de timezone (UTC) e normaliza o sufixo para 'Z'
    return datetime.datetime.now(datetime.UTC)\
        .isoformat(timespec="milliseconds")\
        .replace("+00:00", "Z")


def _hash(text: str) -> str:
    """Hash curto para identificar conteúdo sem expor texto completo."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _truncate(s: Optional[str], max_len: int = 1000) -> Optional[str]:
    """Evita logs gigantes; mantém prévia útil."""
    if s is None:
        return None
    return s if len(s) <= max_len else s[:max_len] + f"... [truncated:{len(s)}]"


def sanitize_payload(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove/mascara campos sensíveis sem perder rastreabilidade.
    - Chaves de API são removidas.
    - Prompts/mensagens têm hash + prévia curta.
    """
    if not SANITIZE:
        return d
    redacted: Dict[str, Any] = {}
    SENSITIVE_KEYS = {
        "api_key", "Authorization", "authorization", "token", "password",
        "OPENAI_API_KEY", "SERPER_API_KEY"
    }
    for k, v in d.items():
        if k in SENSITIVE_KEYS:
            redacted[k] = "[REDACTED]"
        elif k == "prompt" and isinstance(v, str):
            redacted[k] = {"sha": _hash(v), "preview": _truncate(v, 300)}
        elif k == "messages":
            try:
                txt = json.dumps(v, ensure_ascii=False)
            except Exception:
                txt = str(v)
            redacted[k] = {"sha": _hash(txt), "preview": _truncate(txt, 300)}
        else:
            redacted[k] = v
    return redacted


def write_event(event: str, level: str = "INFO", **payload) -> None:
    """
    Grava um evento estruturado (uma linha JSON).
    Use nível DEBUG apenas quando LOG_LEVEL=DEBUG.
    """
    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return
    rec = {
        "ts": _now(),
        "level": level,
        "event": event,
        **sanitize_payload(payload),
    }
    # Escrita simples em append; flush imediato para não perder eventos em crash.
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()


def new_run_id() -> str:
    """Id único por execução (usado para correlacionar spans)."""
    return str(uuid.uuid4())


@contextmanager
def audit_span(event: str, run_id: str, node: Optional[str] = None, **ctx):
    """
    Context manager para instrumentar um “span”.
    Exemplo:
        with audit_span("metrics", run_id, node="metrics"):
            ... sua lógica ...
    Registra: <event>.start / <event>.end / <event>.error
    """
    span_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    write_event(f"{event}.start", run_id=run_id, span_id=span_id, node=node, **ctx)
    try:
        yield {"run_id": run_id, "span_id": span_id}
        dur = int((time.perf_counter() - t0) * 1000)
        write_event(
            f"{event}.end",
            run_id=run_id,
            span_id=span_id,
            node=node,
            duration_ms=dur,
            ok=True
        )
    except Exception as e:
        dur = int((time.perf_counter() - t0) * 1000)
        write_event(
            f"{event}.error",
            level="ERROR",
            run_id=run_id,
            span_id=span_id,
            node=node,
            duration_ms=dur,
            ok=False,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        raise


def log_kv(run_id: str, event: str, **kv) -> None:
    """Atalho para eventos simples (chave-valor)."""
    write_event(event, run_id=run_id, **kv)
