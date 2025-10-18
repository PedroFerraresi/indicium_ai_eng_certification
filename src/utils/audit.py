# src/utils/audit.py
from __future__ import annotations

import os
import json
import time
import uuid
import hashlib
import traceback
from contextlib import contextmanager
import datetime
from typing import Any, Dict, Optional

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


# === Config via .env ===
LOG_DIR = os.getenv("LOG_DIR", "resources/json")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "events.jsonl"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # INFO | DEBUG
SANITIZE = os.getenv("LOG_SANITIZE", "1") == "1"  # 1 = mascara prompts/segredos

# Garante diretório
os.makedirs(LOG_DIR, exist_ok=True)


def _now() -> str:
    """Timestamp ISO8601 com milissegundos (UTC)."""
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="milliseconds") + "Z"


def _hash(text: str) -> str:
    """Hash curto para identificar conteúdo sem expor texto completo."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _truncate(s: str | None, max_len: int = 1000) -> str | None:
    """Evita logs gigantes; mantém prévia útil."""
    if s is None:
        return None
    return s if len(s) <= max_len else s[:max_len] + f"... [truncated:{len(s)}]"


# ---------- Sanitização (recursiva) ----------
_SENSITIVE_KEYS = {
    # comuns
    "api_key",
    "apikey",
    "key",
    "authorization",
    "bearer",
    "token",
    "access_token",
    "secret",
    "password",
    # nomes específicos do projeto
    "openai_api_key",
    "serper_api_key",
}


def _sanitize_value(v: Any, key_hint: Optional[str] = None) -> Any:
    """
    Sanitiza recursivamente valores de dicionários/listas:
    - Se chave é sensível (em qualquer nível) -> "[REDACTED]"
    - `prompt` string -> {sha, preview}
    - `messages` (lista/objeto) -> {sha, count}
    - Strings muito longas são truncadas para evitar vazamento acidental
    """
    # Dicionários
    if isinstance(v, dict):
        out: Dict[str, Any] = {}
        for k, vv in v.items():
            kl = str(k).lower()

            # Campos sensíveis por nome (em qualquer nível)
            if (
                kl in _SENSITIVE_KEYS
                or kl.endswith("api_key")
                or kl.endswith("access_token")
            ):
                out[k] = "[REDACTED]"
                continue

            # Prompt: substitui por hash + preview (independente do nível)
            if kl == "prompt":
                if isinstance(vv, str):
                    out[k] = {"sha": _hash(vv), "preview": _truncate(vv, 300)}
                else:
                    # prompt não-string -> sanitiza recursivamente
                    out[k] = _sanitize_value(vv, kl)
                continue

            # Messages (padrão OpenAI): não logar conteúdo bruto
            if kl == "messages":
                try:
                    txt = json.dumps(vv, ensure_ascii=False)
                except Exception:
                    txt = str(vv)
                count = len(vv) if isinstance(vv, list) else None
                out[k] = {"sha": _hash(txt), "count": count}
                continue

            # Caso geral: segue recursivamente
            out[k] = _sanitize_value(vv, kl)
        return out

    # Listas / tuplas
    if isinstance(v, (list, tuple)):
        return [_sanitize_value(x, key_hint) for x in v]

    # Strings: truncamento defensivo (ex.: stack traces gigantes)
    if isinstance(v, str):
        return _truncate(v, 1000)

    # Demais tipos permanecem
    return v


def sanitize_payload(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove/mascara campos sensíveis sem perder rastreabilidade.
    - Aplica-se recursivamente em qualquer nível.
    - Chaves típicas (api_key, token, authorization, etc.) viram "[REDACTED]".
    - 'prompt' vira {sha, preview}, e 'messages' vira {sha, count}.
    """
    if not SANITIZE:
        return d
    try:
        return _sanitize_value(d)
    except Exception:
        # Qualquer erro de sanitização não deve quebrar o log.
        return d


def write_event(event: str, level: str = "INFO", **payload):
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
            ok=True,
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


def log_kv(run_id: str, event: str, **kv):
    """Atalho para eventos simples (chave-valor)."""
    write_event(event, run_id=run_id, **kv)
