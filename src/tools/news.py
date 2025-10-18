from __future__ import annotations
"""
News Tool com timeouts/retries/backoff e logs de tentativa.
Versão com import DIRETO da OpenAI (requirements: openai>=1.42,<2).
"""

import os
import time
import json
import random
import requests
from typing import List, Dict, Optional
from time import perf_counter

from dotenv import load_dotenv
from openai import OpenAI, RateLimitError, APIConnectionError, APITimeoutError

from src.utils.audit import log_kv

# Carrega variáveis do .env
load_dotenv()

# Chaves e config
SERPER = os.getenv("SERPER_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "2"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))

# Cliente OpenAI (usa a chave do .env)
client = OpenAI(api_key=OPENAI_API_KEY)


def _sleep_backoff(attempt: int) -> None:
    """Backoff exponencial com jitter leve."""
    base = API_BACKOFF_BASE * (2 ** attempt)
    time.sleep(base + random.uniform(0, 0.25))


def search_news(query: str, num: int = 5, run_id: Optional[str] = None) -> List[Dict]:
    """
    Busca notícias no Serper com timeout e re-tentativas para 429/5xx.
    Fail-fast se a SERPER_API_KEY estiver ausente.
    """
    if not SERPER.strip():
        log_kv(run_id or "n/a", "serper.disabled", reason="missing_api_key")
        return []

    num = max(1, min(int(num), 10))  # cap defensivo

    url = "https://google.serper.dev/news"
    headers = {"X-API-KEY": SERPER, "Content-Type": "application/json"}
    payload = {"q": query, "num": num}

    last_err: Optional[str] = None
    for attempt in range(API_MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=API_TIMEOUT)

            # 429/5xx → tentamos de novo
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"http_status={r.status_code}"
                log_kv(run_id or "n/a", "serper.retry",
                       attempt=attempt, status=r.status_code, query=query)
                if attempt < API_MAX_RETRIES:
                    _sleep_backoff(attempt)
                    continue
                r.raise_for_status()

            r.raise_for_status()
            try:
                data = r.json()
            except json.JSONDecodeError as e:
                last_err = f"json_decode_error: {e}"
                log_kv(run_id or "n/a", "serper.retry.exception",
                       attempt=attempt, error=str(e), query=query)
                if attempt < API_MAX_RETRIES:
                    _sleep_backoff(attempt)
                    continue
                raise
            return data.get("news", [])[:num]

        except requests.RequestException as e:
            last_err = str(e)
            log_kv(run_id or "n/a", "serper.retry.exception",
                   attempt=attempt, error=str(e), query=query)
            if attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            raise

    raise RuntimeError(f"Serper failed after retries: {last_err}")


def summarize_news(items: List[Dict], run_id: Optional[str] = None) -> str:
    """
    Sumariza itens com OpenAI, com timeout e retries em erros transitórios.
    Fail-fast se a OPENAI_API_KEY estiver ausente.
    """
    if not items:
        return "Sem notícias recentes encontradas."

    if not OPENAI_API_KEY.strip():
        log_kv(run_id or "n/a", "openai.disabled", reason="missing_api_key")
        return "Resumo de notícias indisponível (OPENAI_API_KEY ausente)."

    bullets = "\n".join(
        f"- {i.get('title')} ({i.get('source')}) – {i.get('link')}" for i in items
    )
    prompt = (
        "Você é um analista epidemiológico. Resuma, em 4–6 frases, "
        "o panorama de SRAG no Brasil com base nas manchetes abaixo. "
        "Inclua cautelas/viés e cite 2–3 fontes por nome.\n"
        f"Manchetes:\n{bullets}"
    )

    last_err: Optional[str] = None
    for attempt in range(API_MAX_RETRIES + 1):
        try:
            t0 = perf_counter()
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                timeout=API_TIMEOUT,  # suportado no client v1.x
            )
            dt_ms = int((perf_counter() - t0) * 1000)

            usage = getattr(resp, "usage", None)
            log_kv(
                run_id or "n/a",
                "llm.openai.usage",
                model=OPENAI_MODEL,
                duration_ms=dt_ms,
                prompt_tokens=getattr(usage, "prompt_tokens", None),
                completion_tokens=getattr(usage, "completion_tokens", None),
                total_tokens=getattr(usage, "total_tokens", None),
                prompt={"len": len(prompt), "preview": prompt[:200]},
            )
            return resp.choices[0].message.content.strip()

        # Retries explícitos para erros transitórios
        except (RateLimitError, APIConnectionError, APITimeoutError) as e:
            last_err = str(e)
            log_kv(run_id or "n/a", "openai.retry",
                   attempt=attempt, retryable=True, error=last_err)
            if attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            raise

        # Fallback genérico (qualquer outro erro não-óbvio)
        except Exception as e:
            last_err = str(e)
            # tenta reconhecer erros “retryable” por mensagem
            retryable = any(x in last_err for x in ("429", "RateLimit", "timeout", "Connection"))
            log_kv(run_id or "n/a", "openai.retry",
                   attempt=attempt, retryable=retryable, error=last_err)
            if retryable and attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            raise

    raise RuntimeError(f"OpenAI summarize failed after retries: {last_err}")
