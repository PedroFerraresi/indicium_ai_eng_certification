from __future__ import annotations

import json
import os
import random
import time
from time import perf_counter

from dotenv import load_dotenv
from openai import APIConnectionError, APITimeoutError, OpenAI, RateLimitError
import requests

from src.utils.audit import log_kv

# Carrega variáveis do .env
load_dotenv()

# --- Config / credenciais ---
SERPER = os.getenv("SERPER_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "2"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))

# Mensagens amigáveis (fallbacks)
NO_ITEMS_MSG = "Sem notícias recentes encontradas."
NO_OPENAI_MSG = "Resumo de notícias indisponível (OPENAI_API_KEY ausente)."
GENERIC_FAIL_MSG = "Resumo de notícias indisponível no momento."

# Cliente OpenAI (chave via .env)
client = OpenAI(api_key=OPENAI_API_KEY)


def _sleep_backoff(attempt: int) -> None:
    """Backoff exponencial com jitter leve (evita thundering herd)."""
    base = API_BACKOFF_BASE * (2**attempt)
    time.sleep(base + random.uniform(0, 0.25))


def _normalize_items(items: list[dict]) -> list[dict]:
    """Mantém apenas campos úteis e evita None."""
    out: list[dict] = []
    for it in items:
        out.append(
            {
                "title": (it.get("title") or "").strip(),
                "source": (it.get("source") or "").strip(),
                "link": (it.get("link") or "").strip(),
                # mantemos publishedDate se existir (pode ajudar no futuro)
                "date": (it.get("date") or it.get("publishedDate") or "").strip(),
            }
        )
    return out


def search_news(query: str, num: int = 5, run_id: str | None = None) -> list[dict]:
    """
    Busca notícias no Serper com timeout e re-tentativas para 429/5xx.
    - Fail-fast: se SERPER_API_KEY ausente OU query vazia → retorna [].
    - Não levanta exceção: em falhas, loga e retorna [].
    """
    rid = run_id or "n/a"

    # Sanitização defensiva
    q = (query or "").strip()
    if not SERPER.strip():
        log_kv(rid, "serper.disabled", reason="missing_api_key")
        return []
    if not q:
        log_kv(rid, "serper.skip", reason="empty_query")
        return []

    # Limitamos 'num' a [1, 10] por cautela
    num = max(1, min(int(num), 10))

    url = "https://google.serper.dev/news"
    headers = {"X-API-KEY": SERPER, "Content-Type": "application/json"}
    payload = {"q": q, "num": num}

    last_err: str | None = None
    for attempt in range(API_MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=API_TIMEOUT)

            # 429/5xx → tentamos de novo
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"http_status={r.status_code}"
                log_kv(
                    rid, "serper.retry", attempt=attempt, status=r.status_code, query=q
                )
                if attempt < API_MAX_RETRIES:
                    _sleep_backoff(attempt)
                    continue
                # sem mais retries — trata abaixo como erro final

            # 4xx (exceto 429) → não repetir
            if 400 <= r.status_code < 500 and r.status_code != 429:
                log_kv(rid, "serper.client_error", status=r.status_code, query=q)
                return []

            # OK → parseia JSON
            try:
                r.raise_for_status()
                data = r.json()
            except json.JSONDecodeError as e:
                last_err = f"json_decode_error: {e}"
                log_kv(rid, "serper.json_error", attempt=attempt, error=str(e))
                if attempt < API_MAX_RETRIES:
                    _sleep_backoff(attempt)
                    continue
                return []
            except requests.RequestException as e:
                # Aqui só cai se for um 429/5xx e sem retries restantes
                last_err = str(e)
                log_kv(
                    rid, "serper.http_error_final", status=r.status_code, error=str(e)
                )
                return []

            items = data.get("news", [])[:num]
            return _normalize_items(items)

        except requests.RequestException as e:
            # timeouts, DNS, conexão etc.
            last_err = str(e)
            log_kv(
                rid, "serper.retry.exception", attempt=attempt, error=str(e), query=q
            )
            if attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            # falha final
            log_kv(rid, "serper.fail", error=last_err)
            return []

    # Em teoria não chega aqui; apenas por segurança
    log_kv(rid, "serper.fail.unknown", last_error=last_err)
    return []


def summarize_news(items: list[dict], run_id: str | None = None) -> str:
    """
    Sumariza itens com OpenAI, com timeout e retries em erros transitórios.

    Regras de fail-fast/amigáveis:
    - Lista vazia → retorna mensagem padrão sem chamar LLM.
    - Sem OPENAI_API_KEY → mensagem amigável e log.
    - Qualquer falha final → mensagem amigável, sem propagar exceção.
    """
    rid = run_id or "n/a"

    if not items:
        return NO_ITEMS_MSG

    if not OPENAI_API_KEY.strip():
        log_kv(rid, "openai.disabled", reason="missing_api_key")
        return NO_OPENAI_MSG

    # Constrói bullets para o prompt
    bullets = "\n".join(
        f"- {i.get('title')} ({i.get('source')}) – {i.get('link')}" for i in items
    )
    prompt = (
        "Você é um analista epidemiológico. Resuma, em 4–6 frases, "
        "o panorama de SRAG no Brasil com base nas manchetes abaixo. "
        "Inclua cautelas/viés e cite 2–3 fontes por nome.\n"
        f"Manchetes:\n{bullets}"
    )

    last_err: str | None = None
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
                rid,
                "llm.openai.usage",
                model=OPENAI_MODEL,
                duration_ms=dt_ms,
                prompt_tokens=getattr(usage, "prompt_tokens", None),
                completion_tokens=getattr(usage, "completion_tokens", None),
                total_tokens=getattr(usage, "total_tokens", None),
                prompt={"len": len(prompt), "preview": prompt[:200]},
            )
            return resp.choices[0].message.content.strip()

        # Erros transitórios com retries explícitos
        except (RateLimitError, APIConnectionError, APITimeoutError) as e:
            last_err = str(e)
            log_kv(rid, "openai.retry", attempt=attempt, retryable=True, error=last_err)
            if attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            log_kv(rid, "openai.fail", error=last_err)
            return GENERIC_FAIL_MSG

        # Outros erros (tenta detectar por string se é retryable)
        except Exception as e:
            last_err = str(e)
            retryable = any(
                x in last_err for x in ("429", "RateLimit", "timeout", "Connection")
            )
            log_kv(
                rid,
                "openai.retry",
                attempt=attempt,
                retryable=retryable,
                error=last_err,
            )
            if retryable and attempt < API_MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            log_kv(rid, "openai.fail", error=last_err)
            return GENERIC_FAIL_MSG

    # Segurança (não deve chegar aqui)
    log_kv(rid, "openai.fail.unknown", last_error=last_err)
    return GENERIC_FAIL_MSG
