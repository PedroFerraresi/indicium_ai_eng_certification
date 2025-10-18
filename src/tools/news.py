from __future__ import annotations
"""
Busca e resumo de notícias (News Summary Agent):

- search_news(query, num): chama a API do Serper e retorna itens de notícia.
- summarize_news(items, run_id): pede ao OpenAI um parágrafo com 4–6 frases.

Observações:
- Este módulo assume que as variáveis SERPER_API_KEY e OPENAI_API_KEY
  estão definidas no .env (carregado via dotenv).
- Qualquer exceção (rede, quota 429, auth 401...) deve ser tratada pelo
  orquestrador (node_news) — lá usamos try/except e fallback.
- Registramos métrica de uso do LLM (tokens/latência) via audit.log_kv.
"""

import os
import requests
from typing import List, Dict, Optional
from time import perf_counter

from dotenv import load_dotenv
from openai import OpenAI

from src.utils.audit import log_kv

# Carrega variáveis de ambiente do .env (se ainda não carregadas)
load_dotenv()

# Chaves necessárias (podem estar ausentes; o orquestrador trata falhas)
SERPER = os.getenv("SERPER_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Cliente OpenAI (se a chave estiver ausente/ruim, a chamada levantará exceção)
client = OpenAI(api_key=OPENAI_API_KEY)


def search_news(query: str, num: int = 5) -> List[Dict]:
    """
    Consulta a API do Serper (endpoint /news) para trazer as notícias mais recentes.

    Parâmetros:
    - query: string de busca (ex.: "SRAG Brasil").
    - num: quantidade máxima de itens a retornar (default: 5).

    Retorno:
    - Lista de dicts com, tipicamente, "title", "source", "link", "date".
    """
    url = "https://google.serper.dev/news"
    headers = {"X-API-KEY": SERPER or "", "Content-Type": "application/json"}
    payload = {"q": query, "num": num}

    # Se a chave estiver faltando, a API retornará 401; deixamos propagar
    r = requests.post(url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("news", [])[:num]


def summarize_news(items: List[Dict], run_id: Optional[str] = None) -> str:
    """
    Recebe a lista de itens (Serper) e produz um resumo textual com o OpenAI.

    - O resumo deve ter 4–6 frases, citar 2–3 fontes e mencionar cautelas/viés.
    - `run_id` (opcional) é usado apenas para audit (tokens/latência).
    - Se `items` vier vazio, retornamos uma mensagem padrão.

    Obs.: Exceções desta função (auth, quota, rede) devem ser tratadas
    pelo orquestrador, que aplicará fallback para não quebrar o pipeline.
    """
    if not items:
        return "Sem notícias recentes encontradas."

    # Monta bullets curtos para compor o prompt (título + fonte + link)
    bullets = "\n".join(
        f"- {i.get('title')} ({i.get('source')}) – {i.get('link')}" for i in items
    )

    prompt = (
        "Você é um analista epidemiológico. Resuma, em 4–6 frases, "
        "o panorama de SRAG no Brasil com base nas manchetes abaixo. "
        "Inclua cautelas/viés e cite 2–3 fontes por nome.\n"
        f"Manchetes:\n{bullets}"
    )

    # Medimos latência de ponta-a-ponta da chamada ao LLM
    t0 = perf_counter()
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    dt_ms = int((perf_counter() - t0) * 1000)

    # Se a API retornar uso, registramos (sem vazar o prompt completo)
    usage = getattr(resp, "usage", None)
    log_kv(
        run_id or "n/a",
        "llm.openai.usage",
        model="gpt-4o-mini",
        duration_ms=dt_ms,
        prompt_tokens=getattr(usage, "prompt_tokens", None),
        completion_tokens=getattr(usage, "completion_tokens", None),
        total_tokens=getattr(usage, "total_tokens", None),
        prompt={"len": len(prompt), "preview": prompt[:200]},
    )

    return resp.choices[0].message.content.strip()
