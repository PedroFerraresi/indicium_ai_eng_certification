from __future__ import annotations
"""
Ferramenta de notícias:
- Faz busca de notícias recentes usando a API do Serper (Google News-like).
- Usa um LLM (OpenAI) para resumir as manchetes em um parágrafo objetivo,
  citando 2–3 fontes explicitamente (transparência/explicabilidade).
"""

import os, requests
from typing import List, Dict
from dotenv import load_dotenv
from openai import OpenAI

# Carrega variáveis (OPENAI_API_KEY, SERPER_API_KEY)
load_dotenv()
SERPER = os.getenv("SERPER_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Cliente OpenAI para sumarização
client = OpenAI(api_key=OPENAI_API_KEY)


def search_news(query: str, num: int = 5) -> List[Dict]:
    """
    Consulta a API do Serper para retornar 'num' notícias recentes
    relacionadas ao termo 'query' (ex.: 'SRAG Brasil').
    Retorna lista de dicts com campos como 'title', 'source', 'link'.
    """
    url = "https://google.serper.dev/news"
    headers = {"X-API-KEY": SERPER, "Content-Type": "application/json"}
    payload = {"q": query, "num": num}

    r = requests.post(url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Limitamos a 'num' itens e devolvemos estrutura simples
    return data.get("news", [])[:num]


def summarize_news(items: List[Dict]) -> str:
    """
    Recebe uma lista de notícias (title/source/link) e pede ao LLM um resumo:
    - 4 a 6 frases
    - mencionar 2–3 fontes por nome
    - incluir cautelas/viés, quando aplicável
    Em caso de lista vazia, retorna mensagem padrão.
    """
    if not items:
        return "Sem notícias recentes encontradas."

    # Monta um 'bullet list' textual para o prompt
    bullets = "\n".join([f"- {i.get('title')} ({i.get('source')}) – {i.get('link')}" for i in items])

    prompt = f"""Você é um analista epidemiológico. Resuma, em 4–6 frases, o panorama SRAG no Brasil com base nas manchetes abaixo.
Inclua cautelas/viés e cite 2–3 fontes por nome. Manchetes:
{bullets}"""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",  # modelo leve com bom custo/latência
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3  # baixa aleatoriedade para outputs mais estáveis
    )
    return resp.choices[0].message.content.strip()
