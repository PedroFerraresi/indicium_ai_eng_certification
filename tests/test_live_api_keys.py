import json
import os

from dotenv import find_dotenv, load_dotenv
import pytest
import requests

# Carrega .env uma vez
load_dotenv(find_dotenv(usecwd=True), override=False)

RUN_LIVE = os.getenv("RUN_LIVE_API_TESTS", "0") == "1"

pytestmark = pytest.mark.integration  # marca todos como "integration"


def _skip_if_not_live():
    if not RUN_LIVE:
        pytest.skip("Defina RUN_LIVE_API_TESTS=1 para rodar testes live (externos).")


# ---------------------------
# OpenAI
# ---------------------------
def test_openai_key_live_chat_completion():
    _skip_if_not_live()

    key = os.getenv("OPENAI_API_KEY", "")
    assert key.strip(), "OPENAI_API_KEY ausente/vazia no .env"

    try:
        from openai import OpenAI, RateLimitError  # lib oficial 1.x
    except Exception as e:
        pytest.skip(f"Pacote openai não disponível: {e}")

    client = OpenAI(api_key=key)
    model = os.getenv("OPENAI_TEST_MODEL", "gpt-4o-mini")
    alt_model = os.getenv("OPENAI_ALT_TEST_MODEL", "gpt-4o-mini")  # pode repetir

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "pong?"}],
            max_tokens=1,
            temperature=0,
        )
        assert resp.choices and resp.choices[0].message.content is not None
    except RateLimitError as e:
        pytest.xfail(f"OPENAI_API_KEY válida, mas sem cota no momento: {e}")
    except Exception as e:
        msg = str(e).lower()
        # 401/unauthorized/invalid key
        if "401" in msg or "unauthorized" in msg or "invalid" in msg:
            pytest.fail(f"OPENAI_API_KEY inválida/sem permissão: {e}")
        # model not found → tenta fallback uma vez
        if "model" in msg and ("not found" in msg or "does not exist" in msg):
            resp = client.chat.completions.create(
                model=alt_model,
                messages=[{"role": "user", "content": "pong?"}],
                max_tokens=1,
                temperature=0,
            )
            assert resp.choices and resp.choices[0].message.content is not None
        else:
            # rede/ssl/etc.: trate como xfail para não travar o dev
            pytest.xfail(f"Falha externa ao validar OpenAI (rede/serviço): {e}")


# ---------------------------
# Serper (Google News-like)
# ---------------------------
def test_serper_key_live_news():
    _skip_if_not_live()

    key = os.getenv("SERPER_API_KEY", "")
    assert key.strip(), "SERPER_API_KEY ausente/vazia no .env"

    url = "https://google.serper.dev/news"
    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    payload = {"q": "SRAG Brasil", "num": 1}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
    except Exception as e:
        pytest.xfail(f"Falha de rede ao chamar Serper: {e}")

    if r.status_code == 401:
        pytest.fail("SERPER_API_KEY inválida (HTTP 401).")
    if r.status_code == 429:
        pytest.xfail("SERPER_API_KEY válida, mas sem cota (HTTP 429).")

    r.raise_for_status()
    data = r.json()
    # resposta deve conter lista 'news' (pode estar vazia dependendo da query)
    assert (
        isinstance(data, dict) and "news" in data
    ), f"Resposta Serper inesperada: {json.dumps(data)[:300]}"

    # valida formato básico de um item (quando houver)
    if data["news"]:
        item = data["news"][0]
        assert (
            "title" in item and "source" in item
        ), "Item de notícia sem campos esperados ('title'/'source')."
