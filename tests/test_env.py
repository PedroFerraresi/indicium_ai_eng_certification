import os
import pathlib
import re

from dotenv import find_dotenv, load_dotenv
import pytest


@pytest.fixture(scope="session", autouse=True)
def _load_env() -> None:
    """
    Carrega o .env uma única vez para toda a suíte sem sobrescrever
    variáveis já existentes no ambiente.
    """
    env_file = find_dotenv(filename=".env", usecwd=True)
    assert env_file, "Arquivo .env não encontrado no diretório do projeto."
    load_dotenv(env_file, override=False)


# ------------------------------
# Presenças básicas no .env
# ------------------------------
@pytest.mark.parametrize(
    "var_name",
    [
        "DB_PATH",
        "UF_INICIAL",
        "INGEST_MODE",
        "OPENAI_API_KEY",
        "SERPER_API_KEY",
        "NEWS_QUERY",
    ],
)
def test_env_vars_presence(var_name: str):
    val = os.getenv(var_name)
    assert (
        val is not None and val.strip() != ""
    ), f"Variável {var_name} ausente ou vazia no .env"


def test_db_path_parent_dir_exists():
    db_path = os.getenv("DB_PATH")
    parent = pathlib.Path(db_path).parent
    assert parent.exists(), f"Diretório pai de DB_PATH não existe: {parent}"


def test_uf_inicial_format():
    uf = os.getenv("UF_INICIAL", "")
    assert (
        re.fullmatch(r"[A-Z]{2}", uf) is not None
    ), f"UF_INICIAL inválido: '{uf}'. Use duas letras maiúsculas (ex.: 'SP')."


# ------------------------------
# Chaves (checagens leves)
# ------------------------------
def test_api_keys_look_sane():
    openai_key = os.getenv("OPENAI_API_KEY", "")
    serper_key = os.getenv("SERPER_API_KEY", "")
    assert (
        len(openai_key) >= 15 and "coloque" not in openai_key.lower()
    ), "OPENAI_API_KEY parece placeholder."
    assert (
        len(serper_key) >= 10 and "coloque" not in serper_key.lower()
    ), "SERPER_API_KEY parece placeholder."


def test_news_query_nonempty():
    assert os.getenv("NEWS_QUERY", "").strip() != "", "NEWS_QUERY não deve estar vazia."


# ------------------------------
# INGEST_MODE e SRAG_URLS
# ------------------------------
def test_ingest_mode_allowed_values():
    mode = os.getenv("INGEST_MODE", "").lower()
    assert mode in {
        "auto",
        "local",
        "remote",
    }, f"INGEST_MODE inválido: '{mode}'. Use: auto | local | remote."


def _split_env_urls(env_val: str) -> list[str]:
    return [u.strip() for u in env_val.split(",") if u.strip()]


def test_srag_urls_required_when_remote():
    """
    Quando INGEST_MODE=remote, SRAG_URLS deve existir e conter >=1 URL.
    (Para auto/local, não exigimos.)
    """
    mode = os.getenv("INGEST_MODE", "").lower()
    urls_env = os.getenv("SRAG_URLS", "")
    urls = _split_env_urls(urls_env)

    if mode == "remote":
        assert (
            urls
        ), "INGEST_MODE=remote exige SRAG_URLS no .env (1+ URLs separadas por vírgula)."


def test_orchestrator_parses_urls_like_env():
    """
    Garante que o orquestrador lê/parsa SRAG_URLS conforme o .env.
    Também valida formato básico das URLs (http/https, .csv ou .zip).
    """
    # importa após load_dotenv
    from src.tools.database_orchestrator_sqlite import DB_PATH, INGEST_MODE, SRAG_URLS

    # 1) INGEST_MODE e DB_PATH sincronizados com o .env
    assert INGEST_MODE == os.getenv("INGEST_MODE", "").lower()
    assert DB_PATH == os.getenv("DB_PATH")

    # 2) SRAG_URLS é lista (pode estar vazia se não for remoto)
    assert isinstance(SRAG_URLS, list), "SRAG_URLS no orquestrador deve ser uma lista."

    # 3) Se modo remoto, lista deve ter 1+ itens válidos
    if INGEST_MODE == "remote":
        assert len(SRAG_URLS) >= 1, "Modo remoto requer pelo menos 1 URL."
        url_re = re.compile(r"^https?://.+\.(csv|zip)$", re.IGNORECASE)
        for u in SRAG_URLS:
            assert url_re.match(u), f"URL inválida em SRAG_URLS: {u}"
            assert u == u.strip(), f"URL com espaços extras: '{u}'"
