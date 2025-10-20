import os
import pathlib
import re

from dotenv import find_dotenv, load_dotenv
import pytest

# Quando RUN_LIVE_API_TESTS=1, exigimos chaves reais e não vazias.
RUN_LIVE = os.getenv("RUN_LIVE_API_TESTS", "0") == "1"


@pytest.fixture(scope="session", autouse=True)
def _load_env() -> None:
    """
    Carrega o .env uma única vez (sem sobrescrever variáveis já definidas).
    No CI, o workflow cria um .env mínimo – então mantemos a asserção.
    """
    env_file = find_dotenv(filename=".env", usecwd=True)
    assert env_file, "Arquivo .env não encontrado no diretório do projeto."
    load_dotenv(env_file, override=False)


# ------------------------------
# Presenças básicas no .env
# (variáveis core: sempre não vazias)
# ------------------------------
@pytest.mark.parametrize(
    "var_name",
    [
        "DB_PATH",
        "UF_INICIAL",
        "INGEST_MODE",
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
# Chaves de APIs externas
# (flexíveis no CI/offline; estritas em modo live)
# ------------------------------
@pytest.mark.parametrize("var_name", ["OPENAI_API_KEY", "SERPER_API_KEY"])
def test_external_keys_present_or_required_when_live(var_name: str):
    """
    - OFFLINE/CI (RUN_LIVE_API_TESTS != 1): a variável deve EXISTIR,
      mas pode estar vazia ("").
    - LIVE (RUN_LIVE_API_TESTS = 1): a variável deve estar NÃO VAZIA.
    """
    val = os.getenv(var_name)
    assert val is not None, f"Variável {var_name} ausente no ambiente/.env"

    if RUN_LIVE:
        assert val.strip() != "", f"{var_name} vazia com RUN_LIVE_API_TESTS=1"


def test_api_keys_look_sane():
    """
    Apenas em modo LIVE validamos formato mínimo das chaves.
    No CI/offline não exigimos conteúdo real.
    """
    openai_key = os.getenv("OPENAI_API_KEY", "")
    serper_key = os.getenv("SERPER_API_KEY", "")

    if not RUN_LIVE:
        # Só garantimos que existem (podem estar vazias para evitar chamadas externas no CI)
        assert openai_key is not None and serper_key is not None
        return

    assert (
        len(openai_key.strip()) >= 15 and "coloque" not in openai_key.lower()
    ), "OPENAI_API_KEY parece placeholder."
    assert (
        len(serper_key.strip()) >= 10 and "coloque" not in serper_key.lower()
    ), "SERPER_API_KEY parece placeholder."


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
    from src.tools.db_orchestrator import DB_PATH, INGEST_MODE, SRAG_URLS

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
