import os, pathlib
import pandas as pd
import pytest
from sqlalchemy import create_engine

@pytest.fixture(scope="session")
def eng():
    db = os.getenv("DB_PATH", "data/srag.sqlite")
    assert pathlib.Path(db).exists(), "Banco SQLite não encontrado: rode a ingestão antes dos testes."
    return create_engine(f"sqlite:///{db}", future=True)

def test_tables_exist(eng):
    q = "SELECT name FROM sqlite_master WHERE type='table'"
    names = set(pd.read_sql_query(q, eng)['name'].tolist())
    for t in ["srag_staging", "srag_base", "srag_daily", "srag_monthly"]:
        assert t in names, f"Tabela {t} não foi criada."

def test_daily_monthly_have_rows(eng):
    n_daily  = pd.read_sql_query("SELECT COUNT(*) n FROM srag_daily", eng)['n'][0]
    n_month  = pd.read_sql_query("SELECT COUNT(*) n FROM srag_monthly", eng)['n'][0]
    assert n_daily  > 0, "srag_daily vazia."
    assert n_month  > 0, "srag_monthly vazia."
