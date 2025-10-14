import os
from src.tools.database_orchestrator_sqlite import compute_metrics

def test_metrics_for_env_uf():
    uf = os.getenv("UF_INICIAL","SP")
    m = compute_metrics(uf)
    # se não houver dados no período, pode ser None, então testamos tipos/chaves
    assert set(["increase_rate","mortality_rate","icu_rate","vaccination_rate","series_30d","series_12m"]).issubset(m.keys())
    # DataFrames presentes:
    assert hasattr(m["series_30d"], "shape")
    assert hasattr(m["series_12m"], "shape")
