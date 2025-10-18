"""
Teste de INTEGRAÇÃO da auditoria com o pipeline real.

Este teste NÃO cria logs por conta própria: ele verifica o arquivo
'resources/json/events.jsonl' que o pipeline (python main.py) gera.

O que validamos:
- Existe pelo menos um ciclo de execução com eventos essenciais:
  run.start, ingest.end, metrics.end, report.end
- (Opcional) você pode ampliar para verificar 'news.end' e 'charts.end'.

Dica: rode o pipeline antes do teste:
    python main.py
e depois rode:
    pytest -q -m integration
"""

import json
import pathlib
import pytest

pytestmark = pytest.mark.integration  # marca como teste de integração

def test_audit_log_exists_and_has_spans():
    p = pathlib.Path("resources/json/events.jsonl")
    if not p.exists():
        pytest.skip("events.jsonl não encontrado — rode o pipeline antes (python main.py).")

    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert lines, "Log vazio — rode o pipeline para gerar eventos."

    # Vamos agrupar eventos por run_id e checar se ALGUM run_id tem o conjunto completo
    required = {"run.start", "ingest.end", "metrics.end", "report.end"}
    seen_by_run = {}

    # Limitamos a leitura às últimas N linhas para acelerar (ajuste se quiser)
    for ln in lines[-1000:]:
        ev = json.loads(ln)
        rid = ev.get("run_id", "unknown")
        seen_by_run.setdefault(rid, set()).add(ev.get("event"))

    # Procura um run_id que atenda aos requisitos
    ok = any(required.issubset(events) for events in seen_by_run.values())
    if not ok:
        # Ajuda na depuração: mostra os últimos eventos por run_id
        debug = {rid: sorted(list(evts & required)) for rid, evts in seen_by_run.items()}
        pytest.fail(f"Não encontramos um run completo com {required}. Vistos: {debug}")

    # Checagem bônus: pelo menos um run_id deve ter 'charts.end' e 'news.end' também
    bonus_required = {"charts.end", "news.end"}
    bonus_ok = any(bonus_required.issubset(events) for events in seen_by_run.values())
    # Não falhamos se não tiver, mas avisamos para incentivar cobertura completa
    if not bonus_ok:
        pytest.skip("Aviso: não encontramos charts.end/news.end nas últimas execuções (ok, mas verifique o pipeline).")
