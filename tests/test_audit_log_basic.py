import importlib
import json
import pathlib

"""
Teste UNITÁRIO do módulo de auditoria (src/utils/audit.py).

Valida:
- spans *.start / *.end / *.error correlacionados por run_id/span_id;
- escrita em JSONL no arquivo configurado via ambiente;
- evento simples via log_kv.

IMPORTANTE: audit.py lê LOG_DIR/LOG_FILE AO IMPORTAR o módulo.
Por isso, configuramos o ambiente e RECARREGAMOS o módulo (importlib.reload)
para garantir que ele capte os paths temporários do teste.
"""


def test_audit_span_and_events(tmp_path, monkeypatch):
    # 1) Redireciona log para uma pasta/arquivo temporários
    log_dir = tmp_path / "logs"
    log_file = log_dir / "events.jsonl"
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    monkeypatch.setenv("LOG_FILE", str(log_file))
    monkeypatch.setenv("LOG_SANITIZE", "1")  # mantém sanitização ligada

    # 2) Recarrega o módulo DEPOIS de configurar o ambiente,
    #    pois ele lê LOG_DIR/LOG_FILE ao importar
    import src.utils.audit as audit

    audit = importlib.reload(audit)

    run_id = audit.new_run_id()

    # 3) Span de sucesso + evento chave-valor
    with audit.audit_span("unit_test_success", run_id, node="test-node", foo=123):
        audit.log_kv(run_id, "kv.event", bar="ok")

    # 4) Span que levanta erro (para registrar *.error)
    try:
        with audit.audit_span("unit_test_error", run_id, node="test-node"):
            raise ValueError("boom")
    except ValueError:
        pass  # esperado

    # 5) Lê o arquivo JSONL que o módulo realmente usa
    p = pathlib.Path(audit.LOG_FILE)
    assert p.exists(), f"Log JSONL não foi escrito em {p}."
    content = p.read_text(encoding="utf-8").strip()
    assert content, "Log JSONL está vazio."

    lines = content.splitlines()
    assert len(lines) >= 3, "Esperávamos pelo menos start/end/kv ou start/error."

    events = [json.loads(line) for line in lines]
    kinds = {e["event"] for e in events}

    # Eventos essenciais gerados acima
    assert "unit_test_success.start" in kinds
    assert "unit_test_success.end" in kinds
    assert "unit_test_error.start" in kinds
    assert "unit_test_error.error" in kinds
    assert "kv.event" in kinds

    # Campos mínimos
    sample = events[-1]
    for k in ("ts", "event", "run_id"):
        assert k in sample

    # duration_ms em .end e .error
    end_events = [e for e in events if e["event"].endswith(".end")]
    error_events = [e for e in events if e["event"].endswith(".error")]
    assert all("duration_ms" in e for e in end_events), "Span .end sem duration_ms"
    assert all("duration_ms" in e for e in error_events), "Span .error sem duration_ms"
