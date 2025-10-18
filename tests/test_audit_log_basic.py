"""
Teste UNITÁRIO do módulo de auditoria (src/utils/audit.py).

O objetivo aqui é validar, de forma isolada e sem depender do pipeline:
- a criação de spans (eventos *.start, *.end, *.error),
- a correlação por run_id/span_id,
- a escrita em JSONL no arquivo configurado via ambiente,
- e um evento simples (log_kv).

Como o audit.py lê LOG_DIR/LOG_FILE AO IMPORTAR o módulo, usamos
monkeypatch para setar as variáveis de ambiente ANTES de importar.
Assim o teste é auto-contido: escreve em uma pasta temporária (tmp_path).
"""

import json
import pathlib

def test_audit_span_and_events(tmp_path, monkeypatch):
    # 1) Redireciona o log para uma pasta/arquivo temporários
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "events.jsonl"

    monkeypatch.setenv("LOG_DIR", str(log_dir))
    monkeypatch.setenv("LOG_FILE", str(log_file))
    monkeypatch.setenv("LOG_SANITIZE", "1")  # mantém sanitização ligada

    # 2) Importa o módulo DEPOIS de configurar o ambiente
    from src.utils import audit

    run_id = audit.new_run_id()

    # 3) Abre um span de SUCESSO e registra um evento chave-valor
    with audit.audit_span("unit_test_success", run_id, node="test-node", foo=123):
        audit.log_kv(run_id, "kv.event", bar="ok")

    # 4) Abre um span que LEVANTA ERRO (para validar *.error)
    try:
        with audit.audit_span("unit_test_error", run_id, node="test-node"):
            raise ValueError("boom")
    except ValueError:
        pass  # esperado

    # 5) Lê o arquivo JSONL e valida estrutura e eventos esperados
    content = log_file.read_text(encoding="utf-8").strip()
    assert content, "Log JSONL não foi escrito."

    lines = content.splitlines()
    assert len(lines) >= 3, "Esperávamos pelo menos start/end/kv ou start/error."

    events = [json.loads(l) for l in lines]

    # Deve conter os eventos essenciais gerados acima
    kinds = {e["event"] for e in events}
    assert "unit_test_success.start" in kinds
    assert "unit_test_success.end" in kinds
    assert "unit_test_error.start" in kinds
    assert "unit_test_error.error" in kinds
    assert "kv.event" in kinds

    # Campos mínimos em qualquer registro
    sample = events[-1]
    for k in ("ts", "event", "run_id"):
        assert k in sample

    # Para spans de fim/erro, esperamos duration_ms
    end_events = [e for e in events if e["event"].endswith(".end")]
    error_events = [e for e in events if e["event"].endswith(".error")]
    assert all("duration_ms" in e for e in end_events), "Span .end sem duration_ms"
    assert all("duration_ms" in e for e in error_events), "Span .error sem duration_ms"
