# tests/test_report_contract.py
import pathlib
import re
from src.reports.renderer import render_html


def _ensure_report():
    """
    Garante que resources/reports/relatorio.html exista.
    Se não existir, gera um HTML mínimo chamando render_html diretamente.
    """
    p = pathlib.Path("resources/reports/relatorio.html")
    if p.exists():
        return p

    ctx = {
        "uf": "SP",
        "increase_rate": 0.12,
        "mortality_rate": 0.034,
        "icu_rate": 0.18,
        "vaccination_rate": 0.77,
        # use caminhos RELATIVOS; o orquestrador normalmente coloca ../charts/...
        "chart_30d": "charts/casos_30d.png",
        "chart_12m": "charts/casos_12m.png",
        "news_summary": "Resumo fake para testes.",
        "now": "01/01/2025 00:00",
    }
    out = render_html(ctx)
    return pathlib.Path(out)


def test_report_contract_exists_and_has_sections():
    """
    Contrato do HTML:
    - arquivo existe
    - KPIs com data-testids
    - rótulos/seções exigidos (tolerando pequenas variações de texto)
    """
    p = _ensure_report()
    assert p.exists(), "Falha ao gerar/achar o relatório HTML."
    html = p.read_text(encoding="utf-8")
    html_lc = html.lower()

    # KPIs via data-testid (adicionados no template)
    must_testids = [
        'data-testid="kpi-increase"',
        'data-testid="kpi-mortality"',
        'data-testid="kpi-icu"',
        'data-testid="kpi-vaccination"',
    ]
    missing_ids = [t for t in must_testids if t not in html]
    assert not missing_ids, f"KPI(s) ausente(s) no HTML: {missing_ids}"

    # Aceita sinônimos para compatibilizar o template atual
    # Se quiser ser estrito, deixe só a 1ª opção de cada lista.
    label_variants = {
        "taxa de aumento": ["taxa de aumento", "variação de casos"],
        "taxa de mortalidade": ["taxa de mortalidade"],
        "taxa de ocupação de uti": ["taxa de ocupação de uti", "taxa de uti"],
        "taxa de vacinação": ["taxa de vacinação"],
    }

    missing_labels = []
    for canonical, options in label_variants.items():
        if not any(opt in html_lc for opt in options):
            missing_labels.append(canonical)
    assert not missing_labels, f"Rótulos faltando no relatório: {missing_labels}"

    # Seções principais (com o em dash do template)
    must_sections = [
        "casos — últimos 30 dias",
        "casos — últimos 12 meses",
        "contexto de notícias",
        "relatório srag — uf sp",
    ]
    missing_sections = [s for s in must_sections if s not in html_lc]
    assert not missing_sections, f"Seções faltando no relatório: {missing_sections}"


def test_report_contract_image_paths_are_relative():
    """
    Caminhos dos gráficos devem ser RELATIVOS.
    Aceita:
      - charts/casos_30d.png
      - ../charts/casos_30d.png
    (idem para 12m)
    E aceita aspas simples OU duplas no atributo src.
    """
    html = pathlib.Path(r"resources\reports\relatorio.html").read_text(encoding="utf-8")

    # Expressões mais tolerantes: aspas simples/duplas e charts/ ou ../charts/
    pat_30d = r'src=["\'](?:\.\./)?charts/casos_30d\.png["\']'
    pat_12m = r'src=["\'](?:\.\./)?charts/casos_12m\.png["\']'

    assert re.search(pat_30d, html), "Gráfico 30d não embutido como caminho relativo."
    assert re.search(pat_12m, html), "Gráfico 12m não embutido como caminho relativo."
