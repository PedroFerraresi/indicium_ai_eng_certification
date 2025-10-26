# Descrição geral

Este projeto automatiza a vigilância da **Síndrome Respiratória Aguda Grave (SRAG)** no Brasil, gerando um relatório executivo por **UF** a partir de dados públicos do [Open DATASUS](https://opendatasus.saude.gov.br). A aplicação orquestra, de ponta a ponta, a **ingestão**, os **cálculos de indicadores**, a **visualização de séries**, a **síntese de notícias** e a **renderização do relatório** — tudo com observabilidade, guardrails e modo offline.

O pipeline (exibido mais abaixo) segue o fluxo: **ingest → metrics → charts → news → report**. A ingestão pode ser **local** (com arquivos `.csv` em `data/raw/`) ou **remota** (URLs configuráveis no arquivo `.env`). As métricas são determinísticas (SQL/SQLite + Pandas) e incluem:

- **Variação de casos** mês a mês (`increase_rate`)
- **Taxa de mortalidade** (`deaths / cases`)
- **Taxa de UTI** (`icu_cases / cases`)
- **Proxy de vacinação** (`vaccinated_cases / cases`)

As **séries** são sequências temporais agregadas por **UF** usadas para visualizar a tendência de casos de SRAG. As séries de **30 dias** (diária) e **12 meses** (mensal) são plotadas em PNG. Opcionalmente, um LLM resume manchetes recentes sobre SRAG (Serper + OpenAI) com **fallback seguro** quando chaves/serviços não estão disponíveis — garantindo que a pipeline **não quebre** na ausência do LLM.

### Principais resultados gerados

- **Relatório HTML**: `resources/reports/relatorio.html`. Relatório contendo as métricas **calculadas** e o resumo das notícias sobre SRAG.
- **PDF**: `resources/reports/relatorio.pdf`. Conversão direta do `.html` (via xhtml2pdf, quando disponível).
- **Gráficos**: `resources/charts/`. Séries diárias (30d) e mensais (12m) em PNG.
- **Log de auditoria**: `resources/json/events.jsonl`. Logs estruturados de **toda a execução** (pipeline e chamadas ao LLM), com spans `*.start/*.end/*.error`, duração e `run_id` de correlação.

### Qualidade, segurança e transparência

- **Observabilidade**: spans por etapa, duração e erros em JSONL.
- **Guardrails**: validação de UF, corte de datas futuras, timeouts/retries/backoff em APIs, sanitização de dados sensíveis nos logs e caminhos POSIX no HTML gerado (compatível com Windows).
- **Privacidade**: o template bloqueia DataFrames/linhas brutas (somente agregados/indicadores e imagens).
- **Confiabilidade**: contrato do relatório testado (KPIs com `data-testid`, imagens por caminhos relativos), suíte de testes e CI.
