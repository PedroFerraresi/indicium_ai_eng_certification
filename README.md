# Descrição geral

Este projeto automatiza a vigilância da **Síndrome Respiratória Aguda Grave (SRAG)** no Brasil, gerando um relatório executivo por **UF** a partir de dados públicos do [OpenDataSUS](https://opendatasus.saude.gov.br). A aplicação orquestra, de ponta a ponta, a **ingestão**, os **cálculos de indicadores**, a **visualização de séries**, a **síntese de notícias** e a **renderização do relatório** — tudo com observabilidade, guardrails e modo offline.

O pipeline (exibido mais abaixo) segue o fluxo: **ingest → metrics → charts → news → report**. A ingestão pode ser **local** (com arquivos `.csv` em `data/raw/`) ou **remota** (URLs configuráveis no arquivo `.env`). As métricas são determinísticas (SQL/SQLite + Pandas) e incluem:

- **Variação de casos** mês a mês (`increase_rate`)
- **Taxa de mortalidade** (`deaths / cases`)
- **Taxa de UTI** (`icu_cases / cases`)
- **Proxy de vacinação** (`vaccinated_cases / cases`)

As **séries** são sequências temporais agregadas por **UF** usadas para visualizar a tendência de casos de SRAG. As séries de **30 dias** (diária) e **12 meses** (mensal) são plotadas em PNG. Opcionalmente, um LLM resume manchetes recentes sobre SRAG (Serper + OpenAI), com **fallback seguro** quando chaves/serviços não estão disponíveis — garantindo que a pipeline **não quebre** na ausência do LLM.

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

## Objetivo & Dados

### Objetivo
Gerar, para uma **UF** escolhida, um **relatório executivo** de vigilância da **SRAG** que consolida:
1) **Indicadores** determinísticos e comparáveis ao longo do tempo,  
2) **Séries temporais** (30 dias e 12 meses) para leitura visual de tendência,  
3) **Contexto de notícias** (opcional) para apoiar a interpretação.

### Fontes de dados

- **SRAG – OpenDataSUS**: arquivos `.csv` do Ministério da Saúde.  
  - **Local**: `data/raw/*.csv`  
  - **Remoto**: variável `.env` `SRAG_URLS` (lista de URLs separadas por vírgula)
- **Notícias (opcional)** — desligado por padrão no CI e em ambientes sem chaves; o pipeline segue sem quebrar:  
  - API **Serper** (Google News-like): títulos/links  
  - **OpenAI**: resumo curto

### Modelo de dados (SQLite)

Durante a ingestão, são criadas 4 tabelas com responsabilidades claras:

| Tabela           | Colunas principais                                     | Função                                                                 |
|------------------|---------------------------------------------------------|-------------------------------------------------------------------------|
| `srag_staging`   | `DT_SIN_PRI`, `EVOLUCAO`, `UTI`, `VACINA_COV`, `UF`    | **Raw minimal** (colunas essenciais), já com parsing e normalizações.   |
| `srag_base`      | `event_date`, `uf`, `death_flag`, `icu_flag`, `vaccinated_flag` | **Fatos** diários com flags derivadas e datas saneadas.                 |
| `srag_daily`     | `day`, `uf`, `cases`, `icu_cases`, `deaths`, `vaccinated_cases` | Agregação **diária** por UF.                                           |
| `srag_monthly`   | `month`, `uf`, `cases`, `icu_cases`, `deaths`, `vaccinated_cases` | Agregação **mensal** por UF (normalizada para `YYYY-MM-01`).           |

**Transformações-chave na ingestão**
- **Datas**: `DT_SIN_PRI` parseada de forma robusta (ISO `YYYY-MM-DD` ou `DD/MM/YYYY`).  
- **UF**: derivada por prioridade entre `SG_UF_NOT`, `SG_UF`, `SG_UF_RES` (fallback para UF padrão).  
- **Flags**:  
  - `death_flag = 1` se `EVOLUCAO == 2`  
  - `icu_flag = 1` se `UTI == 1`  
  - `vaccinated_flag = 1` se `VACINA_COV == 1`  
- **Agregações**:  
  - `srag_daily`: contagem por `day, uf`  
  - `srag_monthly`: contagem por `month, uf`  
- **Guardrails**: remoção de **datas futuras** (clamp), tipagens numéricas e defaults seguros quando colunas faltam.

### Indicadores calculados

| Indicador               | Fórmula / Definição                                                                          |
|-------------------------|-----------------------------------------------------------------------------------------------|
| **Variação de casos**   | `increase_rate = (cases_mês_atual - cases_mês_anterior) / cases_mês_anterior` (se ambos > 0) |
| **Taxa de mortalidade** | `mortality_rate = deaths / cases` no **mês mais recente**                                    |
| **Taxa de UTI**         | `icu_rate = icu_cases / cases` no **mês mais recente**                                       |
| **Proxy de vacinação**  | `vaccination_rate = vaccinated_cases / cases` no **mês mais recente**                        |

> Observação: os indicadores são **determinísticos** (SQL/Pandas). O LLM é usado somente para o **resumo de notícias**, nunca para métricas.

### Séries temporais

- **30 dias (diária)**: útil para perceber **aceleração / desaceleração** recente.  
- **12 meses (mensal)**: útil para contextualizar a **sazonalidade** e mudanças estruturais.

Ambas são salvas como **PNG** em `resources/charts/` e embutidas nos relatórios (`.html` e `.pdf`).
