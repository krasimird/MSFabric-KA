# GenDWH Knowledge Assistant — Context Handoff

Продължаваме работата по GenDWH Knowledge Assistant. Ето пълния контекст:

---

## Какво е GenDWH

GenDWH е Data Warehouse платформа на Generali, имплементирана в Microsoft Fabric. Медальонна архитектура с 5 слоя:
- **Bronze** (GenDWH_Bronze_LH) — 225 таблици, FULL copy от GeneraliDWH SQL Server
- **Silver Raw** (GenDWH_SilverRaw_LH) — 24 таблици, SCD1 merge
- **Silver Staging** (GenDWH_SilverStg_LH) — 26 таблици, IFRS изчисления
- **Gold** (GenDWH_GoldDWH_LH) — 78 таблици, dim_ + fact_, SCD2
- **Platinum** — Underwriting/Controlling, async trigger

Оркестрация: GenDWH_Orchestration_DP pipeline, metadata-driven (нито едно хардкоднато таблично име), schedule Mon-Fri 03:00 FLE.

## Fabric Environment

12 workspaces, Dev/Test/Prod по naming convention (_WS_D, _WS_T, _WS_P):
- GenDWH_Data_WS_D (25 items) — Lakehouses, Warehouses
- GenDWH_Dev_WS_D (84 items) — Pipelines, Notebooks, SemanticModels, Reports
- GenDWH_Controlling_UWS_D (6 items)
- GenDWH_Underwriting_UWS (14 items)

102 уникални items в Dev (след dedup): 50 Notebooks, 11 SemanticModels, 9 Lakehouses, 9 Reports, 7 Pipelines, 2 Warehouses, 2 Dashboards, 1 VariableLibrary, 1 Environment, 1 PaginatedReport.

## Какво е Knowledge Assistant (v2.0 архитектура)

Системата има две части с ясно разделение:

### 1. Universal Extraction (Fabric Notebook)
Един лек notebook който:
- Открива всички workspaces и items чрез REST API (универсално, без хардкоднати типове)
- Извлича raw definitions чрез getDefinition за всеки item
- Извлича table schemas чрез Spark SQL (Lakehouses + Warehouses)
- Извлича metadata queries от gen_adm_* таблици (SQL трансформациите)
- Експортира всичко в един JSON файл (`gendwh_raw_export.json`)
- **БЕЗ AI, БЕЗ external API calls, БЕЗ secrets**

### 2. Knowledge Assistant (RAG Web App на Azure Static Web Apps)
Web приложение което е AI мозъкът:
- Зарежда raw JSON export
- AI анализира SQL заявките → field-level lineage (кеширано)
- RAG Q&A — отговаря на въпроси с данни от платформата
- Chat Skills — генерира документи on-demand:
  - Data Lineage Excel (с InspirIT styling и навигация)
  - Data Dictionary Excel
  - Business Glossary Word
  - Custom reports
- Entra ID автентикация
- Claude API ключ само server-side (Azure Function)

## Ключови архитектурни решения (v2.0)

- **API ключ извън Fabric** — Anthropic API key живее само в Azure Function App Settings
- **Universal extraction** — notebook-ът не знае предварително какви item types ще намери
- **AI в Web App** — анализ на SQL, lineage tracing, document generation — всичко в браузъра/Azure
- **Documents on-demand** — не по schedule, а при поискване в чата
- **Lineage чрез execution chains** — AI проследява Pipeline → Notebook → SQL → tables
- **Entra ID auth** — корпоративен логин, не API keys per user
- **InspirIT брандинг** — Teal (#1C8D7A) headers, Calibri/Courier New, #F2F3F4 alternate rows

## Оркестрация и Lineage

Трансформационната логика в GenDWH е разпределена:
- **Pipeline activities** → dependency chains, какво вика какво
- **GenDWH_Orchestrator_NB** → чете SQL от gen_adm_meta_* → изпълнява динамично
- **gen_adm_* metadata таблици** → 126 SQL заявки с CTE вериги, IFRS логика
- **Warehouse views** → reporting layer SQL
- **Semantic Models** → DAX measures, relationships

AI-ят получава всичко като raw data и проследява пълната верига.

## Repo и workflow

**Repo:** https://github.com/krasimird/MSFabric-KA

**Структура:**
```
MSFabric-KA/
├── notebooks/              # Fabric notebook (.py → .ipynb)
│   ├── GenDWH_KA_Extraction.py
│   └── legacy/             # Reference: original PoC notebook
├── webapp/                 # RAG Web App
│   ├── src/                # Frontend
│   ├── api/chat/           # Azure Function (Claude proxy)
│   └── staticwebapp.config.json  # Entra ID auth
├── docs/                   # Specifications (4 documents)
├── scripts/
│   ├── py_to_ipynb.py      # Notebook converter
│   └── sync-jsonl.sh       # JSON deploy helper
└── .github/workflows/      # CI/CD
```

**Workflow:**
- Claude (в claude.ai) = delivery manager — дава task prompts на английски
- Augment Code agent (Opus 4.6) = кодер — пише целия код
- User = комуникационен канал + UI testing + Fabric imports
- Flow: Claude prompt → User → AC executes → User → Claude reviews → next task

**GitHub Project Board:** https://github.com/users/krasimird/projects/1
Kanban: Backlog → In Progress → Done

## Имплементационен план (v2.0)

5 спринта, 12 работни дни:
1. **Sprint 1 (2 дни)** — Universal Extraction Notebook
2. **Sprint 2 (3 дни)** — Web App Foundation (Chat UI, RAG Q&A)
3. **Sprint 3 (2 дни)** — AI Analysis Engine (lineage, knowledge base)
4. **Sprint 4 (3 дни)** — Chat Skills (Excel, Dictionary, Glossary generation)
5. **Sprint 5 (2 дни)** — Deploy, Entra ID, monitoring, handover

## Claude API

- Model: claude-sonnet-4-20250514
- Max tokens: 16384
- API key: в Azure Function App Settings (НИКОГА във Fabric)
- Cost: ~$1.90 за пълен AI analysis run, ~$0 за cached runs

## InspirIT стил

Excel документите следват InspirIT брандинг:
- Calibri Light headings, Courier New за code
- Teal (#1C8D7A) headers, Dark blue (#0B3052) CASE WHEN badges, Green (#2C6B5F) COALESCE badges
- #F2F3F4 alternate rows, #FFF8F0 warm background
- Навигация: INDEX → Zone Index → Detail sheet → обратно

---

Моля продължи от текущата задача по имплементационния план.
