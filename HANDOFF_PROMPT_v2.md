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
- GenDWH_Dev_WS_D (86 items) — Pipelines, Notebooks, SemanticModels, Reports
- GenDWH_Controlling_UWS_D (6 items)
- GenDWH_Underwriting_UWS (14 items)

131 items в Dev (4 workspace-а).

## Какво е Knowledge Assistant (v2.0 архитектура)

Системата има две части с ясно разделение:

### 1. Universal Extraction (Fabric Notebook)
Един notebook (`GenDWH_KA_Extraction`) който:
- Открива всички workspaces и items чрез REST API (универсално, без хардкоднати типове)
- Филтрира по environment (Dev/Test/Prod по suffix)
- Извлича raw definitions чрез getDefinition за всеки item (LRO polling за Notebooks/SemanticModels/Reports)
- Извлича Lakehouse table schemas чрез Spark SQL (`SHOW TABLES` + `listColumns`)
- Извлича Warehouse schemas чрез JDBC (`INFORMATION_SCHEMA.COLUMNS/VIEWS/ROUTINES`)
- Извлича metadata queries от gen_adm_* таблици (126 SQL заявки)
- Експортира всичко в JSON файл → OneLake + Azure Blob Storage
- **БЕЗ AI, БЕЗ external API calls (освен Blob upload), БЕЗ secrets в кода**
- Runtime: ~14 минути

### 2. Knowledge Assistant (RAG Web App на Azure Static Web Apps)
Web приложение което е AI мозъкът:
- Зарежда raw JSON export от Blob Storage
- Server-side AI analysis: 126 SQL заявки → 2364 JSONL chunks (field-level lineage), cached в Blob
- Chain-aware RAG: проследява lineage Platinum → Gold → Silver → Bronze
- RAG Q&A — отговаря на въпроси с данни от платформата
- Chat Skills — генерира документи on-demand:
  - Data Lineage Excel (с InspirIT styling, навигация, chain до Bronze)
  - Data Dictionary Excel (всички Lakehouses + Warehouses с колони и типове)
  - Business Glossary Excel (AI-генерирани дефиниции на 100 термина)
- In-app документация (Onboarding Guide, Export Process, Monitoring)
- Entra ID автентикация (InspirIT tenant)
- Claude API ключ от Azure Key Vault (kv-ai-site-builder / anthropicapikey)

## Production URLs и Infrastructure

| Компонент | URL / Идентификатор |
|-----------|-------------------|
| **Web App** | https://kind-beach-0fdf0e803.1.azurestaticapps.net |
| **Blob Storage** | sainspiritka.blob.core.windows.net / container: gendwh-exports |
| **JSON Export** | .../gendwh-exports/latest/gendwh_raw_export.json (10.28 MB) |
| **JSONL Knowledge Base** | .../gendwh-exports/latest/gendwh_knowledge.jsonl (1.27 MB, 2364 chunks) |
| **Analysis Cache** | .../gendwh-exports/latest/analysis_cache.json (785 KB) |
| **Key Vault** | kv-ai-site-builder / secret: anthropicapikey |
| **Entra ID App** | "GenDWH Knowledge Assistant" / App ID: 1e63f473-1a2c-48c4-8990-95d6105a4083 |
| **Azure Tenant** | InspirIT / 74585519-bfbf-4f14-b9f3-6d075cc9bde4 |
| **Fabric Workspace** | GenDWH_Dev_WS_D / f5ae753e-9be5-4236-8e0b-89e2ca21084c |
| **Notebook Item ID** | 8aefd8c4-73f9-4389-a9e1-2f8ee5c62847 |
| **GitHub Repo** | https://github.com/krasimird/MSFabric-KA |
| **GitHub Project** | https://github.com/users/krasimird/projects/1 |

## Repo Structure

```
MSFabric-KA/
├── notebooks/
│   ├── GenDWH_KA_Extraction.py    # Main extraction notebook (8 cells)
│   └── legacy/                     # Original PoC notebook for reference
├── webapp/
│   ├── src/
│   │   ├── index.html             # Full SPA: Chat UI + RAG + Excel generation (~1800 lines)
│   │   ├── kb-cache.js            # IndexedDB cache helpers
│   │   ├── ai-analysis.js         # Client-side analysis helpers
│   │   └── docs/                  # In-app documentation (.md files)
│   ├── api/
│   │   ├── chat/                  # Azure Function: Claude API proxy + Key Vault
│   │   ├── analyze/               # Azure Function: server-side AI analysis
│   │   └── package.json           # API dependencies (@azure/identity, @azure/keyvault-secrets, @azure/storage-blob)
│   ├── staticwebapp.config.json   # Entra ID auth config
│   └── package.json               # SWA CLI dev dependency
├── scripts/
│   ├── py_to_ipynb.py             # Converts .py notebooks to .ipynb for Fabric import
│   └── sync-jsonl.sh              # JSONL sync helper
├── docs/
│   ├── 01_Product_Vision.md
│   ├── 02_Functional_Specification.md
│   ├── 03_Technical_Specification.md
│   └── 04_Implementation_Plan.md
├── .github/workflows/             # Azure SWA auto-deploy on push
├── HANDOFF_PROMPT.md              # This file
└── README.md
```

## Workflow

- **Claude (в claude.ai)** = delivery manager — дава task prompts на английски
- **Augment Code agent (Opus 4.6)** = кодер — пише целия код
- **User** = комуникационен канал + UI testing + Fabric imports
- Flow: Claude prompt → User → AC executes → User → Claude reviews → next task
- Claude НЕ пише код — дава high-level task descriptions
- AC agent пише всичко
- User тества UI, импортира notebooks в Fabric, дава обратна връзка

## Notebook Cell Convention

Notebooks се пишат като `.py` файлове с cell маркери:
```python
# CELL 0 ── Title ─────────────────────────────────────────
# Description
# ─────────────────────────────────────────────────────────
<code>
```
Конвертират се до `.ipynb`: `python scripts/py_to_ipynb.py` → `build/` → import в Fabric.

## Extraction Notebook Details (8 cells)

| Cell | Title | What it does |
|------|-------|-------------|
| 0 | Configuration | CONFIG dict, imports |
| 1 | API Helpers | get_fabric_token(), fabric_api_get/post(), _poll_lro(), sha256 |
| 2 | Discovery | List workspaces, filter by environment, enumerate items |
| 3 | Definition Extraction | getDefinition for all items, LRO polling, skip-known-unsupported |
| 4 | Schema Extraction | Lakehouse: SHOW TABLES + listColumns. Warehouse: JDBC + INFORMATION_SCHEMA |
| 5 | Metadata Query Extraction | Read 11 gen_adm_* tables + gen_adm_meta_bronze |
| 6 | Export | Build JSON, write to OneLake + upload to Blob Storage |
| 7 | Main | Call all steps, print summary |

Key technical decisions:
- LRO for getDefinition: poll /operations/{id}, then GET /operations/{id}/result for actual content
- Skip unsupported types after first 400 (SQLEndpoint, Dashboard, Warehouse, PaginatedReport)
- Lakehouse schema: SHOW TABLES (~2s) + spark.catalog.listColumns (~400ms/table)
- Warehouse schema: JDBC to SQL Analytics Endpoint with Fabric token
- Blob upload: SAS connection string in CONFIG (manually set before running)

## Web App Technical Details

- **Frontend**: Single HTML file, vanilla JS, no build step
- **Markdown rendering**: marked.js from CDN
- **Excel generation**: ExcelJS from CDN, runs in browser
- **RAG**: keyword search + knowledge base search + chain-following
- **Chain RAG**: followLineageChain() traces Platinum → Gold → Silver → Bronze
- **API proxy**: Azure Function with Key Vault for Claude API key
- **AI Analysis**: /api/analyze endpoint, processes 126 queries in batches of 5, caches by query hash
- **Auth**: Entra ID via staticwebapp.config.json, InspirIT tenant only

## What was completed (v1.0)

All 5 sprints done:
1. ✅ Sprint 1 — Universal Extraction Notebook
2. ✅ Sprint 2 — Web App Foundation (Chat UI, RAG Q&A)
3. ✅ Sprint 3 — AI Analysis Engine (lineage, knowledge base)
4. ✅ Sprint 4 — Chat Skills (Lineage Excel, Dictionary, Glossary)
5. ✅ Sprint 5 — Deploy, Entra ID, monitoring, onboarding

## Known Limitations (v1.0)

- Pipeline info not prioritized in RAG scoring — questions about pipelines may get poor answers
- Bronze chain incomplete for some lineage traces — RAG context budget runs out
- 2 SemanticModels (Controlling_LH, GenDWH_PolicyPremiumsCommissions_SM) have empty definitions
- 2 Lakehouses (Controlling_LH, GenDWH_Documentation_LH) fail schema extraction (cross-workspace)
- PaginatedReport getDefinition not supported by Fabric API
- Extraction notebook blob_connection_string must be set manually in CONFIG

## v1.1 Roadmap

- Pipeline info in RAG (better scoring for pipeline questions)
- Bronze chain completeness (larger context budget for chain queries)
- Custom report skill (generate file, not just text)
- Pipeline scheduling for extraction notebook
- Mermaid diagram rendering in chat
- Suppress expected 400 errors in extraction log (summary instead of per-item)

## v2.0 Roadmap

- Universal framework (configurable for other Fabric implementations)
- Semantic search (vector embeddings instead of keyword RAG)
- Auto-deploy JSON on notebook run (Blob trigger → analyze)
- Microsoft Teams integration (chat bot)

## InspirIT Branding

Excel documents follow InspirIT style:
- Calibri Light headings, Courier New for code
- Teal (#1C8D7A) headers, white text
- Dark Blue (#0B3052) for CASE WHEN badges
- Green (#2C6B5F) for COALESCE badges
- #F2F3F4 alternate rows
- Navigation: INDEX → Zone Catalog → Detail sheet → back

## Claude API

- Model: claude-sonnet-4-20250514
- Max tokens: 4096 (chat), 8192 (analysis)
- API key: Azure Key Vault (kv-ai-site-builder / anthropicapikey)
- Cost: ~$2/month (analysis cached, Q&A ~50 q/day)

---

Моля продължи от текущата задача по плана.
