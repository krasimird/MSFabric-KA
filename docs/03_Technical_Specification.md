# GenDWH Knowledge Assistant — Техническа спецификация

**Версия:** 1.1  
**Дата:** 2026-03-06  
**Автор:** InspirIT AI  
**Статус:** Draft

---

## 1. Обхват

Този документ описва техническата имплементация на GenDWH Knowledge Assistant — архитектура, инфраструктура, компоненти, API-та, data model, orchestration и deployment. Функционалните изисквания са описани в 02_Functional_Specification.md.

---

## 2. Архитектура — общ изглед

```
┌─────────────────────────────────────────────────────────────────┐
│                     MICROSOFT FABRIC TENANT                     │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
│  │ GenDWH_Data  │  │ GenDWH_Dev   │  │ Underwriting /        │  │
│  │ _WS_D        │  │ _WS_D        │  │ Controlling _UWS_D    │  │
│  │              │  │              │  │                       │  │
│  │ Lakehouses   │  │ Pipelines    │  │ Lakehouses            │  │
│  │ Warehouses   │  │ Notebooks    │  │ Reports               │  │
│  │ SQL Endpoints│  │ Sem.Models   │  │ Semantic Models       │  │
│  │              │  │ Reports      │  │                       │  │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬───────────┘  │
│         └─────────────────┼──────────────────────┘              │
│                           │                                     │
│         ┌─────────────────▼─────────────────────┐               │
│         │   GenDWH_Documentation_DP (Pipeline)   │              │
│         │                                        │              │
│         │   Phase_0 → Phase_1 → Phase_2 →        │              │
│         │   Phase_3 → Phase_4 → Phase_5          │              │
│         └─────────────────┬─────────────────────┘               │
│                           │                                     │
│                    ┌──────▼───────┐                              │
│                    │ GenDWH_      │                              │
│                    │ Documentation│                              │
│                    │ _LH          │                              │
│                    │              │                              │
│                    │ Tables/      │  Delta tables                │
│                    │ Files/       │  JSONL, Excel, cache         │
│                    └──────┬───────┘                              │
└───────────────────────────┼─────────────────────────────────────┘
                            │
              ┌─────────────▼──────────────┐
              │   gendwh_knowledge.jsonl    │
              └─────────────┬──────────────┘
                            │ deploy
              ┌─────────────▼──────────────┐
              │  Azure Static Web Apps      │
              │                             │
              │  index.html (Chat UI)       │
              │  gendwh_knowledge.jsonl     │
              │  /api/chat (Azure Function) │
              └──────┬──────────────────────┘
                     │
              ┌──────▼──────────────────────┐
              │  Claude API                  │
              │  api.anthropic.com/v1/messages│
              └─────────────────────────────┘
```

---

## 3. Orchestration Architecture

### 3.1 Проблемът с единичен notebook

Единичен notebook с всички фази има следните проблеми:
- Kernel restart = всички дефиниции и променливи се губят
- Ако Phase 2 гръмне на 80-тата заявка, трябва ръчна намеса
- Няма retry per phase — целият notebook е all-or-nothing
- Не може да се schedule-ира надеждно — зависи от runtime state между клетки
- Няма alerting при failure

### 3.2 Решение — Library + Phase Notebooks + Pipeline

Архитектурата се разделя на три слоя:

```
┌──────────────────────────────────────────────────┐
│ Layer 1: Library Notebook                         │
│ GenDWH_Documentation_LIB                          │
│                                                   │
│ Съдържа САМО дефиниции:                           │
│  - CONFIG, PATHS                                  │
│  - Brand, XS (style classes)                      │
│  - API helpers (get_fabric_token, call_claude)     │
│  - All phase_* functions                           │
│  - All generate_* functions                        │
│  - Helper functions                                │
│                                                   │
│ НЕ изпълнява нищо. Няма output.                   │
└────────────────────┬─────────────────────────────┘
                     │ %run
┌────────────────────▼─────────────────────────────┐
│ Layer 2: Phase Notebooks (един per фаза)          │
│                                                   │
│ Phase_0_Discovery.ipynb                            │
│   %run GenDWH_Documentation_LIB                   │
│   inventory = phase_0_discovery()                  │
│                                                   │
│ Phase_1_Extraction.ipynb                           │
│   %run GenDWH_Documentation_LIB                   │
│   meta = phase_1_metadata_extraction()             │
│                                                   │
│ Phase_2_AI_Analysis.ipynb                          │
│   %run GenDWH_Documentation_LIB                   │
│   ai_results = phase_2_ai_analysis()               │
│                                                   │
│ Phase_3_Knowledge_Build.ipynb                      │
│   %run GenDWH_Documentation_LIB                   │
│   phase_3_knowledge_build()                        │
│                                                   │
│ Phase_4_Doc_Generation.ipynb                       │
│   %run GenDWH_Documentation_LIB                   │
│   docs = phase_4_doc_generation()                  │
│                                                   │
│ Phase_5_Versioning.ipynb                           │
│   %run GenDWH_Documentation_LIB                   │
│   version = phase_5_versioning()                   │
│                                                   │
│ Всеки notebook:                                    │
│  - Зарежда LIB чрез %run (≈ import)               │
│  - Чете input от Delta tables (не от variables)    │
│  - Пише output в Delta tables и/или Files          │
│  - Self-contained: kernel restart не е проблем     │
└────────────────────┬─────────────────────────────┘
                     │ orchestrated by
┌────────────────────▼─────────────────────────────┐
│ Layer 3: Orchestration Pipeline                    │
│ GenDWH_Documentation_DP                            │
│                                                   │
│ Fabric Data Factory pipeline с Notebook Activities │
│ Retry, timeout, alerting, scheduling               │
└──────────────────────────────────────────────────┘
```

### 3.3 Notebook Inventory

| Notebook | Тип | Описание |
|----------|-----|----------|
| `GenDWH_Documentation_LIB` | Library | Всички дефиниции — CONFIG, helpers, phase functions |
| `Phase_0_Discovery` | Phase | Workspace discovery, environment filter, dedup |
| `Phase_1_Extraction` | Phase | Metadata extraction от всички item types |
| `Phase_2_AI_Analysis` | Phase | Claude API lineage analysis с кеширане |
| `Phase_3_Knowledge_Build` | Phase | JSONL knowledge base generation |
| `Phase_4_Doc_Generation` | Phase | Data Lineage Excel + Data Dictionary Excel |
| `Phase_5_Versioning` | Phase | Fingerprint, diff, archive |

Всички notebooks имат **GenDWH_Documentation_LH** като default lakehouse.

### 3.4 Data Contract между фазите

Фазите комуникират САМО чрез Delta tables и Files — никога чрез Python variables:

```
Phase 0 writes → doc_workspace_inventory
Phase 1 reads  ← doc_workspace_inventory
Phase 1 writes → doc_pipeline_activities
                 doc_lakehouse_tables
                 doc_warehouse_objects
                 doc_notebook_definitions
                 doc_semantic_models
                 doc_report_definitions
                 doc_variable_library
                 doc_metadata_queries
                 doc_bronze_metadata
Phase 2 reads  ← doc_metadata_queries
Phase 2 writes → doc_ai_lineage
                 Files/cache/ai_lineage_cache.json
Phase 3 reads  ← ALL tables from Phase 0, 1, 2
Phase 3 writes → Files/documentation/gendwh_knowledge.jsonl
Phase 4 reads  ← doc_ai_lineage, doc_lakehouse_tables, doc_bronze_metadata, etc.
Phase 4 writes → Files/documentation/excel/*.xlsx
Phase 5 reads  ← doc_workspace_inventory, doc_metadata_queries
Phase 5 writes → doc_version_history
                 Files/documentation/versions/v{n}_{ts}/
```

Ако Phase 2 гръмне и се retry-не — чете от `doc_metadata_queries` (Phase 1 output), проверява cache, продължава от където е спрял. Нула зависимост от Python state.

### 3.5 Pipeline Definition

```
GenDWH_Documentation_DP
│
│  Schedule: Mon-Fri 06:00 FLE Standard Time
│  (след основния GenDWH_Orchestration_DP ETL)
│
├── Phase_0_Discovery
│   ├── type: Notebook Activity
│   ├── notebook: Phase_0_Discovery
│   ├── lakehouse: GenDWH_Documentation_LH
│   ├── retry: 2
│   ├── retryInterval: 30 sec
│   ├── timeout: 5 min
│   └── on failure → email notification + Fail activity
│
├── Phase_1_Extraction
│   ├── depends on: Phase_0 [Succeeded]
│   ├── retry: 2
│   ├── retryInterval: 60 sec
│   ├── timeout: 15 min
│   └── on failure → email notification + Fail activity
│
├── Phase_2_AI_Analysis
│   ├── depends on: Phase_1 [Succeeded]
│   ├── retry: 1
│   │   (кешът пази прогреса — retry продължава от последно успешно)
│   ├── retryInterval: 120 sec
│   ├── timeout: 45 min
│   └── on failure → email notification + Fail activity
│
├── Phase_3_Knowledge_Build
│   ├── depends on: Phase_2 [Succeeded]
│   ├── retry: 2
│   ├── timeout: 5 min
│   └── on failure → email notification + Fail activity
│
├── Phase_4_Doc_Generation
│   ├── depends on: Phase_3 [Succeeded]
│   ├── retry: 2
│   ├── timeout: 10 min
│   └── on failure → email notification + Fail activity
│
└── Phase_5_Versioning
    ├── depends on: Phase_4 [Succeeded]
    ├── retry: 2
    ├── timeout: 5 min
    └── on failure → email notification + Fail activity
```

### 3.6 Error Handling & Recovery

| Сценарий | Какво се случва | Recovery |
|----------|----------------|----------|
| Phase 0 failure (Fabric API down) | Pipeline retry × 2 | Ако не мине — email, ръчен restart |
| Phase 1 failure (LH недостъпен) | Pipeline retry × 2 | Частични данни не се записват (overwrite mode) |
| Phase 2 failure (Claude API) | Cache пази успешните | Retry продължава от последно кеширано |
| Phase 2 partial (4 failed от 126) | 122 успешни в Delta + cache | Следващ run с по-голям max_tokens |
| Phase 4 failure (Excel generation) | Retry × 2 | Данните са в Delta — генерацията е идемпотентна |
| Kernel restart по средата на Phase | Pipeline retry-ва целия notebook | %run зарежда LIB, чете от Delta, продължава |

### 3.7 Manual Override

Pipeline-ът е за scheduled runs. За ad-hoc работа:

```python
# В отделен notebook:
%run GenDWH_Documentation_LIB

# Пусни само Phase 2 (например след добавяне на нова таблица)
ai_results = phase_2_ai_analysis(force_rerun=True)

# Или пусни само Phase 4 (регенерирай Excel-ите)
docs = phase_4_doc_generation()
```

---

## 4. Storage Architecture

### 4.1 GenDWH_Documentation_LH

Dedicated lakehouse за документационната система. Съдържа всички Delta tables и файлове.

**Защо отделен lakehouse:**
- Изолация — документационните данни не се смесват с бизнес данните
- Permissions — може да се даде read-only достъп на по-широк екип
- Lifecycle — може да се изтрие и регенерира без impact на ETL
- Default lakehouse — всички notebooks го ползват, `spark.sql()` работи без qualified names

### 4.2 Delta Tables

```
GenDWH_Documentation_LH/
└── Tables/
    │
    │── ── Phase 0 outputs ───────────────────────────
    ├── doc_workspace_inventory          ~100 rows
    │   PK: (item_name, item_type)
    │   Columns: workspace_id, workspace_name, environment,
    │            item_id, item_type, item_name, all_workspaces,
    │            discovery_run_ts
    │
    │── ── Phase 1 outputs ───────────────────────────
    ├── doc_pipeline_activities           ~170 rows
    │   PK: (pipeline_name, activity_name)
    │   Columns: workspace_id, workspace_name, pipeline_id,
    │            pipeline_name, activity_name, activity_type,
    │            description, state, depends_on (JSON),
    │            policy (JSON), type_props (JSON)
    │
    ├── doc_lakehouse_tables              ~410 rows
    │   PK: (lakehouse, table_name)
    │   Columns: workspace_id, workspace_name, item_id,
    │            lakehouse, table_name, column_count,
    │            row_count, schema_json
    │
    ├── doc_warehouse_objects             TBD
    │   PK: (warehouse, object_name, object_type)
    │   Columns: workspace_id, warehouse, object_name,
    │            object_type (table/view/sproc),
    │            definition_sql, schema_json
    │
    ├── doc_notebook_definitions          ~50 rows
    │   PK: (notebook_name)
    │   Columns: workspace_id, workspace_name, notebook_name,
    │            parameters (JSON), cell_count, source_code,
    │            markdown_cells
    │
    ├── doc_semantic_models               TBD
    │   PK: (model_name, object_name, object_type)
    │   Columns: workspace_id, model_name, object_type
    │            (measure/relationship/column), object_name,
    │            expression, format_string, source_table,
    │            target_table
    │
    ├── doc_report_definitions            ~10 rows
    │   PK: (report_name)
    │   Columns: workspace_id, report_name, report_type
    │            (Report/PaginatedReport), semantic_model,
    │            pages (JSON), filters (JSON), bookmarks (JSON)
    │
    ├── doc_variable_library              TBD
    │   PK: (variable_name)
    │   Columns: workspace_id, library_name, variable_name,
    │            variable_value, variable_type
    │
    ├── doc_metadata_queries              ~126 rows
    │   PK: (meta_table, target_table)
    │   Columns: meta_table, target_table, layer, mode, level,
    │            source_query, merge_key, query_hash,
    │            has_current, is_active
    │
    ├── doc_bronze_metadata               ~225 rows
    │   PK: (target_table)
    │   Columns: source_schema, source_table, target_table,
    │            source_columns, is_active
    │
    │── ── Phase 2 outputs ───────────────────────────
    ├── doc_ai_lineage                    ~2300 rows
    │   PK: (target_table, target_field)
    │   Columns: target_table, layer, mode, level,
    │            target_field, data_type, source_table,
    │            source_column, transformation_type,
    │            expression, business_logic, join_key,
    │            query_hash, analyzed_at
    │
    │── ── Phase 5 outputs ───────────────────────────
    └── doc_version_history               grows (append)
        PK: (version, run_timestamp)
        Columns: version, run_timestamp, fingerprint,
                 change_type, prev_version, workspace_count,
                 item_count, query_count, field_count,
                 documents (JSON), archive_path
```

### 4.3 File Storage

```
GenDWH_Documentation_LH/
└── Files/
    ├── cache/
    │   └── ai_lineage_cache.json        ← AI results cache
    │       Format: { "query_hash": { lineage_json }, ... }
    │       Size: ~3-5 MB
    │       Updated by: Phase 2
    │       Read by: Phase 2 (for skip logic)
    │
    └── documentation/
        ├── gendwh_knowledge.jsonl       ← Knowledge base for RAG
        │   Format: one JSON object per line
        │   Size: ~3-8 MB
        │   Updated by: Phase 3
        │   Read by: Web App
        │
        ├── excel/
        │   ├── GenDWH_Data_Lineage.xlsx  ← 354 sheets
        │   └── GenDWH_Data_Dictionary.xlsx ← 409 sheets
        │   Updated by: Phase 4
        │
        └── versions/
            ├── v1.0_20260306_060000/     ← Full archive
            │   ├── gendwh_knowledge.jsonl
            │   ├── GenDWH_Data_Lineage.xlsx
            │   └── GenDWH_Data_Dictionary.xlsx
            ├── v1.1_20260307_060000/
            └── ...
            Updated by: Phase 5
```

### 4.4 Access Patterns

| Consumer | Какво чете | Как |
|----------|-----------|-----|
| Phase notebooks | Delta tables | `spark.sql("SELECT * FROM doc_...")` |
| Phase 2 | AI cache | `json.load(open(cache_path))` |
| Phase 3 | All Delta tables | Spark SQL |
| Phase 4 | Delta tables | Spark SQL |
| Web App | gendwh_knowledge.jsonl | HTTP fetch (static file) |
| Екипът | Excel файлове | Download от Lakehouse UI / OneLake File Explorer |
| Мениджмънт | Excel файлове | SharePoint link / email |

---

## 5. Компонент: Library Notebook

### 5.1 Runtime

| Параметър | Стойност |
|-----------|----------|
| Платформа | Microsoft Fabric Notebook |
| Език | Python 3.11 (PySpark kernel) |
| Default Lakehouse | GenDWH_Documentation_LH |

### 5.2 Dependencies

```
python-docx    — Word document generation (за бъдеща употреба)
openpyxl       — Excel document generation
requests       — HTTP calls (Fabric API, Claude API)
```

Инсталация: в отделна Cell 0 клетка:
```python
%pip install python-docx openpyxl --quiet
```

### 5.3 Configuration

```python
CONFIG = {
    # Claude API
    "claude_model":         "claude-sonnet-4-20250514",
    "claude_max_tokens":    16384,

    # Fabric REST API
    "fabric_api_base":      "https://api.fabric.microsoft.com/v1",

    # Documentation Lakehouse paths
    "doc_lakehouse_files":  "/lakehouse/default/Files",
    "output_dir":           "/lakehouse/default/Files/documentation",

    # Source metadata
    "admin_lakehouse_name": "GenDWH_Administration_LH",

    # Environment detection
    "environment_rules": {
        "Dev":  ["_WS_D", "_UWS_D", "_UWS"],
        "Test": ["_WS_T", "_UWS_T"],
        "Prod": ["_WS_P", "_UWS_P"],
    },
    "target_environment": "Dev",
    "include_untagged":   False,

    # Caching
    "skip_unchanged":     True,
    "max_retries":        3,
    "retry_delay":        5,
}
```

### 5.4 Authentication

| Ресурс | Метод | Детайл |
|--------|-------|--------|
| Fabric REST API | `mssparkutils.credentials.getToken("pbi")` | Bearer token, notebook identity |
| Spark SQL | Implicit | Notebook identity, default lakehouse |
| Claude API | Hardcoded key (v1.0) / Azure Key Vault (prod) | `CLAUDE_API_KEY` variable |

**Production:** API ключът мигрира към Key Vault:
```python
API_KEY = mssparkutils.credentials.getSecret("kv-gendwh", "anthropic-api-key")
```

### 5.5 LIB Structure (cells)

| Cell | Съдържание |
|------|-----------|
| 0 | `%pip install` dependencies |
| 1 | Imports + CONFIG + PATHS + `CLAUDE_API_KEY` |
| 2 | Brand, XS (Excel styles), docx helpers |
| 3 | API helpers: `get_fabric_token`, `get_claude_api_key`, `fabric_api_get`, `call_claude`, `sha256` |
| 4 | `_detect_environment`, `phase_0_discovery` |
| 5 | `phase_1_metadata_extraction` + all extraction helpers |
| 6 | `LINEAGE_SYSTEM_PROMPT`, `phase_2_ai_analysis` + cache helpers |
| 7 | `phase_3_knowledge_build` |
| 8 | Excel generation: `generate_lineage_excel`, `generate_dictionary_excel` |
| 9 | `phase_4_doc_generation` |
| 10 | `phase_5_versioning` |
| 11 | `run_all()` — convenience function за manual full run |

Нито една клетка не извиква функции — само ги дефинира. `print("✓ LIB loaded")` на последния ред за потвърждение.

---

## 6. Phase Implementation Details

### 6.1 Phase 0 — Discovery

**API calls:**
```
GET /v1/workspaces
GET /v1/workspaces/{workspace_id}/items  (per selected workspace)
```

**Environment detection:** suffix matching на workspace name → Dev/Test/Prod/Other

**Deduplication:** по `(item_name, item_type)`. `all_workspaces` агрегира.

**Output:** `doc_workspace_inventory` (overwrite with schema merge)

### 6.2 Phase 1 — Extraction

**Per item type:**

| Item | API / Method | Output Table |
|------|-------------|-------------|
| Lakehouse | Spark SQL: `SHOW TABLES`, `DESCRIBE TABLE`, `COUNT(*)` | `doc_lakehouse_tables` |
| Warehouse | `getDefinition` + SQL Analytics: `INFORMATION_SCHEMA.*` | `doc_warehouse_objects` |
| DataPipeline | `getDefinition` → Base64 → JSON → parse activities | `doc_pipeline_activities` |
| Notebook | `getDefinition` → Base64 → .ipynb JSON → cells | `doc_notebook_definitions` |
| SemanticModel | `getDefinition` → TMDL/TMSL → measures, relationships | `doc_semantic_models` |
| Report | `getDefinition` → report.json → pages, SM binding, filters | `doc_report_definitions` |
| PaginatedReport | `getDefinition` → RDL (XML) → params, data sources | `doc_report_definitions` |
| VariableLibrary | REST API → properties → variables | `doc_variable_library` |
| Metadata queries | Spark SQL from `gen_adm_*` tables | `doc_metadata_queries` |
| Bronze meta | Spark SQL from `gen_adm_meta_bronze` | `doc_bronze_metadata` |

### 6.3 Phase 2 — AI Analysis

**Claude API call:**
```
POST https://api.anthropic.com/v1/messages
Model: claude-sonnet-4-20250514
Max tokens: 16384
System: LINEAGE_SYSTEM_PROMPT
```

**Caching:** `sha256(source_query)[:16]` → `ai_lineage_cache.json`. Skip if hash exists.

**Error handling:** 429 → exponential backoff. 5xx → retry. JSON parse error → skip. Timeout: 120s.

### 6.4 Phase 3 — Knowledge Build

**Input:** All Delta tables from Phase 0, 1, 2.

**Output:** `gendwh_knowledge.jsonl` — one chunk per line.

**Chunk types:** table_lineage, field_detail, pipeline_overview, pipeline_activity, table_schema, warehouse_view, warehouse_sproc, notebook_definition, semantic_measure, semantic_relationship, report_definition, paginated_report, variable, glossary_term, architecture.

**Chunk sizing:** Target ~500-2000 tokens per chunk.

### 6.5 Phase 4 — Document Generation

**Input:** Delta tables (same as Phase 3).

**Output:** 2 Excel files with InspirIT styling.

**Excel generation:** openpyxl. Hyperlinks, color badges, freeze panes, alternate rows.

### 6.6 Phase 5 — Versioning

**Input:** `doc_workspace_inventory`, `doc_metadata_queries`.

**Fingerprint:** `sha256(sorted item_ids + sorted query_hashes)[:16]`

**Output:** `doc_version_history` (append), archived files in `versions/`.

---

## 7. Компонент: RAG Web App

### 7.1 Hosting

| Параметър | Стойност |
|-----------|----------|
| Платформа | Azure Static Web Apps |
| Frontend | Single HTML file (vanilla JS) |
| Backend | Azure Function (Node.js) — API proxy |
| Storage | JSONL file served as static asset |
| Cost | Free tier |

### 7.2 Frontend

**JSONL loading:**
```javascript
const resp = await fetch('/gendwh_knowledge.jsonl');
const text = await resp.text();
KB = text.trim().split('\n').map(line => JSON.parse(line));
```

**Retrieval (v1.0 keyword):**
- Parse question for entity names
- Score chunks by exact/partial match
- Return top-15 chunks within ~12,000 char budget

**Retrieval (v2.0 semantic):**
- JSONL → embeddings → Azure AI Search vector index
- Query → embed → cosine similarity → top-K

### 7.3 Backend (Azure Function)

Single function `/api/chat` — proxy to Claude API. Avoids CORS.

```
Browser → POST /api/chat { api_key, messages, system } 
       → Azure Function 
       → POST api.anthropic.com/v1/messages
       → response back to browser
```

### 7.4 System Prompt

Инструктира Claude да отговаря на езика на въпроса, да цитира конкретни имена и SQL изрази, да прави impact analysis когато е подходящо.

---

## 8. API Reference

### 8.1 Fabric REST API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v1/workspaces` | GET | List workspaces |
| `/v1/workspaces/{id}/items` | GET | List items |
| `/v1/workspaces/{id}/items/{id}/getDefinition` | POST | Get definition (Pipeline, NB, SM, Report) |

### 8.2 Claude API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v1/messages` | POST | Lineage analysis + Q&A answers |

### 8.3 Web App API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/chat` | POST | Proxy to Claude API |

---

## 9. Security

| Concern | Mitigation |
|---------|------------|
| Fabric API access | Notebook identity — no service principal needed |
| Claude API key (notebook) | Key Vault for production; hardcoded for dev |
| Claude API key (web app) | Per-user in localStorage; never server-side |
| JSONL data exposure | Metadata only — no PII, no financial values |
| Web app access | Public by default; Azure AD auth optional |

---

## 10. Performance

| Operation | Duration |
|-----------|----------|
| Phase 0 (Discovery) | ~15 sec |
| Phase 1 (Extraction) | ~3 min |
| Phase 2 (AI) — first run | ~20 min |
| Phase 2 (AI) — cached | ~5 sec |
| Phase 3 (Knowledge Build) | ~30 sec |
| Phase 4 (Doc Generation) | ~30 sec |
| Phase 5 (Versioning) | ~5 sec |
| **Total first run** | **~25 min** |
| **Total cached run** | **~5 min** |
| Web App — Q&A response | ~5-10 sec |

---

## 11. Cost

| Component | Monthly (22 days) |
|-----------|-------------------|
| Claude API (notebook, first run) | ~$1.90 total |
| Claude API (Q&A, ~50 q/day) | ~$3.30 |
| Fabric CU (notebook) | ~4.4 CU-hr |
| Azure Static Web Apps | Free |
| Azure AI Search (v2.0) | ~$25 (Basic tier) |
| **Total v1.0** | **~$5/month** |

---

## 12. Deployment

### 12.1 Notebook deployment

1. Създай `GenDWH_Documentation_LH` в workspace
2. Import всички notebooks (LIB + 6 Phase notebooks)
3. Attach `GenDWH_Documentation_LH` като default lakehouse на всеки
4. Настрой API ключ в LIB Cell 1
5. Ръчен тест: пусни Phase notebooks последователно
6. Създай `GenDWH_Documentation_DP` pipeline с Notebook Activities
7. Настрой schedule

### 12.2 Web App deployment

```bash
npm install -g @azure/static-web-apps-cli
az staticwebapp create --name gendwh-qa --resource-group your-rg --location westeurope
# Copy gendwh_knowledge.jsonl to app folder
swa deploy ./gendwh-qa-app --deployment-token <token> --api-location ./gendwh-qa-app/api
```

### 12.3 Update cycle

```
Daily scheduled pipeline run
    ↓ produces new JSONL + Excel
Copy JSONL to web app (manual v1.0 / automated v2.1)
    ↓
Users refresh browser
```

---

*Следващ документ: Имплементационен план*
