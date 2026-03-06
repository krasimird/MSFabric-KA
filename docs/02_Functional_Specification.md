# GenDWH Knowledge Assistant — Функционална спецификация

**Версия:** 1.3  
**Дата:** 2026-03-06  
**Автор:** InspirIT AI  
**Статус:** Draft

---

## 1. Обхват

Този документ описва функционалностите на GenDWH Knowledge Assistant от гледна точка на потребителя — какво може да прави системата, как се взаимодейства с нея, и какви резултати произвежда. Техническата имплементация е описана в отделен документ (03_Technical_Specification.md).

Системата се състои от два модула:

- **Модул A — Documentation Engine** (Fabric Notebook) — открива, извлича, анализира и структурира знанието
- **Модул B — Knowledge Assistant** (RAG Web Application) — позволява на екипа да пита и получава отговори базирани на това знание

---

## 2. Ключови артефакти

Системата произвежда два справочни документа и една knowledge base:

| Артефакт | Формат | Предназначение |
|----------|--------|----------------|
| **Data Lineage** | Excel (.xlsx) | Field-level lineage с навигация INDEX → Zone → Detail за всяка таблица. Показва откъде идва всяко поле, какви трансформации са приложени, с какъв SQL израз. |
| **Data Dictionary** | Excel (.xlsx) | Table и column-level описание на всички Lakehouses и Warehouses — column names, data types, row counts. |
| **Knowledge Base** | JSONL | Структурирани chunks оптимизирани за semantic search. Source of truth за RAG системата. |

Data Lineage и Data Dictionary са самодостатъчни справочни документи. Knowledge Base захранва Web App-а.

---

## 3. Модул A — Documentation Engine

### 3.1 Описание

Automated pipeline който открива, извлича, анализира и структурира цялото знание за GenDWH платформата. Изпълнява се като Fabric Notebook по schedule или on-demand.

### 3.2 Phases

```
Phase 0: Discovery       — Fabric REST API → workspaces + items
Phase 1: Extraction      — Metadata от всички item types
Phase 2: AI Analysis     — Claude API анализира SQL → field-level lineage
Phase 3: Knowledge Build — Структурира всичко в JSONL chunks
Phase 4: Doc Generation  — Генерира Data Lineage Excel + Data Dictionary Excel
Phase 5: Versioning      — Fingerprint, diff, archive
```

### 3.3 Функционалности

#### F-A01: Workspace Discovery

**Какво прави:** Автоматично открива всички Microsoft Fabric workspaces до които notebook identity-то има достъп.

**Поведение:**
- Потребителят не конфигурира списък от workspaces — системата ги открива чрез Fabric REST API
- Нов workspace утре → автоматично влиза в следващия run
- Workspace-ите се класифицират по naming convention: `_WS_D` = Dev, `_WS_T` = Test, `_WS_P` = Prod
- CONFIG параметър `target_environment` определя кой environment се документира
- Items с еднакво име и тип, видими в повече от един workspace (shortcuts), се записват веднъж

**Покрити item types:**

| Item Type | Брой (Dev) | Какво се документира |
|-----------|-----------|---------------------|
| Lakehouse | 9 | Table schemas, column types, row counts |
| Warehouse | 2 | Views, stored procedures, table schemas |
| DataPipeline | 7 | Activities, dependencies, retry policies |
| Notebook | 50 | Source code, parameters, описание на логиката |
| SemanticModel | 11 | Measures, relationships, calculated columns |
| VariableLibrary | 1 | Shared variables и техните стойности |
| Report | 9 | Страници, използван Semantic Model, филтри |
| PaginatedReport | 1 | Параметри, data sources, RDL query definitions |
| Dashboard | 2 | Регистрирани в inventory |
| SQLEndpoint | 9 | Регистрирани (идват с LH/WH) |
| Environment | 1 | Регистриран в inventory |

**Output:** Delta таблица `doc_workspace_inventory`.

---

#### F-A02: Metadata Extraction

**Какво прави:** За всеки открит item извлича детайлна информация спрямо типа му.

**Lakehouse:**
- Чрез Spark SQL: `SHOW TABLES` + `DESCRIBE TABLE`
- Извлича table list, column names, data types, row counts
- Output: `doc_lakehouse_tables`

**Warehouse:**
- Чрез SQL Analytics Endpoint или Fabric REST API
- Извлича tables, views (включително view definitions), stored procedures
- Views са критични — съдържат трансформационна логика подобна на Silver/Gold SQL
- Output: `doc_warehouse_objects`

**DataPipeline:**
- Чрез Fabric REST API `getDefinition` endpoint
- Извлича activity name, type, description, dependencies, retry policies
- Парсва JSON structure в плоски записи
- Output: `doc_pipeline_activities`

**Notebook:**
- Чрез Fabric REST API `getDefinition` endpoint
- Извлича source code (Python/Spark), markdown cells, параметри
- Особено важно за Orchestrator Notebook и transformation notebooks
- Output: `doc_notebook_definitions`

**SemanticModel:**
- Чрез Fabric REST API или TMSL endpoint
- Извлича measures (DAX expressions), relationships (FK → PK), calculated columns, hierarchies
- Ключово за разбиране как Gold таблиците се консумират от BI layer-а
- Output: `doc_semantic_models`

**Report:**
- Чрез Fabric REST API `getDefinition` endpoint
- Извлича report pages (имена, подредба)
- Извлича свързания Semantic Model (dataset binding)
- Извлича report-level и page-level филтри
- Извлича bookmarks (запазени изгледи)
- Позволява impact analysis: ако променя Gold таблица X → кои отчети са засегнати
- Output: `doc_report_definitions`

**PaginatedReport:**
- Чрез Fabric REST API — извлича RDL definition (XML)
- Парсва параметри, data sources, query definitions
- Output: включва се в `doc_report_definitions` с тип `Paginated`

**VariableLibrary:**
- Чрез Fabric REST API
- Извлича variable names и стойности (connection strings, paths, feature flags)
- Тези variables се използват от pipeline-ите като `@pipeline().libraryVariables.<n>`
- Output: `doc_variable_library`

**SQL заявки за lineage (от Administration LH):**
- Чете от `gen_adm_*` metadata таблици
- 11 metadata таблици покриват Silver (L1-L2, merge + overwrite) и Gold (L1-L5, merge + overwrite)
- Всяка заявка получава hash за change detection
- Output: `doc_metadata_queries`

**Bronze metadata:**
- Source schema, source table, target table, column list
- 225 таблици от `gen_adm_meta_bronze`
- Output: `doc_bronze_metadata`

---

#### F-A03: AI-Powered Lineage Analysis

**Какво прави:** Изпраща всяка SQL заявка към Claude API и получава field-level lineage с пълна CTE resolution.

**Защо AI, а не regex:**
- ~30% от заявките съдържат 2-5 нива CTE вериги
- IFRS бизнес логика (cat_id, uoa_id) изисква семантично разбиране
- CASE WHEN с 8+ клона не може да се документира с pattern matching

**За всяко поле AI връща:**
- Target field name и data type
- Source table (резолвиран до оригиналната Bronze/Silver таблица, не CTE alias)
- Source column
- Transformation type (direct_map, case_when, coalesce, cast, arithmetic, hash, literal, и др.)
- SQL expression
- Business logic описание на естествен език
- Join key (ако полето идва чрез JOIN)

**Кеширане:**
- Резултатите се кешират по query_hash
- При следващ run — ако SQL заявката не е променена, AI не се вика отново
- Първи run: ~$1.90 за 126 заявки. Следващи run-ове: ~$0

**Output:** Delta таблица `doc_ai_lineage` + JSON cache файл.

---

#### F-A04: Knowledge Build (JSONL)

**Какво прави:** Структурира цялото събрано знание в JSONL формат — self-contained chunks оптимизирани за RAG retrieval.

**Видове chunks:**

| Тип | Източник | Описание |
|-----|----------|----------|
| `table_lineage` | AI analysis | Пълен lineage на таблица — source tables, CTE chain, всички полета, summary |
| `field_detail` | AI analysis | Детайл за едно поле — source, transformation, expression, бизнес логика |
| `pipeline_overview` | Pipeline extraction | Цялостно описание на pipeline — фази, execution flow, activity count |
| `pipeline_activity` | Pipeline extraction | Отделна activity — тип, dependencies, description, retry policy |
| `table_schema` | LH/WH extraction | Schema на таблица — columns, types, row count, lakehouse/warehouse |
| `warehouse_view` | Warehouse extraction | View definition — SQL, source tables, описание |
| `warehouse_sproc` | Warehouse extraction | Stored procedure — параметри, логика |
| `notebook_definition` | Notebook extraction | Notebook описание — какво прави, параметри, ключов код |
| `semantic_measure` | SemanticModel extraction | DAX measure — expression, format, описание |
| `semantic_relationship` | SemanticModel extraction | Relationship — from table.column → to table.column, type |
| `report_definition` | Report extraction | Report pages, свързан Semantic Model, филтри, bookmarks |
| `paginated_report` | PaginatedReport extraction | Параметри, data sources, queries |
| `variable` | VariableLibrary extraction | Variable name, value, usage context |
| `glossary_term` | AI generated | Бизнес или технически термин с дефиниция |
| `architecture` | Discovery + AI | Архитектурен компонент — workspace, layer, design decision |

**Свойства на всеки chunk:**
- **Self-contained** — може да бъде разбран без допълнителен контекст
- **Tagged** — `type`, `layer`, `category`, `table_name` за филтриране
- **Embeddable** — готов за vector embedding
- **Summarized** — всеки chunk включва `summary` поле на естествен език

**Примерни JSONL chunks:**

```json
{
  "type": "table_lineage",
  "id": "stg_claim_expenses",
  "layer": "Silver",
  "level": "L2",
  "source_tables": ["landing_payments_liquidationcosts", "landing_dbo_polici", "landing_dbo_prepiski"],
  "cte_chain": ["claims_expenses → base joins on polici+prepiski+liquidationcosts", "CTE_Categorized → adds lookups for zastrahovki, IDZ_BLANKA, ACTUARIAL_PFs"],
  "field_count": 25,
  "summary": "Staging table for claim expenses. Joins payment data with policy info, converts currency to BGN, and classifies each expense into IFRS categories (cat_id) based on sign, year, and reinsurance status.",
  "fields": ["..."]
}
```

```json
{
  "type": "report_definition",
  "id": "GenDWH_PolicyPremiumsCommissions_Report",
  "report_name": "GenDWH_PolicyPremiumsCommissions_Report",
  "semantic_model": "GenDWH_PolicyPremiumsCommissions_SM",
  "pages": ["Overview", "Premium Detail", "Commissions", "Policy Drill-through"],
  "report_filters": ["Year", "Insurance Company", "Product Family"],
  "bookmarks": ["Current Year", "Previous Year Comparison"],
  "summary": "Main business report for policy premiums and commissions. Consumes the PolicyPremiumsCommissions semantic model. 4 pages covering overview, premium detail, commissions breakdown, and policy drill-through. Filtered by year, company, and product family."
}
```

```json
{
  "type": "notebook_definition",
  "id": "GenDWH_Orchestrator_NB",
  "notebook_name": "GenDWH_Orchestrator_NB",
  "parameters": ["meta_table", "mode", "load_type", "layer"],
  "summary": "Universal orchestrator notebook. Called by pipeline for every Silver and Gold transformation. Reads the target table definition from the specified gen_adm_* metadata table, executes the SQL query, and writes results using SCD1 merge, SCD2 merge, or overwrite depending on mode parameter."
}
```

**Output:** `gendwh_knowledge.jsonl` в OneLake Files.

---

#### F-A05: Document Generation

**Какво прави:** Генерира двата ключови Excel документа от knowledge base.

**Data Lineage Excel:**
- INDEX sheet → Zone Index (Bronze, Silver, Gold) → Detail sheet за всяка таблица
- Навигационни хиперлинкове на всяко ниво (напред и назад)
- Color-coded transformation badges: Teal = Direct map, Dark blue = CASE WHEN, Green = COALESCE, Grey = Literal
- Alternate row shading, Courier New за code, Calibri за текст
- 354 sheets (225 Bronze + 45 Silver + 80 Gold + 4 навигационни)

**Data Dictionary Excel:**
- INDEX → Detail sheet за всяка таблица
- Включва таблици от Lakehouses И Warehouses
- Column name, data type, row count
- За Warehouse views — включва view definition

**Стил:** InspirIT branded — Calibri/Courier New, Teal (#1C8D7A) headers, #F2F3F4 alternate rows.

---

#### F-A06: Versioning

**Какво прави:** Проследява промените между run-ове.

**Механизъм:**
- Fingerprint на текущото състояние (inventory + query hashes)
- Сравнение с предходен fingerprint
- Ако има промяна → нова версия, нов JSONL, нови Excel-и
- Ако няма промяна → skip, записва `no_change`

**Архивиране:** Всяка версия в `Files/documentation/versions/v{version}_{timestamp}/`.

---

## 4. Модул B — Knowledge Assistant (RAG Web App)

### 4.1 Описание

Web приложение което имплементира RAG (Retrieval-Augmented Generation). Потребителят задава въпрос на естествен език. Системата намира релевантни chunks от knowledge base чрез search, подава ги като контекст на Claude API, и генерира отговор базиран на реални данни от платформата.

Знанието покрива всички аспекти на платформата:
- **Data Lineage** — откъде идва всяко поле, как се трансформира
- **Table Schemas** — какви колони има, data types, row counts
- **Pipeline Architecture** — кои activities, в какъв ред, с какви зависимости
- **Notebook Logic** — какво прави orchestrator-а, как се параметризира
- **Warehouse Views** — какъв SQL е зад reporting views
- **Semantic Models** — какви DAX measures, какви relationships
- **Reports** — кои страници, кой semantic model, какви филтри
- **Shared Variables** — какви connection strings, paths, flags се ползват
- **Бизнес контекст** — IFRS 17 термини, застрахователни концепции
- **Impact Analysis** — ако променя таблица X, кои views/models/reports са засегнати

### 4.2 User Journey

```
Developer отваря URL в браузъра
        ↓
Първи път: въвежда Anthropic API key (запазва се в localStorage)
        ↓
Задава въпрос: "Откъде идва полето suma в stg_claim_expenses?"
        ↓
RAG Retrieval: search в JSONL chunks → top-K релевантни
        ↓
Claude API: контекст (chunks) + въпрос + chat history → отговор
        ↓
"Полето suma идва от landing_payments_liquidationcosts.Suma.
 При валута различна от BGN (CurrencyId not in 0,11) се умножава
 по CurrencyRate. SQL: CASE CurrencyId IN (0,11) THEN Suma
 ELSE Suma * CurrencyRate"
```

### 4.3 Функционалности

#### F-B01: Чат интерфейс

**Какво вижда потребителят:**
- Header с GenDWH брандинг и статус индикатор
- Sidebar с platform статистики и Quick Question бутони
- Чат зона с user/assistant bubbles и markdown rendering
- Input area (Enter = изпрати, Shift+Enter = нов ред)

---

#### F-B02: RAG Retrieval

**Какво прави:** Преди всеки въпрос намира най-релевантните chunks от knowledge base.

**v1.0 — Keyword search:**
- Парсва въпроса за table names, field names, pipeline names, report names
- Търси exact и partial match в JSONL chunks
- Подбира до ~12,000 символа контекст

**v2.0 — Semantic search:**
- JSONL chunks → vector embeddings
- Embeddings → Azure AI Search (vector index)
- При въпрос: embed query → cosine similarity → top-K chunks
- Разбира "как се конвертира валутата" ≈ "suma currency conversion"

---

#### F-B03: AI-Powered Отговори

**Какво прави:** Подава retrieved chunks + въпрос + chat history към Claude API.

**System prompt:**
- Отговаряй на езика на въпроса (български или английски)
- Цитирай конкретни table names, field names, SQL expressions, DAX measures от контекста
- Ако информацията не е в контекста — кажи го явно
- Markdown formatting за четимост

**Chat history:** Последните 10 съобщения за follow-up контекст.

---

#### F-B04: Quick Questions

Предефинирани бутони за чести въпроси:
- "Какви полета има dim_policy?"
- "Как се изчислява cat_id в stg_claim_expenses?"
- "Кои таблици захранват Gold слоя?"
- "Обясни pipeline GenDWH_Orchestration_DP"
- "Откъде идва полето suma?"
- "Какви measures има в semantic model-а за premiums?"
- "Кои отчети ползват dim_policy?"
- "Ако променя fact_premiums_written, кои reports ще бъдат засегнати?"

---

#### F-B05: API Key Management

- При първо отваряне — modal за въвеждане на ключ
- Ключът се записва в browser localStorage
- Не се пази на сървъра
- Shared key вариант е възможен чрез Azure Function App Setting

---

## 5. End-to-End Lineage

Системата позволява проследяване на данните от source до visualization:

```
GeneraliDWH SQL Server (source)
    ↓ FULL load
Bronze Lakehouse (1:1 copy)
    ↓ SCD1 merge / overwrite
Silver Lakehouse (cleansed, enriched, IFRS staging)
    ↓ SCD2 merge / overwrite
Gold Lakehouse (dimensional model: dim_ + fact_)
    ↓ views
Warehouse (reporting views, stored procs)
    ↓ consumes
Semantic Model (DAX measures, relationships)
    ↓ visualizes
Report (pages, visuals, filters)
    ↓
Business User
```

Всяко ниво е документирано в JSONL knowledge base и е достъпно за Q&A.

## 6. Data Flow

```
Fabric Tenant
    ↓ (REST API + Spark SQL)
Phase 0: Discovery → workspaces, items (LH, WH, Pipeline, NB, SM, Report, VL)
Phase 1: Extraction → metadata от всеки item type
Phase 2: AI Analysis → field-level lineage от SQL заявки
    ↓ (Delta tables)
Phase 3: Knowledge Build
    ↓
gendwh_knowledge.jsonl  ←── source of truth
    ↓                          ↓
Phase 4: Excel docs        RAG Web App
  ├── Data Lineage           ↓
  └── Data Dictionary    Search (keyword → semantic)
                             ↓
                         Claude API → отговор
```

## 7. Нефункционални изисквания

| Изискване | Стойност |
|-----------|----------|
| Време за пълен run (Phases 0-5) | < 30 минути |
| Време за cached run (без AI calls) | < 5 минути |
| Време за отговор на Q&A въпрос | < 15 секунди |
| Поддържани езици на Q&A | Български, English |
| Browser support | Chrome, Edge, Firefox (последни 2 версии) |
| Knowledge base freshness | < 24 часа (при daily schedule) |
| JSONL chunk max size | ~2000 tokens (оптимално за embedding) |

## 8. Ограничения на v1.0

- **dim_policy** (1 от 126 таблици) не е анализирана от AI поради дължина на SQL — ще бъде добавена
- **Bronze detail sheets** показват table-level info (SELECT *) — field-level lineage не е приложим за Bronze
- **RAG retrieval е keyword-based** — semantic search ще дойде в v2.0
- **Notebook extraction** извлича source code но не анализира логиката с AI (може да дойде в v1.1)
- **SemanticModel extraction** зависи от наличието на TMSL endpoint достъп
- **Report extraction** е report-level (pages, SM binding, filters) — не visual-level
- **Всеки потребител трябва да има API ключ** — shared key възможен чрез Azure Function App Setting

## 9. Roadmap

| Версия | Какво добавя |
|--------|-------------|
| **v1.0** | Documentation Engine + JSONL Knowledge Base + Excel docs + Web App с keyword RAG |
| **v1.1** | AI summaries за всеки chunk. dim_policy. AI анализ на Notebook логика. |
| **v1.2** | Visual-level lineage в Reports (кое поле в коя визуализация). |
| **v2.0** | Semantic search с vector embeddings (Azure AI Search) |
| **v2.1** | Auto-deploy на JSONL → vector store при всеки notebook run |
| **v3.0** | Teams integration — чат бот директно в Microsoft Teams |

---

*Следващ документ: Техническа спецификация*
