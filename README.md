# GenDWH Knowledge Assistant

AI-powered documentation and Q&A system for the GenDWH Data Warehouse platform on Microsoft Fabric.

## Architecture (v2.0)

Two cleanly separated components:

### 1. Universal Extraction (Fabric Notebook)
A single lightweight notebook (`GenDWH_KA_Extraction`) that runs inside Fabric and:
- Discovers all workspaces and items via REST API
- Extracts raw definitions via `getDefinition` for every item type
- Extracts table schemas via Spark SQL (Lakehouses + Warehouses)
- Extracts metadata queries from `gen_adm_*` tables
- Exports everything to a single JSON file (`gendwh_raw_export.json`)
- **No AI, no external API calls, no secrets**

### 2. Knowledge Assistant (Web App)
An Azure Static Web App that is the AI brain:
- Loads the raw JSON export from extraction
- AI-analyzes SQL queries → field-level lineage (cached)
- RAG Q&A — answers questions using platform knowledge
- Chat Skills — generates documents on-demand:
  - Data Lineage Excel (with InspirIT styling and navigation)
  - Data Dictionary Excel
  - Business Glossary Word
  - Custom reports
- Entra ID authentication
- Claude API key server-side only (Azure Function)

## Repository Structure

```
MSFabric-KA/
├── notebooks/                  # Fabric notebook source (.py → .ipynb)
│   ├── GenDWH_KA_Extraction.py # Universal extraction notebook
│   └── legacy/                 # Reference: original PoC notebook
├── webapp/                     # RAG Web App (Azure Static Web Apps)
│   ├── src/                    # Frontend (chat UI, RAG, skills)
│   ├── api/chat/               # Azure Function (Claude proxy)
│   └── staticwebapp.config.json
├── docs/                       # Specification documents
├── scripts/                    # Build & utility scripts
│   └── py_to_ipynb.py          # Notebook converter
├── build/                      # Generated .ipynb files (git-ignored)
└── .github/workflows/          # CI/CD
```

## Quick Start — Notebook

```bash
python scripts/py_to_ipynb.py
```

Converts `notebooks/*.py` → `build/*.ipynb` for import into Fabric.

## Quick Start — Web App

```bash
cd webapp
npm install
npm run dev
```

Requires `.env` — see `webapp/.env.example` for required secrets.

## Documentation

| Document | Description |
|----------|-------------|
| [Product Vision](docs/01_Product_Vision.md) | Problem, vision, success criteria |
| [Functional Spec](docs/02_Functional_Specification.md) | Features, user stories, UI |
| [Technical Spec](docs/03_Technical_Specification.md) | Architecture, APIs, data flow |
| [Implementation Plan](docs/04_Implementation_Plan.md) | Sprints, tasks, timeline |

