# 📖 GenDWH Knowledge Assistant — Onboarding Guide

## What is GenDWH Knowledge Assistant?

GenDWH Knowledge Assistant is an AI-powered chat application for the **GenDWH Data Warehouse** platform built on Microsoft Fabric. It provides:

- **Chat Q&A** — Ask questions about tables, pipelines, lineage, schemas, notebooks, and more
- **Document Generation** — Generate Data Lineage Excel, Data Dictionary, and Business Glossary reports with one click
- **Full Lineage Tracing** — Trace field-level data flow from Platinum (Warehouse) through Gold, Silver, down to Bronze (source systems)

## How to Access

1. Open **[https://kind-beach-0fdf0e803.1.azurestaticapps.net](https://kind-beach-0fdf0e803.1.azurestaticapps.net)**
2. Sign in with your **Microsoft corporate account** (InspirIT tenant)
3. The chat interface loads automatically with the latest data warehouse metadata

> **Note:** Only InspirIT tenant users can access the application. External accounts are not supported.

## What You Can Ask

### General Questions
- "What tables are in the Gold layer?"
- "Show me all pipelines and their activities"
- "What stored procedures exist in the warehouses?"

### Lineage Questions (Bulgarian & English)
- "Откъде идва полето `suma` в `stg_claim_expenses`?"
- "Trace lineage of `premium_paid_amount_bgn_mode` from Platinum to Bronze"
- "Покажи пълния lineage на `dim_policy`"

### Schema & Structure
- "What columns does `fact_premium` have?"
- "Какви са типовете данни в `stg_broker_commission`?"

## Document Generation

Use the **sidebar buttons** to generate Excel reports:

| Button | Output |
|--------|--------|
| 📥 Generate Data Lineage Excel | Multi-sheet Excel with full chain navigation Platinum→Gold→Silver→Bronze |
| 📥 Generate Data Dictionary | All Lakehouses + Warehouses with columns, types, nullable info |
| 📥 Generate Business Glossary | AI-generated definitions for ~100 terms, categorized by domain |

Reports are downloaded as `.xlsx` files with InspirIT corporate styling.

## Tips for Better Results

1. **Be specific** — Use exact table and field names: `stg_claim_expenses.suma` not just "suma"
2. **Mention the layer** — "Gold layer", "Platinum warehouse", "Bronze mapping"
3. **Ask in any language** — The assistant responds in the same language as your question (Bulgarian or English)
4. **Use the Quick Questions** — The sidebar has pre-built questions for common tasks
5. **Chain queries** — Ask follow-up questions; the assistant remembers context from the last 10 messages

## Known Limitations

- **Pipeline activity details** may be incomplete for some complex pipelines
- **Bronze layer chain** may be partial if source system metadata is not fully extracted
- **AI analysis** needs to be run at least once to enable lineage-based answers (click "Analyze SQL Queries" in sidebar)
- **Large reports** (Data Dictionary with many tables) may take 10-30 seconds to generate

## Need Help?

If you encounter issues or have questions:
- Contact the **Data Engineering team** at InspirIT
- Report bugs via the [GitHub repository](https://github.com/krasimird/MSFabric-KA/issues)

