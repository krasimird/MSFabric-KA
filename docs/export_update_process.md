# 🔄 Export & Update Process

## Overview

The GenDWH Knowledge Assistant uses metadata extracted from Microsoft Fabric. This metadata needs to be refreshed periodically to keep the assistant's answers up-to-date.

## Step-by-Step Update Process

### Step 1 — Run the Extraction Notebook

1. Open **Microsoft Fabric** → Navigate to the GenDWH workspace
2. Open the **`GenDWH_Documentation_LIB`** notebook
3. Click **Run All** to execute the extraction
4. **Expected runtime:** ~14 minutes
5. The notebook extracts: workspace metadata, table schemas, pipeline definitions, notebook source code, SQL queries, warehouse procedures/views, and Bronze mappings

### Step 2 — JSON Auto-Uploads to Blob Storage

The extraction notebook automatically uploads the output to Azure Blob Storage:
- **Storage account:** `sainspiritka`
- **Container:** `gendwh-exports`
- **Path:** `latest/gendwh_raw_export.json`

No manual upload is needed — this happens at the end of the notebook run.

### Step 3 — Trigger AI Analysis

The AI analysis processes all SQL queries in the export to generate field-level lineage data:

**Option A — Via the Web App:**
1. Open the Knowledge Assistant
2. In the sidebar, find the **🧠 AI Analysis** section
3. Click **"Analyze SQL Queries"**
4. Wait for completion (~16 minutes on first run, seconds if cached)

**Option B — Via API:**
```bash
curl -X POST https://kind-beach-0fdf0e803.1.azurestaticapps.net/api/analyze \
  -H "Content-Type: application/json" \
  -d "{}"
```

The analysis output is saved as:
- **Path:** `latest/gendwh_knowledge.jsonl`
- **Format:** JSON Lines (one chunk per line)

### Step 4 — Users Refresh Browser

After the analysis completes:
1. Users should **refresh their browser** (F5 or Ctrl+R)
2. The app automatically loads the latest data from Blob Storage
3. The sidebar will show updated **Platform Stats** and **Knowledge base loaded** status

### Step 5 — Verification

Check that the update was successful:

1. **Blob Storage** — Verify `latest/` folder has fresh timestamps:
   ```
   az storage blob list --account-name sainspiritka --container-name gendwh-exports --prefix "latest/" --output table
   ```
2. **Web App** — Sidebar should show "✅ Knowledge base loaded — N chunks"
3. **Test a query** — Ask about a recently changed table or field

## Recommended Schedule

| Frequency | Action |
|-----------|--------|
| **Weekly** | Run extraction notebook + AI analysis |
| **After schema changes** | Run extraction immediately |
| **After pipeline changes** | Run extraction to capture new definitions |

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Data loaded" but no lineage answers | AI analysis not run | Click "Analyze SQL Queries" |
| Stale data in answers | Extraction not refreshed | Re-run notebook |
| Analysis fails | API key issue or timeout | Check App Settings in Azure portal |
| Blob upload fails | Notebook token expired | Re-authenticate in Fabric |

