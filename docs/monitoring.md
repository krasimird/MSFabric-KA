# 📊 Monitoring & Costs

## Component Overview

| Component | Service | Expected Cost |
|-----------|---------|---------------|
| Web Application | Azure Static Web Apps (Free tier) | **$0/month** |
| Blob Storage | Azure Storage (sainspiritka) | **< $0.10/month** |
| Key Vault | Azure Key Vault (kv-ai-site-builder) | **< $0.10/month** |
| AI API | Anthropic Claude API | **~$2/month** |
| Data Extraction | Microsoft Fabric Notebook | Included in Fabric capacity |

**Total estimated cost: ~$2-3/month**

## Fabric Notebook

- **Notebook:** `GenDWH_Documentation_LIB`
- **Expected runtime:** ~14 minutes per execution
- **What to monitor:**
  - Notebook completes without errors
  - Output JSON is uploaded to Blob Storage
  - File size is reasonable (typically 5-15 MB)

### Health Check
```bash
az storage blob show --account-name sainspiritka --container-name gendwh-exports \
  --name "latest/gendwh_raw_export.json" --query "{size:properties.contentLength, modified:properties.lastModified}" -o table
```

## Claude API Usage

- **Provider:** Anthropic (Claude Sonnet)
- **Usage pattern:** ~$2/month with normal usage
- **Cost alert threshold:** $20/month (investigate if exceeded)

### What consumes API tokens:
1. **Chat questions** — Each user question sends context + history to Claude (~2K-8K tokens per query)
2. **AI Analysis** — One-time batch analysis of SQL queries (~100K tokens total, runs once per export)
3. **Business Glossary** — Generates definitions in batches (~40K tokens per generation)

### Monitor Usage
- Check the [Anthropic Console](https://console.anthropic.com/) for usage dashboard
- API key is stored in Azure Key Vault (`kv-ai-site-builder` / `anthropicapikey`)

## Azure Static Web App

- **Tier:** Free
- **URL:** [https://kind-beach-0fdf0e803.1.azurestaticapps.net](https://kind-beach-0fdf0e803.1.azurestaticapps.net)
- **Authentication:** Entra ID (InspirIT tenant only)
- **CI/CD:** Automatic deploy on push to `main` branch

### Health Check
```bash
# Should return 302 (redirect to login) for unauthenticated requests
curl -s -o /dev/null -w "%{http_code}" https://kind-beach-0fdf0e803.1.azurestaticapps.net
```

## Azure Blob Storage

- **Account:** `sainspiritka`
- **Container:** `gendwh-exports`
- **Key paths:**
  - `latest/gendwh_raw_export.json` — Raw metadata export
  - `latest/gendwh_knowledge.jsonl` — AI-analyzed knowledge base
  - `docs/` — Documentation markdown files

### Health Check
```bash
az storage blob list --account-name sainspiritka --container-name gendwh-exports \
  --prefix "latest/" --query "[].{name:name, size:properties.contentLength, modified:properties.lastModified}" -o table
```

## Quick Health Dashboard

Run these checks to verify everything is working:

| Check | How | Expected |
|-------|-----|----------|
| Web app loads | Open URL in browser | Redirects to Microsoft login |
| API responds | POST to /api/chat after login | JSON response from Claude |
| Blob data fresh | Check `lastModified` on raw export | Within last 7 days |
| Knowledge base | Check sidebar after login | "✅ Knowledge base loaded — N chunks" |
| Extraction notebook | Check Fabric monitoring | Last run succeeded |

## Alerting

Set up these alerts in the Azure Portal:

1. **Blob Storage** — Alert if `gendwh_raw_export.json` hasn't been modified in 14+ days
2. **Claude API** — Alert if monthly cost exceeds $20 (check Anthropic Console)
3. **SWA** — Monitor GitHub Actions for failed deployments

## Cost Optimization Tips

- The Free tier SWA supports up to 100 GB bandwidth/month — more than enough
- Blob Storage costs are negligible at current data volumes
- Claude API is the main variable cost — limit batch analysis runs
- Key Vault charges per operation — the caching in the API function minimizes calls

