/**
 * Azure Function — POST /api/analyze
 *
 * Server-side AI analysis pipeline:
 *   1. Reads gendwh_raw_export.json from Blob Storage
 *   2. Analyzes SQL queries with Claude for field-level lineage
 *   3. Builds execution chains (Pipeline → Notebook → SQL → Tables)
 *   4. Includes Warehouse stored procedures in lineage
 *   5. Assembles JSONL knowledge base
 *   6. Uploads to Blob Storage (latest + archive)
 *
 * Caching: stores analysis_cache.json in Blob — skips unchanged queries.
 * Rate limiting: 5 parallel queries, 1s between batches.
 */

const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");
const crypto = require("crypto");

// ── Config ──────────────────────────────────────────────────
const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-sonnet-4-20250514";
const MAX_TOKENS = 8192;

const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const KV_SECRET_NAME = "anthropicapikey";

const CONTAINER = "gendwh-exports";
const RAW_BLOB = "latest/gendwh_raw_export.json";
const JSONL_BLOB = "latest/gendwh_knowledge.jsonl";
const CACHE_BLOB = "latest/analysis_cache.json";

const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 1000;

// ── Shared state (cached per function instance) ─────────────
let cachedApiKey = null;

// ── API Key (reuse pattern from /api/chat) ──────────────────
async function getApiKey(log) {
  if (cachedApiKey) return cachedApiKey;
  if (process.env.ANTHROPIC_API_KEY) {
    cachedApiKey = process.env.ANTHROPIC_API_KEY;
    return cachedApiKey;
  }
  try {
    const cred = new DefaultAzureCredential();
    const client = new SecretClient(KV_URL, cred);
    const secret = await client.getSecret(KV_SECRET_NAME);
    cachedApiKey = secret.value;
    log("Anthropic API key loaded from Key Vault.");
    return cachedApiKey;
  } catch (err) {
    log("Failed to fetch API key from Key Vault:", err.message);
    return null;
  }
}

// ── Load local.settings.json fallback (SWA CLI doesn't always inject Values as env vars) ──
function loadLocalSettings() {
  try {
    const path = require("path");
    const fs = require("fs");
    const fp = path.join(__dirname, "..", "local.settings.json");
    const data = JSON.parse(fs.readFileSync(fp, "utf8"));
    for (const [k, v] of Object.entries(data.Values || {})) {
      if (!process.env[k] && v) process.env[k] = v;
    }
  } catch { /* not critical */ }
}
loadLocalSettings();

// ── Blob helpers ────────────────────────────────────────────
function getBlobClient() {
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) throw new Error("BLOB_CONNECTION_STRING not configured");
  return BlobServiceClient.fromConnectionString(connStr);
}

async function downloadJSON(blobPath, log) {
  const svc = getBlobClient();
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  log(`Downloading ${blobPath}...`);
  const resp = await blob.download(0);
  const chunks = [];
  for await (const chunk of resp.readableStreamBody) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function uploadBlob(blobPath, content, log) {
  const svc = getBlobClient();
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  const buf = Buffer.from(content, "utf8");
  await blob.upload(buf, buf.length, {
    blobHTTPHeaders: { blobContentType: blobPath.endsWith(".json") ? "application/json" : "application/x-ndjson" }
  });
  log(`Uploaded ${blobPath} (${buf.length} bytes)`);
}

async function downloadJSONSafe(blobPath, fallback, log) {
  try { return await downloadJSON(blobPath, log); }
  catch (err) {
    if (err.statusCode === 404) { log(`${blobPath} not found, using fallback.`); return fallback; }
    throw err;
  }
}

// ── Query hashing ───────────────────────────────────────────
function hashQuery(sql) {
  return crypto.createHash("sha256").update(sql).digest("hex").slice(0, 16);
}

// ── Claude API call ─────────────────────────────────────────
const LINEAGE_SYSTEM_PROMPT = `You are a SQL lineage analyzer for a Microsoft Fabric data warehouse.

Given a SQL query (INSERT INTO ... SELECT or CREATE VIEW), extract field-level lineage.

For EACH target field, return:
- target_field: exact column name in the target table
- data_type: if determinable from CAST/CONVERT or context
- source_table: the ORIGINAL source table (resolve through CTEs to the base table, not CTE aliases)
- source_column: the source column name
- transformation_type: one of: direct_map, cast, case_when, coalesce, arithmetic, hash, concat, literal, aggregate, lookup, window_function, iif, expression
- expression: the SQL expression (abbreviated if very long)
- business_logic: 1-sentence human-readable explanation
- join_key: if the field comes through a JOIN, which key was used

Return ONLY valid JSON array. No markdown, no explanation.`;

async function analyzeQuery(apiKey, targetTable, layer, mode, sql, log) {
  const userMsg = `Analyze this ${layer} ${mode} SQL query for table "${targetTable}":\n\n${sql.slice(0, 15000)}`;
  const resp = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: MAX_TOKENS,
      system: LINEAGE_SYSTEM_PROMPT,
      messages: [{ role: "user", content: userMsg }],
    }),
  });
  if (!resp.ok) {
    const errText = await resp.text().catch(() => "");
    throw new Error(`Claude API ${resp.status}: ${errText.slice(0, 200)}`);
  }
  const data = await resp.json();
  const text = data.content ? data.content.map(c => c.text).join("") : "";
  return parseLineageJSON(text);
}

function parseLineageJSON(text) {
  try {
    const start = text.indexOf("[");
    const end = text.lastIndexOf("]");
    if (start === -1 || end === -1) return null;
    return JSON.parse(text.slice(start, end + 1));
  } catch { return null; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Execution chain builder ─────────────────────────────────
function buildExecutionChains(KB) {
  const chains = [];
  if (!KB.workspaces) return chains;
  let id = 0;
  for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      if (item.type !== "DataPipeline" || !item.definition) continue;
      const def = typeof item.definition === "string" ? item.definition : JSON.stringify(item.definition);
      const nbMatches = def.match(/Phase_\d+_\w+|GenDWH_\w+/g) || [];
      const notebooks = [...new Set(nbMatches)];
      const queries = (KB.metadata && KB.metadata.queries) || [];
      const affectedTables = queries.map(q => q.target_table).filter(Boolean);
      chains.push({
        type: "execution_chain",
        id: `chain_${id++}`,
        pipeline: item.name,
        workspace: ws.name,
        notebooks,
        target_tables: [...new Set(affectedTables)].slice(0, 50),
        activity_count: (def.match(/activity/gi) || []).length,
      });
    }
  }
  return chains;
}

// ── Warehouse stored procedure lineage ──────────────────────
function buildWarehouseLineage(KB) {
  const chunks = [];
  if (!KB.schemas) return chunks;
  for (const [itemId, val] of Object.entries(KB.schemas)) {
    if (!val || Array.isArray(val) || !val.item_type) continue;
    // Views
    for (const vw of (val.views || [])) {
      chunks.push({
        type: "warehouse_view",
        id: `${vw.schema || "dbo"}.${vw.name}`,
        warehouse_id: itemId,
        schema: vw.schema || "dbo",
        name: vw.name,
        definition: (vw.definition || "").slice(0, 4000),
      });
    }
    // Stored procedures
    for (const sp of (val.procedures || [])) {
      chunks.push({
        type: "warehouse_sproc",
        id: `${sp.schema || "dbo"}.${sp.name}`,
        warehouse_id: itemId,
        schema: sp.schema || "dbo",
        name: sp.name,
        definition: (sp.definition || "").slice(0, 4000),
        proc_type: sp.type || "PROCEDURE",
      });
    }
  }
  return chunks;
}

// ── Assemble JSONL ──────────────────────────────────────────
function assembleJSONL(lineageByTable, chains, warehouseChunks) {
  const lines = [];

  // Table lineage chunks
  for (const [table, record] of Object.entries(lineageByTable)) {
    const fields = record.fields || [];
    const sourceTables = [...new Set(fields.map(f => f.source_table).filter(Boolean))];
    const transformTypes = [...new Set(fields.map(f => f.transformation_type).filter(Boolean))];

    // table_lineage chunk
    lines.push(JSON.stringify({
      type: "table_lineage",
      id: table,
      layer: record.layer || "",
      mode: record.mode || "",
      source_tables: sourceTables,
      field_count: fields.length,
      transformation_types: transformTypes,
      summary: `${record.layer} ${record.mode} table with ${fields.length} fields from ${sourceTables.join(", ") || "unknown"}`,
      fields: fields.map(f => f.target_field),
    }));

    // field_detail chunks
    for (const f of fields) {
      lines.push(JSON.stringify({
        type: "field_detail",
        id: `${table}.${f.target_field}`,
        table: table,
        layer: record.layer || "",
        target_field: f.target_field,
        data_type: f.data_type || "",
        source_table: f.source_table || "",
        source_column: f.source_column || "",
        transformation_type: f.transformation_type || "",
        expression: f.expression || "",
        business_logic: f.business_logic || "",
        join_key: f.join_key || null,
      }));
    }
  }

  // Execution chain chunks
  for (const c of chains) lines.push(JSON.stringify(c));

  // Warehouse view/sproc chunks
  for (const w of warehouseChunks) lines.push(JSON.stringify(w));

  return lines.join("\n");
}


// ═══════════════════════════════════════════════════════════════
// MAIN HANDLER
// ═══════════════════════════════════════════════════════════════
module.exports = async function (context, req) {
  const log = (...args) => context.log.info(...args);
  const startTime = Date.now();

  try {
    // 1. Get API key
    const apiKey = await getApiKey(log);
    if (!apiKey) {
      context.res = { status: 500, body: { error: "ANTHROPIC_API_KEY not configured." } };
      return;
    }

    // 2. Read raw export from Blob
    log("Step 1: Reading raw export from Blob...");
    const KB = await downloadJSON(RAW_BLOB, log);
    const queries = (KB.metadata && KB.metadata.queries) || [];
    log(`Found ${queries.length} SQL queries to analyze.`);

    // 3. Load existing cache
    log("Step 2: Loading analysis cache...");
    const cache = await downloadJSONSafe(CACHE_BLOB, {}, log);
    log(`Cache has ${Object.keys(cache).length} entries.`);

    // 4. Analyze queries (with caching)
    log("Step 3: Analyzing queries with Claude...");
    const lineageByTable = {};
    let analyzed = 0, skipped = 0, failed = 0;

    for (let i = 0; i < queries.length; i += BATCH_SIZE) {
      const batch = queries.slice(i, i + BATCH_SIZE);
      const promises = batch.map(async (q) => {
        const sql = q.source_query || "";
        if (!sql || sql.length < 20) { skipped++; return; }

        const qHash = hashQuery(sql);
        const targetTable = q.target_table || q.meta_table || "unknown";
        const layer = q.layer || "";
        const mode = q.mode || "";

        // Check cache
        if (cache[qHash]) {
          lineageByTable[targetTable] = { layer, mode, fields: cache[qHash] };
          skipped++;
          return;
        }

        // Call Claude
        try {
          const fields = await analyzeQuery(apiKey, targetTable, layer, mode, sql, log);
          if (fields && fields.length > 0) {
            cache[qHash] = fields;
            lineageByTable[targetTable] = { layer, mode, fields };
            analyzed++;
            log(`  ✓ ${targetTable}: ${fields.length} fields`);
          } else {
            log(`  ⚠ ${targetTable}: no fields parsed`);
            failed++;
          }
        } catch (err) {
          log(`  ✗ ${targetTable}: ${err.message}`);
          failed++;
          // Retry on 429 (rate limit)
          if (err.message.includes("429")) {
            log("  Rate limited — waiting 10s...");
            await sleep(10000);
          }
        }
      });

      await Promise.all(promises);
      log(`Progress: ${Math.min(i + BATCH_SIZE, queries.length)}/${queries.length} (analyzed=${analyzed}, cached=${skipped}, failed=${failed})`);

      if (i + BATCH_SIZE < queries.length) await sleep(BATCH_DELAY_MS);
    }

    // 5. Build execution chains
    log("Step 4: Building execution chains...");
    const chains = buildExecutionChains(KB);
    log(`Built ${chains.length} execution chains.`);

    // 6. Build warehouse lineage (views + sprocs)
    log("Step 5: Building warehouse lineage...");
    const warehouseChunks = buildWarehouseLineage(KB);
    log(`Built ${warehouseChunks.length} warehouse chunks.`);

    // 7. Assemble JSONL
    log("Step 6: Assembling JSONL...");
    const jsonl = assembleJSONL(lineageByTable, chains, warehouseChunks);
    const lineCount = jsonl.split("\n").length;
    log(`JSONL: ${lineCount} lines, ${jsonl.length} bytes.`);

    // 8. Upload JSONL to Blob (latest + archive)
    log("Step 7: Uploading to Blob Storage...");
    await uploadBlob(JSONL_BLOB, jsonl, log);

    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    await uploadBlob(`archive/${ts}_knowledge.jsonl`, jsonl, log);

    // 9. Upload updated cache
    await uploadBlob(CACHE_BLOB, JSON.stringify(cache, null, 2), log);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const summary = {
      status: "complete",
      elapsed_seconds: parseFloat(elapsed),
      queries_total: queries.length,
      queries_analyzed: analyzed,
      queries_cached: skipped,
      queries_failed: failed,
      lineage_tables: Object.keys(lineageByTable).length,
      execution_chains: chains.length,
      warehouse_chunks: warehouseChunks.length,
      jsonl_lines: lineCount,
      jsonl_bytes: jsonl.length,
    };
    log("Done!", JSON.stringify(summary));

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: summary,
    };
  } catch (err) {
    context.log.error("Analysis failed:", err);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: { error: err.message, elapsed_seconds: ((Date.now() - startTime) / 1000).toFixed(1) },
    };
  }
};