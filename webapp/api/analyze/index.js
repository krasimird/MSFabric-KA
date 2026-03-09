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

// ── Safe module imports (prevent host crash on cold start) ──
let BlobServiceClient, DefaultAzureCredential, SecretClient;
let moduleLoadError = null;
try {
  ({ BlobServiceClient } = require("@azure/storage-blob"));
  ({ DefaultAzureCredential } = require("@azure/identity"));
  ({ SecretClient } = require("@azure/keyvault-secrets"));
} catch (err) {
  moduleLoadError = `Failed to load Azure SDK modules: ${err.message}`;
}
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
const TIMEOUT_MS = 38000; // 38s hard limit — leave 7s margin for cache upload + response

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
    const code = err.statusCode || (err.details && err.details.errorCode) || "";
    if (code === 404 || String(err.message).includes("BlobNotFound") || String(err.message).includes("404")) {
      log(`${blobPath} not found (${code}), using fallback.`);
      return fallback;
    }
    log(`downloadJSONSafe error for ${blobPath}: ${err.message}`);
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
  // Build warehouse_id → name mapping
  const whNameMap = {};
  if (KB.workspaces) {
    for (const ws of KB.workspaces) {
      for (const item of (ws.items || [])) {
        if (item.type === 'Warehouse') whNameMap[item.id] = item.displayName || item.id;
      }
    }
  }
  for (const [itemId, val] of Object.entries(KB.schemas)) {
    if (!val || Array.isArray(val) || !val.item_type) continue;
    const warehouseName = whNameMap[itemId] || '';
    // Views
    for (const vw of (val.views || [])) {
      chunks.push({
        type: "warehouse_view",
        id: `${vw.schema || "dbo"}.${vw.name}`,
        warehouse_id: itemId,
        warehouse_name: warehouseName,
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
        warehouse_name: warehouseName,
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
// ═══════════════════════════════════════════════════════════════
// MAIN HANDLER
// ═══════════════════════════════════════════════════════════════
module.exports = async function (context, req) {
  context.log("Analyze function invoked");
  const log = (...args) => context.log.info(...args);
  const startTime = Date.now();
  const elapsed = () => ((Date.now() - startTime) / 1000).toFixed(1);

  // Fail fast if Azure SDK modules didn't load
  if (moduleLoadError) {
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: moduleLoadError }),
    };
    return;
  }

  // ── Test mode: incremental step testing ──
  const body = req.body || {};
  if (body.test) {
    const step = body.step || "blob";
    try {
      if (step === "blob") {
        const svc = getBlobClient();
        const container = svc.getContainerClient(CONTAINER);
        const exists = await container.exists();
        context.res = {
          status: 200,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ status: "test_ok", step, containerExists: exists, elapsed: elapsed() }),
        };
      } else if (step === "download") {
        const KB = await downloadJSON(RAW_BLOB, log);
        const queries = (KB.metadata && KB.metadata.queries) || [];
        context.res = {
          status: 200,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            status: "test_ok", step,
            rawKeys: Object.keys(KB).slice(0, 20),
            queryCount: queries.length,
            approxSizeMB: (JSON.stringify(KB).length / 1048576).toFixed(2),
            elapsed: elapsed(),
          }),
        };
      } else if (step === "apikey") {
        const apiKey = await getApiKey(log);
        context.res = {
          status: 200,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            status: "test_ok", step,
            hasApiKey: !!apiKey,
            keyPrefix: apiKey ? apiKey.slice(0, 8) + "..." : null,
            elapsed: elapsed(),
          }),
        };
      } else if (step === "full-dry") {
        // Run entire handler EXCEPT Claude API calls — to isolate crash point
        const apiKey = await getApiKey(log);
        log(`[${elapsed()}s] apiKey: ${!!apiKey}`);
        const KB2 = await downloadJSON(RAW_BLOB, log);
        const queries2 = (KB2.metadata && KB2.metadata.queries) || [];
        log(`[${elapsed()}s] queries: ${queries2.length}`);
        const cache2 = await downloadJSONSafe(CACHE_BLOB, {}, log);
        log(`[${elapsed()}s] cache entries: ${Object.keys(cache2).length}`);
        context.res = {
          status: 200,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            status: "test_ok", step,
            hasApiKey: !!apiKey,
            queryCount: queries2.length,
            cacheEntries: Object.keys(cache2).length,
            elapsed: elapsed(),
          }),
        };
      } else {
        context.res = {
          status: 400,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ error: "Unknown step. Use: blob, download, apikey, full-dry" }),
        };
      }
    } catch (err) {
      context.res = {
        status: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: err.message, stack: (err.stack || "").slice(0, 2000), step, elapsed: elapsed() }),
      };
    }
    return;
  }

  try {
    // 1. Get API key
    const apiKey = await getApiKey(log);
    if (!apiKey) {
      context.res = {
        status: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "ANTHROPIC_API_KEY not configured." }),
      };
      return;
    }
    log(`[${elapsed()}s] API key ready`);

    // 2. Read raw export from Blob
    log(`[${elapsed()}s] Step 1: Reading raw export from Blob...`);
    const KB = await downloadJSON(RAW_BLOB, log);
    const queries = (KB.metadata && KB.metadata.queries) || [];
    log(`[${elapsed()}s] Found ${queries.length} SQL queries to analyze.`);

    // 3. Load existing cache
    log(`[${elapsed()}s] Step 2: Loading analysis cache...`);
    const cache = await downloadJSONSafe(CACHE_BLOB, {}, log);
    log(`[${elapsed()}s] Cache has ${Object.keys(cache).length} entries.`);

    // 4. Analyze queries (with caching + hard timeout)
    log(`[${elapsed()}s] Step 3: Analyzing queries with Claude...`);
    const lineageByTable = {};
    let analyzed = 0, skipped = 0, failed = 0;
    let timedOut = false;
    let lastProcessedIndex = 0;

    // First pass: populate lineageByTable from cache (instant, no API calls)
    for (const q of queries) {
      const sql = q.source_query || "";
      if (!sql || sql.length < 20) { skipped++; continue; }
      const qHash = hashQuery(sql);
      const targetTable = q.target_table || q.meta_table || "unknown";
      const layer = q.layer || "";
      const mode = q.mode || "";
      if (cache[qHash]) {
        lineageByTable[targetTable] = { layer, mode, fields: cache[qHash] };
        skipped++;
      }
    }
    log(`[${elapsed()}s] ${skipped} queries already cached, ${queries.length - skipped} remaining.`);

    // Second pass: analyze uncached queries with timeout guard
    for (let i = 0; i < queries.length; i += BATCH_SIZE) {
      // ── Hard timeout check ──
      if (Date.now() - startTime > TIMEOUT_MS) {
        timedOut = true;
        lastProcessedIndex = i;
        log(`[${elapsed()}s] ⏱ Timeout approaching — stopping after ${i} queries.`);
        break;
      }

      const batch = queries.slice(i, i + BATCH_SIZE);
      const promises = batch.map(async (q) => {
        const sql = q.source_query || "";
        if (!sql || sql.length < 20) return;
        const qHash = hashQuery(sql);
        if (cache[qHash]) return; // already handled in first pass

        const targetTable = q.target_table || q.meta_table || "unknown";
        const layer = q.layer || "";
        const mode = q.mode || "";

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
          if (err.message.includes("429")) {
            log("  Rate limited — waiting 5s...");
            await sleep(5000);
          }
        }
      });

      await Promise.all(promises);
      lastProcessedIndex = Math.min(i + BATCH_SIZE, queries.length);
      log(`[${elapsed()}s] Progress: ${lastProcessedIndex}/${queries.length} (new=${analyzed}, cached=${skipped}, failed=${failed})`);

      // Check timeout again after batch completes
      if (Date.now() - startTime > TIMEOUT_MS) {
        timedOut = true;
        log(`[${elapsed()}s] ⏱ Timeout after batch — stopping.`);
        break;
      }

      if (i + BATCH_SIZE < queries.length) await sleep(BATCH_DELAY_MS);
    }

    // 5. Save cache (always — even on partial runs)
    log(`[${elapsed()}s] Saving cache (${Object.keys(cache).length} entries)...`);
    await uploadBlob(CACHE_BLOB, JSON.stringify(cache, null, 2), log);

    if (timedOut) {
      // Return partial result — frontend will re-trigger
      const summary = {
        status: "partial",
        elapsed_seconds: parseFloat(elapsed()),
        queries_total: queries.length,
        queries_analyzed: analyzed,
        queries_cached: skipped,
        queries_failed: failed,
        queries_remaining: queries.length - skipped - analyzed - failed,
        cache_entries: Object.keys(cache).length,
      };
      log(`[${elapsed()}s] Partial result:`, JSON.stringify(summary));
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(summary),
      };
      return;
    }

    // 6. Full completion — build final outputs
    log(`[${elapsed()}s] Step 4: Building execution chains...`);
    const chains = buildExecutionChains(KB);
    log(`Built ${chains.length} execution chains.`);

    log(`[${elapsed()}s] Step 5: Building warehouse lineage...`);
    const warehouseChunks = buildWarehouseLineage(KB);
    log(`Built ${warehouseChunks.length} warehouse chunks.`);

    log(`[${elapsed()}s] Step 6: Assembling JSONL...`);
    const jsonl = assembleJSONL(lineageByTable, chains, warehouseChunks);
    const lineCount = jsonl.split("\n").length;
    log(`JSONL: ${lineCount} lines, ${jsonl.length} bytes.`);

    log(`[${elapsed()}s] Step 7: Uploading to Blob Storage...`);
    await uploadBlob(JSONL_BLOB, jsonl, log);
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    await uploadBlob(`archive/${ts}_knowledge.jsonl`, jsonl, log);

    const summary = {
      status: "complete",
      elapsed_seconds: parseFloat(elapsed()),
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
    log(`[${elapsed()}s] Done!`, JSON.stringify(summary));

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(summary),
    };
  } catch (err) {
    // Only log strings — passing raw SDK error objects to context.log can crash the host
    context.log.error(`[${elapsed()}s] Fatal error: ${err.message}`);
    context.log.error(err.stack || "(no stack)");
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: err.message,
        stack: (err.stack || "").slice(0, 2000),
        elapsed_seconds: elapsed(),
      }),
    };
  }
};