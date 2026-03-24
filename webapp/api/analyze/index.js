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
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";

const EMBEDDING_MODEL = "text-embedding-ada-002";
const EMBED_BATCH_SIZE = 20;
const EMBED_BATCH_DELAY_MS = 500;

const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 1000;
const TIMEOUT_MS = 38000; // 38s hard limit — leave 7s margin for cache upload + response

// ── Shared state (cached per function instance) ─────────────
let cachedApiKey = null;
let cachedOpenAIKey = null;
let cachedOpenAIEndpoint = null;

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

// ── Azure OpenAI credentials ─────────────────────────────────
async function getOpenAICredentials(log) {
  if (cachedOpenAIKey && cachedOpenAIEndpoint) return { apiKey: cachedOpenAIKey, endpoint: cachedOpenAIEndpoint };
  if (process.env.AZURE_OPENAI_KEY && process.env.AZURE_OPENAI_ENDPOINT) {
    cachedOpenAIKey = process.env.AZURE_OPENAI_KEY;
    cachedOpenAIEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
    return { apiKey: cachedOpenAIKey, endpoint: cachedOpenAIEndpoint };
  }
  try {
    const cred = new DefaultAzureCredential();
    const kv = new SecretClient(KV_URL, cred);
    if (!cachedOpenAIKey) cachedOpenAIKey = (await kv.getSecret("azure-openai-key")).value;
    if (!cachedOpenAIEndpoint) cachedOpenAIEndpoint = (await kv.getSecret("azure-openai-endpoint")).value;
    log("Azure OpenAI credentials loaded from Key Vault.");
    return { apiKey: cachedOpenAIKey, endpoint: cachedOpenAIEndpoint };
  } catch (err) {
    log("Failed to fetch OpenAI credentials from Key Vault:", err.message);
    return null;
  }
}

// ── Embedding helpers ────────────────────────────────────────
function chunkToText(chunk) {
  const parts = [];
  if (chunk.type) parts.push(`type: ${chunk.type}`);
  if (chunk.id) parts.push(`id: ${chunk.id}`);
  if (chunk.table) parts.push(`table: ${chunk.table}`);
  if (chunk.table_name) parts.push(`table: ${chunk.table_name}`);
  if (chunk.lakehouse) parts.push(`lakehouse: ${chunk.lakehouse}`);
  if (chunk.model_name) parts.push(`model: ${chunk.model_name}`);
  if (chunk.pipeline) parts.push(`pipeline: ${chunk.pipeline}`);
  if (chunk.notebook) parts.push(`notebook: ${chunk.notebook}`);
  if (chunk.report_name) parts.push(`report: ${chunk.report_name}`);
  if (chunk.layer) parts.push(`layer: ${chunk.layer}`);
  if (chunk.zone) parts.push(`zone: ${chunk.zone}`);
  if (chunk.warehouse_name) parts.push(`warehouse: ${chunk.warehouse_name}`);
  if (chunk.summary) parts.push(chunk.summary);
  if (chunk.target_field) parts.push(`field: ${chunk.target_field}`);
  if (chunk.source_table) parts.push(`source: ${chunk.source_table}.${chunk.source_column || ""}`);
  if (chunk.expression) parts.push(`expr: ${chunk.expression}`);
  if (chunk.business_logic) parts.push(chunk.business_logic);
  if (chunk.definition) parts.push(chunk.definition.slice(0, 2000));
  if (chunk.description) parts.push(chunk.description.slice(0, 2000));
  if (chunk.tables_detail) parts.push(JSON.stringify(chunk.tables_detail).slice(0, 3000));
  if (chunk.measures) parts.push(chunk.measures.map(m => `${m.name}: ${m.expression || ""}`).join("; ").slice(0, 3000));
  if (chunk.columns) parts.push(chunk.columns.map(c => c.name || c).join(", ").slice(0, 1000));
  if (chunk.items) parts.push((Array.isArray(chunk.items) ? chunk.items : []).join(", ").slice(0, 2000));
  if (chunk.schema && chunk.name) parts.push(`${chunk.schema}.${chunk.name}`);
  if (chunk.table_references) parts.push(`references: ${chunk.table_references.join(", ").slice(0, 1000)}`);
  if (chunk.activities) parts.push(`activities: ${chunk.activities.map(a => a.name || a).join(", ").slice(0, 1000)}`);
  return parts.join("\n") || JSON.stringify(chunk).slice(0, 4000);
}

function chunkHash(chunk) {
  const text = chunkToText(chunk);
  return crypto.createHash("sha256").update(text).digest("hex").slice(0, 16);
}

async function embedBatch(texts, apiKey, endpoint) {
  const url = `${endpoint}openai/deployments/${EMBEDDING_MODEL}/embeddings?api-version=2023-05-15`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "api-key": apiKey },
    body: JSON.stringify({ input: texts }),
  });
  if (!resp.ok) {
    const errText = await resp.text().catch(() => "");
    throw new Error(`Embedding API ${resp.status}: ${errText.slice(0, 300)}`);
  }
  const data = await resp.json();
  return data.data.map(d => d.embedding);
}

async function downloadTextSafe(container, blobPath) {
  const blob = container.getBlockBlobClient(blobPath);
  try {
    const resp = await blob.download(0);
    const chunks = [];
    for await (const c of resp.readableStreamBody) chunks.push(c);
    return Buffer.concat(chunks).toString("utf8");
  } catch (err) {
    if (String(err.statusCode) === "404" || String(err.message).includes("BlobNotFound")) return null;
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
    // Tables
    for (const tbl of (val.tables || [])) {
      chunks.push({
        type: "warehouse_table",
        id: `${tbl.schema || "dbo"}.${tbl.name}`,
        warehouse_id: itemId,
        warehouse_name: warehouseName,
        schema: tbl.schema || "dbo",
        name: tbl.name,
        columns: (tbl.columns || []).map(c => ({ name: c.name, dataType: c.dataType, nullable: c.nullable })),
        column_count: (tbl.columns || []).length,
      });
    }
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

// ── Helper: build item lookup (id → {name, type, workspace}) ─
function buildItemLookup(KB) {
  const map = {};
  if (!KB.workspaces) return map;
  for (const ws of KB.workspaces) {
    const wsName = ws.displayName || ws.name || '';
    for (const item of (ws.items || [])) {
      map[item.id] = { name: item.displayName || item.name || item.id, type: item.type, workspace: wsName };
    }
  }
  return map;
}

// ── Detect data zone from Lakehouse/Warehouse name ──────────
function detectZone(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('bronze')) return 'Bronze';
  if (n.includes('silverraw') || n.includes('silver_raw')) return 'Silver Raw';
  if (n.includes('silverstg') || n.includes('silver_stg') || n.includes('silver_stage')) return 'Silver Stage';
  if (n.includes('gold')) return 'Gold';
  if (n.includes('cdwh') || n.includes('platinum')) return 'Platinum';
  if (n.includes('administration') || n.includes('admin')) return 'Administration';
  return 'Unknown';
}

// ── Lakehouse chunks (schema arrays) ────────────────────────
function buildLakehouseChunks(KB, itemLookup) {
  const chunks = [];
  if (!KB.schemas) return chunks;
  for (const [itemId, val] of Object.entries(KB.schemas)) {
    if (!val || !Array.isArray(val)) continue; // Lakehouse schemas are arrays
    const info = itemLookup[itemId] || {};
    const lhName = info.name || itemId;
    const wsName = info.workspace || '';
    const zone = detectZone(lhName);
    for (const tbl of val) {
      const cols = (tbl.columns || []).map(c => ({ name: c.name, dataType: c.dataType, nullable: c.nullable }));
      chunks.push({
        type: "lakehouse_table",
        id: `${lhName}::${tbl.table_name}`,
        lakehouse: lhName,
        workspace: wsName,
        zone: zone,
        table_name: tbl.table_name,
        table_type: tbl.table_type || 'managed',
        columns: cols,
        column_count: cols.length,
      });
    }
  }
  return chunks;
}

// ── Bronze metadata chunks ──────────────────────────────────
function buildBronzeMetaChunks(KB) {
  const chunks = [];
  if (!KB.metadata || !KB.metadata.bronze_meta) return chunks;
  for (const entry of KB.metadata.bronze_meta) {
    const tblName = entry.table_name || entry.name || '';
    if (!tblName) continue;
    const cols = (entry.columns || []).map(c => ({ name: c.name, dataType: c.dataType || c.data_type, nullable: c.nullable }));
    chunks.push({
      type: "bronze_table",
      id: `bronze::${tblName}`,
      table_name: tblName,
      zone: "Bronze",
      source_system: entry.source_system || entry.source || '',
      columns: cols,
      column_count: cols.length,
      row_count: entry.row_count || null,
      last_loaded: entry.last_loaded || entry.last_modified || null,
    });
  }
  return chunks;
}

// ── TMDL table parser ───────────────────────────────────────
function _parseTmdlTable(payload) {
  const lines = payload.split('\n');
  if (lines.length === 0) return null;
  const tblMatch = lines[0].match(/^table\s+(?:'([^']+)'|"([^"]+)"|(.+))/);
  if (!tblMatch) return null;
  const tableName = (tblMatch[1] || tblMatch[2] || tblMatch[3] || '').trim();
  const result = { name: tableName, columns: [], measures: [], isCalculated: false };
  let i = 1;
  while (i < lines.length) {
    const trimmed = lines[i].trimStart();
    if (/^column\s+/i.test(trimmed)) {
      const col = _parseTmdlColumn(lines, i);
      result.columns.push(col.data);
      i = col.nextLine; continue;
    }
    if (/^measure\s+/i.test(trimmed)) {
      const meas = _parseTmdlMeasure(lines, i);
      result.measures.push(meas.data);
      i = meas.nextLine; continue;
    }
    if (/^partition\s+/i.test(trimmed)) {
      if (/=\s*calculated/i.test(trimmed)) result.isCalculated = true;
    }
    i++;
  }
  return result;
}

function _parseTmdlColumn(lines, startIdx) {
  const header = lines[startIdx].trimStart();
  const nameMatch = header.match(/^column\s+(?:'([^']+)'|"([^"]+)"|(\S+))/i);
  const name = nameMatch ? (nameMatch[1] || nameMatch[2] || nameMatch[3] || '').trim() : 'unknown';
  const col = { name };
  let i = startIdx + 1;
  while (i < lines.length) {
    const trimmed = lines[i].trimStart();
    if (/^(column|measure|hierarchy|partition|table|annotation\s+PBI_Id)\s*/i.test(trimmed) && trimmed !== '') break;
    if (/^dataType:\s*/i.test(trimmed)) col.dataType = trimmed.split(':')[1].trim();
    else if (/^sourceColumn:\s*/i.test(trimmed)) col.sourceColumn = trimmed.split(':').slice(1).join(':').trim();
    else if (/^summarizeBy:\s*/i.test(trimmed)) col.summarizeBy = trimmed.split(':')[1].trim();
    else if (/^isHidden/i.test(trimmed) && !trimmed.includes(':')) col.isHidden = true;
    else if (/^description:\s*/i.test(trimmed)) col.description = trimmed.replace(/^description:\s*/i, '').trim();
    i++;
  }
  return { data: col, nextLine: i };
}

function _parseTmdlMeasure(lines, startIdx) {
  const header = lines[startIdx].trimStart();
  const mMatch = header.match(/^measure\s+(?:'([^']+)'|"([^"]+)"|(\S+?))\s*=/i);
  const name = mMatch ? (mMatch[1] || mMatch[2] || mMatch[3] || '').trim() : 'unknown';
  const eqIdx = header.indexOf('=');
  let afterEq = eqIdx >= 0 ? header.slice(eqIdx + 1).trim() : '';
  const exprLines = [];
  let inBacktickBlock = false;
  if (afterEq.startsWith('```')) { inBacktickBlock = true; exprLines.push(afterEq); }
  else if (afterEq) exprLines.push(afterEq);
  const mData = { name, expression: '', formatString: '', description: '', displayFolder: '' };
  let i = startIdx + 1;
  while (i < lines.length) {
    const trimmed = lines[i].trimStart();
    if (/^(column|measure|hierarchy|partition|table|annotation\s+PBI_Id)\s*/i.test(trimmed) && !inBacktickBlock) break;
    if (inBacktickBlock) { exprLines.push(lines[i]); if (trimmed.includes('```')) inBacktickBlock = false; i++; continue; }
    if (/^formatString:\s*/i.test(trimmed)) { mData.formatString = trimmed.replace(/^formatString:\s*/i, '').trim(); i++; continue; }
    if (/^description:\s*/i.test(trimmed)) { mData.description = trimmed.replace(/^description:\s*/i, '').trim(); i++; continue; }
    if (/^displayFolder:\s*/i.test(trimmed)) { mData.displayFolder = trimmed.replace(/^displayFolder:\s*/i, '').trim(); i++; continue; }
    if (/^(lineageTag|changedProperty|annotation|isHidden)\s*/i.test(trimmed)) { i++; continue; }
    if (trimmed.includes('```')) { exprLines.push(lines[i]); inBacktickBlock = !inBacktickBlock; i++; continue; }
    if (trimmed === '') { i++; continue; }
    if (/^\t/.test(lines[i]) || /^\s{2,}/.test(lines[i])) exprLines.push(lines[i]);
    i++;
  }
  mData.expression = exprLines.join('\n').replace(/```/g, '').trim();
  return { data: mData, nextLine: i };
}

function _parseTmdlRelationships(payload) {
  const rels = [];
  const blocks = payload.split(/^relationship\s+/m);
  for (let bi = 1; bi < blocks.length; bi++) {
    const block = blocks[bi];
    const fromCol = block.match(/fromColumn:\s*(.+)/);
    const toCol = block.match(/toColumn:\s*(.+)/);
    const crossFilter = block.match(/crossFilteringBehavior:\s*(.+)/);
    const isActiveMatch = block.match(/isActive:\s*(.+)/);
    if (fromCol && toCol) {
      const fp = fromCol[1].trim().split('.'), tp = toCol[1].trim().split('.');
      rels.push({
        from_table: fp[0].replace(/^['"]|['"]$/g, ''), from_column: (fp[1] || '').replace(/^['"]|['"]$/g, ''),
        to_table: tp[0].replace(/^['"]|['"]$/g, ''), to_column: (tp[1] || '').replace(/^['"]|['"]$/g, ''),
        crossFilteringBehavior: crossFilter ? crossFilter[1].trim() : 'oneDirection',
        isActive: isActiveMatch ? isActiveMatch[1].trim().toLowerCase() !== 'false' : true,
      });
    }
  }
  return rels;
}

// ── Semantic Model chunks ───────────────────────────────────
function buildSemanticModelChunks(KB, itemLookup) {
  const chunks = [];
  if (!KB.definitions) return chunks;
  for (const [itemId, defParts] of Object.entries(KB.definitions)) {
    const info = itemLookup[itemId] || {};
    if (info.type !== 'SemanticModel') continue;
    if (!Array.isArray(defParts)) continue;
    const modelName = info.name || itemId;
    const wsName = info.workspace || '';

    const tablesMap = {};
    let relationships = [];
    for (const part of defParts) {
      const p = part.path || '';
      const payload = part.payload || '';
      if (p.startsWith('definition/tables/') && p.endsWith('.tmdl')) {
        const parsed = _parseTmdlTable(payload);
        if (!parsed) continue;
        if (parsed.name.startsWith('LocalDateTable') || parsed.name.startsWith('DateTableTemplate')) continue;
        tablesMap[parsed.name] = parsed;
      }
      if (p.endsWith('relationships.tmdl')) {
        relationships = _parseTmdlRelationships(payload);
      }
    }

    const tableNames = Object.keys(tablesMap);
    if (tableNames.length === 0) continue;
    const allMeasures = [];
    for (const [tbl, inf] of Object.entries(tablesMap)) {
      if (inf.measures.length > 0) allMeasures.push(...inf.measures);
    }
    const totalCols = Object.values(tablesMap).reduce((s, t) => s + t.columns.length, 0);

    // Overview chunk
    chunks.push({
      type: 'semantic_model_overview', id: modelName,
      model_name: modelName, model_id: itemId, workspace: wsName,
      table_count: tableNames.length, measure_count: allMeasures.length,
      column_count: totalCols, relationship_count: relationships.length,
      tables: tableNames,
      tables_detail: Object.fromEntries(Object.entries(tablesMap).map(([k, v]) => [k, {
        columns: v.columns, measure_count: v.measures.length, isCalculated: v.isCalculated,
      }])),
      summary: `Semantic Model ${modelName}: ${tableNames.length} tables, ${totalCols} columns, ${allMeasures.length} measures, ${relationships.length} relationships`,
    });

    // Per-table measures chunks
    for (const [tblName, tblInfo] of Object.entries(tablesMap)) {
      if (tblInfo.measures.length === 0) continue;
      const measText = tblInfo.measures.map(m => `${m.name} = ${m.expression}`).join('\n');
      chunks.push({
        type: 'semantic_model_measures', id: `${modelName}::${tblName}`,
        model_name: modelName, model_id: itemId, table_name: tblName,
        measures: tblInfo.measures,
        summary: `Measures in ${modelName}.${tblName}: ${tblInfo.measures.map(m => m.name).join(', ')}`,
        description: measText.slice(0, 3000),
      });
    }

    // Relationships chunk
    if (relationships.length > 0) {
      const relText = relationships.map(r => `${r.from_table}.${r.from_column} → ${r.to_table}.${r.to_column}`).join('\n');
      chunks.push({
        type: 'semantic_model_relationships', id: `${modelName}::relationships`,
        model_name: modelName, model_id: itemId,
        relationships: relationships,
        summary: `Relationships in ${modelName}: ${relationships.length} relationships`,
        description: relText.slice(0, 3000),
      });
    }
  }
  return chunks;
}

// ── Pipeline chunks ─────────────────────────────────────────
function buildPipelineChunks(KB, itemLookup) {
  const chunks = [];
  if (!KB.definitions) return chunks;
  for (const [itemId, defParts] of Object.entries(KB.definitions)) {
    const info = itemLookup[itemId] || {};
    if (info.type !== 'DataPipeline') continue;
    if (!Array.isArray(defParts)) continue;
    const pName = info.name || itemId;
    const wsName = info.workspace || '';
    const pipelinePart = defParts.find(p => p.path === 'pipeline-content.json');
    if (!pipelinePart || !pipelinePart.payload) continue;
    let pDef;
    try { pDef = JSON.parse(pipelinePart.payload); } catch { continue; }
    const activities = (pDef.properties && pDef.properties.activities) || [];
    const actSummaries = activities.map(a => {
      const deps = (a.dependsOn || []).map(d => d.activity).filter(Boolean);
      return { name: a.name, type: a.type, dependsOn: deps };
    });
    // Extract notebook references
    const nbRefs = [];
    const nbMatches = pipelinePart.payload.match(/Phase_\d+_\w+|GenDWH_\w+/g) || [];
    for (const nb of new Set(nbMatches)) nbRefs.push(nb);

    chunks.push({
      type: 'pipeline_overview', id: pName,
      pipeline: pName, pipeline_id: itemId, workspace: wsName,
      activity_count: activities.length,
      activities: actSummaries,
      notebook_references: nbRefs,
      summary: `Pipeline ${pName}: ${activities.length} activities (${actSummaries.map(a => a.name).join(', ')})`,
    });
  }
  return chunks;
}

// ── Notebook chunks ─────────────────────────────────────────
function buildNotebookChunks(KB, itemLookup) {
  const chunks = [];
  if (!KB.definitions) return chunks;
  for (const [itemId, defParts] of Object.entries(KB.definitions)) {
    const info = itemLookup[itemId] || {};
    if (info.type !== 'Notebook') continue;
    if (!Array.isArray(defParts)) continue;
    const nbName = info.name || itemId;
    const wsName = info.workspace || '';
    // Find the notebook source (usually notebook-content.py or .ipynb)
    let sourceCode = '';
    for (const part of defParts) {
      if (part.payload && part.payload.length > sourceCode.length) sourceCode = part.payload;
    }
    if (!sourceCode) continue;
    // Extract table references from code
    const tableRefs = [];
    const tblMatches = sourceCode.match(/(?:FROM|JOIN|INTO|TABLE)\s+[`"']?([a-z_][a-z0-9_.]*)/gi) || [];
    for (const m of tblMatches) {
      const t = m.replace(/^(?:FROM|JOIN|INTO|TABLE)\s+[`"']?/i, '').trim();
      if (t && t.length > 2 && !['true', 'false', 'none', 'null'].includes(t.toLowerCase())) tableRefs.push(t);
    }
    // Detect language
    const lang = sourceCode.includes('spark.sql') || sourceCode.includes('import pyspark') ? 'PySpark'
      : sourceCode.includes('CREATE ') || sourceCode.includes('SELECT ') ? 'SQL' : 'Python';

    chunks.push({
      type: 'notebook_overview', id: nbName,
      notebook: nbName, notebook_id: itemId, workspace: wsName,
      language: lang,
      size_chars: sourceCode.length,
      table_references: [...new Set(tableRefs)].slice(0, 100),
      summary: `Notebook ${nbName} (${lang}, ${Math.round(sourceCode.length / 1024)}KB): references tables ${[...new Set(tableRefs)].slice(0, 20).join(', ')}`,
      description: sourceCode.slice(0, 2000),
    });
  }
  return chunks;
}

// ── Report chunks ───────────────────────────────────────────
function buildReportChunks(KB, itemLookup) {
  const chunks = [];
  if (!KB.definitions) return chunks;
  for (const [itemId, defParts] of Object.entries(KB.definitions)) {
    const info = itemLookup[itemId] || {};
    if (info.type !== 'Report') continue;
    if (!Array.isArray(defParts)) continue;
    const rptName = info.name || itemId;
    const wsName = info.workspace || '';
    // Find semantic model link
    let smName = '';
    const pbirFile = defParts.find(f => f.path === 'definition.pbir');
    if (pbirFile) {
      try {
        const pbir = JSON.parse(pbirFile.payload);
        const cs = ((pbir.datasetReference || {}).byConnection || {}).connectionString || '';
        const mCat = cs.match(/initial catalog=([^;]+)/i);
        if (mCat) smName = mCat[1];
      } catch { /* malformed */ }
    }
    // Count pages and extract field references
    const pageFiles = defParts.filter(f => (f.path || '').match(/page\.json$/));
    const allFields = new Set();
    let pageCount = pageFiles.length;
    for (const pf of pageFiles) {
      try {
        const content = pf.payload || '';
        // Extract "Property": "FieldName" patterns from visual configs
        const fieldMatches = content.match(/"(?:Column|Measure|Property)":\s*"([^"]+)"/g) || [];
        for (const fm of fieldMatches) {
          const m = fm.match(/":\s*"([^"]+)"/);
          if (m) allFields.add(m[1]);
        }
      } catch {}
    }
    // If no PBIP pages, try classic report.json
    if (pageCount === 0) {
      const rptJson = defParts.find(f => f.path === 'report.json');
      if (rptJson && rptJson.payload) {
        try {
          const rpt = JSON.parse(rptJson.payload);
          const sections = rpt.sections || [];
          pageCount = sections.length;
        } catch {}
      }
    }
    chunks.push({
      type: 'report_overview', id: rptName,
      report_name: rptName, report_id: itemId, workspace: wsName,
      semantic_model_name: smName,
      page_count: pageCount,
      fields_used: [...allFields].sort().slice(0, 200),
      summary: `Report ${rptName}: ${pageCount} pages, linked to SM ${smName || '(unknown)'}. Fields: ${[...allFields].slice(0, 30).join(', ')}`,
    });
  }
  return chunks;
}

// ── Catalog chunks (summary per type for broad queries) ─────
function buildCatalogChunks(allChunks) {
  const catalogs = [];
  const grouped = {};
  for (const c of allChunks) {
    if (!grouped[c.type]) grouped[c.type] = [];
    grouped[c.type].push(c);
  }
  // Lakehouse tables by zone
  const lhTables = grouped['lakehouse_table'] || [];
  const byZone = {};
  for (const t of lhTables) {
    const z = t.zone || 'Unknown';
    if (!byZone[z]) byZone[z] = [];
    byZone[z].push(t.table_name);
  }
  for (const [zone, tables] of Object.entries(byZone)) {
    catalogs.push({
      type: 'catalog', id: `catalog::lakehouse_tables::${zone}`,
      category: 'lakehouse_tables', zone: zone,
      item_count: tables.length,
      items: tables.sort(),
      summary: `${zone} zone Lakehouse tables (${tables.length}): ${tables.sort().join(', ')}`,
    });
  }
  // Warehouse objects
  for (const wType of ['warehouse_table', 'warehouse_view', 'warehouse_sproc']) {
    const items = grouped[wType] || [];
    if (items.length === 0) continue;
    catalogs.push({
      type: 'catalog', id: `catalog::${wType}`,
      category: wType, item_count: items.length,
      items: items.map(i => i.id).sort(),
      summary: `All ${wType.replace('warehouse_', 'warehouse ')}s (${items.length}): ${items.map(i => i.id).sort().join(', ')}`,
    });
  }
  // Semantic models
  const smOverviews = grouped['semantic_model_overview'] || [];
  if (smOverviews.length > 0) {
    catalogs.push({
      type: 'catalog', id: 'catalog::semantic_models',
      category: 'semantic_models', item_count: smOverviews.length,
      items: smOverviews.map(s => s.model_name).sort(),
      summary: `All Semantic Models (${smOverviews.length}): ${smOverviews.map(s => `${s.model_name} (${s.table_count} tables, ${s.measure_count} measures)`).join('; ')}`,
    });
  }
  // Pipelines, Notebooks, Reports
  for (const [typeKey, label] of [['pipeline_overview', 'Pipelines'], ['notebook_overview', 'Notebooks'], ['report_overview', 'Reports']]) {
    const items = grouped[typeKey] || [];
    if (items.length === 0) continue;
    const nameKey = typeKey === 'pipeline_overview' ? 'pipeline' : typeKey === 'notebook_overview' ? 'notebook' : 'report_name';
    catalogs.push({
      type: 'catalog', id: `catalog::${typeKey}`,
      category: label.toLowerCase(), item_count: items.length,
      items: items.map(i => i[nameKey]).sort(),
      summary: `All ${label} (${items.length}): ${items.map(i => i[nameKey]).sort().join(', ')}`,
    });
  }
  return catalogs;
}


// ── Assemble JSONL ──────────────────────────────────────────
function assembleJSONL(lineageByTable, chains, warehouseChunks, extraChunkArrays) {
  const lines = [];

  // Table lineage chunks
  for (const [table, record] of Object.entries(lineageByTable)) {
    const fields = record.fields || [];
    const sourceTables = [...new Set(fields.map(f => f.source_table).filter(Boolean))];
    const transformTypes = [...new Set(fields.map(f => f.transformation_type).filter(Boolean))];

    lines.push(JSON.stringify({
      type: "table_lineage", id: table,
      layer: record.layer || "", mode: record.mode || "",
      source_tables: sourceTables, field_count: fields.length,
      transformation_types: transformTypes,
      summary: `${record.layer} ${record.mode} table with ${fields.length} fields from ${sourceTables.join(", ") || "unknown"}`,
      fields: fields.map(f => f.target_field),
    }));

    for (const f of fields) {
      lines.push(JSON.stringify({
        type: "field_detail", id: `${table}.${f.target_field}`,
        table: table, layer: record.layer || "",
        target_field: f.target_field, data_type: f.data_type || "",
        source_table: f.source_table || "", source_column: f.source_column || "",
        transformation_type: f.transformation_type || "",
        expression: f.expression || "", business_logic: f.business_logic || "",
        join_key: f.join_key || null,
      }));
    }
  }

  // Execution chain chunks
  for (const c of chains) lines.push(JSON.stringify(c));
  // Warehouse view/sproc/table chunks
  for (const w of warehouseChunks) lines.push(JSON.stringify(w));
  // All extra chunk arrays (lakehouse, bronze, SM, pipeline, notebook, report, catalog)
  for (const arr of (extraChunkArrays || [])) {
    for (const c of arr) lines.push(JSON.stringify(c));
  }

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

    log(`[${elapsed()}s] Step 5b: Building item lookup...`);
    const itemLookup = buildItemLookup(KB);
    log(`Item lookup: ${Object.keys(itemLookup).length} items.`);

    log(`[${elapsed()}s] Step 5c: Building lakehouse chunks...`);
    const lakehouseChunks = buildLakehouseChunks(KB, itemLookup);
    log(`Built ${lakehouseChunks.length} lakehouse table chunks.`);

    log(`[${elapsed()}s] Step 5d: Building bronze meta chunks...`);
    const bronzeChunks = buildBronzeMetaChunks(KB);
    log(`Built ${bronzeChunks.length} bronze meta chunks.`);

    log(`[${elapsed()}s] Step 5e: Building semantic model chunks...`);
    const smChunks = buildSemanticModelChunks(KB, itemLookup);
    log(`Built ${smChunks.length} semantic model chunks.`);

    log(`[${elapsed()}s] Step 5f: Building pipeline chunks...`);
    const pipelineChunks = buildPipelineChunks(KB, itemLookup);
    log(`Built ${pipelineChunks.length} pipeline chunks.`);

    log(`[${elapsed()}s] Step 5g: Building notebook chunks...`);
    const notebookChunks = buildNotebookChunks(KB, itemLookup);
    log(`Built ${notebookChunks.length} notebook chunks.`);

    log(`[${elapsed()}s] Step 5h: Building report chunks...`);
    const reportChunks = buildReportChunks(KB, itemLookup);
    log(`Built ${reportChunks.length} report chunks.`);

    // Collect all extra chunks for catalog generation
    const allExtraChunks = [...lakehouseChunks, ...bronzeChunks, ...smChunks, ...pipelineChunks, ...notebookChunks, ...reportChunks];
    log(`[${elapsed()}s] Step 5i: Building catalog chunks...`);
    const catalogChunks = buildCatalogChunks([...warehouseChunks, ...allExtraChunks]);
    log(`Built ${catalogChunks.length} catalog chunks.`);

    const extraChunkArrays = [lakehouseChunks, bronzeChunks, smChunks, pipelineChunks, notebookChunks, reportChunks, catalogChunks];

    log(`[${elapsed()}s] Step 6: Assembling JSONL...`);
    const jsonl = assembleJSONL(lineageByTable, chains, warehouseChunks, extraChunkArrays);
    const lineCount = jsonl.split("\n").length;
    log(`JSONL: ${lineCount} lines, ${jsonl.length} bytes.`);

    log(`[${elapsed()}s] Step 7: Uploading to Blob Storage...`);
    await uploadBlob(JSONL_BLOB, jsonl, log);
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    await uploadBlob(`archive/${ts}_knowledge.jsonl`, jsonl, log);

    // ── Step 8: Embed new/changed chunks ─────────────────────
    let embeddedCount = 0;
    let embedSkipped = 0;
    let embedError = null;
    try {
      const openaiCreds = await getOpenAICredentials(log);
      if (!openaiCreds) throw new Error("Azure OpenAI credentials not available");

      log(`[${elapsed()}s] Step 8: Embedding new/changed chunks...`);
      const svc = getBlobClient();
      const containerClient = svc.getContainerClient(CONTAINER);

      // Parse new JSONL into chunks
      const newChunks = jsonl.split("\n").map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);

      // Load existing vectors
      const existingText = await downloadTextSafe(containerClient, VECTORS_BLOB);
      const existingMap = new Map(); // hash → line
      if (existingText) {
        for (const line of existingText.trim().split("\n")) {
          try { const v = JSON.parse(line); if (v.chunk_hash) existingMap.set(v.chunk_hash, line); } catch {}
        }
        log(`  Existing vectors: ${existingMap.size}`);
      }

      // Find chunks needing embedding
      const toEmbed = [];
      const newHashes = new Set();
      for (const chunk of newChunks) {
        const hash = chunkHash(chunk);
        newHashes.add(hash);
        if (!existingMap.has(hash)) toEmbed.push({ chunk, hash, text: chunkToText(chunk) });
      }
      embedSkipped = existingMap.size - toEmbed.length;
      log(`  Chunks to embed: ${toEmbed.length} (${newChunks.length - toEmbed.length} unchanged)`);

      // Remove vectors for chunks no longer in JSONL
      const outputLines = [];
      for (const [hash, line] of existingMap) {
        if (newHashes.has(hash)) outputLines.push(line);
      }

      // Embed in batches
      for (let i = 0; i < toEmbed.length; i += EMBED_BATCH_SIZE) {
        if (Date.now() - startTime > TIMEOUT_MS - 5000) {
          log(`  ⏱ Embedding timeout — embedded ${embeddedCount}/${toEmbed.length}`);
          break;
        }
        const batch = toEmbed.slice(i, i + EMBED_BATCH_SIZE);
        const texts = batch.map(b => b.text.slice(0, 8000));
        try {
          const embeddings = await embedBatch(texts, openaiCreds.apiKey, openaiCreds.endpoint);
          for (let j = 0; j < batch.length; j++) {
            const { chunk, hash } = batch[j];
            const vec = { chunk_hash: hash, chunk_type: chunk.type, id: chunk.id || "", embedding: embeddings[j] };
            for (const k of ["table", "model_name", "pipeline", "report_name", "layer", "target_field", "source_table"]) {
              if (chunk[k]) vec[k] = chunk[k];
            }
            vec.text = batch[j].text;
            outputLines.push(JSON.stringify(vec));
          }
          embeddedCount += batch.length;
        } catch (err) {
          log(`  Embed batch error at ${i}: ${err.message}`);
        }
        if (i + EMBED_BATCH_SIZE < toEmbed.length) await sleep(EMBED_BATCH_DELAY_MS);
      }

      // Upload updated vectors
      log(`[${elapsed()}s] Uploading gendwh_vectors.jsonl (${outputLines.length} vectors)...`);
      await uploadBlob(VECTORS_BLOB, outputLines.join("\n"), log);
    } catch (err) {
      embedError = err.message;
      log(`[${elapsed()}s] Embedding step failed (non-fatal): ${err.message}`);
    }

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
      lakehouse_chunks: lakehouseChunks.length,
      bronze_chunks: bronzeChunks.length,
      sm_chunks: smChunks.length,
      pipeline_chunks: pipelineChunks.length,
      notebook_chunks: notebookChunks.length,
      report_chunks: reportChunks.length,
      catalog_chunks: catalogChunks.length,
      jsonl_lines: lineCount,
      jsonl_bytes: jsonl.length,
      vectors_embedded: embeddedCount,
      vectors_skipped: embedSkipped,
      embed_error: embedError,
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