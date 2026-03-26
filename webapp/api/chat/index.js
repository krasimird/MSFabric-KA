/**
 * Azure Function — POST /api/chat
 *
 * Two modes:
 *   1. PROXY mode: { messages, system, max_tokens } → forward to Claude (for skills / generic)
 *   2. RAG mode:   { question, chatHistory }         → embed query → vector search → Claude
 *
 * API key resolution: env var → Azure Key Vault
 */

let BlobServiceClient, DefaultAzureCredential, SecretClient;
let moduleLoadError = null;
try {
  ({ BlobServiceClient } = require("@azure/storage-blob"));
  ({ DefaultAzureCredential } = require("@azure/identity"));
  ({ SecretClient } = require("@azure/keyvault-secrets"));
} catch (err) {
  moduleLoadError = `Failed to load Azure SDK modules: ${err.message}`;
}

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-sonnet-4-6";
const CLASSIFIER_MODEL = "claude-sonnet-4-6";
const CLASSIFIER_MAX_TOKENS = 128;
const DEFAULT_MAX_TOKENS = 4096;
const MAX_ALLOWED_TOKENS = 16384;

const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const KV_SECRET_NAME = "anthropicapikey";
const CONTAINER = "gendwh-exports";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";
const EMBEDDING_MODEL = "text-embedding-ada-002";
const TOP_K = 15;

// ── Context routing: chunk type filters per classification ──
const CONTEXT_LEVELS = {
  FULL:          null, // no filter — use all chunks
  INVENTORY:     null, // special: direct metadata, no vector search
  LINEAGE:       new Set(["field_detail", "table_lineage", "execution_chain", "lakehouse_table", "bronze_meta"]),
  WAREHOUSE:     new Set(["warehouse_table", "warehouse_view", "warehouse_sproc", "field_detail", "table_lineage"]),
  BI:            new Set(["report_overview", "semantic_model_overview", "semantic_model_measures", "semantic_model_relationships"]),
  ORCHESTRATION: new Set(["pipeline_overview", "notebook_overview", "execution_chain"]),
  BRONZE_META:   new Set(["bronze_meta", "lakehouse_table"]),
  // Tree-based levels (resolve to custom filter functions in LEVEL_FILTER_FN)
  MEDALLION: null, BRONZE: null, SILVER: null, SILVER_RAW: null, SILVER_STG: null,
  GOLD: null, PLATINUM: null, SEMANTIC_MODELS: null, REPORTS: null,
  PIPELINES: null, NOTEBOOKS: null, WORKSPACES: null,
};

// ── Advanced filter functions for tree-based levels ──────────
const ZONE_PATTERN = { BRONZE: /bronze/i, SILVER_RAW: /silver.?raw/i, SILVER_STG: /silver.?(stg|stage)/i, GOLD: /gold/i, PLATINUM: /platinum|cdwh/i };
const LEVEL_FILTER_FN = {
  BRONZE:     v => ZONE_PATTERN.BRONZE.test(v.zone || v.data_zone || ''),
  SILVER_RAW: v => ZONE_PATTERN.SILVER_RAW.test(v.zone || v.data_zone || ''),
  SILVER_STG: v => ZONE_PATTERN.SILVER_STG.test(v.zone || v.data_zone || ''),
  GOLD:       v => ZONE_PATTERN.GOLD.test(v.zone || v.data_zone || ''),
  PLATINUM:   v => ZONE_PATTERN.PLATINUM.test(v.zone || v.data_zone || ''),
  SILVER:     v => LEVEL_FILTER_FN.SILVER_RAW(v) || LEVEL_FILTER_FN.SILVER_STG(v),
  MEDALLION:  v => LEVEL_FILTER_FN.BRONZE(v) || LEVEL_FILTER_FN.SILVER(v) || LEVEL_FILTER_FN.GOLD(v) || LEVEL_FILTER_FN.PLATINUM(v),
  SEMANTIC_MODELS: v => (v.chunk_type || '').startsWith('semantic_model'),
  REPORTS:    v => (v.chunk_type || '').startsWith('report'),
  PIPELINES:  v => (v.chunk_type || '').startsWith('pipeline'),
  NOTEBOOKS:  v => (v.chunk_type || '').startsWith('notebook'),
  WORKSPACES: v => !!(v.workspace),
};

const CLASSIFIER_PROMPT = `Classify this data warehouse question into exactly ONE category. Reply with ONLY the category name, nothing else.

Categories:
- INVENTORY: counting items, listing all items, "how many", "list all", "show all workspaces/tables/pipelines/reports"
- LINEAGE: field lineage, data flow, transformation, source-to-target mapping, "where does X come from"
- WAREHOUSE: Platinum/DWH layer, warehouse tables, views, stored procedures
- BI: reports, semantic models, DAX measures, Power BI, dashboards
- ORCHESTRATION: pipelines, notebooks, scheduling, execution chains, activities
- BRONZE_META: bronze metadata, landing tables, source systems, ingestion
- FULL: general/broad questions, architecture overview, or doesn't fit above

Question: `;

// ── Cached state (per function instance) ─────────────────────
let cachedApiKey = null;
let cachedOpenAIKey = null;
let cachedOpenAIEndpoint = null;
let cachedVectors = null;       // [{chunk_hash, text, embedding, ...}]
let cachedVectorsAge = 0;       // timestamp of last load
const VECTORS_TTL_MS = 5 * 60 * 1000; // refresh every 5 min

// ── SYSTEM PROMPT (moved server-side) ────────────────────────
const SYSTEM_PROMPT = `You are GenDWH Knowledge Assistant — an AI expert on the GenDWH Data Warehouse platform built on Microsoft Fabric.

Your knowledge comes from the CONTEXT section below, which contains real metadata extracted from the platform: table schemas, pipeline definitions, notebook source code, SQL queries, DAX measures, warehouse stored procedures, and more.

When "AI Lineage" entries are present in the context, these are pre-analyzed field-level lineage results. Prefer them over raw SQL — they contain resolved source tables, transformation types, and business logic explanations.

When "Stored Procedure" entries are present, these contain the actual SQL definitions from the Platinum (Warehouse) layer. For Platinum/DWH lineage questions, these are the PRIMARY source of truth — they show exactly how fields are mapped from Gold layer tables into the Warehouse.

Rules:
- Answer in the SAME LANGUAGE as the question (Bulgarian or English)
- Cite specific table names, column names, SQL expressions, DAX measures from the context
- If the information is NOT in the context, say so explicitly — do not guess
- Use Markdown formatting: headers, bullet lists, code blocks, tables
- For lineage questions, trace data flow: source → transformations → target
- For Platinum/Warehouse fields, show the column mapping from the stored procedure SQL (e.g. [source_col] AS target_col)
- Be concise but thorough`;

// ── Key helpers ──────────────────────────────────────────────
async function getApiKey(log) {
  if (cachedApiKey) return cachedApiKey;
  if (process.env.ANTHROPIC_API_KEY) { cachedApiKey = process.env.ANTHROPIC_API_KEY; return cachedApiKey; }
  try {
    const cred = new DefaultAzureCredential();
    const kv = new SecretClient(KV_URL, cred);
    cachedApiKey = (await kv.getSecret(KV_SECRET_NAME)).value;
    log("Anthropic API key loaded from Key Vault.");
    return cachedApiKey;
  } catch (err) { log("KV fetch failed: " + err.message); return null; }
}

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
    cachedOpenAIKey = (await kv.getSecret("azure-openai-key")).value;
    cachedOpenAIEndpoint = (await kv.getSecret("azure-openai-endpoint")).value;
    log("Azure OpenAI credentials loaded from Key Vault.");
    return { apiKey: cachedOpenAIKey, endpoint: cachedOpenAIEndpoint };
  } catch (err) { log("OpenAI KV fetch failed: " + err.message); return null; }
}

// ── Vector helpers ───────────────────────────────────────────
async function loadVectors(log) {
  if (cachedVectors && (Date.now() - cachedVectorsAge < VECTORS_TTL_MS)) return cachedVectors;
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) throw new Error("BLOB_CONNECTION_STRING not configured");
  const svc = BlobServiceClient.fromConnectionString(connStr);
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(VECTORS_BLOB);
  log("Loading vectors from Blob...");
  const resp = await blob.download(0);
  const chunks = [];
  for await (const c of resp.readableStreamBody) chunks.push(c);
  const text = Buffer.concat(chunks).toString("utf8");
  const vectors = [];
  for (const line of text.trim().split("\n")) {
    try { vectors.push(JSON.parse(line)); } catch {}
  }
  cachedVectors = vectors;
  cachedVectorsAge = Date.now();
  log(`Loaded ${vectors.length} vectors (${(text.length / 1048576).toFixed(1)} MB)`);
  return vectors;
}

async function embedQuery(text, apiKey, endpoint) {
  const url = `${endpoint}openai/deployments/${EMBEDDING_MODEL}/embeddings?api-version=2023-05-15`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "api-key": apiKey },
    body: JSON.stringify({ input: [text] }),
  });
  if (!resp.ok) {
    const errText = await resp.text().catch(() => "");
    throw new Error(`Embedding API ${resp.status}: ${errText.slice(0, 300)}`);
  }
  const data = await resp.json();
  return data.data[0].embedding;
}

function cosineSimilarity(a, b) {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  return dot / (Math.sqrt(na) * Math.sqrt(nb) + 1e-10);
}

function retrieveTopK(queryEmbedding, vectors, k) {
  const scored = vectors.map(v => ({
    ...v,
    score: cosineSimilarity(queryEmbedding, v.embedding),
  }));
  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, k);
}

// ── Claude call ──────────────────────────────────────────────
async function callClaude(apiKey, messages, system, maxTokens, model) {
  const response = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({ model: model || MODEL, max_tokens: maxTokens, system, messages }),
  });
  const data = await response.json();
  return { status: response.ok ? 200 : response.status, data };
}

// ── Question classifier ─────────────────────────────────────
async function classifyQuestion(apiKey, question, log) {
  try {
    const { status, data } = await callClaude(
      apiKey,
      [{ role: "user", content: CLASSIFIER_PROMPT + question }],
      "Respond with exactly one word: FULL, INVENTORY, LINEAGE, WAREHOUSE, BI, ORCHESTRATION, or BRONZE_META.",
      CLASSIFIER_MAX_TOKENS,
      CLASSIFIER_MODEL
    );
    if (status !== 200) { log("[CLASSIFY] API error, defaulting to FULL"); return "FULL"; }
    const raw = ((data.content || [])[0] || {}).text || "";
    const level = raw.trim().toUpperCase().replace(/[^A-Z_]/g, "");
    if (CONTEXT_LEVELS.hasOwnProperty(level)) return level;
    log(`[CLASSIFY] Unknown level "${raw}", defaulting to FULL`);
    return "FULL";
  } catch (err) {
    log("[CLASSIFY] Error: " + err.message + ", defaulting to FULL");
    return "FULL";
  }
}

// ── INVENTORY handler: direct metadata answers ──────────────
function handleInventory(question, vectors, log) {
  const q = question.toLowerCase();
  // Build metadata summaries from vector chunks
  const byType = {};
  const byWorkspace = {};
  for (const v of vectors) {
    const t = v.chunk_type || "unknown";
    byType[t] = (byType[t] || 0) + 1;
    const ws = v.workspace || "";
    if (ws) {
      if (!byWorkspace[ws]) byWorkspace[ws] = {};
      byWorkspace[ws][t] = (byWorkspace[ws][t] || 0) + 1;
    }
  }

  // Detect workspace filter
  const wsNames = Object.keys(byWorkspace);
  let filterWs = null;
  for (const ws of wsNames) {
    if (q.includes(ws.toLowerCase())) { filterWs = ws; break; }
  }

  // Build answer parts
  const parts = [];
  const source = filterWs ? byWorkspace[filterWs] || {} : byType;
  const label = filterWs ? ` in workspace **${filterWs}**` : "";

  // Map chunk types to user-friendly names
  const typeLabels = {
    lakehouse_table: "Lakehouse Tables", warehouse_table: "Warehouse Tables",
    warehouse_view: "Warehouse Views", warehouse_sproc: "Warehouse Stored Procedures",
    field_detail: "Field Lineage Entries", table_lineage: "Table Lineage Entries",
    pipeline_overview: "Pipelines", notebook_overview: "Notebooks",
    report_overview: "Reports", semantic_model_overview: "Semantic Models",
    semantic_model_measures: "SM Measures", semantic_model_relationships: "SM Relationships",
    execution_chain: "Execution Chains", bronze_meta: "Bronze Metadata Entries",
    catalog: "Catalog Entries",
  };

  parts.push(`## Inventory${label}\n`);
  let total = 0;
  for (const [t, count] of Object.entries(source).sort((a, b) => b[1] - a[1])) {
    const name = typeLabels[t] || t;
    parts.push(`- **${name}**: ${count}`);
    total += count;
  }
  parts.push(`\n**Total chunks**: ${total}`);
  if (!filterWs && wsNames.length > 0) {
    parts.push(`\n**Workspaces**: ${wsNames.sort().join(", ")}`);
  }

  log(`[INVENTORY] Direct answer: ${total} chunks, ${wsNames.length} workspaces`);
  return parts.join("\n");
}

// ═════════════════════════════════════════════════════════════
// MAIN HANDLER
// ═════════════════════════════════════════════════════════════
module.exports = async function (context, req) {
  const log = (...args) => context.log.info(...args);

  if (moduleLoadError) {
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: moduleLoadError }) };
    return;
  }

  const body = req.body || {};

  // ── MODE DETECTION ──
  // RAG mode: { question, chatHistory? }
  // Proxy mode: { messages, system?, max_tokens? }
  const isRAG = typeof body.question === "string" && body.question.trim().length > 0;

  const apiKey = await getApiKey(log);
  if (!apiKey) {
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "ANTHROPIC_API_KEY not configured." }) };
    return;
  }

  // ═══════════════════════════════════════════════════════════
  // PROXY MODE (backward compat for skills, glossary, reports)
  // ═══════════════════════════════════════════════════════════
  if (!isRAG) {
    const { messages, system, max_tokens } = body;
    if (!messages || !Array.isArray(messages)) {
      context.res = { status: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "Provide 'question' for RAG mode or 'messages' for proxy mode." }) };
      return;
    }
    const tokensToUse = Math.min(Math.max(parseInt(max_tokens, 10) || DEFAULT_MAX_TOKENS, 256), MAX_ALLOWED_TOKENS);
    try {
      const { status, data } = await callClaude(apiKey, messages, system || undefined, tokensToUse);
      context.res = { status, headers: { "Content-Type": "application/json" }, body: data };
    } catch (err) {
      log("Claude proxy error: " + err.message);
      context.res = { status: 502, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "Failed to reach Claude API." }) };
    }
    return;
  }

  // ═══════════════════════════════════════════════════════════
  // RAG MODE — Hierarchical Context Routing
  // ═══════════════════════════════════════════════════════════
  try {
    const question = body.question.trim();
    const chatHistory = Array.isArray(body.chatHistory) ? body.chatHistory : [];
    const requestedLevel = (body.contextLevel || "").toUpperCase();
    log(`[RAG] Question: "${question.slice(0, 100)}..."`);

    // 1. Load vectors (cached in-memory with TTL)
    const vectors = await loadVectors(log);

    // 2. Classify the question (or use explicit override)
    let classification;
    const isKnownLevel = requestedLevel && (CONTEXT_LEVELS.hasOwnProperty(requestedLevel) || requestedLevel.startsWith('WS_'));
    if (isKnownLevel) {
      classification = requestedLevel;
      log(`[RAG] Context level override: ${classification}`);
    } else {
      classification = await classifyQuestion(apiKey, question, log);
      log(`[RAG] Classified as: ${classification}`);
    }

    // 3. INVENTORY — direct metadata answer, no vector search needed
    if (classification === "INVENTORY") {
      const inventoryAnswer = handleInventory(question, vectors, log);
      // Still call Claude for a polished answer with the inventory data as context
      const messages = [];
      const historySlice = chatHistory.slice(-10);
      for (const msg of historySlice) {
        if (msg.role && msg.content) messages.push({ role: msg.role, content: msg.content });
      }
      messages.push({ role: "user", content: question });
      const systemPrompt = SYSTEM_PROMPT + "\n\nCONTEXT (direct metadata inventory):\n" + inventoryAnswer;
      const { status, data } = await callClaude(apiKey, messages, systemPrompt, DEFAULT_MAX_TOKENS);
      const responseBody = { ...data };
      responseBody._rag = {
        classification,
        chunks_retrieved: 0,
        vectors_total: vectors.length,
        context_note: "Direct metadata inventory — no vector search used",
      };
      context.res = { status, headers: { "Content-Type": "application/json" }, body: responseBody };
      return;
    }

    // 4. Get OpenAI credentials for embedding
    const openaiCreds = await getOpenAICredentials(log);
    if (!openaiCreds) throw new Error("Azure OpenAI credentials not available");

    // 5. Embed the question
    const queryEmbedding = await embedQuery(question, openaiCreds.apiKey, openaiCreds.endpoint);
    log(`[RAG] Query embedded (${queryEmbedding.length} dims)`);

    // 6. Filter vectors by classification
    let searchPool;
    const typeFilter = CONTEXT_LEVELS[classification];
    if (typeFilter instanceof Set) {
      searchPool = vectors.filter(v => typeFilter.has(v.chunk_type));
    } else if (LEVEL_FILTER_FN[classification]) {
      searchPool = vectors.filter(LEVEL_FILTER_FN[classification]);
    } else if (classification.startsWith('WS_')) {
      const wsName = classification.slice(3);
      searchPool = vectors.filter(v => (v.workspace || '') === wsName);
    } else {
      searchPool = vectors;
    }
    log(`[RAG] Search pool: ${searchPool.length}/${vectors.length} vectors (filter: ${classification})`);

    // 7. Cosine similarity → top-K on filtered pool
    const topChunks = retrieveTopK(queryEmbedding, searchPool, TOP_K);
    log(`[RAG] Top ${topChunks.length} chunks (scores: ${topChunks.map(c => c.score.toFixed(3)).join(", ")})`);

    // 8. Build context from chunk texts
    const contextParts = topChunks.map((c, i) =>
      `[${i + 1}] (score=${c.score.toFixed(3)}, type=${c.chunk_type || "?"}, id=${c.id || "?"}):\n${c.text || "(no text)"}`
    );
    const contextStr = contextParts.join("\n---\n");

    // 9. Build system prompt with classification hint
    let systemPrompt = SYSTEM_PROMPT;
    if (classification === "LINEAGE") {
      systemPrompt += "\n\nIMPORTANT: This is a lineage question. Context has been filtered to field-level lineage, table lineage, and related chunks. Trace data flow layer by layer: Bronze → Silver → Gold → Platinum.";
    } else if (classification === "WAREHOUSE") {
      systemPrompt += "\n\nIMPORTANT: This is a Warehouse/Platinum layer question. Context has been filtered to warehouse tables, views, stored procedures, and related lineage.";
    } else if (classification === "BI") {
      systemPrompt += "\n\nIMPORTANT: This is a BI/reporting question. Context has been filtered to reports, semantic models, measures, and relationships.";
    } else if (classification === "ORCHESTRATION") {
      systemPrompt += "\n\nIMPORTANT: This is an orchestration question. Context has been filtered to pipelines, notebooks, and execution chains.";
    }
    systemPrompt += "\n\nCONTEXT:\n" + contextStr;

    // 10. Build messages: chat history + current question
    const messages = [];
    const historySlice = chatHistory.slice(-10);
    for (const msg of historySlice) {
      if (msg.role && msg.content) messages.push({ role: msg.role, content: msg.content });
    }
    messages.push({ role: "user", content: question });

    // 11. Call Claude
    const { status, data } = await callClaude(apiKey, messages, systemPrompt, DEFAULT_MAX_TOKENS);

    // 12. Return response with retrieval + classification metadata
    const responseBody = { ...data };
    responseBody._rag = {
      classification,
      chunks_retrieved: topChunks.length,
      search_pool_size: searchPool.length,
      top_scores: topChunks.slice(0, 5).map(c => ({ id: c.id, type: c.chunk_type, score: +c.score.toFixed(3) })),
      vectors_total: vectors.length,
    };
    if (typeFilter instanceof Set) {
      responseBody._rag.context_note = `Context filtered to: ${[...typeFilter].join(", ")}`;
    } else if (LEVEL_FILTER_FN[classification] || classification.startsWith('WS_')) {
      responseBody._rag.context_note = `Context filtered by: ${classification}`;
    }

    context.res = { status, headers: { "Content-Type": "application/json" }, body: responseBody };
  } catch (err) {
    log("RAG error: " + err.message);
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: err.message, stack: (err.stack || "").slice(0, 1000) }) };
  }
};
