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
const DEFAULT_MAX_TOKENS = 4096;
const MAX_ALLOWED_TOKENS = 16384;

const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const KV_SECRET_NAME = "anthropicapikey";
const CONTAINER = "gendwh-exports";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";
const EMBEDDING_MODEL = "text-embedding-ada-002";
const TOP_K = 15;

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
async function callClaude(apiKey, messages, system, maxTokens) {
  const response = await fetch(ANTHROPIC_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({ model: MODEL, max_tokens: maxTokens, system, messages }),
  });
  const data = await response.json();
  return { status: response.ok ? 200 : response.status, data };
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
  // RAG MODE — Semantic Search
  // ═══════════════════════════════════════════════════════════
  try {
    const question = body.question.trim();
    const chatHistory = Array.isArray(body.chatHistory) ? body.chatHistory : [];
    log(`[RAG] Question: "${question.slice(0, 100)}..."`);

    // 1. Get OpenAI credentials
    const openaiCreds = await getOpenAICredentials(log);
    if (!openaiCreds) throw new Error("Azure OpenAI credentials not available");

    // 2. Embed the question
    const queryEmbedding = await embedQuery(question, openaiCreds.apiKey, openaiCreds.endpoint);
    log(`[RAG] Query embedded (${queryEmbedding.length} dims)`);

    // 3. Load vectors (cached in-memory with TTL)
    const vectors = await loadVectors(log);

    // 4. Cosine similarity → top-K
    const topChunks = retrieveTopK(queryEmbedding, vectors, TOP_K);
    log(`[RAG] Top ${topChunks.length} chunks (scores: ${topChunks.map(c => c.score.toFixed(3)).join(", ")})`);

    // 5. Build context from chunk texts
    const contextParts = topChunks.map((c, i) =>
      `[${i + 1}] (score=${c.score.toFixed(3)}, type=${c.chunk_type || "?"}, id=${c.id || "?"}):\n${c.text || "(no text)"}`
    );
    const contextStr = contextParts.join("\n---\n");

    // 6. Build system prompt
    const systemPrompt = SYSTEM_PROMPT + "\n\nCONTEXT:\n" + contextStr;

    // 7. Build messages: chat history + current question
    const messages = [];
    // Include last N history messages (keep it lean)
    const historySlice = chatHistory.slice(-10);
    for (const msg of historySlice) {
      if (msg.role && msg.content) messages.push({ role: msg.role, content: msg.content });
    }
    messages.push({ role: "user", content: question });

    // 8. Call Claude
    const { status, data } = await callClaude(apiKey, messages, systemPrompt, DEFAULT_MAX_TOKENS);

    // 9. Return response with retrieval metadata
    const responseBody = { ...data };
    responseBody._rag = {
      chunks_retrieved: topChunks.length,
      top_scores: topChunks.slice(0, 5).map(c => ({ id: c.id, type: c.chunk_type, score: +c.score.toFixed(3) })),
      vectors_total: vectors.length,
    };

    context.res = { status, headers: { "Content-Type": "application/json" }, body: responseBody };
  } catch (err) {
    log("RAG error: " + err.message);
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: err.message, stack: (err.stack || "").slice(0, 1000) }) };
  }
};
