/**
 * Azure Function — POST /api/embed
 *
 * Incremental vector embedding for the knowledge base.
 * Reads gendwh_knowledge.jsonl, compares hashes with existing vectors,
 * embeds only new/changed chunks, uploads updated gendwh_vectors.jsonl.
 *
 * Idempotent — safe to call multiple times until all chunks are embedded.
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
const crypto = require("crypto");

// ── Config ──────────────────────────────────────────────────
const KV_URL = "https://kv-ai-site-builder.vault.azure.net";
const CONTAINER = "gendwh-exports";
const JSONL_BLOB = "latest/gendwh_knowledge.jsonl";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";

const EMBEDDING_MODEL = "text-embedding-ada-002";
const EMBED_BATCH_SIZE = 20;
const EMBED_BATCH_DELAY_MS = 500;
const TIMEOUT_MS = 35000; // 35s — leave margin for upload + response

// ── Cached credentials ──────────────────────────────────────
let cachedOpenAIKey = null;
let cachedOpenAIEndpoint = null;

// ── Blob helpers ────────────────────────────────────────────
function getBlobClient() {
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) throw new Error("BLOB_CONNECTION_STRING not configured");
  return BlobServiceClient.fromConnectionString(connStr);
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

async function uploadBlob(blobPath, content, log) {
  const svc = getBlobClient();
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  const buf = Buffer.from(content, "utf8");
  await blob.upload(buf, buf.length, {
    blobHTTPHeaders: { blobContentType: "application/x-ndjson" },
  });
  log(`Uploaded ${blobPath} (${buf.length} bytes)`);
}

// ── Azure OpenAI credentials ────────────────────────────────
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
    log("Failed to fetch OpenAI credentials:", err.message);
    return null;
  }
}

// ── Embedding helpers ───────────────────────────────────────
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
  if (Array.isArray(chunk.measures)) parts.push(chunk.measures.map(m => `${m.name}: ${m.expression || ""}`).join("; ").slice(0, 3000));
  if (Array.isArray(chunk.columns)) parts.push(chunk.columns.map(c => c.name || c).join(", ").slice(0, 1000));
  if (Array.isArray(chunk.items)) parts.push(chunk.items.join(", ").slice(0, 2000));
  if (chunk.schema && chunk.name) parts.push(`${chunk.schema}.${chunk.name}`);
  if (Array.isArray(chunk.table_references)) parts.push(`references: ${chunk.table_references.join(", ").slice(0, 1000)}`);
  if (Array.isArray(chunk.activities)) parts.push(`activities: ${chunk.activities.map(a => a.name || a).join(", ").slice(0, 1000)}`);
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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }



// ═══════════════════════════════════════════════════════════════
// MAIN HANDLER
// ═══════════════════════════════════════════════════════════════
module.exports = async function (context, req) {
  context.log("Embed function invoked");
  const log = (...args) => context.log.info(...args);
  const startTime = Date.now();
  const elapsed = () => ((Date.now() - startTime) / 1000).toFixed(1);

  if (moduleLoadError) {
    context.res = { status: 500, headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: moduleLoadError }) };
    return;
  }

  try {
    const openaiCreds = await getOpenAICredentials(log);
    if (!openaiCreds) throw new Error("Azure OpenAI credentials not available");
    log(`[${elapsed()}s] OpenAI credentials ready`);

    const svc = getBlobClient();
    const containerClient = svc.getContainerClient(CONTAINER);

    // 1. Download JSONL knowledge base
    log(`[${elapsed()}s] Downloading JSONL...`);
    const jsonlText = await downloadTextSafe(containerClient, JSONL_BLOB);
    if (!jsonlText) throw new Error("gendwh_knowledge.jsonl not found — run /api/analyze first");
    const newChunks = jsonlText.trim().split("\n").map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
    log(`[${elapsed()}s] JSONL: ${newChunks.length} chunks`);

    // 2. Download existing vectors
    log(`[${elapsed()}s] Downloading existing vectors...`);
    const existingText = await downloadTextSafe(containerClient, VECTORS_BLOB);
    const existingMap = new Map();
    if (existingText) {
      for (const line of existingText.trim().split("\n")) {
        try { const v = JSON.parse(line); if (v.chunk_hash) existingMap.set(v.chunk_hash, line); } catch {}
      }
    }
    log(`[${elapsed()}s] Existing vectors: ${existingMap.size}`);

    // 3. Diff: find chunks needing embedding
    const toEmbed = [];
    const newHashes = new Set();
    for (const chunk of newChunks) {
      const hash = chunkHash(chunk);
      newHashes.add(hash);
      if (!existingMap.has(hash)) toEmbed.push({ chunk, hash, text: chunkToText(chunk) });
    }
    log(`[${elapsed()}s] To embed: ${toEmbed.length}, unchanged: ${newChunks.length - toEmbed.length}`);

    // 4. Keep only vectors for chunks still in JSONL
    const outputLines = [];
    for (const [hash, line] of existingMap) {
      if (newHashes.has(hash)) outputLines.push(line);
    }

    // 5. Embed in batches with timeout guard
    let embeddedCount = 0;
    let timedOut = false;
    for (let i = 0; i < toEmbed.length; i += EMBED_BATCH_SIZE) {
      if (Date.now() - startTime > TIMEOUT_MS - 5000) {
        timedOut = true;
        log(`[${elapsed()}s] ⏱ Timeout — embedded ${embeddedCount}/${toEmbed.length}`);
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
        log(`[${elapsed()}s] Embed batch error at ${i}: ${err.message}`);
      }
      if (i + EMBED_BATCH_SIZE < toEmbed.length) await sleep(EMBED_BATCH_DELAY_MS);
    }

    // 6. Upload updated vectors
    log(`[${elapsed()}s] Uploading vectors (${outputLines.length} total)...`);
    await uploadBlob(VECTORS_BLOB, outputLines.join("\n"), log);

    const remaining = toEmbed.length - embeddedCount;
    const summary = {
      status: remaining > 0 ? "partial" : "complete",
      elapsed_seconds: parseFloat(elapsed()),
      total_chunks: newChunks.length,
      vectors_total: outputLines.length,
      vectors_embedded_now: embeddedCount,
      vectors_remaining: remaining,
      vectors_unchanged: newChunks.length - toEmbed.length,
      timed_out: timedOut,
    };
    log(`[${elapsed()}s] Done!`, JSON.stringify(summary));

    context.res = { status: 200, headers: { "Content-Type": "application/json" },
      body: JSON.stringify(summary) };
  } catch (err) {
    context.log.error(`[${elapsed()}s] Fatal: ${err.message}`);
    context.log.error(err.stack || "(no stack)");
    context.res = { status: 500, headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: err.message, stack: (err.stack || "").slice(0, 2000), elapsed_seconds: elapsed() }) };
  }
};