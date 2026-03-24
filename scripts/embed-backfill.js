/**
 * Backfill script — embed all JSONL knowledge chunks via Azure OpenAI.
 *
 * Usage:
 *   set BLOB_CONNECTION_STRING=...
 *   set AZURE_OPENAI_KEY=...          (or fetched from Key Vault)
 *   set AZURE_OPENAI_ENDPOINT=...     (or fetched from Key Vault)
 *   node scripts/embed-backfill.js
 *
 * Reads:  gendwh-exports/latest/gendwh_knowledge.jsonl
 * Writes: gendwh-exports/latest/gendwh_vectors.jsonl
 *
 * Skips chunks already present in gendwh_vectors.jsonl (by chunk_hash).
 */

const crypto = require("crypto");
const { BlobServiceClient } = require("../webapp/node_modules/@azure/storage-blob");
const { DefaultAzureCredential } = require("../webapp/node_modules/@azure/identity");
const { SecretClient } = require("../webapp/node_modules/@azure/keyvault-secrets");

// ── Config ──────────────────────────────────────────────────
const CONTAINER = "gendwh-exports";
const JSONL_BLOB = "latest/gendwh_knowledge.jsonl";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";
const EMBEDDING_MODEL = "text-embedding-ada-002";
const BATCH_SIZE = 20;
const BATCH_DELAY_MS = 500;

const KV_URL = "https://kv-ai-site-builder.vault.azure.net";

// ── Helpers ─────────────────────────────────────────────────
function chunkHash(chunk) {
  const text = chunkToText(chunk);
  return crypto.createHash("sha256").update(text).digest("hex").slice(0, 16);
}

function chunkToText(chunk) {
  // Build a searchable text representation of the chunk (same as what RAG would use)
  const parts = [];
  if (chunk.type) parts.push(`type: ${chunk.type}`);
  if (chunk.id) parts.push(`id: ${chunk.id}`);
  if (chunk.table) parts.push(`table: ${chunk.table}`);
  if (chunk.model_name) parts.push(`model: ${chunk.model_name}`);
  if (chunk.pipeline) parts.push(`pipeline: ${chunk.pipeline}`);
  if (chunk.report_name) parts.push(`report: ${chunk.report_name}`);
  if (chunk.layer) parts.push(`layer: ${chunk.layer}`);
  if (chunk.summary) parts.push(chunk.summary);
  if (chunk.target_field) parts.push(`field: ${chunk.target_field}`);
  if (chunk.source_table) parts.push(`source: ${chunk.source_table}.${chunk.source_column || ""}`);
  if (chunk.expression) parts.push(`expr: ${chunk.expression}`);
  if (chunk.business_logic) parts.push(chunk.business_logic);
  if (chunk.definition) parts.push(chunk.definition.slice(0, 2000));
  if (chunk.description) parts.push(chunk.description);
  // SM-specific
  if (chunk.tables_detail) parts.push(JSON.stringify(chunk.tables_detail).slice(0, 3000));
  if (chunk.measures) parts.push(chunk.measures.map(m => `${m.name}: ${m.expression || ""}`).join("; ").slice(0, 3000));
  if (chunk.columns) parts.push(chunk.columns.map(c => c.name || c).join(", ").slice(0, 1000));
  // Warehouse
  if (chunk.schema && chunk.name) parts.push(`${chunk.schema}.${chunk.name}`);
  return parts.join("\n") || JSON.stringify(chunk).slice(0, 4000);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function downloadText(container, blobPath) {
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

async function uploadText(container, blobPath, content) {
  const blob = container.getBlockBlobClient(blobPath);
  const buf = Buffer.from(content, "utf8");
  await blob.upload(buf, buf.length, {
    blobHTTPHeaders: { blobContentType: "application/x-ndjson" },
  });
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

async function getSecrets() {
  let apiKey = process.env.AZURE_OPENAI_KEY;
  let endpoint = process.env.AZURE_OPENAI_ENDPOINT;
  if (apiKey && endpoint) return { apiKey, endpoint };
  console.log("Fetching Azure OpenAI credentials from Key Vault...");
  const cred = new DefaultAzureCredential();
  const kv = new SecretClient(KV_URL, cred);
  if (!apiKey) apiKey = (await kv.getSecret("azure-openai-key")).value;
  if (!endpoint) endpoint = (await kv.getSecret("azure-openai-endpoint")).value;
  return { apiKey, endpoint };
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) { console.error("Set BLOB_CONNECTION_STRING env var"); process.exit(1); }

  const { apiKey, endpoint } = await getSecrets();
  console.log(`Endpoint: ${endpoint}`);
  console.log(`Model: ${EMBEDDING_MODEL}`);

  const svc = BlobServiceClient.fromConnectionString(connStr);
  const container = svc.getContainerClient(CONTAINER);

  // 1. Download knowledge JSONL
  console.log("Downloading gendwh_knowledge.jsonl...");
  const jsonlText = await downloadText(container, JSONL_BLOB);
  if (!jsonlText) { console.error("Knowledge JSONL not found"); process.exit(1); }
  const chunks = jsonlText.trim().split("\n").map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  console.log(`Loaded ${chunks.length} chunks.`);

  // 2. Download existing vectors (if any)
  console.log("Downloading existing gendwh_vectors.jsonl...");
  const existingText = await downloadText(container, VECTORS_BLOB);
  const existingMap = new Map(); // hash → line
  if (existingText) {
    for (const line of existingText.trim().split("\n")) {
      try { const v = JSON.parse(line); if (v.chunk_hash) existingMap.set(v.chunk_hash, line); } catch {}
    }
    console.log(`Existing vectors: ${existingMap.size}`);
  }

  // 3. Find chunks that need embedding
  const toEmbed = [];
  for (const chunk of chunks) {
    const hash = chunkHash(chunk);
    if (!existingMap.has(hash)) toEmbed.push({ chunk, hash, text: chunkToText(chunk) });
  }
  console.log(`Chunks to embed: ${toEmbed.length} (${existingMap.size} already done)`);
  if (toEmbed.length === 0) { console.log("Nothing to do."); return; }

  // 4. Embed in batches
  const outputLines = [...existingMap.values()]; // keep existing
  let done = 0;
  for (let i = 0; i < toEmbed.length; i += BATCH_SIZE) {
    const batch = toEmbed.slice(i, i + BATCH_SIZE);
    const texts = batch.map(b => b.text.slice(0, 8000)); // ada-002 limit ~8191 tokens
    try {
      const embeddings = await embedBatch(texts, apiKey, endpoint);
      for (let j = 0; j < batch.length; j++) {
        const { chunk, hash } = batch[j];
        const vec = { chunk_hash: hash, chunk_type: chunk.type, id: chunk.id || "", embedding: embeddings[j] };
        // Copy key metadata fields
        for (const k of ["table", "model_name", "pipeline", "report_name", "layer", "target_field", "source_table"]) {
          if (chunk[k]) vec[k] = chunk[k];
        }
        vec.text = batch[j].text; // store text for retrieval
        outputLines.push(JSON.stringify(vec));
      }
      done += batch.length;
      console.log(`  Embedded ${done}/${toEmbed.length} (batch ${Math.floor(i / BATCH_SIZE) + 1})`);
    } catch (err) {
      console.error(`  Batch error at ${i}: ${err.message}`);
    }
    if (i + BATCH_SIZE < toEmbed.length) await sleep(BATCH_DELAY_MS);
  }

  // 5. Upload
  console.log(`Uploading gendwh_vectors.jsonl (${outputLines.length} vectors)...`);
  await uploadText(container, VECTORS_BLOB, outputLines.join("\n"));
  console.log("Done!");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });

