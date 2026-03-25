#!/usr/bin/env node
/**
 * Standalone pipeline: Analyze (Claude lineage) + Build chunks + Embed vectors.
 * Runs outside Azure Functions — no timeout, no memory limits.
 *
 * Required env vars:
 *   BLOB_CONNECTION_STRING
 *   ANTHROPIC_API_KEY
 *   AZURE_OPENAI_KEY
 *   AZURE_OPENAI_ENDPOINT
 *
 * Usage:
 *   node scripts/analyze-and-embed.js
 */

const path = require("path");
const crypto = require("crypto");

// Resolve Azure SDK from webapp/api/node_modules
const apiDir = path.join(__dirname, "..", "webapp", "api");
const { BlobServiceClient } = require(path.join(apiDir, "node_modules", "@azure", "storage-blob"));

// Import builder functions from analyze/index.js
const analyze = require(path.join(apiDir, "analyze", "index.js"));
const {
  buildExecutionChains, buildWarehouseLineage, buildItemLookup,
  buildLakehouseChunks, buildBronzeMetaChunks, buildSemanticModelChunks,
  buildPipelineChunks, buildNotebookChunks, buildReportChunks,
  buildCatalogChunks, assembleJSONL, chunkToText, chunkHash,
  hashQuery, analyzeQuery,
} = analyze;

// ── Config ──────────────────────────────────────────────────
const CONTAINER = "gendwh-exports";
const RAW_BLOB = "latest/gendwh_raw_export.json";
const JSONL_BLOB = "latest/gendwh_knowledge.jsonl";
const CACHE_BLOB = "latest/analysis_cache.json";
const VECTORS_BLOB = "latest/gendwh_vectors.jsonl";
const EMBEDDING_MODEL = "text-embedding-ada-002";
const BATCH_SIZE = 5;       // Claude batches
const EMBED_BATCH_SIZE = 20;
const EMBED_BATCH_DELAY_MS = 500;
const BATCH_DELAY_MS = 1000;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
const t0 = Date.now();
const elapsed = () => ((Date.now() - t0) / 1000).toFixed(1);
const log = (...args) => console.log(`[${elapsed()}s]`, ...args);

// ── Blob helpers ────────────────────────────────────────────
let svc, containerClient;
function initBlob() {
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) { console.error("Set BLOB_CONNECTION_STRING env var"); process.exit(1); }
  svc = BlobServiceClient.fromConnectionString(connStr);
  containerClient = svc.getContainerClient(CONTAINER);
}

async function downloadJSON(blobPath) {
  const blob = containerClient.getBlockBlobClient(blobPath);
  const resp = await blob.download(0);
  const chunks = [];
  for await (const c of resp.readableStreamBody) chunks.push(c);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function downloadJSONSafe(blobPath, fallback) {
  try { return await downloadJSON(blobPath); }
  catch (err) {
    if (String(err.statusCode) === "404" || String(err.message).includes("BlobNotFound")) return fallback;
    throw err;
  }
}

async function downloadTextSafe(blobPath) {
  const blob = containerClient.getBlockBlobClient(blobPath);
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

async function uploadBlob(blobPath, content) {
  const blob = containerClient.getBlockBlobClient(blobPath);
  const buf = Buffer.from(content, "utf8");
  const ct = blobPath.endsWith(".json") ? "application/json" : "application/x-ndjson";
  await blob.upload(buf, buf.length, { blobHTTPHeaders: { blobContentType: ct } });
  log(`Uploaded ${blobPath} (${(buf.length / 1048576).toFixed(2)} MB)`);
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

// ── Main ────────────────────────────────────────────────────
async function main() {
  initBlob();
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) { console.error("Set ANTHROPIC_API_KEY env var"); process.exit(1); }
  const openaiKey = process.env.AZURE_OPENAI_KEY;
  const openaiEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  if (!openaiKey || !openaiEndpoint) { console.error("Set AZURE_OPENAI_KEY and AZURE_OPENAI_ENDPOINT"); process.exit(1); }

  // ═══ PHASE 1: ANALYZE ═══

  // Analyze queries with Claude (cached ones are instant)
  const lineageByTable = {};
  let analyzed = 0, skipped = 0, failed = 0;

  // First pass: populate from cache
  for (const q of queries) {
    const sql = q.source_query || "";
    if (!sql || sql.length < 20) { skipped++; continue; }
    const qHash = hashQuery(sql);
    const targetTable = q.target_table || q.meta_table || "unknown";
    if (cache[qHash]) {
      lineageByTable[targetTable] = { layer: q.layer || "", mode: q.mode || "", fields: cache[qHash] };
      skipped++;
    }
  }
  log(`${skipped} cached, ${queries.length - skipped} remaining`);

  // Second pass: analyze uncached with Claude
  const uncached = queries.filter(q => {
    const sql = q.source_query || "";
    if (!sql || sql.length < 20) return false;
    return !cache[hashQuery(sql)];
  });
  log(`${uncached.length} uncached queries to analyze with Claude`);

  for (let i = 0; i < uncached.length; i += BATCH_SIZE) {
    const batch = uncached.slice(i, i + BATCH_SIZE);
    const promises = batch.map(async (q) => {
      const sql = q.source_query || "";
      const qHash = hashQuery(sql);
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
          log("  Rate limited — waiting 10s...");
          await sleep(10000);
        }
      }
    });
    await Promise.all(promises);
    log(`Progress: ${Math.min(i + BATCH_SIZE, uncached.length)}/${uncached.length}`);
    if (i + BATCH_SIZE < uncached.length) await sleep(BATCH_DELAY_MS);
  }

  // Save cache
  log("Saving analysis cache...");
  await uploadBlob(CACHE_BLOB, JSON.stringify(cache, null, 2));

  // ═══ PHASE 2: BUILD CHUNKS ═══
  log("=== PHASE 2: BUILD CHUNKS ===");

  const chains = buildExecutionChains(KB);
  log(`Execution chains: ${chains.length}`);
  const warehouseChunks = buildWarehouseLineage(KB);
  log(`Warehouse chunks: ${warehouseChunks.length}`);
  const itemLookup = buildItemLookup(KB);
  log(`Item lookup: ${Object.keys(itemLookup).length} items`);

  const lakehouseChunks = buildLakehouseChunks(KB, itemLookup);
  log(`Lakehouse chunks: ${lakehouseChunks.length}`);
  const bronzeChunks = buildBronzeMetaChunks(KB);
  log(`Bronze chunks: ${bronzeChunks.length}`);
  const smChunks = buildSemanticModelChunks(KB, itemLookup);
  log(`Semantic model chunks: ${smChunks.length}`);
  const pipelineChunks = buildPipelineChunks(KB, itemLookup);
  log(`Pipeline chunks: ${pipelineChunks.length}`);
  const notebookChunks = buildNotebookChunks(KB, itemLookup);
  log(`Notebook chunks: ${notebookChunks.length}`);
  const reportChunks = buildReportChunks(KB, itemLookup);
  log(`Report chunks: ${reportChunks.length}`);
  const allExtra = [...lakehouseChunks, ...bronzeChunks, ...smChunks, ...pipelineChunks, ...notebookChunks, ...reportChunks];
  const catalogChunks = buildCatalogChunks([...warehouseChunks, ...allExtra]);
  log(`Catalog chunks: ${catalogChunks.length}`);

  // Assemble JSONL
  const jsonl = assembleJSONL(lineageByTable, chains, warehouseChunks,
    [lakehouseChunks, bronzeChunks, smChunks, pipelineChunks, notebookChunks, reportChunks, catalogChunks]);
  const lineCount = jsonl.split("\n").length;
  log(`JSONL: ${lineCount} lines, ${(jsonl.length / 1048576).toFixed(2)} MB`);

  // Upload JSONL
  await uploadBlob(JSONL_BLOB, jsonl);
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  await uploadBlob(`archive/${ts}_knowledge.jsonl`, jsonl);

  // ═══ PHASE 3: EMBED ═══
  log("=== PHASE 3: EMBED ===");

  const newChunks = jsonl.trim().split("\n").map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  log(`Parsed ${newChunks.length} chunks for embedding`);

  // Load existing vectors
  const existingText = await downloadTextSafe(VECTORS_BLOB);
  const existingMap = new Map();
  if (existingText) {
    for (const line of existingText.trim().split("\n")) {
      try { const v = JSON.parse(line); if (v.chunk_hash) existingMap.set(v.chunk_hash, line); } catch {}
    }
  }
  log(`Existing vectors: ${existingMap.size}`);

  // Diff
  const toEmbed = [];
  const newHashes = new Set();
  for (const chunk of newChunks) {
    const hash = chunkHash(chunk);
    newHashes.add(hash);
    if (!existingMap.has(hash)) toEmbed.push({ chunk, hash, text: chunkToText(chunk) });
  }
  log(`To embed: ${toEmbed.length} new/changed (${newChunks.length - toEmbed.length} unchanged)`);

  // Keep only vectors for chunks that still exist
  const outputLines = [];
  for (const [hash, line] of existingMap) {
    if (newHashes.has(hash)) outputLines.push(line);
  }
  log(`Kept ${outputLines.length} existing vectors, pruned ${existingMap.size - outputLines.length}`);

  // Embed in batches
  let embeddedCount = 0;
  for (let i = 0; i < toEmbed.length; i += EMBED_BATCH_SIZE) {
    const batch = toEmbed.slice(i, i + EMBED_BATCH_SIZE);
    const texts = batch.map(b => b.text.slice(0, 8000));
    try {
      const embeddings = await embedBatch(texts, openaiKey, openaiEndpoint);
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
      log(`  Embedded ${embeddedCount}/${toEmbed.length}`);
    } catch (err) {
      log(`  ✗ Batch error at ${i}: ${err.message}`);
      if (err.message.includes("429")) {
        log("  Rate limited — waiting 10s...");
        await sleep(10000);
        i -= EMBED_BATCH_SIZE; // retry
      }
    }
    if (i + EMBED_BATCH_SIZE < toEmbed.length) await sleep(EMBED_BATCH_DELAY_MS);
  }

  // Upload vectors
  log(`Uploading vectors (${outputLines.length} total)...`);
  await uploadBlob(VECTORS_BLOB, outputLines.join("\n"));

  // ═══ SUMMARY ═══
  log("=== DONE ===");
  log(`Queries: ${queries.length} total, ${analyzed} new, ${skipped} cached, ${failed} failed`);
  log(`Chunks: ${lineCount} JSONL lines`);
  log(`Vectors: ${outputLines.length} total, ${embeddedCount} new`);
  log(`Total time: ${elapsed()}s`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
