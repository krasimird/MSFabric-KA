#!/usr/bin/env node
/**
 * Standalone analyze-and-embed pipeline script.
 * Runs in Azure DevOps Pipeline (60 min timeout, ample RAM).
 *
 * Replaces the Azure Functions /api/analyze + /api/embed that crash
 * on SWA Free tier (45s timeout / limited memory).
 *
 * Environment variables (set by pipeline YAML):
 *   ANTHROPIC_API_KEY      — Claude API key (from Key Vault)
 *   AZURE_OPENAI_KEY       — Azure OpenAI key (from Key Vault)
 *   AZURE_OPENAI_ENDPOINT  — Azure OpenAI endpoint URL (from Key Vault)
 *   AZURE_CLIENT_ID        — SP credentials for DefaultAzureCredential
 *   AZURE_CLIENT_SECRET
 *   AZURE_TENANT_ID
 */
"use strict";

const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");

// Import builder functions from the Azure Function module
const analyzeFn = require("../webapp/api/analyze/index");
const {
  hashQuery, analyzeQuery, sleep,
  buildExecutionChains, buildWarehouseLineage, buildItemLookup,
  buildLakehouseChunks, buildBronzeMetaChunks, buildSemanticModelChunks,
  buildPipelineChunks, buildNotebookChunks, buildReportChunks,
  buildCatalogChunks, assembleJSONL, chunkToText, chunkHash,
  CONTAINER, RAW_BLOB, JSONL_BLOB, CACHE_BLOB, VECTORS_BLOB,
  BATCH_SIZE, BATCH_DELAY_MS,
} = analyzeFn._internals;

// ── Config ────────────────────────────────────────────────────
const BLOB_ACCOUNT_URL = "https://sainspiritka.blob.core.windows.net";
const EMBEDDING_MODEL = "text-embedding-ada-002";
const EMBED_BATCH_SIZE = 20;
const EMBED_BATCH_DELAY_MS = 500;

// ── Logging ───────────────────────────────────────────────────
const startTime = Date.now();
const elapsed = () => ((Date.now() - startTime) / 1000).toFixed(1);
const log = (...args) => console.log(`[${elapsed()}s]`, ...args);

// ── Blob helpers (DefaultAzureCredential — no connection string) ──
let _blobSvc = null;
function getBlobService() {
  if (!_blobSvc) _blobSvc = new BlobServiceClient(BLOB_ACCOUNT_URL, new DefaultAzureCredential());
  return _blobSvc;
}

async function downloadJSON(blobPath) {
  const svc = getBlobService();
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  log(`Downloading ${blobPath}...`);
  const resp = await blob.download(0);
  const chunks = [];
  for await (const chunk of resp.readableStreamBody) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function downloadJSONSafe(blobPath, fallback) {
  try { return await downloadJSON(blobPath); }
  catch (err) {
    if (String(err.statusCode) === "404" || String(err.message).includes("BlobNotFound")) {
      log(`${blobPath} not found, using fallback.`);
      return fallback;
    }
    throw err;
  }
}

async function downloadTextSafe(blobPath) {
  const svc = getBlobService();
  const container = svc.getContainerClient(CONTAINER);
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

async function uploadBlob(blobPath, content) {
  const svc = getBlobService();
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  const buf = Buffer.from(content, "utf8");
  await blob.upload(buf, buf.length, {
    blobHTTPHeaders: { blobContentType: blobPath.endsWith(".json") ? "application/json" : "application/x-ndjson" },
  });
  log(`Uploaded ${blobPath} (${buf.length} bytes)`);
}

// ── Embedding helper ──────────────────────────────────────────
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

// ── MAIN ──────────────────────────────────────────────────────
async function main() {
  log("=== Knowledge Assistant — Analyze & Embed Pipeline ===");

  // 1. Validate environment
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  const openaiKey = process.env.AZURE_OPENAI_KEY;
  const openaiEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
  if (!anthropicKey) throw new Error("ANTHROPIC_API_KEY not set");
  if (!openaiKey) throw new Error("AZURE_OPENAI_KEY not set");
  if (!openaiEndpoint) throw new Error("AZURE_OPENAI_ENDPOINT not set");
  log("Environment OK.");

  // 2. Download raw export
  log("── Step 1: Downloading raw export ──");
  const KB = await downloadJSON(RAW_BLOB);
  const queries = (KB.metadata && KB.metadata.queries) || [];
  log(`Raw export: ${Object.keys(KB).length} keys, ${queries.length} SQL queries.`);

  // 3. Load analysis cache
  log("── Step 2: Loading analysis cache ──");
  const cache = await downloadJSONSafe(CACHE_BLOB, {});
  log(`Cache: ${Object.keys(cache).length} entries.`);

  // 4. Claude analysis — NO timeout (pipeline has 60 min)
  log("── Step 3: Claude lineage analysis ──");
  const lineageByTable = {};
  let analyzed = 0, skipped = 0, failed = 0;

  // First pass: populate from cache (instant)
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
  log(`${skipped} cached, ${queries.length - skipped} remaining.`);

  // Second pass: analyze uncached with Claude
  const uncached = queries.filter(q => {
    const sql = q.source_query || "";
    return sql.length >= 20 && !cache[hashQuery(sql)];
  });
  log(`${uncached.length} uncached queries for Claude.`);

  for (let i = 0; i < uncached.length; i += BATCH_SIZE) {
    const batch = uncached.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(async (q) => {
      const sql = q.source_query || "";
      const qHash = hashQuery(sql);
      const targetTable = q.target_table || q.meta_table || "unknown";
      try {
        const fields = await analyzeQuery(anthropicKey, targetTable, q.layer || "", q.mode || "", sql, log);
        if (fields && fields.length > 0) {
          cache[qHash] = fields;
          lineageByTable[targetTable] = { layer: q.layer || "", mode: q.mode || "", fields };
          analyzed++;
          log(`  ✓ ${targetTable}: ${fields.length} fields`);
        } else { failed++; log(`  ⚠ ${targetTable}: no fields`); }
      } catch (err) {
        failed++; log(`  ✗ ${targetTable}: ${err.message}`);
        if (err.message.includes("429")) await sleep(5000);
      }
    }));
    log(`Progress: ${Math.min(i + BATCH_SIZE, uncached.length)}/${uncached.length} (new=${analyzed}, fail=${failed})`);
    if (i + BATCH_SIZE < uncached.length) await sleep(BATCH_DELAY_MS);
  }

  // 5. Save cache
  log("Saving analysis cache...");
  await uploadBlob(CACHE_BLOB, JSON.stringify(cache, null, 2));

  // 6. Build all chunk types
  log("── Step 4: Building chunks ──");
  const safeBuilder = (label, fn) => {
    try { log(`  ${label}...`); const r = fn(); log(`    → ${r.length} chunks`); return r; }
    catch (err) { log(`    ✗ ${label} FAILED: ${err.message}`); return []; }
  };

  const chains = safeBuilder("Execution chains", () => buildExecutionChains(KB));
  const warehouseChunks = safeBuilder("Warehouse lineage", () => buildWarehouseLineage(KB));
  const itemLookup = buildItemLookup(KB);
  const lakehouseChunks = safeBuilder("Lakehouse chunks", () => buildLakehouseChunks(KB, itemLookup));
  const bronzeChunks = safeBuilder("Bronze meta chunks", () => buildBronzeMetaChunks(KB));
  const smChunks = safeBuilder("Semantic model chunks", () => buildSemanticModelChunks(KB, itemLookup));
  const pipelineChunks = safeBuilder("Pipeline chunks", () => buildPipelineChunks(KB, itemLookup));
  const notebookChunks = safeBuilder("Notebook chunks", () => buildNotebookChunks(KB, itemLookup));
  const reportChunks = safeBuilder("Report chunks", () => buildReportChunks(KB, itemLookup));
  const allExtra = [...lakehouseChunks, ...bronzeChunks, ...smChunks, ...pipelineChunks, ...notebookChunks, ...reportChunks];
  const catalogChunks = safeBuilder("Catalog chunks", () => buildCatalogChunks([...warehouseChunks, ...allExtra]));

  // 7. Build table→workspace map from lakehouse chunks (for field_detail & table_lineage)
  const tableWorkspaceMap = {};
  for (const c of lakehouseChunks) {
    if (c.table_name && c.workspace) tableWorkspaceMap[c.table_name] = c.workspace;
  }
  log(`Table→workspace map: ${Object.keys(tableWorkspaceMap).length} entries.`);

  // 8. Assemble & upload JSONL
  log("── Step 5: Assembling JSONL ──");
  const extraArrays = [lakehouseChunks, bronzeChunks, smChunks, pipelineChunks, notebookChunks, reportChunks, catalogChunks];
  const jsonl = assembleJSONL(lineageByTable, chains, warehouseChunks, extraArrays, tableWorkspaceMap);
  const lineCount = jsonl.split("\n").length;
  log(`JSONL: ${lineCount} lines, ${jsonl.length} bytes.`);

  await uploadBlob(JSONL_BLOB, jsonl);
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  await uploadBlob(`archive/${ts}_knowledge.jsonl`, jsonl);

  // ═══════════════════════════════════════════════════════════
  // EMBEDDING PHASE
  // ═══════════════════════════════════════════════════════════
  log("── Step 6: Embedding ──");

  const newChunks = jsonl.trim().split("\n").map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  log(`Chunks to embed: ${newChunks.length}`);

  // Download existing vectors for incremental embedding
  const existingText = await downloadTextSafe(VECTORS_BLOB);
  const existingMap = new Map();
  if (existingText) {
    for (const line of existingText.trim().split("\n")) {
      try { const v = JSON.parse(line); if (v.chunk_hash) existingMap.set(v.chunk_hash, line); } catch {}
    }
  }
  log(`Existing vectors: ${existingMap.size}`);

  // Diff: find chunks needing embedding
  const toEmbed = [];
  const newHashes = new Set();
  const chunkByHash = new Map();
  for (const chunk of newChunks) {
    const hash = chunkHash(chunk);
    newHashes.add(hash);
    chunkByHash.set(hash, chunk);
    if (!existingMap.has(hash)) toEmbed.push({ chunk, hash, text: chunkToText(chunk) });
  }
  log(`To embed: ${toEmbed.length}, unchanged: ${newChunks.length - toEmbed.length}`);

  // Keep only vectors for chunks still present — re-enrich with current metadata
  const META_KEYS = ["table", "model_name", "pipeline", "report_name", "layer", "target_field", "source_table", "workspace", "zone", "warehouse_name", "lakehouse", "table_name", "warehouse_id", "definition_status"];
  const outputLines = [];
  for (const [hash, line] of existingMap) {
    if (newHashes.has(hash)) {
      const vec = JSON.parse(line);
      const chunk = chunkByHash.get(hash);
      if (chunk) {
        for (const k of META_KEYS) {
          if (chunk[k] !== undefined) vec[k] = chunk[k];
        }
      }
      outputLines.push(JSON.stringify(vec));
    }
  }

  // Embed in batches — no timeout
  let embeddedCount = 0;
  for (let i = 0; i < toEmbed.length; i += EMBED_BATCH_SIZE) {
    const batch = toEmbed.slice(i, i + EMBED_BATCH_SIZE);
    const texts = batch.map(b => b.text.slice(0, 8000));
    try {
      const embeddings = await embedBatch(texts, openaiKey, openaiEndpoint);
      for (let j = 0; j < batch.length; j++) {
        const { chunk, hash } = batch[j];
        const vec = { chunk_hash: hash, chunk_type: chunk.type, id: chunk.id || "", embedding: embeddings[j] };
        for (const k of [
          "table", "model_name", "pipeline", "report_name",
          "layer", "target_field", "source_table",
          "workspace", "zone", "warehouse_name",
          "lakehouse", "table_name", "warehouse_id",
          "definition_status"
        ]) {
          if (chunk[k] !== undefined) vec[k] = chunk[k];
        }
        vec.text = batch[j].text;
        outputLines.push(JSON.stringify(vec));
      }
      embeddedCount += batch.length;
    } catch (err) {
      log(`  Embed batch error at ${i}: ${err.message}`);
    }
    if (i + EMBED_BATCH_SIZE < toEmbed.length) await sleep(EMBED_BATCH_DELAY_MS);
    if ((i % 100) === 0 && i > 0) log(`  Embedded ${embeddedCount}/${toEmbed.length}...`);
  }

  // Upload vectors
  log(`Uploading vectors (${outputLines.length} total)...`);
  await uploadBlob(VECTORS_BLOB, outputLines.join("\n"));

  // Summary
  log("── Summary ──");
  log(`  Queries:  ${queries.length} total, ${analyzed} new, ${skipped} cached, ${failed} failed`);
  log(`  Chunks:   ${lineCount} JSONL lines`);
  log(`  Vectors:  ${outputLines.length} total, ${embeddedCount} embedded now`);
  log(`  Elapsed:  ${elapsed()}s`);
}

main().then(() => {
  log("=== Pipeline complete ===");
  process.exit(0);
}).catch(err => {
  console.error(`\nFATAL: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});

