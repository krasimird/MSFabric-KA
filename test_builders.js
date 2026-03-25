/**
 * Standalone test: exercises ALL builder functions from analyze/index.js
 * against real data from Azure Blob Storage.
 * Usage: node test_builders.js
 */
const path = require("path");
// Resolve modules from webapp/api/node_modules
const apiDir = path.join(__dirname, "webapp", "api");
const { BlobServiceClient } = require(path.join(apiDir, "node_modules", "@azure", "storage-blob"));
const crypto = require("crypto");

// Connection string from local.settings.json
const CONN_STR = "BlobEndpoint=https://sainspiritka.blob.core.windows.net/;QueueEndpoint=https://sainspiritka.queue.core.windows.net/;FileEndpoint=https://sainspiritka.file.core.windows.net/;TableEndpoint=https://sainspiritka.table.core.windows.net/;SharedAccessSignature=sv=2024-11-04&ss=b&srt=co&sp=rwlctfx&se=2027-03-07T00:28:21Z&st=2026-03-07T16:13:21Z&spr=https&sig=3XxxVJn8N2QCYUW7kvtS56PuHNPFsQy4tJGO29oLaAI%3D";
const CONTAINER = "gendwh-exports";
const RAW_BLOB = "latest/gendwh_raw_export.json";

async function downloadJSON(blobPath) {
  const svc = BlobServiceClient.fromConnectionString(CONN_STR);
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(blobPath);
  console.log(`Downloading ${blobPath}...`);
  const resp = await blob.download(0);
  const chunks = [];
  for await (const chunk of resp.readableStreamBody) chunks.push(chunk);
  const buf = Buffer.concat(chunks);
  console.log(`Downloaded ${(buf.length / 1048576).toFixed(2)} MB`);
  return JSON.parse(buf.toString("utf8"));
}

// ---- Copy builder functions from index.js (they are pure, no Azure deps) ----
// We require them by extracting from the module. But since index.js exports only the handler,
// we'll re-import the source and eval the functions. Easier: just require and mock context.

// Actually, let's just load the file as text and extract functions via a different approach.
// Simplest: copy the essential functions inline. But that's fragile.
// Best approach: temporarily add module.exports to index.js? No, let's just re-require with a shim.

// The cleanest approach: load the raw export, then call the handler with test mode.
// But the handler needs context object. Let's mock it:

async function main() {
  const t0 = Date.now();
  const el = () => ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`[${el()}s] Downloading raw export...`);
  const KB = await downloadJSON(RAW_BLOB);
  console.log(`[${el()}s] Raw export keys: ${Object.keys(KB).join(", ")}`);
  console.log(`  workspaces: ${(KB.workspaces || []).length}`);
  console.log(`  schemas keys: ${KB.schemas ? Object.keys(KB.schemas).length : 0}`);
  console.log(`  definitions keys: ${KB.definitions ? Object.keys(KB.definitions).length : 0}`);
  console.log(`  queries: ${KB.metadata && KB.metadata.queries ? KB.metadata.queries.length : 0}`);
  console.log(`  bronze_meta: ${KB.metadata && KB.metadata.bronze_meta ? (Array.isArray(KB.metadata.bronze_meta) ? KB.metadata.bronze_meta.length : Object.keys(KB.metadata.bronze_meta).length) : 0}`);

  // Set env so getBlobClient works
  process.env.BLOB_CONNECTION_STRING = CONN_STR;
  // Fake API key to bypass Key Vault
  process.env.ANTHROPIC_API_KEY = "sk-fake-for-testing";

  const handler = require("./webapp/api/analyze/index.js");

  // === Test 1: builders test step ===
  console.log(`\n[${el()}s] === TEST 1: step=builders ===`);
  const ctx1 = makeMockContext();
  await handler(ctx1, { body: { test: true, step: "builders" } });
  console.log(`Status: ${ctx1.res.status}`);
  const r1 = JSON.parse(ctx1.res.body);
  if (r1.builders) {
    for (const [name, info] of Object.entries(r1.builders)) {
      console.log(`  ${info.ok ? "✅" : "❌"} ${name}: ${info.count} chunks${info.error ? ` — ${info.error}` : ""}`);
    }
  }

  // === Test 2: full-dry test step ===
  console.log(`\n[${el()}s] === TEST 2: step=full-dry ===`);
  const ctx2 = makeMockContext();
  await handler(ctx2, { body: { test: true, step: "full-dry" } });
  console.log(`Status: ${ctx2.res.status}`);
  console.log(JSON.parse(ctx2.res.body));

  // === Test 3: Full analyze handler (will try Claude, fail on fake key, but we see WHERE it fails) ===
  console.log(`\n[${el()}s] === TEST 3: Full analyze (real flow, fake API key) ===`);
  const ctx3 = makeMockContext();
  try {
    await handler(ctx3, { body: {} });
    console.log(`\n[${el()}s] Analyze returned.`);
    console.log(`Status: ${ctx3.res ? ctx3.res.status : "no response"}`);
    if (ctx3.res && ctx3.res.body) {
      const result = JSON.parse(ctx3.res.body);
      console.log(JSON.stringify(result, null, 2));
    }
  } catch (err) {
    console.error(`\n[${el()}s] ❌ UNCAUGHT ERROR:`);
    console.error(err.message);
    console.error(err.stack);
  }

  // === Test 4: Embed endpoint ===
  console.log(`\n[${el()}s] === TEST 4: Embed endpoint ===`);
  const embedHandler = require("./webapp/api/embed/index.js");
  const ctx4 = makeMockContext();
  try {
    await embedHandler(ctx4, { body: {} });
    console.log(`\n[${el()}s] Embed returned.`);
    console.log(`Status: ${ctx4.res ? ctx4.res.status : "no response"}`);
    if (ctx4.res && ctx4.res.body) {
      const result = JSON.parse(ctx4.res.body);
      console.log(JSON.stringify(result, null, 2));
    }
  } catch (err) {
    console.error(`\n[${el()}s] ❌ EMBED UNCAUGHT ERROR:`);
    console.error(err.message);
    console.error(err.stack);
  }

  function makeMockContext() {
    const ctx = { res: null, log: (...a) => console.log(`  [LOG] ${a.join(" ")}`) };
    ctx.log.info = ctx.log;
    ctx.log.warn = ctx.log;
    ctx.log.error = (...a) => console.error(`  [ERR] ${a.join(" ")}`);
    return ctx;
  }
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});

