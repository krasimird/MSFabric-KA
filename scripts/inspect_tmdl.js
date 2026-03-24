/**
 * Inspect TMDL payloads from the raw export to see what's available.
 * Usage: node scripts/inspect_tmdl.js
 * Requires BLOB_CONNECTION_STRING env var.
 */
const { BlobServiceClient } = require("../webapp/node_modules/@azure/storage-blob");

const CONTAINER = "gendwh-exports";
const RAW_BLOB = "latest/gendwh_raw_export.json";

async function main() {
  const connStr = process.env.BLOB_CONNECTION_STRING;
  if (!connStr) {
    // Try to read from local_settings or similar
    console.error("Set BLOB_CONNECTION_STRING env var");
    process.exit(1);
  }

  console.log("Downloading raw export...");
  const svc = BlobServiceClient.fromConnectionString(connStr);
  const container = svc.getContainerClient(CONTAINER);
  const blob = container.getBlockBlobClient(RAW_BLOB);
  const resp = await blob.download(0);
  const chunks = [];
  for await (const chunk of resp.readableStreamBody) chunks.push(chunk);
  const KB = JSON.parse(Buffer.concat(chunks).toString("utf8"));

  console.log(`\nExport keys: ${Object.keys(KB).join(", ")}`);
  console.log(`Workspaces: ${KB.workspaces?.length}`);
  console.log(`Definitions: ${Object.keys(KB.definitions || {}).length}`);

  // ── SUMMARY ──
  const out = [];
  const log = (s) => { out.push(s); };

  log("=== ALL SEMANTIC MODELS ===");
  for (const ws of KB.workspaces || []) {
    for (const item of ws.items || []) {
      if (item.type !== "SemanticModel") continue;
      const defParts = KB.definitions[item.id];
      if (!Array.isArray(defParts)) continue;
      const tmdl = defParts.filter(p => (p.path||"").endsWith(".tmdl"));
      const tables = tmdl.filter(p => (p.path||"").includes("/tables/"));
      const realTables = tables.filter(p => !(p.path||"").includes("LocalDateTable") && !(p.path||"").includes("DateTableTemplate"));
      log(`  ${item.displayName} | ws=${ws.displayName} | parts=${defParts.length} | tables=${realTables.length} | has_relationships=${!!tmdl.find(p=>(p.path||"").includes("relationships"))} | has_perspectives=${!!tmdl.find(p=>(p.path||"").includes("perspectives"))}`);
    }
  }

  // ── Show first real model detail ──
  let foundModel = false;
  for (const ws of KB.workspaces || []) {
    if (foundModel) break;
    for (const item of ws.items || []) {
      if (item.type !== "SemanticModel") continue;
      if (item.displayName === "Report Usage Metrics Model") continue;
      const defParts = KB.definitions[item.id];
      if (!Array.isArray(defParts)) continue;
      const realTables = defParts.filter(p => {
        const path = p.path || "";
        return path.includes("/tables/") && path.endsWith(".tmdl")
          && !path.includes("LocalDateTable") && !path.includes("DateTableTemplate");
      });

      log(`\n=== MODEL DETAIL: ${item.displayName} ===`);
      log(`Parts: ${defParts.map(p=>p.path).join(", ")}`);
      log(`Real tables: ${realTables.map(p=>p.path).join(", ")}`);

      // Show first fact table + _Measures table
      const factTable = realTables.find(p => (p.path||"").includes("fact_"));
      const measTable = realTables.find(p => (p.path||"").includes("_Measures") || (p.path||"").includes("measures"));
      for (const part of [factTable, measTable].filter(Boolean)) {
        log(`\n--- ${part.path} (first 150 lines) ---`);
        const lines = (part.payload||"").split("\n").slice(0, 150);
        for (const l of lines) log(l);
        if ((part.payload||"").split("\n").length > 150) log(`... (${(part.payload||"").split("\n").length - 150} more lines)`);
      }

      // Relationships
      const rel = defParts.find(p => (p.path||"").endsWith("relationships.tmdl"));
      if (rel) {
        log(`\n--- ${rel.path} (first 60 lines) ---`);
        const lines = (rel.payload||"").split("\n").slice(0, 60);
        for (const l of lines) log(l);
      }

      // Perspectives
      const persp = defParts.find(p => (p.path||"").endsWith("perspectives.tmdl"));
      if (persp) {
        log(`\n--- ${persp.path} (PERSPECTIVES) ---`);
        const lines = (persp.payload||"").split("\n").slice(0, 40);
        for (const l of lines) log(l);
      } else {
        log("\n(No perspectives.tmdl)");
      }
      foundModel = true;
      break;
    }
  }

  // ── Show first Report detail ──
  log("\n\n=== ALL REPORTS ===");
  for (const ws of KB.workspaces || []) {
    for (const item of ws.items || []) {
      if (item.type !== "Report") continue;
      const defParts = KB.definitions[item.id];
      if (!Array.isArray(defParts)) continue;
      const paths = defParts.map(p => p.path).join(", ");
      log(`  ${item.displayName} | ws=${ws.displayName} | parts=${defParts.length} | files=${paths}`);
    }
  }

  let foundReport = false;
  for (const ws of KB.workspaces || []) {
    if (foundReport) break;
    for (const item of ws.items || []) {
      if (item.type !== "Report") continue;
      if ((item.displayName||"").includes("Usage Metrics")) continue;
      const defParts = KB.definitions[item.id];
      if (!Array.isArray(defParts)) continue;

      log(`\n=== REPORT DETAIL: ${item.displayName} ===`);

      // Show .pbir
      const pbir = defParts.find(p => (p.path||"").endsWith(".pbir"));
      if (pbir) {
        log(`\n--- ${pbir.path} ---`);
        log(pbir.payload || "(empty)");
      }

      // Show report.json structure
      const rj = defParts.find(p => (p.path||"") === "report.json");
      if (rj) {
        let rjson;
        try { rjson = JSON.parse(rj.payload); } catch { log("(parse error)"); continue; }
        log(`\n--- report.json top-level keys: ${Object.keys(rjson).join(", ")} ---`);
        log(`Sections: ${(rjson.sections||[]).length}`);

        // Show first section (page) detail
        const sec = (rjson.sections||[])[0];
        if (sec) {
          log(`\n--- First page: "${sec.displayName||sec.name}" ---`);
          log(`  Keys: ${Object.keys(sec).join(", ")}`);
          log(`  visualContainers: ${(sec.visualContainers||[]).length}`);
          if (sec.filters) log(`  PAGE filters: ${sec.filters}`);

          // Show first 2 visual containers in detail
          for (let vi = 0; vi < Math.min(2, (sec.visualContainers||[]).length); vi++) {
            const vc = sec.visualContainers[vi];
            log(`\n  --- Visual #${vi} ---`);
            log(`    VC keys: ${Object.keys(vc).join(", ")}`);
            if (vc.config) {
              let cfg;
              try { cfg = JSON.parse(vc.config); } catch { log("    (config parse error)"); continue; }
              log(`    config keys: ${Object.keys(cfg).join(", ")}`);
              const sv = cfg.singleVisual || {};
              log(`    visualType: ${sv.visualType}`);
              log(`    singleVisual keys: ${Object.keys(sv).join(", ")}`);
              if (sv.prototypeQuery) {
                log(`    prototypeQuery keys: ${Object.keys(sv.prototypeQuery).join(", ")}`);
                log(`    prototypeQuery: ${JSON.stringify(sv.prototypeQuery).slice(0, 800)}`);
              }
              if (sv.vcObjects) log(`    vcObjects keys: ${Object.keys(sv.vcObjects).join(", ")}`);
            }
            if (vc.filters) log(`    VISUAL filters: ${vc.filters.slice(0, 500)}`);
            if (vc.query) log(`    query: ${vc.query.slice(0, 300)}`);
          }
        }
      }

      foundReport = true;
      break;
    }
  }

  // ── Show PBIP-format report ──
  for (const ws of KB.workspaces || []) {
    for (const item of ws.items || []) {
      if ((item.displayName||"") !== "GEN_PBI_EXPORT_POC") continue;
      log(`\nFOUND: ${item.displayName} type=${item.type} id=${item.id}`);
      const defParts = KB.definitions[item.id];
      if (!Array.isArray(defParts)) { log("  NO DEFINITIONS"); continue; }
      log(`\n=== PBIP REPORT: ${item.displayName} (${defParts.length} parts) ===`);
      // Show page.json
      const pageFile = defParts.find(p => (p.path||"").endsWith("page.json") && (p.path||"").includes("pages/"));
      if (pageFile) {
        log(`\n--- ${pageFile.path} ---`);
        log(pageFile.payload.slice(0, 1500));
      }
      // Show first visual.json
      const vizFile = defParts.find(p => (p.path||"").endsWith("visual.json") && (p.path||"").includes("visuals/"));
      if (vizFile) {
        log(`\n--- ${vizFile.path} ---`);
        log(vizFile.payload.slice(0, 2000));
      }
      // Show definition/report.json (PBIP version)
      const defReport = defParts.find(p => (p.path||"") === "definition/report.json");
      if (defReport) {
        log(`\n--- definition/report.json (first 500 chars) ---`);
        log(defReport.payload.slice(0, 500));
      }
    }
  }

  // Write to file
  const fs = require("fs");
  fs.writeFileSync("scripts/tmdl_inspection.txt", out.join("\n"), "utf8");
  console.log(`\nWrote ${out.length} lines to scripts/tmdl_inspection.txt`);
}

main().catch(err => { console.error(err); process.exit(1); });

