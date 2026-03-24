
// ── Mermaid init (startOnLoad: false — we render manually after each message) ──
mermaid.initialize({ startOnLoad: false, theme: 'default', securityLevel: 'loose' });

/* ═══════════════════════════════════════════════════════════════
   GenDWH Knowledge Assistant — Client-side RAG + Chat
   ═══════════════════════════════════════════════════════════════ */

// ── Config ─────────────────────────────────────────────────────
const DATA_URL = 'https://sainspiritka.blob.core.windows.net/gendwh-exports/latest/gendwh_raw_export.json';
const JSONL_URL = 'https://sainspiritka.blob.core.windows.net/gendwh-exports/latest/gendwh_knowledge.jsonl';

// ── State ──────────────────────────────────────────────────────
let KB = null;           // Raw JSON export
let KNOWLEDGE = [];      // Pre-built JSONL chunks (from server-side analysis)
let chatHistory = [];    // {role, content} pairs for Claude API
const MAX_HISTORY = 10;  // Last N messages sent as context
const kbCache = new KBCache();
let aiAnalysis = null;
let analysisReady = false; // true when JSONL knowledge or IndexedDB has lineage data
let PIPELINE_INDEX = {};  // pipeline name/activity name → pipeline item name (built on load)

// ── DOM refs ───────────────────────────────────────────────────
const chatArea  = document.getElementById('chatArea');
const userInput = document.getElementById('userInput');
const sendBtn   = document.getElementById('sendBtn');
const sidebar   = document.getElementById('sidebar');
const menuToggle= document.getElementById('menuToggle');

// ── Data loading ───────────────────────────────────────────────
async function loadData() {
  const statusEl = document.getElementById('dataStatus');
  const analysisSection = document.getElementById('analysisSection');
  const analysisStatus = document.getElementById('analysisStatus');
  const progressBar = document.getElementById('analysisProgress');

  try {
    statusEl.innerHTML = '<span class="dot dot-loading"></span> Loading data…';
    const resp = await fetch(DATA_URL);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    KB = await resp.json();
    populateStats();
    statusEl.innerHTML = '<span class="dot dot-ok"></span> Data loaded ✓';

    // Try to load pre-built JSONL from Blob (server-side analysis)
    analysisSection.style.display = '';
    analysisStatus.textContent = 'Loading knowledge base…';
    try {
      const jsonlResp = await fetch(JSONL_URL);
      if (jsonlResp.ok) {
        const text = await jsonlResp.text();
        KNOWLEDGE = text.trim().split('\n').map(line => {
          try { return JSON.parse(line); } catch { return null; }
        }).filter(Boolean);
        analysisReady = true;
        buildPipelineIndex();
        buildReportChunksClientSide();
        buildSemanticModelChunks();
        buildCrossReferences();
        progressBar.style.width = '100%';
        analysisStatus.textContent = `✅ Knowledge base loaded — ${KNOWLEDGE.length} chunks`;
        console.log(`Loaded ${KNOWLEDGE.length} JSONL chunks from Blob.`);
      } else {
        throw new Error(`JSONL HTTP ${jsonlResp.status}`);
      }
    } catch (jsonlErr) {
      console.warn('JSONL not available from Blob:', jsonlErr.message);
      // Fallback: check IndexedDB cache
      await kbCache.open();
      const fp = KBCache.fingerprint(KB);
      const cached = await kbCache.isAnalyzed(fp);
      if (cached) {
        analysisReady = true;
        progressBar.style.width = '100%';
        analysisStatus.textContent = '✅ Analysis cached (local) — lineage data ready';
      } else {
        analysisStatus.textContent = `${(KB.metadata?.queries||[]).length} SQL queries — ready to analyze`;
      }
    }
  } catch (err) {
    statusEl.innerHTML = '<span class="dot dot-err"></span> Failed to load data: ' + err.message;
  }
}

function populateStats() {
  if (!KB) return;
  const s = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  s('statWorkspaces', KB.workspaces ? KB.workspaces.length : 0);
  const items = KB.workspaces ? KB.workspaces.reduce((n, ws) => n + (ws.items ? ws.items.length : 0), 0) : 0;
  s('statItems', items);
  // Count tables from schemas (values can be array of tables OR object with .tables/.views/.procedures)
  let tables = 0, cols = 0;
  if (KB.schemas) { for (const val of Object.values(KB.schemas)) {
    if (!val) continue;
    const tblList = Array.isArray(val) ? val : [...(val.tables||[]), ...(val.views||[]), ...(val.procedures||[])];
    for (const tbl of tblList) { tables++; cols += tbl.columns ? tbl.columns.length : 0; }
  } }
  s('statTables', tables);
  s('statColumns', cols);
  // Pipelines
  let pipelines = 0, definitions = 0;
  if (KB.workspaces) for (const ws of KB.workspaces) for (const it of (ws.items||[])) {
    if (it.type === 'DataPipeline') pipelines++;
    if (it.definition) definitions++;
  }
  s('statPipelines', pipelines);
  s('statDefinitions', definitions);
}

// ── AI Analysis trigger (server-side) ─────────────────────────
async function startAnalysis() {
  if (!KB) { alert('Data not loaded yet'); return; }
  const btn = document.getElementById('analyzeBtn');
  const progress = document.getElementById('analysisProgress');
  const status = document.getElementById('analysisStatus');
  const section = document.getElementById('analysisSection');
  if (section) section.style.display = '';
  btn.disabled = true;
  btn.textContent = '⏳ Analyzing…';
  analysisReady = false;
  progress.style.width = '5%';
  status.textContent = 'Sending analysis request to server…';

  let pass = 0;
  let totalAnalyzed = 0, totalCached = 0, totalFailed = 0, totalQueries = 0;

  try {
    // Loop: keep calling /api/analyze until status is "complete" (incremental processing)
    while (true) {
      pass++;
      status.textContent = `⏳ Pass ${pass}: analyzing queries on server…`;

      const resp = await fetch('/api/analyze', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      const text = await resp.text();
      let result;
      try { result = JSON.parse(text); } catch { result = { error: text || `HTTP ${resp.status}` }; }

      if (!resp.ok) throw new Error(result.error || `HTTP ${resp.status}`);

      totalQueries = result.queries_total || totalQueries;
      totalAnalyzed += result.queries_analyzed || 0;
      totalCached = result.queries_cached || totalCached;
      totalFailed += result.queries_failed || 0;

      const done = totalAnalyzed + totalCached + totalFailed;
      const pct = totalQueries > 0 ? Math.min(85, Math.round((done / totalQueries) * 85)) : 10;
      progress.style.width = pct + '%';

      if (result.status === 'partial') {
        const remaining = result.queries_remaining || (totalQueries - done);
        status.textContent = `⏳ Pass ${pass} done (${result.elapsed_seconds}s) — ${done}/${totalQueries} queries processed, ${remaining} remaining…`;
        console.log(`[Analysis] Pass ${pass}:`, result);
        // Brief pause before next pass
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }

      // status === "complete"
      console.log(`[Analysis] Complete after ${pass} pass(es):`, result);
      break;
    }

    progress.style.width = '90%';
    status.textContent = 'Loading updated knowledge base…';

    // Reload JSONL from Blob
    const jsonlResp = await fetch(JSONL_URL + '?t=' + Date.now());
    if (jsonlResp.ok) {
      const jtext = await jsonlResp.text();
      KNOWLEDGE = jtext.trim().split('\n').map(line => { try { return JSON.parse(line); } catch { return null; } }).filter(Boolean);
    }

    analysisReady = true;
    buildPipelineIndex();
    buildReportChunksClientSide();
    buildSemanticModelChunks();
    buildCrossReferences();
    progress.style.width = '100%';
    status.textContent = `✅ Done in ${pass} pass(es) — ${totalAnalyzed} analyzed, ${totalCached} cached, ${totalFailed} failed. ${KNOWLEDGE.length} chunks.`;
    btn.textContent = '🔄 Re-analyze knowledge base';
  } catch (err) {
    status.textContent = '❌ Analysis failed: ' + err.message;
    console.error('Analysis error:', err);
  } finally {
    btn.disabled = false;
    btn.textContent = '🔄 Re-analyze knowledge base';
  }
}

// ── Helper: resolve pipeline definition from KB.definitions ──
function getPipelineContent(itemId) {
  if (!KB || !KB.definitions) return null;
  const parts = KB.definitions[itemId];
  if (!Array.isArray(parts)) return null;
  const pipelinePart = parts.find(p => p.path === 'pipeline-content.json');
  return pipelinePart ? (pipelinePart.payload || null) : null;
}

// ── Pipeline index: map pipeline/activity names → pipeline item name ──
function buildPipelineIndex() {
  PIPELINE_INDEX = {};
  if (!KB || !KB.workspaces) return;
  for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      if (item.type !== 'DataPipeline') continue;
      const pName = (item.displayName || item.name || '').toLowerCase();
      if (pName) PIPELINE_INDEX[pName] = pName;
      // Look up definition from KB.definitions (NOT item.definition)
      const def = getPipelineContent(item.id);
      if (!def) continue;
      // Match "name":"ActivityName" patterns in pipeline JSON
      const activityMatches = def.match(/"name"\s*:\s*"([^"]+)"/gi) || [];
      for (const m of activityMatches) {
        const nameMatch = m.match(/"name"\s*:\s*"([^"]+)"/i);
        if (nameMatch) {
          const actName = nameMatch[1].toLowerCase();
          if (actName.length > 2 && !['true','false','string','int','bool'].includes(actName)) {
            PIPELINE_INDEX[actName] = pName;
          }
        }
      }
      // Also index notebook references found in pipeline definition
      const nbMatches = def.match(/Phase_\d+_\w+|GenDWH_\w+/gi) || [];
      for (const nb of nbMatches) PIPELINE_INDEX[nb.toLowerCase()] = pName;
    }
  }
  // Also index execution_chain chunks from KNOWLEDGE
  for (const chunk of KNOWLEDGE) {
    if (chunk.type !== 'execution_chain') continue;
    const pName = (chunk.pipeline || '').toLowerCase();
    if (pName) PIPELINE_INDEX[pName] = pName;
    for (const nb of (chunk.notebooks || [])) {
      if (nb) PIPELINE_INDEX[nb.toLowerCase()] = pName;
    }
  }
  console.log(`[PIPELINE_INDEX] Built index with ${Object.keys(PIPELINE_INDEX).length} entries`);
}

// ── Client-side report chunk generation ──────────────────────
// ── Client-side report chunk generation (v2 — supports report.json + PBIP) ──
function buildReportChunksClientSide() {
  if (!KB || !KB.workspaces || !KB.definitions) return;

  const impactIndex = {};  // table → [{report_name, page_name, field}]
  let reportCount = 0, chunkCount = 0;

  for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      if (item.type !== 'Report') continue;
      const defParts = KB.definitions[item.id];
      if (!defParts || !Array.isArray(defParts)) continue;

      const rptName = item.displayName || item.name || 'Unknown';
      const wsName = ws.displayName || ws.name || '';

      // Parse definition.pbir → semantic model link
      const pbirFile = defParts.find(f => f.path === 'definition.pbir');
      let smId = '', smName = '';
      if (pbirFile) {
        try {
          const pbir = JSON.parse(pbirFile.payload);
          const cs = ((pbir.datasetReference || {}).byConnection || {}).connectionString || '';
          const mId = cs.match(/semanticmodelid=([a-f0-9-]+)/i);
          if (mId) smId = mId[1];
          const mCat = cs.match(/initial catalog=([^;]+)/i);
          if (mCat) smName = mCat[1];
        } catch { /* malformed */ }
      }

      // Detect format: PBIP (definition/pages/) vs classic (report.json)
      const isPBIP = defParts.some(f => (f.path || '').match(/^definition\/pages\/.*page\.json$/));
      let pages; // [{pageName, visuals:[{type, fields:Set, filters:[]}], pageFilters:[]}]

      if (isPBIP) {
        pages = _parsePBIPReport(defParts);
      } else {
        pages = _parseClassicReport(defParts);
      }

      if (!pages || pages.length === 0) continue;
      reportCount++;

      const allFields = new Set(), allTables = new Set();
      let totalVisuals = 0;

      for (const page of pages) {
        totalVisuals += page.visuals.length;
        const pageFields = new Set();
        const visualTypes = {};
        const visualDetails = [];

        for (const vis of page.visuals) {
          visualTypes[vis.type] = (visualTypes[vis.type] || 0) + 1;
          for (const f of vis.fields) pageFields.add(f);
          // Collect per-visual detail for richer chunks
          if (vis.fields.size > 0) {
            visualDetails.push({
              type: vis.type,
              fields: [...vis.fields].sort(),
              filter_count: (vis.filters || []).length,
            });
          }
        }

        for (const f of pageFields) { allFields.add(f); allTables.add(f.split('.')[0]); }
        const pageTables = [...new Set([...pageFields].map(f => f.split('.')[0]))];

        KNOWLEDGE.push({
          type: 'report_page', id: `${rptName}::${page.pageName}`,
          report_name: rptName, report_id: item.id, workspace: wsName,
          semantic_model_name: smName, semantic_model_id: smId,
          page_name: page.pageName, visual_count: page.visuals.length,
          visual_types: visualTypes, visuals: visualDetails,
          tables_used: pageTables, fields_used: [...pageFields].sort(),
          page_filter_count: (page.pageFilters || []).length,
        });
        chunkCount++;

        for (const fld of pageFields) {
          const tbl = fld.split('.')[0];
          if (!impactIndex[tbl]) impactIndex[tbl] = [];
          impactIndex[tbl].push({ report_name: rptName, page_name: page.pageName, field: fld });
        }
      }

      KNOWLEDGE.push({
        type: 'report_overview', id: rptName,
        report_name: rptName, report_id: item.id, workspace: wsName,
        semantic_model_id: smId, semantic_model_name: smName,
        page_count: pages.length, total_visuals: totalVisuals,
        unique_tables: [...allTables].sort(), unique_fields: [...allFields].sort(),
      });
      chunkCount++;
    }
  }

  // Generate report_impact chunks (one per table)
  for (const [table, usages] of Object.entries(impactIndex)) {
    const seen = new Set();
    const deduped = usages.filter(u => {
      const key = `${u.report_name}|${u.page_name}|${u.field}`;
      if (seen.has(key)) return false; seen.add(key); return true;
    });
    const reportNames = [...new Set(deduped.map(u => u.report_name))];
    KNOWLEDGE.push({
      type: 'report_impact', id: `impact::${table}`, table_name: table,
      used_in_reports: deduped, report_count: reportNames.length, report_names: reportNames,
      summary: `Table ${table} is used in ${reportNames.length} report(s): ${reportNames.join(', ')}`,
    });
    chunkCount++;
  }

  console.log(`[REPORT_CHUNKS] Built ${chunkCount} report chunks from ${reportCount} reports`);
}

// ── Classic report.json parser ──
function _parseClassicReport(defParts) {
  const rjFile = defParts.find(f => f.path === 'report.json');
  if (!rjFile) return null;
  let rjson;
  try { rjson = JSON.parse(rjFile.payload); } catch { return null; }

  const pages = [];
  for (const sec of (rjson.sections || [])) {
    const pageName = sec.displayName || sec.name || 'Untitled';
    const pageFilters = _parseFilterArray(sec.filters);
    const visuals = [];

    for (const vc of (sec.visualContainers || [])) {
      let cfg = {};
      try { cfg = JSON.parse(vc.config || '{}'); } catch { continue; }
      const sv = cfg.singleVisual || {};
      if (!sv.visualType) continue; // skip visual groups
      const vt = sv.visualType;
      const fields = new Set();
      const sourceMap = {};
      _extractFieldRefs(sv.prototypeQuery || {}, sourceMap, fields);
      const filters = _parseFilterArray(vc.filters);
      visuals.push({ type: vt, fields, filters });
    }
    pages.push({ pageName, visuals, pageFilters });
  }
  return pages;
}

// ── PBIP format parser (definition/pages/*/page.json + visuals/*/visual.json) ──
function _parsePBIPReport(defParts) {
  // Find pages.json for page ordering (optional)
  const pageFiles = defParts.filter(f => (f.path || '').match(/^definition\/pages\/[^/]+\/page\.json$/));
  const pages = [];

  for (const pf of pageFiles) {
    let pjson;
    try { pjson = JSON.parse(pf.payload); } catch { continue; }
    const pageName = pjson.displayName || pjson.name || 'Untitled';
    const pageFilters = _parsePBIPFilters(pjson.filterConfig);

    // Find visuals for this page (same directory prefix)
    const pageDir = pf.path.replace(/page\.json$/, '');
    const vizFiles = defParts.filter(f => (f.path || '').startsWith(pageDir + 'visuals/') && (f.path || '').endsWith('/visual.json'));
    const visuals = [];

    for (const vf of vizFiles) {
      let vjson;
      try { vjson = JSON.parse(vf.payload); } catch { continue; }
      const vis = vjson.visual || {};
      const vt = vis.visualType || 'unknown';
      if (['shape', 'image', 'textbox', 'actionButton'].includes(vt)) continue;

      const fields = new Set();
      // PBIP uses visual.query.queryState.{bucket}.projections[].field
      _extractPBIPFields(vis.query, fields);
      const filters = _parsePBIPFilters(vjson.filterConfig);
      visuals.push({ type: vt, fields, filters });
    }
    pages.push({ pageName, visuals, pageFilters });
  }
  return pages;
}

// Extract field refs from PBIP visual.query.queryState
function _extractPBIPFields(query, out) {
  if (!query || !query.queryState) return;
  for (const bucket of Object.values(query.queryState)) {
    if (!bucket || !Array.isArray(bucket.projections)) continue;
    for (const proj of bucket.projections) {
      const field = proj.field || {};
      // Column ref: field.Column.Expression.SourceRef.Entity + field.Column.Property
      const col = field.Column || field.Measure;
      if (col && col.Expression && col.Expression.SourceRef && col.Property) {
        const entity = col.Expression.SourceRef.Entity || '';
        if (entity) out.add(`${entity}.${col.Property}`);
      }
    }
  }
}

// Parse filter arrays from classic format (JSON string or array)
function _parseFilterArray(filters) {
  if (!filters) return [];
  let arr = filters;
  if (typeof filters === 'string') {
    try { arr = JSON.parse(filters); } catch { return []; }
  }
  if (!Array.isArray(arr)) return [];
  return arr.map(f => {
    const expr = f.expression || f.field || {};
    const col = expr.Column || expr.Measure || {};
    const entity = (col.Expression?.SourceRef?.Entity) || '';
    const prop = col.Property || '';
    return { field: entity && prop ? `${entity}.${prop}` : '', type: f.type || '' };
  }).filter(f => f.field);
}

// Parse PBIP filterConfig
function _parsePBIPFilters(filterConfig) {
  if (!filterConfig || !filterConfig.filters) return [];
  return filterConfig.filters.map(f => {
    const field = f.field || {};
    const col = field.Column || field.Measure || {};
    const entity = (col.Expression?.SourceRef?.Entity) || '';
    const prop = col.Property || '';
    return { field: entity && prop ? `${entity}.${prop}` : '', type: f.type || '' };
  }).filter(f => f.field);
}

// Helper: recursively extract table.column refs from Power BI prototypeQuery AST
function _extractFieldRefs(node, sourceMap, out) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) { node.forEach(n => _extractFieldRefs(n, sourceMap, out)); return; }
  if (Array.isArray(node.From)) {
    for (const f of node.From) { if (f.Name && f.Entity) sourceMap[f.Name] = f.Entity; }
  }
  if (node.Property && node.Expression) {
    const sr = node.Expression.SourceRef || {};
    const alias = sr.Source || sr.Entity || '';
    const entity = sourceMap[alias] || alias;
    if (entity) out.add(`${entity}.${node.Property}`);
  }
  for (const v of Object.values(node)) _extractFieldRefs(v, sourceMap, out);
}

// ── Client-side semantic model chunk generation (v2 — full TMDL extraction) ──
function buildSemanticModelChunks() {
  if (!KB || !KB.workspaces || !KB.definitions) return;
  let modelCount = 0, chunkCount = 0;

  for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      if (item.type !== 'SemanticModel') continue;
      const defParts = KB.definitions[item.id];
      if (!defParts || !Array.isArray(defParts)) continue;

      const modelName = item.displayName || item.name || 'Unknown';
      const wsName = ws.displayName || ws.name || '';
      modelCount++;

      // ── Parse all TMDL files ──
      const tablesMap = {};   // tableName → { columns:[], measures:[], partitions:[], isCalculated }
      let relationships = [];

      for (const part of defParts) {
        const p = part.path || '';
        const payload = part.payload || '';

        // ── Parse table TMDL files ──
        if (p.startsWith('definition/tables/') && p.endsWith('.tmdl')) {
          const parsed = _parseTmdlTable(payload);
          if (!parsed) continue;
          // Skip auto-generated date tables
          if (parsed.name.startsWith('LocalDateTable') || parsed.name.startsWith('DateTableTemplate')) continue;
          tablesMap[parsed.name] = parsed;
        }

        // ── Parse relationships.tmdl ──
        if (p.endsWith('relationships.tmdl')) {
          relationships = _parseTmdlRelationships(payload);
        }
      }

      const tableNames = Object.keys(tablesMap);
      const allMeasures = [];
      const measuresPerTable = {};
      for (const [tbl, info] of Object.entries(tablesMap)) {
        if (info.measures.length > 0) {
          measuresPerTable[tbl] = info.measures;
          allMeasures.push(...info.measures);
        }
      }

      // Per-model debug logging
      const totalCols = Object.values(tablesMap).reduce((s, t) => s + t.columns.length, 0);
      console.log(`[SM] ${modelName}: ${tableNames.length} tables, ${totalCols} cols, ${allMeasures.length} measures, ${relationships.length} rels`);

      // ── semantic_model_overview chunk (enriched) ──
      const tablesSummary = tableNames.map(t => {
        const info = tablesMap[t];
        const colCount = info.columns.length;
        const measCount = info.measures.length;
        let desc = `${t} (${colCount} cols`;
        if (measCount > 0) desc += `, ${measCount} measures`;
        if (info.isCalculated) desc += ', calculated';
        desc += ')';
        return desc;
      });
      const overviewText = [
        `Semantic Model: ${modelName}`,
        `Workspace: ${wsName}`,
        `Tables (${tableNames.length}): ${tablesSummary.join('; ')}`,
        `Total columns: ${totalCols}`,
        `Total measures: ${allMeasures.length}`,
        `Relationships: ${relationships.length}`,
      ].join('\n');

      KNOWLEDGE.push({
        type: 'semantic_model_overview', id: modelName,
        model_name: modelName, model_id: item.id, workspace: wsName,
        table_count: tableNames.length, measure_count: allMeasures.length,
        column_count: totalCols, relationship_count: relationships.length,
        tables: tableNames,
        tables_detail: Object.fromEntries(Object.entries(tablesMap).map(([k, v]) => [k, {
          columns: v.columns, measure_count: v.measures.length, isCalculated: v.isCalculated,
        }])),
        text: overviewText,
      });
      chunkCount++;

      // ── semantic_model_measures chunks (one per table with measures) ──
      for (const [tblName, measures] of Object.entries(measuresPerTable)) {
        const measText = measures.map(m => {
          let s = `measure ${m.name} = ${m.expression}`;
          if (m.formatString) s += `\n  formatString: ${m.formatString}`;
          if (m.description) s += `\n  description: ${m.description}`;
          if (m.displayFolder) s += `\n  displayFolder: ${m.displayFolder}`;
          return s;
        }).join('\n\n');
        KNOWLEDGE.push({
          type: 'semantic_model_measures', id: `${modelName}::${tblName}`,
          model_name: modelName, model_id: item.id, table_name: tblName,
          measures: measures,
          text: `Semantic Model: ${modelName}\nTable: ${tblName}\n${measText}`,
        });
        chunkCount++;
      }

      // ── semantic_model_relationships chunk ──
      if (relationships.length > 0) {
        const relText = relationships.map(r => {
          let s = `${r.from_table}.${r.from_column} → ${r.to_table}.${r.to_column}`;
          if (r.crossFilteringBehavior && r.crossFilteringBehavior !== 'oneDirection') s += ` [${r.crossFilteringBehavior}]`;
          if (!r.isActive) s += ' [INACTIVE]';
          return s;
        }).join('\n');
        KNOWLEDGE.push({
          type: 'semantic_model_relationships', id: `${modelName}::relationships`,
          model_name: modelName, model_id: item.id,
          relationships: relationships,
          text: `Semantic Model: ${modelName}\nRelationships:\n${relText}`,
        });
        chunkCount++;
      }
    }
  }

  const modelNames = KNOWLEDGE.filter(c => c.type === 'semantic_model_overview').map(c => c.model_name);
  console.log(`[SM_CHUNKS] Built ${chunkCount} semantic model chunks from ${modelCount} models`);
  console.log(`[SM_CHUNKS] Models: ${modelNames.join(', ')}`);
}

// ── Cross-reference pass: links SM ↔ Report chunks ──
function buildCrossReferences() {
  const smOverviews = KNOWLEDGE.filter(c => c.type === 'semantic_model_overview');
  const smMeasures  = KNOWLEDGE.filter(c => c.type === 'semantic_model_measures');
  const rptOverviews = KNOWLEDGE.filter(c => c.type === 'report_overview');
  const rptPages     = KNOWLEDGE.filter(c => c.type === 'report_page');

  if (smOverviews.length === 0 || (rptOverviews.length === 0 && rptPages.length === 0)) {
    console.log('[XREF] No cross-references to build (missing SM or Report chunks)');
    return;
  }

  // 1. Build flat measure name → model_name map for field matching
  const measureNameToModel = {};
  for (const mc of smMeasures) {
    for (const m of (mc.measures || [])) {
      const key = `${(mc.table_name||'').toLowerCase()}.${(m.name||'').toLowerCase()}`;
      measureNameToModel[key] = mc.model_name;
    }
  }

  // 3. Enrich SM overview chunks with used_in_reports
  for (const sm of smOverviews) {
    const matchedReports = rptOverviews.filter(r =>
      (r.semantic_model_name && r.semantic_model_name.toLowerCase() === (sm.model_name || '').toLowerCase()) ||
      (r.semantic_model_id && r.semantic_model_id === sm.model_id)
    );
    if (matchedReports.length > 0) {
      sm.used_in_reports = matchedReports.map(r => r.report_name);
      sm.text += `\nUsed in reports (${matchedReports.length}): ${sm.used_in_reports.join(', ')}`;
    }
  }

  // 4. Enrich SM measures chunks with used_in_report_pages
  for (const mc of smMeasures) {
    const measureNames = (mc.measures || []).map(m => (m.name || '').toLowerCase());
    const tableName = (mc.table_name || '').toLowerCase();
    const matchedPages = [];

    for (const pg of rptPages) {
      // Match only if report page references a measure name from this table
      const pgFields = (pg.fields_used || []).map(f => f.toLowerCase());
      const hit = pgFields.some(f => {
        const dotIdx = f.indexOf('.');
        if (dotIdx < 0) return false;
        const entity = f.slice(0, dotIdx);
        const prop = f.slice(dotIdx + 1);
        return entity === tableName && measureNames.includes(prop);
      });
      if (hit) {
        matchedPages.push({ report: pg.report_name, page: pg.page_name });
      }
    }

    if (matchedPages.length > 0) {
      mc.used_in_report_pages = matchedPages;
      const uniqueReports = [...new Set(matchedPages.map(p => p.report))];
      mc.text += `\nUsed in ${matchedPages.length} report page(s) across ${uniqueReports.length} report(s): ${uniqueReports.join(', ')}`;
    }
  }

  // 5. Enrich report_page chunks with measures_referenced from SM
  for (const pg of rptPages) {
    const smName = (pg.semantic_model_name || '').toLowerCase();
    if (!smName) continue;

    const measuresReferenced = [];
    for (const fld of (pg.fields_used || [])) {
      const fldLower = fld.toLowerCase();
      // Check if this field is a measure in the linked semantic model
      if (measureNameToModel[fldLower] && measureNameToModel[fldLower].toLowerCase() === smName) {
        measuresReferenced.push(fld);
      }
    }
    if (measuresReferenced.length > 0) {
      pg.measures_referenced = measuresReferenced;
    }
  }

  const xrefCount = smOverviews.filter(s => s.used_in_reports).length
                  + smMeasures.filter(m => m.used_in_report_pages).length
                  + rptPages.filter(p => p.measures_referenced).length;
  console.log(`[XREF] Cross-referenced ${xrefCount} chunks (SM↔Report linkage)`);
}

// ── TMDL Table Parser: extracts columns, measures, partitions from a single table .tmdl ──
function _parseTmdlTable(payload) {
  const lines = payload.split('\n');
  if (lines.length === 0) return null;

  // First line: table <name>
  const tblMatch = lines[0].match(/^table\s+(?:'([^']+)'|"([^"]+)"|(.+))/);
  if (!tblMatch) return null;
  const tableName = (tblMatch[1] || tblMatch[2] || tblMatch[3] || '').trim();

  const result = { name: tableName, columns: [], measures: [], isCalculated: false };

  let i = 1;
  while (i < lines.length) {
    const trimmed = lines[i].trimStart();

    // ── Column ──
    if (/^column\s+/i.test(trimmed)) {
      const col = _parseTmdlColumn(lines, i);
      result.columns.push(col.data);
      i = col.nextLine;
      continue;
    }

    // ── Measure ──
    if (/^measure\s+/i.test(trimmed)) {
      const meas = _parseTmdlMeasure(lines, i);
      result.measures.push(meas.data);
      i = meas.nextLine;
      continue;
    }

    // ── Partition (detect calculated tables) ──
    if (/^partition\s+/i.test(trimmed)) {
      if (/=\s*calculated/i.test(trimmed)) result.isCalculated = true;
      i++;
      continue;
    }

    i++;
  }

  return result;
}

// Parse a column block starting at line index i
function _parseTmdlColumn(lines, startIdx) {
  const header = lines[startIdx].trimStart();
  const nameMatch = header.match(/^column\s+(?:'([^']+)'|"([^"]+)"|(\S+))/i);
  const name = nameMatch ? (nameMatch[1] || nameMatch[2] || nameMatch[3] || '').trim() : 'unknown';

  const col = { name };
  let i = startIdx + 1;

  while (i < lines.length) {
    const trimmed = lines[i].trimStart();
    // Stop at next top-level element
    if (/^(column|measure|hierarchy|partition|table|annotation\s+PBI_Id)\s*/i.test(trimmed) && trimmed !== '') {
      break;
    }
    // Parse properties
    if (/^dataType:\s*/i.test(trimmed)) col.dataType = trimmed.split(':')[1].trim();
    else if (/^formatString:\s*/i.test(trimmed)) col.formatString = trimmed.replace(/^formatString:\s*/i, '').trim();
    else if (/^sourceColumn:\s*/i.test(trimmed)) col.sourceColumn = trimmed.split(':').slice(1).join(':').trim();
    else if (/^summarizeBy:\s*/i.test(trimmed)) col.summarizeBy = trimmed.split(':')[1].trim();
    else if (/^isHidden/i.test(trimmed) && !trimmed.includes(':')) col.isHidden = true;
    else if (/^sortByColumn:\s*/i.test(trimmed)) col.sortByColumn = trimmed.split(':').slice(1).join(':').trim();
    else if (/^description:\s*/i.test(trimmed)) col.description = trimmed.replace(/^description:\s*/i, '').trim();
    else if (/^sourceProviderType:\s*/i.test(trimmed)) col.sourceProviderType = trimmed.split(':').slice(1).join(':').trim();
    else if (/^displayFolder:\s*/i.test(trimmed)) col.displayFolder = trimmed.replace(/^displayFolder:\s*/i, '').trim();
    i++;
  }

  return { data: col, nextLine: i };
}

// Parse a measure block starting at line index i
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

    // Stop at next top-level element (column, measure, hierarchy, partition, table-level annotation)
    if (/^(column|measure|hierarchy|partition|table|annotation\s+PBI_Id)\s*/i.test(trimmed) && !inBacktickBlock) {
      break;
    }

    if (inBacktickBlock) {
      exprLines.push(lines[i]);
      if (trimmed.includes('```')) inBacktickBlock = false;
      i++; continue;
    }

    // Metadata lines (after expression ends)
    if (/^formatString:\s*/i.test(trimmed)) { mData.formatString = trimmed.replace(/^formatString:\s*/i, '').trim(); i++; continue; }
    if (/^description:\s*/i.test(trimmed)) { mData.description = trimmed.replace(/^description:\s*/i, '').trim(); i++; continue; }
    if (/^displayFolder:\s*/i.test(trimmed)) { mData.displayFolder = trimmed.replace(/^displayFolder:\s*/i, '').trim(); i++; continue; }
    if (/^(lineageTag|changedProperty|annotation|isHidden)\s*/i.test(trimmed)) { i++; continue; }

    // ``` block start mid-expression
    if (trimmed.includes('```')) { exprLines.push(lines[i]); inBacktickBlock = !inBacktickBlock; i++; continue; }

    // Blank line — if we have expression content, this may be end of expression
    if (trimmed === '') { i++; continue; }

    // Indented continuation = expression
    if (/^\t/.test(lines[i]) || /^\s{2,}/.test(lines[i])) {
      exprLines.push(lines[i]);
    }
    i++;
  }

  mData.expression = exprLines.join('\n').replace(/```/g, '').trim();
  return { data: mData, nextLine: i };
}

// ── TMDL Relationships Parser ──
function _parseTmdlRelationships(payload) {
  const rels = [];
  // Split by "relationship " at start of line (top-level)
  const blocks = payload.split(/^relationship\s+/m);
  for (let bi = 1; bi < blocks.length; bi++) {
    const block = blocks[bi];
    const fromCol = block.match(/fromColumn:\s*(.+)/);
    const toCol = block.match(/toColumn:\s*(.+)/);
    const crossFilter = block.match(/crossFilteringBehavior:\s*(.+)/);
    const isActiveMatch = block.match(/isActive:\s*(.+)/);

    if (fromCol && toCol) {
      const fromParts = fromCol[1].trim().split('.');
      const toParts = toCol[1].trim().split('.');
      rels.push({
        from_table: fromParts[0].replace(/^['"]|['"]$/g, ''),
        from_column: (fromParts[1] || '').replace(/^['"]|['"]$/g, ''),
        to_table: toParts[0].replace(/^['"]|['"]$/g, ''),
        to_column: (toParts[1] || '').replace(/^['"]|['"]$/g, ''),
        crossFilteringBehavior: crossFilter ? crossFilter[1].trim() : 'oneDirection',
        isActive: isActiveMatch ? isActiveMatch[1].trim().toLowerCase() !== 'false' : true,
      });
    }
  }
  return rels;
}

// ── Chain-aware lineage following ────────────────────────────
function followLineageChain(fieldName) {
  const chain = { consumption: [], platinum: [], gold: [], silver: [], bronze: [], found: false };
  if (!fieldName || KNOWLEDGE.length === 0) return chain;

  const fieldLower = fieldName.toLowerCase();
  console.log(`[CHAIN] Starting chain trace for field: ${fieldName}`);

  // STEP 0: Consumption layer — find SM measures/columns and reports that reference this field
  for (const chunk of KNOWLEDGE) {
    if (chunk.type === 'semantic_model_measures') {
      // Check if any measure's DAX references this field, or if field IS a measure name
      const measures = chunk.measures || [];
      for (const m of measures) {
        const mNameLower = (m.name || '').toLowerCase();
        const mExprLower = (m.expression || '').toLowerCase();
        if (mNameLower === fieldLower || mExprLower.includes(fieldLower)) {
          chain.consumption.push({
            type: 'measure', model: chunk.model_name, table: chunk.table_name,
            measure_name: m.name, expression: m.expression,
            used_in: chunk.used_in_report_pages || [],
          });
          chain.found = true;
        }
      }
    } else if (chunk.type === 'semantic_model_overview' && chunk.tables_detail) {
      // Check if field is a column in any table of this model
      for (const [tbl, info] of Object.entries(chunk.tables_detail)) {
        for (const col of (info.columns || [])) {
          if ((col.name || '').toLowerCase() === fieldLower) {
            chain.consumption.push({
              type: 'column', model: chunk.model_name, table: tbl,
              column: col.name, dataType: col.dataType, isHidden: col.isHidden,
            });
            chain.found = true;
          }
        }
      }
    } else if (chunk.type === 'report_page') {
      const pgFields = (chunk.fields_used || []).map(f => f.toLowerCase());
      if (pgFields.some(f => f.endsWith('.' + fieldLower) || f === fieldLower)) {
        chain.consumption.push({
          type: 'report_usage', report: chunk.report_name, page: chunk.page_name,
          semantic_model: chunk.semantic_model_name || '',
        });
        chain.found = true;
      }
    }
  }
  console.log(`[CHAIN] Consumption: ${chain.consumption.length} refs`);

  // STEP 1: Find in Platinum (warehouse_sproc) — parse definition for field mapping
  for (const chunk of KNOWLEDGE) {
    if (chunk.type !== 'warehouse_sproc' || !chunk.definition) continue;
    const defLower = chunk.definition.toLowerCase();
    if (!defLower.includes(fieldLower)) continue;

    // Parse "AS field_name" pattern to find source column
    // Patterns: [src_col] AS target, src_col AS target, expression AS target
    const defText = chunk.definition;
    const asPatterns = [
      new RegExp(`\\[([^\\]]+)\\]\\s+AS\\s+${fieldName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i'),
      new RegExp(`([a-z_][a-z0-9_.]+)\\s+AS\\s+${fieldName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i'),
    ];
    let sourceCol = null;
    for (const pat of asPatterns) {
      const m = defText.match(pat);
      if (m) { sourceCol = m[1]; break; }
    }

    // Parse FROM clause for source table
    // Pattern: FROM GenDWH_GoldDWH_LH.dbo.table_name or FROM database.schema.table
    const fromMatch = defText.match(/FROM\s+(?:\[?[\w]+\]?\.)?(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?/i);
    const sourceTable = fromMatch ? fromMatch[1] : null;

    chain.platinum.push({
      sproc: `[${chunk.schema}].[${chunk.name}]`,
      field: fieldName,
      sourceCol: sourceCol || '(expression — see SQL)',
      sourceTable: sourceTable || '(see SQL)',
      definition: chunk.definition.slice(0, 3000),
    });
    chain.found = true;
    console.log(`[CHAIN] Platinum: ${chunk.name} → sourceCol=${sourceCol}, sourceTable=${sourceTable}`);
  }

  // STEP 2: Follow to Gold — search field_detail where table≈sourceTable and target_field≈sourceCol
  const goldTables = new Set();
  const silverTargets = []; // {table, field} to search in next step
  for (const p of chain.platinum) {
    if (!p.sourceTable) continue;
    const tblLower = p.sourceTable.toLowerCase();
    const colLower = (p.sourceCol || '').toLowerCase();

    for (const chunk of KNOWLEDGE) {
      if (chunk.type !== 'field_detail') continue;
      if (!chunk.table || !chunk.table.toLowerCase().includes(tblLower)) continue;
      // Match specific field if we know it, otherwise include all fields of this table
      if (colLower && colLower !== '(expression — see sql)' && chunk.target_field) {
        if (chunk.target_field.toLowerCase() !== colLower) continue;
      }
      if (goldTables.has(chunk.id)) continue;
      goldTables.add(chunk.id);
      chain.gold.push(chunk);
      if (chunk.source_table) silverTargets.push({ table: chunk.source_table, field: chunk.source_column || '' });
    }
  }
  // If no specific col match, also grab table_lineage for overview
  if (chain.gold.length === 0) {
    for (const p of chain.platinum) {
      if (!p.sourceTable) continue;
      for (const chunk of KNOWLEDGE) {
        if (chunk.type !== 'table_lineage') continue;
        if (chunk.id && chunk.id.toLowerCase().includes(p.sourceTable.toLowerCase())) {
          chain.gold.push(chunk);
          // Add all source tables as silver targets
          for (const st of (chunk.source_tables || [])) silverTargets.push({ table: st, field: '' });
        }
      }
    }
  }
  console.log(`[CHAIN] Gold: ${chain.gold.length} chunks, silverTargets: ${silverTargets.map(s=>s.table).join(', ')}`);

  // STEP 3: Follow to Silver
  const silverTables = new Set();
  const bronzeTargets = [];
  for (const st of silverTargets) {
    const tblLower = st.table.toLowerCase().replace(/^.*\./, ''); // strip schema prefix
    const colLower = st.field.toLowerCase();
    for (const chunk of KNOWLEDGE) {
      if (chunk.type !== 'field_detail') continue;
      if (!chunk.table || !chunk.table.toLowerCase().includes(tblLower)) continue;
      if (colLower && chunk.target_field && chunk.target_field.toLowerCase() !== colLower) continue;
      if (silverTables.has(chunk.id)) continue;
      silverTables.add(chunk.id);
      chain.silver.push(chunk);
      if (chunk.source_table) bronzeTargets.push({ table: chunk.source_table });
    }
    // Also grab table_lineage for Silver tables
    for (const chunk of KNOWLEDGE) {
      if (chunk.type !== 'table_lineage') continue;
      if (chunk.id && chunk.id.toLowerCase().includes(tblLower)) {
        if (!silverTables.has('tl_' + chunk.id)) {
          silverTables.add('tl_' + chunk.id);
          chain.silver.push(chunk);
          for (const src of (chunk.source_tables || [])) bronzeTargets.push({ table: src });
        }
      }
    }
  }
  console.log(`[CHAIN] Silver: ${chain.silver.length} chunks, bronzeTargets: ${bronzeTargets.map(b=>b.table).join(', ')}`);

  // STEP 4: Follow to Bronze — aggressive matching to ensure completeness
  if (KB && KB.metadata && KB.metadata.bronze_meta) {
    // Collect all Silver source tables as additional Bronze targets
    for (const s of chain.silver) {
      if (s.type === 'field_detail' && s.source_table) {
        const src = s.source_table.toLowerCase().replace(/^.*\./, '');
        if (!bronzeTargets.some(bt => bt.table.toLowerCase().replace(/^.*\./, '') === src)) {
          bronzeTargets.push({ table: src });
        }
      }
      if (s.type === 'table_lineage' && s.source_tables) {
        for (const st of s.source_tables) {
          const src = st.toLowerCase().replace(/^.*\./, '');
          if (!bronzeTargets.some(bt => bt.table.toLowerCase().replace(/^.*\./, '') === src)) {
            bronzeTargets.push({ table: src });
          }
        }
      }
    }
    const addedKeys = new Set();
    for (const bt of bronzeTargets) {
      const tblLower = bt.table.toLowerCase().replace(/^.*\./, '');
      for (const bm of KB.metadata.bronze_meta) {
        const bmTarget = (bm.target_table || '').toLowerCase();
        // Match by target_table or source_table (partial match both ways)
        if (bmTarget.includes(tblLower) || tblLower.includes(bmTarget)
            || (bm.source_table && bm.source_table.toLowerCase().includes(tblLower))) {
          const key = `${bm.source_schema}.${bm.source_table}→${bm.target_table}`;
          if (!addedKeys.has(key)) {
            addedKeys.add(key);
            chain.bronze.push(bm);
          }
        }
      }
    }
  }
  console.log(`[CHAIN] Bronze: ${chain.bronze.length} mappings`);
  console.log(`[CHAIN] Summary: C=${chain.consumption.length} P=${chain.platinum.length} G=${chain.gold.length} S=${chain.silver.length} B=${chain.bronze.length}`);
  return chain;
}

function formatChainContext(chain, fieldName) {
  let ctx = '';

  // Consumption layer (SM + Reports)
  ctx += `=== CONSUMPTION LAYER (Semantic Models & Reports) ===\n`;
  if (chain.consumption && chain.consumption.length > 0) {
    for (const c of chain.consumption) {
      if (c.type === 'measure') {
        ctx += `Measure: ${c.model}.${c.table}.${c.measure_name}\n`;
        ctx += `DAX: ${(c.expression || '').slice(0, 500)}\n`;
        if (c.used_in.length > 0) ctx += `Used in: ${c.used_in.map(u => `${u.report}/${u.page}`).join(', ')}\n`;
        ctx += '\n';
      } else if (c.type === 'column') {
        ctx += `Column: ${c.model}.${c.table}.${c.column} (${c.dataType||'?'})${c.isHidden ? ' [hidden]' : ''}\n`;
      } else if (c.type === 'report_usage') {
        ctx += `Report: ${c.report} → ${c.page}${c.semantic_model ? ' (model: ' + c.semantic_model + ')' : ''}\n`;
      }
    }
  } else {
    ctx += `(No Semantic Model or Report references found for ${fieldName})\n`;
  }

  ctx += `\n=== PLATINUM LAYER (Warehouse) ===\n`;
  if (chain.platinum.length > 0) {
    for (const p of chain.platinum) {
      ctx += `Stored Procedure: ${p.sproc}\n`;
      ctx += `Target field: ${p.field}\n`;
      ctx += `Source column: ${p.sourceCol}\n`;
      ctx += `Source table: ${p.sourceTable}\n`;
      ctx += `SQL Definition:\n${p.definition}\n\n`;
    }
  } else {
    ctx += `(No Platinum mapping found for ${fieldName})\n`;
  }
  ctx += `\n=== GOLD LAYER (DWH) ===\n`;
  if (chain.gold.length > 0) {
    for (const g of chain.gold) {
      if (g.type === 'field_detail') {
        ctx += `AI Lineage: ${g.table}.${g.target_field} ← ${g.source_table||'?'}.${g.source_column||'?'}\n`;
        ctx += `Type: ${g.transformation_type||''} | Expr: ${g.expression||''}\n`;
        ctx += `Logic: ${g.business_logic||''}\n\n`;
      } else if (g.type === 'table_lineage') {
        ctx += `Table: ${g.id} (${g.layer} ${g.mode}) — ${g.field_count} fields\n`;
        ctx += `Sources: ${g.source_tables?.join(', ')||'none'}\n\n`;
      }
    }
  } else {
    ctx += `(No Gold lineage found)\n`;
  }
  ctx += `\n=== SILVER LAYER (Staging) ===\n`;
  if (chain.silver.length > 0) {
    for (const s of chain.silver) {
      if (s.type === 'field_detail') {
        ctx += `AI Lineage: ${s.table}.${s.target_field} ← ${s.source_table||'?'}.${s.source_column||'?'}\n`;
        ctx += `Type: ${s.transformation_type||''} | Expr: ${s.expression||''}\n`;
        ctx += `Logic: ${s.business_logic||''}\n\n`;
      } else if (s.type === 'table_lineage') {
        ctx += `Table: ${s.id} (${s.layer} ${s.mode}) — ${s.field_count} fields\n`;
        ctx += `Sources: ${s.source_tables?.join(', ')||'none'}\n\n`;
      }
    }
  } else {
    ctx += `(No Silver lineage found)\n`;
  }
  ctx += `\n=== BRONZE LAYER (Landing/Source) ===\n`;
  if (chain.bronze.length > 0) {
    for (const b of chain.bronze) {
      ctx += `Bronze Mapping: ${b.target_table} ← ${b.source_schema||''}.${b.source_table}\n`;
      ctx += `Columns: ${b.source_columns || '*'} | Active: ${b.is_active}\n\n`;
    }
  } else {
    ctx += `(No Bronze mapping found)\n`;
  }
  return ctx;
}

// ── Entity Pinning: guarantee all chunks for a named entity enter context ──
function getPinnedChunks(query, scoredChunks) {
  // 1. Build entity name → chunks index from scored chunks
  const entityIndex = {};  // lowercase name → [chunk indices]
  for (let i = 0; i < scoredChunks.length; i++) {
    const raw = scoredChunks[i]._raw || {};
    const names = [
      raw.model_name, raw.target_table, raw.table_name,
      raw.pipeline, raw.report_name,
    ].filter(Boolean);
    for (const n of names) {
      const key = n.toLowerCase();
      if (key.length < 4) continue; // skip short names to avoid false matches
      if (!entityIndex[key]) entityIndex[key] = [];
      entityIndex[key].push(i);
    }
  }

  // 2. Find entity names that appear in the query
  const qLower = query.toLowerCase();
  const matchedIndices = new Set();
  for (const [name, indices] of Object.entries(entityIndex)) {
    if (qLower.includes(name)) {
      for (const idx of indices) matchedIndices.add(idx);
    }
  }

  // 3. Collect pinned chunks, deduplicated, capped at 8000 chars
  const pinned = [];
  let pinnedLen = 0;
  const CAP = 8000;
  for (const idx of matchedIndices) {
    const c = scoredChunks[idx];
    if (pinnedLen + c.text.length > CAP) continue;
    pinned.push(c);
    pinnedLen += c.text.length;
  }
  return pinned;
}

// ── RAG retrieval (keyword + knowledge base) ──────────────────
async function retrieveContext(query) {
  if (!KB) return '(No data loaded)';
  try {
    const queryLower = query.toLowerCase();
    // Keep compound tokens (underscore-joined field names) AND split into parts
    const rawTokens = queryLower.replace(/[^a-z0-9а-яё_]/gi, ' ').split(/\s+/).filter(t => t.length > 2);
    const compoundTokens = rawTokens.filter(t => t.includes('_')); // e.g. "premium_paid_amount_bgn_mode"
    const splitTokens = rawTokens.flatMap(t => t.includes('_') ? t.split('_').filter(p => p.length > 2) : [t]);
    const tokens = [...new Set([...compoundTokens, ...splitTokens])]; // deduplicate

    // Detect warehouse/Platinum queries
    const isWarehouseQuery = /platinum|warehouse|dwh|stored.?proc|sproc|хранилищ|платинум/i.test(query);
    const warehouseBoost = isWarehouseQuery ? 12 : 0;

    // Detect pipeline queries — keyword match OR any token matches a pipeline/activity name
    const isPipelineQuery = /pipeline|пайплайн|оркестрац|orchestrat|activity|активит|datapipeline/i.test(query)
      || tokens.some(t => PIPELINE_INDEX[t]);
    const pipelineBoost = isPipelineQuery ? 15 : 0;

    // Detect report queries — report names (GEN_PBI_*) or report-related keywords
    const isReportQuery = /report|репорт|визуализаци|dashboard|дашборд|GEN_PBI_|засегнати.?report|кои.?report|страниц|page/i.test(query)
      || tokens.some(t => t.startsWith('gen_pbi_'));
    const reportBoost = isReportQuery ? 15 : 0;

    // Detect semantic model queries
    const isSemanticModelQuery = /semantic.?model|семантич|measure|мерк[аи]|dax|relationship|релаци|SM_|GEN_SM_|tmdl/i.test(query)
      || tokens.some(t => t.startsWith('gen_sm_') || t.startsWith('sm_'));
    const semanticModelBoost = isSemanticModelQuery ? 15 : 0;

    // Detect impact analysis queries
    const isImpactQuery = /impact|засегнат|ако промен|if.*change|кои.?report.*използва|използва.?таблиц|affected|зависим/i.test(query);
    const impactBoost = isImpactQuery ? 18 : 0;

    // Detect chain lineage queries (end-to-end tracing)
    const isChainQuery = /от платинум до бронз|end.to.end|full lineage|пълен lineage|проследи до|проследи линеадж|trace.*lineage|от.+до бронз|platinum.*bronze|бронз.*платинум|откъде идва|where does.*come from|lineage.*бронз/i.test(query);

    console.log(`[RAG] Query: "${query}"`);
    console.log(`[RAG] Tokens (${tokens.length}): ${tokens.join(', ')}`);
    console.log(`[RAG] Compound tokens: ${compoundTokens.join(', ') || 'none'}`);
    console.log(`[RAG] Warehouse query: ${isWarehouseQuery}, Pipeline query: ${isPipelineQuery}, Chain query: ${isChainQuery}, Report query: ${isReportQuery}, Impact query: ${isImpactQuery}, SemanticModel query: ${isSemanticModelQuery}`);
    console.log(`[RAG] KNOWLEDGE chunks: ${KNOWLEDGE.length}, analysisReady: ${analysisReady}`);
    if (KNOWLEDGE.length > 0) {
      const types = {};
      for (const c of KNOWLEDGE) types[c.type] = (types[c.type]||0) + 1;
      console.log('[RAG] Chunk types:', JSON.stringify(types));
    }

    // Detect lineage queries (field origin, source tracing — broader than full chain)
    const isLineageQuery = isChainQuery || /откъде|lineage|произход|source.*field|source.*column|where.*from|идва от|трансформац|transformation/i.test(query);

    // ── Chain lineage: follow Platinum→Gold→Silver→Bronze ────
    // Try compound tokens first, then any underscore-containing token as field name
    const chainCandidates = compoundTokens.length > 0 ? compoundTokens : tokens.filter(t => t.includes('_'));
    if (isChainQuery && analysisReady && KNOWLEDGE.length > 0 && chainCandidates.length > 0) {
      const fieldName = chainCandidates[0]; // primary field to trace
      console.log(`[RAG] Chain mode: tracing field "${fieldName}"`);
      const chain = followLineageChain(fieldName);
      if (chain.found) {
        // Chain context gets unlimited budget — Bronze must never be truncated
        const chainCtx = formatChainContext(chain, fieldName);
        console.log(`[RAG] Chain context: ${chainCtx.length} chars (C=${chain.consumption.length} P=${chain.platinum.length} G=${chain.gold.length} S=${chain.silver.length} B=${chain.bronze.length})`);
        window._lastChainQuery = true;
        // Append supplementary normal search with reduced budget (chain has priority)
        // This adds breadth without sacrificing chain completeness
        return chainCtx;
      } else {
        console.log(`[RAG] Chain: no Platinum match found, falling back to normal search`);
        window._lastChainQuery = false;
      }
    } else {
      window._lastChainQuery = false;
    }

    const chunks = [];

    // ── Knowledge Base search (pre-built JSONL from server) ────
    if (analysisReady && KNOWLEDGE.length > 0) {
      // Pipeline index boost: check if any token matches a pipeline/activity name
      const pipelineHitNames = new Set();
      for (const t of tokens) {
        if (PIPELINE_INDEX[t]) pipelineHitNames.add(PIPELINE_INDEX[t]);
      }

      for (const chunk of KNOWLEDGE) {
        const blob = JSON.stringify(chunk).toLowerCase();
        let score = 0;

        // Score compound tokens higher (exact field name match)
        for (const t of compoundTokens) if (blob.includes(t)) score += 5;
        // Score split tokens normally
        for (const t of splitTokens) if (blob.includes(t)) score++;

        if (score === 0) continue;
        // Format based on chunk type
        let text = '';
        if (chunk.type === 'field_detail') {
          text = `AI Lineage: ${chunk.table}.${chunk.target_field} ← ${chunk.source_table||'?'}.${chunk.source_column||'?'}\nType: ${chunk.transformation_type||''}\nExpr: ${chunk.expression||''}\nLogic: ${chunk.business_logic||''}`;
          score += 10;
        } else if (chunk.type === 'table_lineage') {
          text = `Table Summary: ${chunk.id} (${chunk.layer} ${chunk.mode})\nFields: ${chunk.field_count}\nSources: ${chunk.source_tables?.join(', ')||'none'}\nTransformations: ${chunk.transformation_types?.join(', ')||'none'}`;
          score += 8;
        } else if (chunk.type === 'execution_chain') {
          text = `Execution Chain: ${chunk.pipeline}\nWorkspace: ${chunk.workspace||''}\nNotebooks: ${chunk.notebooks?.join(' → ')||'none'}\nActivities: ${chunk.activity_count||0}\nAffects: ${chunk.target_tables?.slice(0,20).join(', ')||'none'}`;
          score += 5 + pipelineBoost;
          // Extra boost if this chain's pipeline name matches a token hit
          if (pipelineHitNames.has((chunk.pipeline||'').toLowerCase())) score += 10;
        } else if (chunk.type === 'warehouse_view') {
          text = `Warehouse View: [${chunk.schema}].[${chunk.name}]\nSQL:\n${(chunk.definition||'').slice(0,3000)}`;
          score += 3 + warehouseBoost;
        } else if (chunk.type === 'warehouse_sproc') {
          text = `Stored Procedure: [${chunk.schema}].[${chunk.name}]\nSQL:\n${(chunk.definition||'').slice(0,3000)}`;
          score += 4 + warehouseBoost;
        } else if (chunk.type === 'report_overview') {
          text = `Report Overview: ${chunk.report_name}\nWorkspace: ${chunk.workspace||''}\nSemantic Model: ${chunk.semantic_model_name||chunk.semantic_model_id||'?'}\nPages: ${chunk.page_count||0}, Visuals: ${chunk.total_visuals||0}\nTables used: ${(chunk.unique_tables||[]).join(', ')}\nFields: ${(chunk.unique_fields||[]).join(', ')}`;
          score += 6 + reportBoost;
        } else if (chunk.type === 'report_page') {
          text = `Report Page: ${chunk.report_name} → ${chunk.page_name}\nSemantic Model: ${chunk.semantic_model_name||''}\nVisuals: ${chunk.visual_count||0} (${Object.entries(chunk.visual_types||{}).map(([k,v])=>`${k}:${v}`).join(', ')})\nTables: ${(chunk.tables_used||[]).join(', ')}\nFields: ${(chunk.fields_used||[]).join(', ')}`;
          if (chunk.measures_referenced && chunk.measures_referenced.length > 0) text += `\nMeasures referenced: ${chunk.measures_referenced.join(', ')}`;
          score += 5 + reportBoost;
        } else if (chunk.type === 'report_impact') {
          text = `Impact Analysis: table "${chunk.table_name}" is used in ${chunk.report_count||0} report(s): ${(chunk.report_names||[]).join(', ')}\nDetails: ${(chunk.used_in_reports||[]).slice(0,30).map(u=>`${u.report_name}/${u.page_name}: ${u.field}`).join('; ')}`;
          score += 4 + reportBoost + impactBoost;
        } else if (chunk.type === 'semantic_model_overview') {
          // Include column details for richer AI context
          let detailLines = chunk.text || `Semantic Model: ${chunk.model_name}`;
          if (chunk.tables_detail) {
            const tblDetails = Object.entries(chunk.tables_detail).map(([tbl, info]) => {
              const cols = (info.columns||[]).map(c => {
                let s = c.name;
                if (c.dataType) s += ` (${c.dataType})`;
                if (c.isHidden) s += ' [hidden]';
                return s;
              }).join(', ');
              return `  ${tbl}: ${cols}`;
            }).join('\n');
            detailLines += '\nColumns per table:\n' + tblDetails;
          }
          if (chunk.used_in_reports && chunk.used_in_reports.length > 0) {
            detailLines += `\nUsed in reports: ${chunk.used_in_reports.join(', ')}`;
          }
          text = detailLines;
          score += 6 + semanticModelBoost;
        } else if (chunk.type === 'semantic_model_measures') {
          text = chunk.text || `Measures: ${chunk.model_name}::${chunk.table_name}`;
          if (chunk.used_in_report_pages && chunk.used_in_report_pages.length > 0) {
            const uniqueRpts = [...new Set(chunk.used_in_report_pages.map(p => p.report))];
            text += `\nUsed in reports: ${uniqueRpts.join(', ')}`;
          }
          score += 8 + semanticModelBoost;
        } else if (chunk.type === 'semantic_model_relationships') {
          text = chunk.text || `Relationships: ${chunk.model_name}`;
          score += 6 + semanticModelBoost;
        } else {
          text = `${chunk.type}: ${chunk.id}\n${JSON.stringify(chunk).slice(0, 500)}`;
        }
        chunks.push({ score, text, type: chunk.type, _raw: chunk });
      }
      // Debug: log top matches by type
      const matched = {};
      for (const c of chunks) matched[c.type] = (matched[c.type]||0) + 1;
      console.log(`[RAG] Matched chunks: ${chunks.length}`, JSON.stringify(matched));
      // Debug: specifically log report chunks
      const rptChunks = chunks.filter(c => ['report_overview','report_page','report_impact'].includes(c.type));
      if (rptChunks.length > 0) {
        console.log(`[RAG] Report chunks matched: ${rptChunks.length}`, rptChunks.slice(0,5).map(c => `${c.type} score=${c.score} ${(c._raw?.report_name||c._raw?.table_name||'').slice(0,40)}`));
      }
      if (chunks.length > 0) {
        chunks.sort((a,b) => b.score - a.score);
        console.log(`[RAG] Top 5:`, chunks.slice(0,5).map(c => `${c.type} score=${c.score} ${c.text.slice(0,80)}`));
      }
    } else if (analysisReady) {
      // Fallback: search IndexedDB cache
      for (const token of tokens) {
        const matches = await kbCache.searchLineage(token);
        for (const m of matches.slice(0, 20)) {
          const text = `AI Lineage: ${m.target_table}.${m.target_field} ← ${m.source_table||'?'}.${m.source_column||'?'}\nType: ${m.transformation_type||''}\nExpr: ${m.expression||''}\nLogic: ${m.business_logic||''}`;
          chunks.push({ score: 10 + (tokens.filter(t => text.toLowerCase().includes(t)).length), text });
        }
      }
    }

    // Search in workspace/item names and definitions (resolve from KB.definitions)
    if (KB.workspaces) for (const ws of KB.workspaces) {
      const wsName = ws.displayName || ws.name || ws.id;
      for (const item of (ws.items || [])) {
        // Skip Report and SemanticModel items — covered by richer KNOWLEDGE chunks
        if (item.type === 'Report' || item.type === 'SemanticModel') continue;
        const itemName = item.displayName || item.name || item.id;
        const blob = (JSON.stringify(item) + ' ' + itemName).toLowerCase();
        let score = 0;
        for (const t of tokens) if (blob.includes(t)) score++;
        const isPipeline = item.type === 'DataPipeline';
        // For pipelines, also check definition content for token matches
        const def = isPipeline ? getPipelineContent(item.id) : null;
        if (def) {
          const defLower = def.toLowerCase();
          for (const t of tokens) if (defLower.includes(t)) score++;
        }
        if (score === 0) continue;
        // For pipeline items: extract activity names and include full definition
        if (isPipeline && def) {
          const actNames = [];
          const actMatches = def.match(/"name"\s*:\s*"([^"]+)"/gi) || [];
          for (const m of actMatches) {
            const nm = m.match(/"name"\s*:\s*"([^"]+)"/i);
            if (nm && nm[1].length > 2) actNames.push(nm[1]);
          }
          const uniqueActs = [...new Set(actNames)];
          const text = `[${wsName}] Pipeline: ${itemName}\nActivities (${uniqueActs.length}): ${uniqueActs.join(', ')}\nDefinition: ${def.slice(0, 6000)}`;
          chunks.push({ score: score + pipelineBoost, text, type: 'raw_pipeline' });
        } else if (isPipeline) {
          // Pipeline without definition — still include basic info
          chunks.push({ score: score + pipelineBoost, text: `[${wsName}] Pipeline: ${itemName}\n(definition not available)`, type: 'raw_pipeline' });
        } else {
          chunks.push({ score, text: `[${wsName}] ${item.type}: ${itemName}\n(no inline definition)` });
        }
      }
    }

    // Search in schemas (values can be array of tables OR object with .tables/.views/.procedures)
    if (KB.schemas) for (const [itemId, val] of Object.entries(KB.schemas)) {
      if (!val) continue;
      const isWarehouse = !Array.isArray(val) && val.item_type;
      const tblList = Array.isArray(val) ? val : (val.tables || []);

      // Search tables
      const tblBlob = JSON.stringify(tblList).toLowerCase();
      let tblScore = 0;
      for (const t of tokens) if (tblBlob.includes(t)) tblScore++;
      if (tblScore > 0) {
        const label = isWarehouse ? `${val.item_type} [${itemId.slice(0,8)}…]` : `Schema [${itemId.slice(0,8)}…]`;
        chunks.push({ score: tblScore, text: `${label}: ${tblList.length} tables\nTables: ${tblList.map(t=>t.table_name||t.name||t).join(', ')}\nColumns: ${tblList.flatMap(t=>(t.columns||[])).slice(0,50).map(c=>`${c.column_name||c.name} (${c.data_type||''})`).join(', ')}` });
      }

      // Search views (Warehouse only)
      if (isWarehouse && val.views) for (const vw of val.views) {
        const vwBlob = `${vw.schema||''} ${vw.name||''} ${(vw.definition||'').slice(0,4000)}`.toLowerCase();
        let score = 0;
        for (const t of compoundTokens) if (vwBlob.includes(t)) score += 5;
        for (const t of splitTokens) if (vwBlob.includes(t)) score++;
        if (score > 0) chunks.push({ score: score + 1 + warehouseBoost, text: `View: [${vw.schema||'dbo'}].[${vw.name}]\nSQL:\n${(vw.definition||'').slice(0, 3000)}`, type: 'raw_view' });
      }

      // Search stored procedures (Warehouse only — critical for lineage)
      if (isWarehouse && val.procedures) for (const sp of val.procedures) {
        const spBlob = `${sp.schema||''} ${sp.name||''} ${(sp.definition||'').slice(0,4000)}`.toLowerCase();
        let score = 0;
        for (const t of compoundTokens) if (spBlob.includes(t)) score += 5;
        for (const t of splitTokens) if (spBlob.includes(t)) score++;
        if (score > 0) chunks.push({ score: score + 2 + warehouseBoost, text: `Stored Procedure: [${sp.schema||'dbo'}].[${sp.name}] (${sp.type||'PROCEDURE'})\nSQL:\n${(sp.definition||'').slice(0, 3000)}`, type: 'raw_sproc' });
      }
    }

    // Search in metadata queries (KB.metadata.queries)
    if (KB.metadata && KB.metadata.queries) for (const mq of KB.metadata.queries) {
      const blob = JSON.stringify(mq).toLowerCase();
      let score = 0;
      for (const t of tokens) if (blob.includes(t)) score++;
      if (score > 0) chunks.push({ score, text: `Metadata Query [${mq.layer||''}]: ${mq.target_table || mq.meta_table || 'unknown'}\nSQL: ${(mq.source_query || '').slice(0, 1500)}` });
    }

    // Search in bronze mappings (KB.metadata.bronze_meta) — boost for lineage queries
    if (KB.metadata && KB.metadata.bronze_meta) for (const bm of KB.metadata.bronze_meta) {
      const blob = JSON.stringify(bm).toLowerCase();
      let score = 0;
      for (const t of tokens) if (blob.includes(t)) score++;
      if (score > 0) {
        const bronzeBoost = isLineageQuery ? 6 : 0;
        chunks.push({ score: score + bronzeBoost, text: `Bronze Mapping: ${bm.target_table || 'unknown'} ← ${bm.source_schema||''}.${bm.source_table || 'unknown'}\nColumns: ${bm.source_columns || '*'} | Active: ${bm.is_active ?? 'unknown'}`, type: 'bronze_mapping' });
      }
    }

    // For lineage queries: prioritise chain-relevant chunks (field_detail, table_lineage, bronze)
    // For report/impact queries: prioritise report chunks within same score tier
    const CHAIN_TYPES = new Set(['field_detail', 'table_lineage']);
    const REPORT_TYPES = new Set(['report_overview', 'report_page', 'report_impact']);
    chunks.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (isLineageQuery) {
        const aChain = CHAIN_TYPES.has(a.type) ? 1 : 0;
        const bChain = CHAIN_TYPES.has(b.type) ? 1 : 0;
        if (bChain !== aChain) return bChain - aChain;
      }
      if (isReportQuery || isImpactQuery) {
        const aRpt = REPORT_TYPES.has(a.type) ? 1 : 0;
        const bRpt = REPORT_TYPES.has(b.type) ? 1 : 0;
        if (bRpt !== aRpt) return bRpt - aRpt;
      }
      return 0;
    });
    // Budget: lineage queries get 20k; warehouse/pipeline/report/SM 16k; default 12k
    let ctx = '', budget = isLineageQuery ? 20000 : (isWarehouseQuery || isPipelineQuery || isReportQuery || isImpactQuery || isSemanticModelQuery) ? 16000 : 12000;

    // ── Entity Pinning: guarantee all chunks for a named entity enter context ──
    const pinned = getPinnedChunks(query, chunks);
    if (pinned.length > 0) {
      for (const p of pinned) {
        ctx += p.text + '\n---\n';
        p._included = true;
      }
      console.log(`[RAG] Entity pinning: ${pinned.length} chunks pinned (${ctx.length} chars)`);
    }

    for (const c of chunks) {
      if (c._included) continue; // skip pinned chunks
      if (ctx.length + c.text.length > budget) { if (ctx.length > 0) break; }
      ctx += c.text + '\n---\n';
      c._included = true;
    }
    // ── Fallback: detect truncated SQL and append full source_query ──
    const TRUNC_PATTERNS = /-- \.\.\.|- \.\.\.|\/\* \.\.\.|тук са пропуснати|-- \(/i;
    if (TRUNC_PATTERNS.test(ctx) && KB.metadata && KB.metadata.queries) {
      // Collect table names from included chunks (target_table, chunk.table, chunk.id)
      const ctxTableNames = new Set();
      for (const c of chunks) {
        if (!c._included) continue; // only check chunks that made it into ctx
        const raw = c._raw;
        if (raw) {
          if (raw.table) ctxTableNames.add(raw.table.toLowerCase());
          if (raw.target_table) ctxTableNames.add(raw.target_table.toLowerCase());
          if (raw.id) ctxTableNames.add(raw.id.toLowerCase());
        }
      }
      // Also extract table-like tokens from the user query (underscore-joined words)
      for (const t of tokens) { if (t.includes('_')) ctxTableNames.add(t); }

      // Find matching metadata queries with full SQL
      const appended = new Set();
      for (const mq of KB.metadata.queries) {
        const tbl = (mq.target_table || mq.meta_table || '').toLowerCase();
        if (!tbl || !ctxTableNames.has(tbl)) continue;
        if (appended.has(tbl)) continue;
        const sql = mq.source_query || '';
        if (!sql || sql.length < 50) continue;
        ctx += `\n--- FULL SOURCE SQL for ${mq.target_table || mq.meta_table} [${mq.layer||''}] ---\n${sql}\n---\n`;
        appended.add(tbl);
        console.log(`[RAG] Appended full SQL for ${tbl} (${sql.length} chars) — truncation fallback`);
      }
    }

    console.log(`[RAG] Final context: ${ctx.length} chars, ${chunks.length} chunks considered, budget: ${budget}`);
    return ctx || '(No matching context found for this query)';
  } catch (err) {
    console.error('retrieveContext error:', err);
    return '(Context retrieval failed — answering without metadata context)';
  }
}

// ── System prompt ──────────────────────────────────────────────
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

// ── Excel generation helpers ───────────────────────────────────
const TEAL = '1C8D7A';
const TEAL_LIGHT = 'E6F5F2';
const DARK_BLUE = '0B3052';
const ALT_ROW = 'F2F3F4';
const WHITE = 'FFFFFF';
const TRANSFORM_COLORS = {
  direct_map: 'D4EDDA', case_when: 'D6E9F8', coalesce: 'D4EDE8',
  literal: 'E8E8E8', expression: 'FFF3CD', aggregate: 'F8D7DA',
};

function safeName(base, usedNames) {
  let name = base.replace(/[\\/*?:\[\]]/g, '').substring(0, 31);
  if (name.length === 0) name = 'Sheet';
  let candidate = name;
  let counter = 2;
  while (usedNames.has(candidate)) {
    const suffix = '_' + counter;
    candidate = name.substring(0, 31 - suffix.length) + suffix;
    counter++;
  }
  usedNames.add(candidate);
  return candidate;
}

function styleHeader(row, colCount) {
  row.height = 28;
  for (let c = 1; c <= colCount; c++) {
    const cell = row.getCell(c);
    cell.font = { name: 'Calibri Light', size: 11, bold: true, color: { argb: WHITE } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
    cell.alignment = { vertical: 'middle', wrapText: true };
    cell.border = { bottom: { style: 'thin', color: { argb: '999999' } } };
  }
}

function styleDataRow(row, colCount, rowIdx, codeColumns = []) {
  const isAlt = rowIdx % 2 === 0;
  for (let c = 1; c <= colCount; c++) {
    const cell = row.getCell(c);
    cell.font = { name: codeColumns.includes(c) ? 'Courier New' : 'Calibri', size: codeColumns.includes(c) ? 9 : 10 };
    if (isAlt) cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: ALT_ROW } };
    cell.alignment = { vertical: 'top', wrapText: true };
  }
}

function addBackLink(ws, targetSheet, label) {
  const cell = ws.getCell('A2');
  cell.value = { text: `← ${label}`, hyperlink: `#'${targetSheet}'!A1` };
  cell.font = { name: 'Calibri', size: 10, color: { argb: TEAL }, underline: true };
}

function styleTitleRow(ws, title, colSpan) {
  ws.mergeCells(1, 1, 1, colSpan);
  const cell = ws.getCell('A1');
  cell.value = title;
  cell.font = { name: 'Calibri Light', size: 16, bold: true, color: { argb: WHITE } };
  cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
  cell.alignment = { vertical: 'middle', horizontal: 'center' };
  ws.getRow(1).height = 36;
}

// ── Layer sub-header colors for Excel ─────────────────────────
const LAYER_COLORS = {
  Platinum: 'E5E4E2', Gold: 'FFF8DC', Silver_Stg: 'F0F0F0',
  Silver_Raw: 'F0F0F0', Bronze: 'F5E6CC',
};

// ── Collect table data for Excel (5-zone split) ──────────────
function collectTablesForExcel() {
  const zones = { Platinum: [], Gold: [], Silver_Stg: [], Silver_Raw: [], Bronze: [] };

  // Index: table_lineage and field_detail by table name (lowercase)
  const lineageMap = {};
  const fieldMap = {};
  for (const chunk of KNOWLEDGE) {
    if (chunk.type === 'table_lineage') lineageMap[chunk.id?.toLowerCase()] = chunk;
    if (chunk.type === 'field_detail') {
      const key = chunk.table?.toLowerCase();
      if (key) { (fieldMap[key] ||= []).push(chunk); }
    }
  }

  // Bronze
  if (KB?.metadata?.bronze_meta) {
    const bronzeByTable = {};
    for (const bm of KB.metadata.bronze_meta) {
      const tbl = bm.target_table || 'unknown';
      if (!bronzeByTable[tbl]) bronzeByTable[tbl] = {
        name: tbl, source: `${bm.source_schema||''}.${bm.source_table||''}`,
        columns: [], active: bm.is_active || 'true',
      };
      bronzeByTable[tbl].source = `${bm.source_schema||''}.${bm.source_table||''}`;
      bronzeByTable[tbl].active = bm.is_active || 'true';
    }
    if (KB.schemas) for (const [, val] of Object.entries(KB.schemas)) {
      if (Array.isArray(val)) for (const tbl of val) {
        const nm = tbl.table_name || tbl.name || '';
        if (bronzeByTable[nm]) bronzeByTable[nm].columns = tbl.columns || [];
      }
    }
    zones.Bronze = Object.values(bronzeByTable);
  }

  // Silver (split Staging vs Raw) + Gold
  for (const chunk of KNOWLEDGE) {
    if (chunk.type !== 'table_lineage') continue;
    const layer = (chunk.layer || '').toLowerCase();
    const tblName = chunk.id || 'unknown';
    const entry = {
      name: tblName,
      source: (chunk.source_tables || []).join(', '),
      fieldCount: chunk.field_count || 0,
      fields: fieldMap[tblName.toLowerCase()] || [],
    };
    if (layer.includes('gold')) {
      zones.Gold.push(entry);
    } else if (layer.includes('silver')) {
      if (tblName.startsWith('stg_')) zones.Silver_Stg.push(entry);
      else zones.Silver_Raw.push(entry);
    }
  }

  // Platinum — warehouse_sproc / warehouse_view
  const whNameMap = {};
  if (KB?.workspaces) {
    for (const ws of KB.workspaces)
      for (const item of (ws.items || []))
        if (item.type === 'Warehouse') whNameMap[item.id] = item.displayName || item.id;
  }
  for (const chunk of KNOWLEDGE) {
    if (chunk.type !== 'warehouse_sproc' && chunk.type !== 'warehouse_view') continue;
    const whName = chunk.warehouse_name || whNameMap[chunk.warehouse_id] || '';
    const prefix = whName ? `${whName}.` : '';
    zones.Platinum.push({
      name: `${prefix}${chunk.schema||'dbo'}.${chunk.name}`,
      source: chunk.type === 'warehouse_sproc' ? 'Stored Procedure' : 'View',
      fieldCount: 0,
      definition: chunk.definition || '',
      chunkType: chunk.type,
    });
  }

  for (const z of Object.keys(zones)) zones[z].sort((a, b) => a.name.localeCompare(b.name));
  return { zones, fieldMap, lineageMap };
}

// ── Trace lineage chain downward from a given table ──────────
function traceTableChain(tableName, startLayer, fieldMap) {
  const chain = [];
  // Collect fields for this table
  const fields = fieldMap[tableName.toLowerCase()] || [];
  // Find downstream source tables
  const sourceTables = new Set();
  for (const f of fields) { if (f.source_table) sourceTables.add(f.source_table); }

  if (startLayer === 'Platinum') {
    // Platinum doesn't have field_detail — parse definition in the caller
    return chain; // handled separately
  }

  // Current layer fields
  if (fields.length > 0) chain.push({ layer: startLayer, table: tableName, fields });

  // Trace to next layer down
  const nextLayer = startLayer === 'Gold' ? 'Silver' : startLayer.startsWith('Silver') ? 'Bronze' : null;
  if (!nextLayer) return chain;

  if (nextLayer === 'Bronze' && KB?.metadata?.bronze_meta) {
    const bronzeHits = [];
    for (const src of sourceTables) {
      const srcLower = src.toLowerCase().replace(/^.*\./, '');
      for (const bm of KB.metadata.bronze_meta) {
        if ((bm.target_table || '').toLowerCase().includes(srcLower) ||
            srcLower.includes((bm.target_table || '').toLowerCase())) {
          const key = `${bm.source_schema}.${bm.source_table}→${bm.target_table}`;
          if (!bronzeHits.some(b => `${b.source_schema}.${b.source_table}→${b.target_table}` === key))
            bronzeHits.push(bm);
        }
      }
    }
    if (bronzeHits.length > 0) chain.push({ layer: 'Bronze', table: '(source systems)', bronzeMeta: bronzeHits });
  } else if (nextLayer === 'Silver') {
    for (const src of sourceTables) {
      const srcLower = src.toLowerCase().replace(/^.*\./, '');
      const silverFields = fieldMap[srcLower] || [];
      if (silverFields.length > 0) {
        chain.push({ layer: 'Silver', table: src, fields: silverFields });
        // Trace Silver → Bronze
        const silverSources = new Set();
        for (const f of silverFields) { if (f.source_table) silverSources.add(f.source_table); }
        if (KB?.metadata?.bronze_meta) {
          const bronzeHits = [];
          for (const ss of silverSources) {
            const ssLower = ss.toLowerCase().replace(/^.*\./, '');
            for (const bm of KB.metadata.bronze_meta) {
              if ((bm.target_table || '').toLowerCase().includes(ssLower) ||
                  ssLower.includes((bm.target_table || '').toLowerCase())) {
                const key = `${bm.source_schema}.${bm.source_table}→${bm.target_table}`;
                if (!bronzeHits.some(b => `${b.source_schema}.${b.source_table}→${b.target_table}` === key))
                  bronzeHits.push(bm);
              }
            }
          }
          if (bronzeHits.length > 0) chain.push({ layer: 'Bronze', table: '(source systems)', bronzeMeta: bronzeHits });
        }
      }
    }
  }
  return chain;
}

// ── Write layer sub-header row ────────────────────────────────
function writeLayerHeader(ws, row, label, colSpan, colorArgb) {
  ws.mergeCells(row, 1, row, colSpan);
  const cell = ws.getRow(row).getCell(1);
  cell.value = label;
  cell.font = { name: 'Calibri Light', size: 12, bold: true };
  cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: colorArgb } };
  cell.alignment = { vertical: 'middle' };
  ws.getRow(row).height = 26;
}

// ── Write field lineage rows into a detail sheet ─────────────
function writeFieldRows(ws, startRow, fields, colSpan) {
  const hdr = ws.getRow(startRow);
  ['#','Target Field','Data Type','Source Table','Source Column','Transformation','SQL Expression']
    .forEach((h, i) => { hdr.getCell(i+1).value = h; });
  styleHeader(hdr, colSpan);
  let r = startRow + 1;
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]; const row = ws.getRow(r);
    row.getCell(1).value = i+1;
    row.getCell(2).value = f.target_field || '';
    row.getCell(3).value = f.data_type || '';
    row.getCell(4).value = f.source_table || '';
    row.getCell(5).value = f.source_column || '';
    row.getCell(6).value = f.transformation_type || '';
    row.getCell(7).value = (f.expression || '').slice(0,500);
    styleDataRow(row, colSpan, r - startRow - 1, [7]);
    const tc = TRANSFORM_COLORS[(f.transformation_type||'').toLowerCase()];
    if (tc) row.getCell(6).fill = { type:'pattern', pattern:'solid', fgColor:{ argb: tc } };
    r++;
  }
  if (fields.length === 0) { ws.getRow(startRow+1).getCell(1).value = '(No field lineage data)'; r++; }
  return r;
}

// ── Write Bronze meta rows into a detail sheet ───────────────
function writeBronzeRows(ws, startRow, bronzeMeta, colSpan) {
  const hdr = ws.getRow(startRow);
  ['#','Source Schema','Source Table','Target Table','Active','','']
    .forEach((h, i) => { hdr.getCell(i+1).value = h; });
  styleHeader(hdr, colSpan);
  let r = startRow + 1;
  for (let i = 0; i < bronzeMeta.length; i++) {
    const bm = bronzeMeta[i]; const row = ws.getRow(r);
    row.getCell(1).value = i+1;
    row.getCell(2).value = bm.source_schema || '';
    row.getCell(3).value = bm.source_table || '';
    row.getCell(4).value = bm.target_table || '';
    row.getCell(5).value = bm.is_active || '';
    styleDataRow(row, colSpan, r - startRow - 1);
    r++;
  }
  if (bronzeMeta.length === 0) { ws.getRow(startRow+1).getCell(1).value = '(No Bronze mapping)'; r++; }
  return r;
}

// ── Generate Data Lineage Excel (v2 — full chain) ────────────
async function generateLineageExcel() {
  if (!KB) { appendMessage('assistant', '⚠️ No data loaded — cannot generate Excel.'); return; }
  const progressDiv = document.createElement('div');
  progressDiv.className = 'message assistant';
  progressDiv.innerHTML = '<div class="msg-label">Assistant</div><div class="msg-bubble" id="excelProgress">📊 <b>Generating Data Lineage Excel...</b><br></div>';
  chatArea.appendChild(progressDiv);
  chatArea.scrollTop = chatArea.scrollHeight;
  const progEl = document.getElementById('excelProgress');
  const log = (msg) => { progEl.innerHTML += msg + '<br>'; chatArea.scrollTop = chatArea.scrollHeight; };
  await new Promise(r => setTimeout(r, 50));

  const { zones, fieldMap } = collectTablesForExcel();
  const wb = new ExcelJS.Workbook();
  wb.creator = 'GenDWH Knowledge Assistant';
  wb.created = new Date();
  const usedNames = new Set();
  const catNames = {};   // zone → catalog sheet name
  const detailMap = {};   // zone::table → detail sheet name
  const COL_SPAN = 7;
  const zoneLabels = {
    Platinum: 'Platinum — Warehouse Procedures & Views',
    Gold: 'Gold — Business-Ready Aggregations',
    Silver_Stg: 'Silver Staging — Cleansed & Enriched',
    Silver_Raw: 'Silver Raw — Initial Transformations',
    Bronze: 'Bronze — Landing / Source Copy',
  };
  const zoneDesc = {
    Platinum: 'Warehouse stored procedures and views',
    Gold: 'Business-ready dim_/fact_ tables (SCD2)',
    Silver_Stg: 'Staging tables (stg_*) — cleansed, IFRS',
    Silver_Raw: 'Raw tables — initial transformations',
    Bronze: 'Landing / 1:1 copy from source systems',
  };
  const totalTables = Object.values(zones).reduce((s, z) => s + z.length, 0);
  const totalFields = Object.values(fieldMap).reduce((s, arr) => s + arr.length, 0);

  // ── Sheet 1: TITLE PAGE ───────────────────────────────────
  log('Creating Title page...');
  const titleWs = wb.addWorksheet(safeName('Title', usedNames));
  titleWs.mergeCells('A1:G1');
  const tc = titleWs.getCell('A1');
  tc.value = 'GenDWH Data Lineage';
  tc.font = { name: 'Calibri Light', size: 28, bold: true, color: { argb: WHITE } };
  tc.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
  tc.alignment = { vertical: 'middle', horizontal: 'center' };
  titleWs.getRow(1).height = 56;
  titleWs.getCell('A3').value = `Generated: ${new Date().toISOString().slice(0,10)}`;
  titleWs.getCell('A3').font = { name: 'Calibri', size: 11, italic: true, color: { argb: '666666' } };
  titleWs.getCell('A5').value = 'Summary';
  titleWs.getCell('A5').font = { name: 'Calibri Light', size: 14, bold: true, color: { argb: TEAL } };
  let tRow = 6;
  for (const [z, tbls] of Object.entries(zones)) {
    titleWs.getCell(`A${tRow}`).value = zoneLabels[z] || z;
    titleWs.getCell(`A${tRow}`).font = { name: 'Calibri', size: 10 };
    titleWs.getCell(`C${tRow}`).value = `${tbls.length} tables`;
    titleWs.getCell(`C${tRow}`).font = { name: 'Calibri', size: 10, bold: true };
    tRow++;
  }
  titleWs.getCell(`A${tRow+1}`).value = `Total: ${totalTables} tables, ${totalFields} fields analyzed`;
  titleWs.getCell(`A${tRow+1}`).font = { name: 'Calibri', size: 11, bold: true, color: { argb: TEAL } };
  titleWs.getCell(`A${tRow+3}`).value = 'InspirIT — GenDWH Knowledge Assistant';
  titleWs.getCell(`A${tRow+3}`).font = { name: 'Calibri Light', size: 10, italic: true, color: { argb: '999999' } };
  titleWs.columns = [{ width: 20 },{ width: 15 },{ width: 15 },{ width: 15 },{ width: 15 },{ width: 15 },{ width: 15 }];

  // ── Sheet 2: INDEX ────────────────────────────────────────
  log('Creating INDEX...');
  const idxWs = wb.addWorksheet(safeName('INDEX', usedNames));
  styleTitleRow(idxWs, 'Data Lineage — Zone Index', 5);
  addBackLink(idxWs, 'Title', 'Back to Title');
  const idxHdr = idxWs.getRow(4);
  ['Zone','Tables','Description','','Navigate'].forEach((h,i) => { idxHdr.getCell(i+1).value = h; });
  styleHeader(idxHdr, 5);
  let iRow = 5;
  for (const [zone, tbls] of Object.entries(zones)) {
    const catName = safeName(zone.replace('_',' '), usedNames);
    catNames[zone] = catName;
    const r = idxWs.getRow(iRow);
    r.getCell(1).value = zone.replace('_',' ');
    r.getCell(1).font = { name: 'Calibri', size: 11, bold: true };
    r.getCell(2).value = tbls.length;
    r.getCell(3).value = zoneDesc[zone] || '';
    r.getCell(5).value = { text: `→ ${zone.replace('_',' ')}`, hyperlink: `#'${catName}'!A1` };
    r.getCell(5).font = { name: 'Calibri', size: 10, color: { argb: TEAL }, underline: true };
    styleDataRow(r, 5, iRow - 5);
    iRow++;
  }
  idxWs.columns = [{ width: 18 },{ width: 10 },{ width: 44 },{ width: 2 },{ width: 18 }];
  idxWs.views = [{ state: 'frozen', ySplit: 4 }];
  await new Promise(r => setTimeout(r, 10));

  // ── Catalog sheets (one per zone) + pre-register detail names ─
  for (const [zone, tables] of Object.entries(zones)) {
    log(`Creating ${zone.replace('_',' ')} catalog (${tables.length} tables)...`);
    const catWs = wb.addWorksheet(catNames[zone]);
    styleTitleRow(catWs, zoneLabels[zone] || zone, 5);
    addBackLink(catWs, 'INDEX', 'Back to INDEX');

    const isBronze = zone === 'Bronze';
    const headers = isBronze
      ? ['#','Table Name','Source System Table','Columns','Active']
      : ['#','Name', zone==='Platinum' ? 'Type' : 'Fields','Source Tables','→ Detail'];
    const hRow = catWs.getRow(4);
    headers.forEach((h,i) => { hRow.getCell(i+1).value = h; });
    styleHeader(hRow, 5);

    let cRow = 5;
    for (let ti = 0; ti < tables.length; ti++) {
      const tbl = tables[ti];
      const r = catWs.getRow(cRow);
      r.getCell(1).value = ti + 1;
      r.getCell(2).value = tbl.name;

      if (isBronze) {
        r.getCell(3).value = tbl.source || '';
        r.getCell(4).value = tbl.columns?.length || 0;
        r.getCell(5).value = tbl.active || '';
      } else {
        if (zone === 'Platinum') {
          r.getCell(3).value = tbl.source || '';  // Procedure/View
        } else {
          r.getCell(3).value = tbl.fieldCount || tbl.fields?.length || 0;
        }
        r.getCell(4).value = tbl.source || '';
        // Pre-register detail sheet name
        const dk = zone + '::' + tbl.name;
        detailMap[dk] = safeName(tbl.name, usedNames);
        r.getCell(5).value = { text: '→ Detail', hyperlink: `#'${detailMap[dk]}'!A1` };
        r.getCell(5).font = { name: 'Calibri', size: 10, color: { argb: TEAL }, underline: true };
      }
      styleDataRow(r, 5, cRow - 5);
      cRow++;
    }
    catWs.columns = [{ width: 6 },{ width: 42 },{ width: 16 },{ width: 44 },{ width: 12 }];
    catWs.views = [{ state: 'frozen', ySplit: 4 }];
    await new Promise(r => setTimeout(r, 10));
  }

  // ── Detail sheets (full chain) — skip Bronze ──────────────
  let detailCount = 0;
  for (const [zone, tables] of Object.entries(zones)) {
    if (zone === 'Bronze') continue;  // Bronze has no detail sheets
    for (let ti = 0; ti < tables.length; ti++) {
      const tbl = tables[ti];
      const dk = zone + '::' + tbl.name;
      if (!detailMap[dk]) continue;
      detailCount++;
      if (detailCount % 20 === 0) {
        log(`Creating detail sheet ${detailCount}...`);
        await new Promise(r => setTimeout(r, 0));
      }

      const dWs = wb.addWorksheet(detailMap[dk]);
      styleTitleRow(dWs, tbl.name, COL_SPAN);
      addBackLink(dWs, catNames[zone], `Back to ${zone.replace('_',' ')}`);
      let row = 4;

      if (zone === 'Platinum') {
        // === PLATINUM SECTION ===
        writeLayerHeader(dWs, row, '═══ PLATINUM — Warehouse Definition ═══', COL_SPAN, LAYER_COLORS.Platinum);
        row++;
        const pHdr = dWs.getRow(row);
        ['Property','Value','','','','',''].forEach((h,i) => { pHdr.getCell(i+1).value = h; });
        styleHeader(pHdr, COL_SPAN);
        row++;
        dWs.getRow(row).getCell(1).value = 'Type';
        dWs.getRow(row).getCell(2).value = tbl.chunkType === 'warehouse_sproc' ? 'Stored Procedure' : 'View';
        styleDataRow(dWs.getRow(row), COL_SPAN, 0); row++;
        dWs.getRow(row).getCell(1).value = 'SQL Definition';
        dWs.getRow(row).getCell(2).value = (tbl.definition || '').slice(0, 8000);
        dWs.getRow(row).getCell(2).font = { name: 'Courier New', size: 9 };
        dWs.getRow(row).getCell(2).alignment = { wrapText: true, vertical: 'top' };
        styleDataRow(dWs.getRow(row), COL_SPAN, 1, [2]); row++;

        // Parse definition for FROM tables → trace to Gold
        const def = tbl.definition || '';
        const fromMatches = [...def.matchAll(/FROM\s+(?:\[?[\w]+\]?\.)?(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?/gi)];
        const goldSrcTables = [...new Set(fromMatches.map(m => m[1].toLowerCase()))];
        row++;
        for (const goldTbl of goldSrcTables) {
          const gFields = fieldMap[goldTbl] || [];
          if (gFields.length === 0) continue;
          writeLayerHeader(dWs, row, `═══ GOLD — ${goldTbl} ═══`, COL_SPAN, LAYER_COLORS.Gold);
          row++;
          row = writeFieldRows(dWs, row, gFields, COL_SPAN);
          // Trace Gold → Silver
          const silverSrcs = new Set();
          for (const f of gFields) { if (f.source_table) silverSrcs.add(f.source_table); }
          for (const sSrc of silverSrcs) {
            const sKey = sSrc.toLowerCase().replace(/^.*\./, '');
            const sFields = fieldMap[sKey] || [];
            if (sFields.length === 0) continue;
            row++;
            writeLayerHeader(dWs, row, `═══ SILVER — ${sSrc} ═══`, COL_SPAN, LAYER_COLORS.Silver_Stg);
            row++;
            row = writeFieldRows(dWs, row, sFields, COL_SPAN);
            // Trace Silver → Bronze
            const bronzeSrcs = new Set();
            for (const f of sFields) { if (f.source_table) bronzeSrcs.add(f.source_table); }
            if (bronzeSrcs.size > 0 && KB?.metadata?.bronze_meta) {
              const bHits = [];
              for (const bs of bronzeSrcs) {
                const bsLower = bs.toLowerCase().replace(/^.*\./, '');
                for (const bm of KB.metadata.bronze_meta) {
                  if ((bm.target_table||'').toLowerCase().includes(bsLower) ||
                      bsLower.includes((bm.target_table||'').toLowerCase())) {
                    const key = `${bm.source_schema}.${bm.source_table}→${bm.target_table}`;
                    if (!bHits.some(b => `${b.source_schema}.${b.source_table}→${b.target_table}` === key))
                      bHits.push(bm);
                  }
                }
              }
              if (bHits.length > 0) {
                row++;
                writeLayerHeader(dWs, row, '═══ BRONZE — Source Systems ═══', COL_SPAN, LAYER_COLORS.Bronze);
                row++;
                row = writeBronzeRows(dWs, row, bHits, COL_SPAN);
              }
            }
          }
        }
      } else {
        // Gold / Silver detail — start with own fields, then chain down
        const startLabel = zone.startsWith('Silver') ? 'SILVER' : 'GOLD';
        const layerColor = zone === 'Gold' ? LAYER_COLORS.Gold : LAYER_COLORS.Silver_Stg;
        writeLayerHeader(dWs, row, `═══ ${startLabel} — ${tbl.name} ═══`, COL_SPAN, layerColor);
        row++;
        const fields = tbl.fields || [];
        row = writeFieldRows(dWs, row, fields, COL_SPAN);

        // Chain down
        const chain = traceTableChain(tbl.name, zone === 'Gold' ? 'Gold' : 'Silver', fieldMap);
        for (const step of chain) {
          if (step.table === tbl.name && step.layer === startLabel) continue; // skip self
          row++;
          const stepColor = step.layer === 'Bronze' ? LAYER_COLORS.Bronze
            : step.layer === 'Silver' ? LAYER_COLORS.Silver_Stg : LAYER_COLORS.Gold;
          writeLayerHeader(dWs, row, `═══ ${step.layer.toUpperCase()} — ${step.table} ═══`, COL_SPAN, stepColor);
          row++;
          if (step.bronzeMeta) {
            row = writeBronzeRows(dWs, row, step.bronzeMeta, COL_SPAN);
          } else {
            row = writeFieldRows(dWs, row, step.fields || [], COL_SPAN);
          }
        }
      }
      dWs.columns = [{ width: 5 },{ width: 30 },{ width: 15 },{ width: 30 },{ width: 25 },{ width: 16 },{ width: 50 }];
      dWs.views = [{ state: 'frozen', ySplit: 4 }];
    }
  }

  // ── Generate & download ────────────────────────────────────
  log('Writing Excel file...');
  const buffer = await wb.xlsx.writeBuffer();
  const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  const url = URL.createObjectURL(blob);
  const totalSheets = wb.worksheets.length;

  progEl.innerHTML = `📥 <b>Data Lineage Excel ready!</b><br>
    ${totalTables} tables across ${Object.keys(zones).length} zones, ${totalSheets} sheets<br>
    <a href="${url}" download="GenDWH_Data_Lineage_${new Date().toISOString().slice(0,10)}.xlsx"
       style="display:inline-block;margin-top:8px;padding:8px 16px;background:#1C8D7A;color:white;border-radius:6px;text-decoration:none;font-weight:bold;">
       📥 Download Excel
    </a>`;
  chatArea.scrollTop = chatArea.scrollHeight;
  chatHistory.push({ role: 'assistant', content: `[Generated Data Lineage Excel: ${totalTables} tables, ${totalSheets} sheets]` });
  console.log(`[SKILL] Lineage Excel generated: ${totalTables} tables, ${totalSheets} sheets, ${(buffer.byteLength/1024).toFixed(0)} KB`);
}

// ── Generate Data Dictionary Excel ───────────────────────────
async function generateDictionaryExcel() {
  if (!KB) { appendMessage('assistant', '⚠️ No data loaded — cannot generate Excel.'); return; }
  const progressDiv = document.createElement('div');
  progressDiv.className = 'message assistant';
  progressDiv.innerHTML = '<div class="msg-label">Assistant</div><div class="msg-bubble" id="dictProgress">📖 <b>Generating Data Dictionary Excel...</b><br></div>';
  chatArea.appendChild(progressDiv);
  chatArea.scrollTop = chatArea.scrollHeight;
  const progEl = document.getElementById('dictProgress');
  const log = (msg) => { progEl.innerHTML += msg + '<br>'; chatArea.scrollTop = chatArea.scrollHeight; };
  await new Promise(r => setTimeout(r, 50));

  // Build item name map: itemId → { name, type }
  const itemMap = {};
  if (KB.workspaces) for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      itemMap[item.id] = { name: item.displayName || item.name || item.id, type: item.type, workspace: ws.name };
    }
  }

  // Build field description map from KNOWLEDGE field_detail chunks
  const descMap = {}; // "tableLower::fieldLower" → business_logic
  for (const chunk of KNOWLEDGE) {
    if (chunk.type !== 'field_detail') continue;
    const tKey = (chunk.table || '').toLowerCase();
    const fKey = (chunk.target_field || '').toLowerCase();
    if (tKey && fKey) descMap[`${tKey}::${fKey}`] = chunk.business_logic || chunk.expression || '';
  }

  const wb = new ExcelJS.Workbook();
  wb.creator = 'GenDWH Knowledge Assistant';
  wb.created = new Date();
  const usedNames = new Set();
  const COL = 5;

  // Collect sections: one per schema entry
  const sections = [];
  if (KB.schemas) for (const [itemId, val] of Object.entries(KB.schemas)) {
    if (!val) continue;
    const info = itemMap[itemId] || { name: itemId.slice(0, 12), type: 'Unknown' };
    const isWarehouse = !Array.isArray(val) && val.item_type;
    const tables = Array.isArray(val) ? val : (val.tables || []);
    const views = isWarehouse ? (val.views || []) : [];
    const procedures = isWarehouse ? (val.procedures || []) : [];
    sections.push({ id: itemId, name: info.name, type: isWarehouse ? 'Warehouse' : 'Lakehouse', workspace: info.workspace, tables, views, procedures });
  }
  sections.sort((a, b) => a.name.localeCompare(b.name));

  let totalTables = 0, totalCols = 0, totalViews = 0, totalProcs = 0;
  for (const sec of sections) {
    totalTables += sec.tables.length;
    for (const t of sec.tables) totalCols += (t.columns || []).length;
    totalViews += sec.views.length;
    totalProcs += sec.procedures.length;
  }

  // ── TITLE PAGE ─────────────────────────────────────────────
  log('Creating Title page...');
  const titleWs = wb.addWorksheet(safeName('Title', usedNames));
  titleWs.mergeCells('A1:E1');
  const tc = titleWs.getCell('A1');
  tc.value = 'GenDWH Data Dictionary';
  tc.font = { name: 'Calibri Light', size: 28, bold: true, color: { argb: WHITE } };
  tc.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
  tc.alignment = { vertical: 'middle', horizontal: 'center' };
  titleWs.getRow(1).height = 56;
  titleWs.getCell('A3').value = `Generated: ${new Date().toISOString().slice(0,10)}`;
  titleWs.getCell('A3').font = { name: 'Calibri', size: 11, italic: true, color: { argb: '666666' } };
  titleWs.getCell('A5').value = 'Summary';
  titleWs.getCell('A5').font = { name: 'Calibri Light', size: 14, bold: true, color: { argb: TEAL } };
  const stats = [
    [`Lakehouses / Warehouses`, sections.length],
    [`Tables`, totalTables],
    [`Total Columns`, totalCols],
    [`Views`, totalViews],
    [`Stored Procedures`, totalProcs],
  ];
  let tRow = 6;
  for (const [label, val] of stats) {
    titleWs.getCell(`A${tRow}`).value = label;
    titleWs.getCell(`A${tRow}`).font = { name: 'Calibri', size: 10 };
    titleWs.getCell(`C${tRow}`).value = val;
    titleWs.getCell(`C${tRow}`).font = { name: 'Calibri', size: 10, bold: true };
    tRow++;
  }
  titleWs.getCell(`A${tRow+1}`).value = 'InspirIT — GenDWH Knowledge Assistant';
  titleWs.getCell(`A${tRow+1}`).font = { name: 'Calibri Light', size: 10, italic: true, color: { argb: '999999' } };
  titleWs.columns = [{ width: 22 },{ width: 15 },{ width: 15 },{ width: 15 },{ width: 15 }];

  // ── INDEX ──────────────────────────────────────────────────
  log('Creating INDEX...');
  const idxWs = wb.addWorksheet(safeName('INDEX', usedNames));
  styleTitleRow(idxWs, 'Data Dictionary — Index', COL);
  addBackLink(idxWs, 'Title', 'Back to Title');
  const idxHdr = idxWs.getRow(4);
  ['#','Name','Type','Tables','Navigate'].forEach((h,i) => { idxHdr.getCell(i+1).value = h; });
  styleHeader(idxHdr, COL);

  const secSheetNames = {};
  let iRow = 5;
  for (let si = 0; si < sections.length; si++) {
    const sec = sections[si];
    const shName = safeName(sec.name, usedNames);
    secSheetNames[sec.id] = shName;
    const r = idxWs.getRow(iRow);
    r.getCell(1).value = si + 1;
    r.getCell(2).value = sec.name;
    r.getCell(3).value = sec.type;
    r.getCell(4).value = sec.tables.length + (sec.views.length > 0 ? ` + ${sec.views.length}v` : '') + (sec.procedures.length > 0 ? ` + ${sec.procedures.length}p` : '');
    r.getCell(5).value = { text: '→ Section', hyperlink: `#'${shName}'!A1` };
    r.getCell(5).font = { name: 'Calibri', size: 10, color: { argb: TEAL }, underline: true };
    styleDataRow(r, COL, iRow - 5);
    iRow++;
  }
  idxWs.columns = [{ width: 5 },{ width: 35 },{ width: 14 },{ width: 18 },{ width: 14 }];
  idxWs.views = [{ state: 'frozen', ySplit: 4 }];
  await new Promise(r => setTimeout(r, 10));

  // ── Section sheets (one per Lakehouse/Warehouse) ───────────
  for (let si = 0; si < sections.length; si++) {
    const sec = sections[si];
    log(`Creating ${sec.name} (${sec.tables.length} tables)...`);
    const secWs = wb.addWorksheet(secSheetNames[sec.id]);
    styleTitleRow(secWs, `${sec.name} (${sec.type})`, COL);
    addBackLink(secWs, 'INDEX', 'Back to INDEX');
    let row = 4;

    // ── Tables section ───────────────────────────────────────
    for (const tbl of sec.tables) {
      const tblName = tbl.table_name || tbl.name || 'unknown';
      // Sub-header for table
      secWs.mergeCells(row, 1, row, COL);
      const thCell = secWs.getRow(row).getCell(1);
      thCell.value = `📋 ${tblName}`;
      thCell.font = { name: 'Calibri', size: 11, bold: true, color: { argb: WHITE } };
      thCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
      thCell.alignment = { vertical: 'middle' };
      secWs.getRow(row).height = 24;
      row++;

      // Column headers
      const hdr = secWs.getRow(row);
      ['#','Column Name','Data Type','Nullable','Description'].forEach((h,i) => { hdr.getCell(i+1).value = h; });
      styleHeader(hdr, COL);
      row++;

      const cols = tbl.columns || [];
      for (let ci = 0; ci < cols.length; ci++) {
        const c = cols[ci];
        const cName = c.column_name || c.COLUMN_NAME || c.name || '';
        const cType = c.data_type || c.DATA_TYPE || c.dataType || '';
        const cNullable = c.is_nullable != null ? String(c.is_nullable)
          : c.IS_NULLABLE != null ? String(c.IS_NULLABLE)
          : c.nullable != null ? String(c.nullable) : '';
        const r = secWs.getRow(row);
        r.getCell(1).value = ci + 1;
        r.getCell(2).value = cName;
        r.getCell(3).value = cType;
        r.getCell(4).value = cNullable;
        // Look up description from field_detail
        const descKey = `${tblName.toLowerCase()}::${cName.toLowerCase()}`;
        r.getCell(5).value = descMap[descKey] || '';
        if (descMap[descKey]) r.getCell(5).font = { name: 'Calibri', size: 9, italic: true };
        styleDataRow(r, COL, ci);
        row++;
      }
      if (cols.length === 0) {
        secWs.getRow(row).getCell(1).value = '(No columns)';
        row++;
      }
      row++; // blank spacer
    }

    // ── Views section (Warehouse only) ───────────────────────
    if (sec.views.length > 0) {
      secWs.mergeCells(row, 1, row, COL);
      const vhCell = secWs.getRow(row).getCell(1);
      vhCell.value = `👁️ Views (${sec.views.length})`;
      vhCell.font = { name: 'Calibri Light', size: 13, bold: true, color: { argb: DARK_BLUE } };
      vhCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: LAYER_COLORS.Platinum } };
      secWs.getRow(row).height = 28;
      row++;
      const vHdr = secWs.getRow(row);
      ['#','View Name','Schema','SQL Definition',''].forEach((h,i) => { vHdr.getCell(i+1).value = h; });
      styleHeader(vHdr, COL);
      row++;
      for (let vi = 0; vi < sec.views.length; vi++) {
        const vw = sec.views[vi];
        const r = secWs.getRow(row);
        r.getCell(1).value = vi + 1;
        r.getCell(2).value = vw.name || '';
        r.getCell(3).value = vw.schema || 'dbo';
        r.getCell(4).value = (vw.definition || '').slice(0, 500);
        r.getCell(4).font = { name: 'Courier New', size: 9 };
        r.getCell(4).alignment = { wrapText: true, vertical: 'top' };
        styleDataRow(r, COL, vi, [4]);
        row++;
      }
      row++;
    }

    // ── Procedures section (Warehouse only) ──────────────────
    if (sec.procedures.length > 0) {
      secWs.mergeCells(row, 1, row, COL);
      const phCell = secWs.getRow(row).getCell(1);
      phCell.value = `⚙️ Stored Procedures (${sec.procedures.length})`;
      phCell.font = { name: 'Calibri Light', size: 13, bold: true, color: { argb: DARK_BLUE } };
      phCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: LAYER_COLORS.Gold } };
      secWs.getRow(row).height = 28;
      row++;
      const pHdr = secWs.getRow(row);
      ['#','Procedure Name','Schema','Type','SQL Definition'].forEach((h,i) => { pHdr.getCell(i+1).value = h; });
      styleHeader(pHdr, COL);
      row++;
      for (let pi = 0; pi < sec.procedures.length; pi++) {
        const sp = sec.procedures[pi];
        const r = secWs.getRow(row);
        r.getCell(1).value = pi + 1;
        r.getCell(2).value = sp.name || '';
        r.getCell(3).value = sp.schema || 'dbo';
        r.getCell(4).value = sp.type || 'PROCEDURE';
        r.getCell(5).value = (sp.definition || '').slice(0, 500);
        r.getCell(5).font = { name: 'Courier New', size: 9 };
        r.getCell(5).alignment = { wrapText: true, vertical: 'top' };
        styleDataRow(r, COL, pi, [5]);
        row++;
      }
    }

    secWs.columns = [{ width: 5 },{ width: 35 },{ width: 18 },{ width: 14 },{ width: 60 }];
    secWs.views = [{ state: 'frozen', ySplit: 3 }];
    await new Promise(r => setTimeout(r, 10));
  }

  // ── Generate & download ────────────────────────────────────
  log('Writing Excel file...');
  const buffer = await wb.xlsx.writeBuffer();
  const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  const url = URL.createObjectURL(blob);
  const totalSheets = wb.worksheets.length;

  progEl.innerHTML = `📥 <b>Data Dictionary Excel ready!</b><br>
    ${sections.length} sections, ${totalTables} tables, ${totalCols} columns, ${totalSheets} sheets<br>
    <a href="${url}" download="GenDWH_Data_Dictionary_${new Date().toISOString().slice(0,10)}.xlsx"
       style="display:inline-block;margin-top:8px;padding:8px 16px;background:#1C8D7A;color:white;border-radius:6px;text-decoration:none;font-weight:bold;">
       📥 Download Excel
    </a>`;
  chatArea.scrollTop = chatArea.scrollHeight;
  chatHistory.push({ role: 'assistant', content: `[Generated Data Dictionary: ${totalTables} tables, ${totalCols} columns]` });
  console.log(`[SKILL] Dictionary Excel generated: ${totalSheets} sheets, ${(buffer.byteLength/1024).toFixed(0)} KB`);
}

// ── Generate Business Glossary (Excel) ────────────────────────
async function generateBusinessGlossary() {
  if (!KB) { appendMessage('assistant', '⚠️ No data loaded — cannot generate glossary.'); return; }
  const progressDiv = document.createElement('div');
  progressDiv.className = 'message assistant';
  progressDiv.innerHTML = '<div class="msg-label">Assistant</div><div class="msg-bubble" id="glossProgress">📘 <b>Generating Business Glossary Excel...</b><br></div>';
  chatArea.appendChild(progressDiv);
  chatArea.scrollTop = chatArea.scrollHeight;
  const progEl = document.getElementById('glossProgress');
  const log = (msg) => { progEl.innerHTML += msg + '<br>'; chatArea.scrollTop = chatArea.scrollHeight; };
  await new Promise(r => setTimeout(r, 50));

  // ── Collect terms ──────────────────────────────────────────
  log('Collecting terms from schemas and knowledge base...');
  const termList = []; // { term, hint }
  const seen = new Set();
  const addTerm = (term, hint) => { if (!term || seen.has(term)) return; seen.add(term); termList.push({ term, hint }); };

  // Item name map
  const itemMap = {};
  if (KB.workspaces) for (const ws of KB.workspaces)
    for (const item of (ws.items || []))
      itemMap[item.id] = { name: item.displayName || item.name || item.id, type: item.type };

  // Table names from schemas
  const colPatterns = new Map(); // suffix/prefix → count
  if (KB.schemas) for (const [itemId, val] of Object.entries(KB.schemas)) {
    if (!val) continue;
    const info = itemMap[itemId] || { name: itemId.slice(0, 12), type: 'Unknown' };
    const isWarehouse = !Array.isArray(val) && val.item_type;
    const tables = Array.isArray(val) ? val : (val.tables || []);
    for (const tbl of tables) {
      const tName = tbl.table_name || tbl.name || '';
      if (tName) addTerm(tName, `Table in ${info.name}`);
      // Track column patterns
      for (const c of (tbl.columns || [])) {
        const cn = (c.column_name || c.COLUMN_NAME || c.name || '').toLowerCase();
        for (const pat of ['_bk', '_sk', '_id', '_key', '_date', '_flag', '_code', '_name', '_desc', '_amt', '_cnt', '_pct']) {
          if (cn.endsWith(pat)) { colPatterns.set(pat, (colPatterns.get(pat) || 0) + 1); break; }
        }
        for (const pat of ['is_', 'has_', 'valid_', 'src_', 'stg_', 'dim_', 'fact_']) {
          if (cn.startsWith(pat)) { colPatterns.set(pat, (colPatterns.get(pat) || 0) + 1); break; }
        }
      }
    }
    if (isWarehouse && val.views) for (const vw of val.views)
      if (vw.name) addTerm(vw.name, `View in ${info.name}`);
    if (isWarehouse && val.procedures) for (const sp of val.procedures)
      if (sp.name) addTerm(sp.name, `Procedure in ${info.name}`);
  }

  // Add column pattern terms
  for (const [pat, cnt] of colPatterns) if (cnt >= 2) addTerm(pat, `Column pattern (${cnt} occurrences)`);

  // IFRS / business terms from KNOWLEDGE field_detail
  const ifrsTerms = new Set();
  const ifrsRe = /\b(IFRS\s*1[67]|UoA|BSP|DAC|UPR|IBNR|SCD[12]|Type\s*[12]|Slowly\s+Changing|surrogate\s+key|business\s+key|natural\s+key)\b/gi;
  const xformTypes = new Set();
  for (const chunk of KNOWLEDGE) {
    if (chunk.type === 'field_detail') {
      const bl = chunk.business_logic || '';
      let m; while ((m = ifrsRe.exec(bl)) !== null) ifrsTerms.add(m[1].trim());
      if (chunk.transformation_type) xformTypes.add(chunk.transformation_type);
      if (chunk.table) addTerm(chunk.table, `Table from knowledge base`);
    }
    if (chunk.type === 'warehouse_lineage' && chunk.target_table)
      addTerm(chunk.target_table, `Lineage target`);
  }
  for (const t of ifrsTerms) addTerm(t, 'IFRS / insurance term');
  for (const t of xformTypes) addTerm(t, 'Transformation type');

  // Architecture terms
  for (const t of ['Bronze', 'Silver Raw', 'Silver Staging', 'Gold', 'Platinum', 'Medallion Architecture', 'Lakehouse', 'Warehouse', 'Data Pipeline', 'ETL', 'ELT'])
    addTerm(t, 'Architecture concept');

  log(`Found <b>${termList.length}</b> terms. Asking Claude for categorized definitions...`);
  await new Promise(r => setTimeout(r, 10));

  // ── Call Claude for definitions (batched, max 100 terms) ───
  const capped = termList.slice(0, 100);
  const BATCH = 40;
  const glossaryEntries = []; // { term, category, definition, example }
  for (let i = 0; i < capped.length; i += BATCH) {
    const batch = capped.slice(i, i + BATCH);
    const prompt = batch.map(t => `- ${t.term} (${t.hint})`).join('\n');
    log(`Defining terms ${i + 1}–${Math.min(i + BATCH, capped.length)} of ${capped.length}...`);
    try {
      const resp = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: [{ role: 'user', content: `Generate a business glossary for an insurance data warehouse. Return ONLY a JSON array. Each item: {"term","category","definition","example"}. category must be one of: Business, Technical, IFRS, Architecture. Terms to define:\n\n${prompt}` }],
          system: 'You are a data warehouse documentation expert for an insurance company using Medallion architecture (Bronze/Silver/Gold/Platinum). Return valid JSON only — an array of objects. No markdown, no code fences.'
        })
      });
      const data = await resp.json();
      const raw = data.content ? data.content.map(c => c.text).join('') : '[]';
      const cleaned = raw.replace(/```json\s*/gi, '').replace(/```/g, '').trim();
      try {
        const parsed = JSON.parse(cleaned);
        for (const e of parsed) glossaryEntries.push({ term: e.term || '', category: e.category || 'Technical', definition: e.definition || '', example: e.example || '' });
      } catch (_) {
        for (const t of batch) glossaryEntries.push({ term: t.term, category: 'Technical', definition: '(definition unavailable)', example: '' });
      }
    } catch (_) {
      for (const t of batch) glossaryEntries.push({ term: t.term, category: 'Technical', definition: '(API error)', example: '' });
    }
    await new Promise(r => setTimeout(r, 10));
  }
  glossaryEntries.sort((a, b) => a.term.localeCompare(b.term));

  // ── Group by category ──────────────────────────────────────
  const CATS = ['Business', 'Technical', 'IFRS', 'Architecture'];
  const grouped = {};
  for (const cat of CATS) grouped[cat] = [];
  for (const e of glossaryEntries) {
    const cat = CATS.includes(e.category) ? e.category : 'Technical';
    grouped[cat].push(e);
  }

  // ── Build Excel workbook ───────────────────────────────────
  log('Building Excel workbook...');
  const wb = new ExcelJS.Workbook();
  wb.creator = 'GenDWH Knowledge Assistant';
  wb.created = new Date();
  const usedNames = new Set();
  const COL = 4;

  // ── TITLE PAGE ─────────────────────────────────────────────
  const titleWs = wb.addWorksheet(safeName('Title', usedNames));
  titleWs.mergeCells('A1:D1');
  const tc = titleWs.getCell('A1');
  tc.value = 'GenDWH Business Glossary';
  tc.font = { name: 'Calibri Light', size: 28, bold: true, color: { argb: WHITE } };
  tc.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
  tc.alignment = { vertical: 'middle', horizontal: 'center' };
  titleWs.getRow(1).height = 56;
  titleWs.getCell('A3').value = `Generated: ${new Date().toISOString().slice(0,10)}`;
  titleWs.getCell('A3').font = { name: 'Calibri', size: 11, italic: true, color: { argb: '666666' } };
  titleWs.getCell('A5').value = 'Summary';
  titleWs.getCell('A5').font = { name: 'Calibri Light', size: 14, bold: true, color: { argb: TEAL } };
  let tRow = 6;
  for (const cat of CATS) {
    titleWs.getCell(`A${tRow}`).value = `${cat} Terms`;
    titleWs.getCell(`A${tRow}`).font = { name: 'Calibri', size: 10 };
    titleWs.getCell(`C${tRow}`).value = grouped[cat].length;
    titleWs.getCell(`C${tRow}`).font = { name: 'Calibri', size: 10, bold: true };
    tRow++;
  }
  titleWs.getCell(`A${tRow}`).value = 'Total';
  titleWs.getCell(`A${tRow}`).font = { name: 'Calibri', size: 10, bold: true };
  titleWs.getCell(`C${tRow}`).value = glossaryEntries.length;
  titleWs.getCell(`C${tRow}`).font = { name: 'Calibri', size: 10, bold: true };
  titleWs.getCell(`A${tRow+2}`).value = 'InspirIT — GenDWH Knowledge Assistant';
  titleWs.getCell(`A${tRow+2}`).font = { name: 'Calibri Light', size: 10, italic: true, color: { argb: '999999' } };
  titleWs.columns = [{ width: 22 },{ width: 15 },{ width: 12 },{ width: 15 }];

  // ── INDEX ──────────────────────────────────────────────────
  const idxWs = wb.addWorksheet(safeName('INDEX', usedNames));
  styleTitleRow(idxWs, 'Business Glossary — Index', COL);
  addBackLink(idxWs, 'Title', 'Back to Title');
  const idxHdr = idxWs.getRow(4);
  ['#','Category','Terms','Navigate'].forEach((h,i) => { idxHdr.getCell(i+1).value = h; });
  styleHeader(idxHdr, COL);
  const catSheetNames = {};
  let iRow = 5;
  for (let ci = 0; ci < CATS.length; ci++) {
    const cat = CATS[ci];
    const shName = safeName(cat + ' Terms', usedNames);
    catSheetNames[cat] = shName;
    const r = idxWs.getRow(iRow);
    r.getCell(1).value = ci + 1;
    r.getCell(2).value = cat;
    r.getCell(3).value = grouped[cat].length;
    r.getCell(4).value = { text: '→ Sheet', hyperlink: `#'${shName}'!A1` };
    r.getCell(4).font = { name: 'Calibri', size: 10, color: { argb: TEAL }, underline: true };
    styleDataRow(r, COL, iRow - 5);
    iRow++;
  }
  idxWs.columns = [{ width: 5 },{ width: 22 },{ width: 10 },{ width: 14 }];
  idxWs.views = [{ state: 'frozen', ySplit: 4 }];

  // ── Category sheets ────────────────────────────────────────
  const catColors = { Business: LAYER_COLORS.Gold, Technical: LAYER_COLORS.Silver_Stg, IFRS: LAYER_COLORS.Platinum, Architecture: 'D6EAF8' };
  const GCOL = 4;
  for (const cat of CATS) {
    const entries = grouped[cat];
    log(`Creating ${cat} sheet (${entries.length} terms)...`);
    const ws = wb.addWorksheet(catSheetNames[cat]);
    styleTitleRow(ws, `${cat} Terms`, GCOL);
    addBackLink(ws, 'INDEX', 'Back to INDEX');
    const hdr = ws.getRow(4);
    ['#','Term','Definition','Example'].forEach((h,i) => { hdr.getCell(i+1).value = h; });
    styleHeader(hdr, GCOL);
    let row = 5;
    for (let ei = 0; ei < entries.length; ei++) {
      const e = entries[ei];
      const r = ws.getRow(row);
      r.getCell(1).value = ei + 1;
      r.getCell(2).value = e.term;
      r.getCell(2).font = { name: 'Calibri', size: 10, bold: true };
      r.getCell(3).value = e.definition;
      r.getCell(3).alignment = { wrapText: true, vertical: 'top' };
      r.getCell(4).value = e.example;
      r.getCell(4).font = { name: 'Calibri', size: 9, italic: true };
      r.getCell(4).alignment = { wrapText: true, vertical: 'top' };
      styleDataRow(r, GCOL, ei);
      row++;
    }
    if (entries.length === 0) {
      ws.getRow(row).getCell(1).value = '(No terms in this category)';
      ws.getRow(row).getCell(1).font = { name: 'Calibri', size: 10, italic: true, color: { argb: '999999' } };
    }
    ws.columns = [{ width: 5 },{ width: 35 },{ width: 60 },{ width: 40 }];
    ws.views = [{ state: 'frozen', ySplit: 4 }];
    await new Promise(r => setTimeout(r, 10));
  }

  // ── Generate & download ────────────────────────────────────
  log('Writing Excel file...');
  const buffer = await wb.xlsx.writeBuffer();
  const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  const url = URL.createObjectURL(blob);
  const totalSheets = wb.worksheets.length;

  progEl.innerHTML = `📘 <b>Business Glossary Excel ready!</b><br>
    ${glossaryEntries.length} terms across ${CATS.length} categories, ${totalSheets} sheets<br>
    <a href="${url}" download="GenDWH_Business_Glossary_${new Date().toISOString().slice(0,10)}.xlsx"
       style="display:inline-block;margin-top:8px;padding:8px 16px;background:#1C8D7A;color:white;border-radius:6px;text-decoration:none;font-weight:bold;">
       📥 Download Excel
    </a>`;
  chatArea.scrollTop = chatArea.scrollHeight;
  chatHistory.push({ role: 'assistant', content: `[Generated Business Glossary: ${glossaryEntries.length} terms, ${totalSheets} sheets]` });
  console.log(`[SKILL] Business Glossary Excel generated: ${glossaryEntries.length} terms, ${totalSheets} sheets, ${(buffer.byteLength/1024).toFixed(0)} KB`);
}

// ── Generate Custom Report (Excel) ───────────────────────────
async function generateCustomReportExcel(query) {
  if (!KB) { appendMessage('assistant', '⚠️ No data loaded — cannot generate report.'); return; }
  const progressDiv = document.createElement('div');
  progressDiv.className = 'message assistant';
  progressDiv.innerHTML = '<div class="msg-label">Assistant</div><div class="msg-bubble" id="reportProgress">📋 <b>Generating Custom Report Excel...</b><br></div>';
  chatArea.appendChild(progressDiv);
  chatArea.scrollTop = chatArea.scrollHeight;
  const progEl = document.getElementById('reportProgress');
  const log = (msg) => { progEl.innerHTML += msg + '<br>'; chatArea.scrollTop = chatArea.scrollHeight; };
  await new Promise(r => setTimeout(r, 50));

  // ── Build context for Claude ───────────────────────────────
  log('Gathering context from knowledge base...');
  const context = analysisReady ? retrieveContext(query) : '';

  // ── Ask Claude for structured report data ──────────────────
  log('Asking Claude to generate report data...');
  let reportTitle = 'Custom Report';
  let headers = [];
  let rows = [];
  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [{ role: 'user', content: `${query}\n\nContext from knowledge base:\n${context}` }],
        system: `You are a data warehouse documentation expert for an insurance company (Generali) using Medallion architecture (Bronze/Silver/Gold/Platinum) in Microsoft Fabric.
The user wants a custom report exported to Excel. Analyze the request and return ONLY valid JSON with this structure:
{"title": "Report Title", "headers": ["Col1", "Col2", ...], "rows": [["val1", "val2", ...], ...]}
Rules:
- title: a short descriptive title for the report (max 60 chars)
- headers: column names relevant to the query
- rows: data rows with values matching the headers
- Include as much relevant data as you can find in the context
- If the context has tables, fields, pipelines, lineage info — use it
- No markdown, no code fences, just the JSON object`
      })
    });
    const data = await resp.json();
    const raw = data.content ? data.content.map(c => c.text).join('') : '{}';
    const cleaned = raw.replace(/```json\s*/gi, '').replace(/```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    reportTitle = parsed.title || reportTitle;
    headers = Array.isArray(parsed.headers) ? parsed.headers : [];
    rows = Array.isArray(parsed.rows) ? parsed.rows : [];
  } catch (err) {
    log(`⚠️ Claude API error: ${err.message}. Generating fallback report...`);
    headers = ['Info'];
    rows = [['Report generation failed — please try again or rephrase your query.']];
  }

  if (headers.length === 0) {
    headers = ['Info'];
    rows = [['No data returned for this query.']];
  }
  log(`Report: <b>${reportTitle}</b> — ${headers.length} columns, ${rows.length} rows`);

  // ── Build Excel workbook ───────────────────────────────────
  log('Building Excel workbook...');
  const wb = new ExcelJS.Workbook();
  wb.creator = 'GenDWH Knowledge Assistant';
  wb.created = new Date();
  const usedNames = new Set();
  const COL = headers.length;

  const ws = wb.addWorksheet(safeName('Report', usedNames));

  // Title row (merged)
  ws.mergeCells(1, 1, 1, COL);
  const tc = ws.getCell('A1');
  tc.value = reportTitle;
  tc.font = { name: 'Calibri Light', size: 16, bold: true, color: { argb: WHITE } };
  tc.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: TEAL } };
  tc.alignment = { vertical: 'middle', horizontal: 'center' };
  ws.getRow(1).height = 36;

  // Metadata row
  ws.getCell('A2').value = `Generated: ${new Date().toISOString().slice(0,10)} | Query: ${query.slice(0, 120)}`;
  ws.getCell('A2').font = { name: 'Calibri', size: 9, italic: true, color: { argb: '666666' } };

  // Header row (row 4)
  const hdr = ws.getRow(4);
  headers.forEach((h, i) => { hdr.getCell(i + 1).value = h; });
  styleHeader(hdr, COL);

  // Data rows
  for (let ri = 0; ri < rows.length; ri++) {
    const r = ws.getRow(5 + ri);
    const rowData = rows[ri];
    for (let ci = 0; ci < COL; ci++) {
      r.getCell(ci + 1).value = Array.isArray(rowData) ? (rowData[ci] ?? '') : '';
    }
    styleDataRow(r, COL, ri);
  }

  // Auto-size columns (estimate)
  ws.columns = headers.map((h, i) => {
    let maxLen = h.length;
    for (const row of rows) {
      const val = Array.isArray(row) ? String(row[i] || '') : '';
      if (val.length > maxLen) maxLen = val.length;
    }
    return { width: Math.min(Math.max(maxLen + 4, 12), 60) };
  });
  ws.views = [{ state: 'frozen', ySplit: 4 }];

  // Footer
  const footerRow = 5 + rows.length + 1;
  ws.getCell(`A${footerRow}`).value = 'InspirIT — GenDWH Knowledge Assistant';
  ws.getCell(`A${footerRow}`).font = { name: 'Calibri Light', size: 10, italic: true, color: { argb: '999999' } };

  // ── Generate & download ────────────────────────────────────
  log('Writing Excel file...');
  const buffer = await wb.xlsx.writeBuffer();
  const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
  const url = URL.createObjectURL(blob);

  const safeTitle = reportTitle.replace(/[^a-zA-Z0-9_\- ]/g, '').replace(/\s+/g, '_').slice(0, 40);
  progEl.innerHTML = `📋 <b>Custom Report Excel ready!</b><br>
    <b>${reportTitle}</b> — ${rows.length} rows, ${headers.length} columns<br>
    <a href="${url}" download="GenDWH_Report_${safeTitle}_${new Date().toISOString().slice(0,10)}.xlsx"
       style="display:inline-block;margin-top:8px;padding:8px 16px;background:#1C8D7A;color:white;border-radius:6px;text-decoration:none;font-weight:bold;">
       📥 Download Excel
    </a>`;
  chatArea.scrollTop = chatArea.scrollHeight;
  chatHistory.push({ role: 'assistant', content: `[Generated Custom Report: ${reportTitle} — ${rows.length} rows]` });
  console.log(`[SKILL] Custom Report Excel generated: ${rows.length} rows, ${headers.length} cols, ${(buffer.byteLength/1024).toFixed(0)} KB`);
}

// ── Pipeline Visualization (Vis.js) ───────────────────────────
const ACTIVITY_COLORS = {
  TridentNotebook: '#1C8D7A', Copy: '#0B3052', IfCondition: '#E8A838',
  ForEach: '#6B4E9B', Office365Email: '#C0392B', Email: '#C0392B',
  ExecutePipeline: '#2E86C1', SetVariable: '#7F8C8D', Wait: '#7F8C8D',
  Fail: '#C0392B', Switch: '#E8A838', Lookup: '#2E86C1',
  WebActivity: '#2E86C1', Script: '#0B3052', _default: '#95A5A6'
};
const ACTIVITY_SHAPES = {
  IfCondition: 'diamond', ForEach: 'box', Switch: 'diamond',
  Fail: 'triangleDown', _default: 'box'
};

function findPipelineByName(name) {
  if (!KB || !KB.workspaces) return null;
  const q = name.toLowerCase();
  let best = null;
  for (const ws of KB.workspaces) {
    for (const item of (ws.items || [])) {
      if (item.type !== 'DataPipeline') continue;
      const dn = (item.displayName || item.name || '').toLowerCase();
      if (dn === q) return item; // exact match
      if (dn.includes(q) || q.includes(dn)) best = best || item;
    }
  }
  return best;
}

function listPipelineNames() {
  if (!KB || !KB.workspaces) return [];
  const names = [];
  for (const ws of KB.workspaces)
    for (const item of (ws.items || []))
      if (item.type === 'DataPipeline') names.push(item.displayName || item.name || item.id);
  return names;
}

function parsePipelineActivities(item) {
  const raw = getPipelineContent(item.id);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    // Could be { properties: { activities: [] } } or { activities: [] } or just []
    if (Array.isArray(parsed)) return parsed;
    if (parsed.properties && Array.isArray(parsed.properties.activities)) return parsed.properties.activities;
    if (Array.isArray(parsed.activities)) return parsed.activities;
    return [];
  } catch { return []; }
}

function buildVisGraph(activities) {
  const nodes = []; const edges = [];
  activities.forEach((act, i) => {
    const color = ACTIVITY_COLORS[act.type] || ACTIVITY_COLORS._default;
    const shape = ACTIVITY_SHAPES[act.type] || ACTIVITY_SHAPES._default;
    nodes.push({
      id: act.name, label: act.name, color: { background: color, border: color, highlight: { background: color, border: '#000' } },
      font: { color: '#fff', size: 13, face: 'Segoe UI' }, shape, margin: 10,
      title: act.type, _act: act
    });
    if (act.dependsOn) for (const dep of act.dependsOn) {
      const cond = (dep.dependencyConditions || ['Succeeded'])[0];
      const edgeColor = cond === 'Failed' ? '#C0392B' : cond === 'Skipped' ? '#95A5A6' : '#1C8D7A';
      edges.push({ from: dep.activity, to: act.name, arrows: 'to', color: { color: edgeColor }, label: cond !== 'Succeeded' ? cond : '', font: { size: 10, color: edgeColor } });
    }
    // Nest inner activities for ForEach / IfCondition
    const inner = act.typeProperties?.activities || act.typeProperties?.ifTrueActivities || [];
    const innerFalse = act.typeProperties?.ifFalseActivities || [];
    [...inner, ...innerFalse].forEach(sub => {
      const sc = ACTIVITY_COLORS[sub.type] || ACTIVITY_COLORS._default;
      const ss = ACTIVITY_SHAPES[sub.type] || ACTIVITY_SHAPES._default;
      const subId = `${act.name}/${sub.name}`;
      nodes.push({ id: subId, label: sub.name, color: { background: sc, border: sc, highlight: { background: sc, border: '#000' } },
        font: { color: '#fff', size: 12, face: 'Segoe UI' }, shape: ss, margin: 8, title: sub.type, _act: sub,
        borderWidth: 2, shapeProperties: { borderDashes: [4, 4] }
      });
      edges.push({ from: act.name, to: subId, arrows: 'to', dashes: true, color: { color: '#aaa' }, label: innerFalse.includes(sub) ? 'false' : '' });
    });
  });
  return { nodes, edges };
}

async function ensureVisLoaded() {
  if (window.vis && window.vis.Network) return;
  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/vis-network@9.1.2/standalone/umd/vis-network.min.js';
    script.onload = () => {
      if (window.vis && window.vis.Network) resolve();
      else reject(new Error('Vis.js loaded but vis.Network is not available'));
    };
    script.onerror = () => reject(new Error('Failed to load Vis.js from CDN'));
    document.head.appendChild(script);
  });
}

async function visualizePipeline(userText) {
  await ensureVisLoaded();
  // Extract pipeline name from user text
  const cleaned = userText.replace(/визуализирай\s+pipeline|visualize\s+pipeline|покажи\s+pipeline|нарисувай\s+pipeline|pipeline\s+diagram|pipeline\s+graph|^\/pipeline\s*/gi, '').trim();
  const pipelineName = cleaned || '';

  if (!KB) { appendMessage('assistant', '⚠️ Knowledge base not loaded yet.'); return; }

  const item = pipelineName ? findPipelineByName(pipelineName) : null;
  if (!item) {
    const available = listPipelineNames();
    if (available.length === 0) {
      appendMessage('assistant', '⚠️ No DataPipeline items found in the knowledge base.');
    } else {
      appendMessage('assistant', `⚠️ Pipeline "${pipelineName || '(not specified)'}" not found.\n\n**Available pipelines:**\n${available.map(n => '- `' + n + '`').join('\n')}\n\nTry: \`визуализирай pipeline ${available[0]}\``);
    }
    return;
  }

  const activities = parsePipelineActivities(item);
  if (activities.length === 0) {
    appendMessage('assistant', `⚠️ Pipeline **${item.displayName}** has no activities or its definition is not available in the knowledge base.`);
    return;
  }

  const displayName = item.displayName || item.name;
  const { nodes, edges } = buildVisGraph(activities);

  // Create the message with graph container
  const msgDiv = document.createElement('div');
  msgDiv.className = 'message assistant';
  const label = document.createElement('div');
  label.className = 'msg-label';
  label.textContent = 'Assistant';
  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble';

  const wrap = document.createElement('div');
  wrap.className = 'pipeline-graph-wrap';

  const toolbar = document.createElement('div');
  toolbar.className = 'pipeline-graph-toolbar';
  toolbar.innerHTML = `<span>📊 ${displayName} — ${activities.length} activities</span>`;
  const btnGroup = document.createElement('span');
  btnGroup.style.cssText = 'display:flex;gap:6px;';

  const autoLayoutBtn = document.createElement('button');
  autoLayoutBtn.textContent = '⚡ Auto-layout';
  btnGroup.appendChild(autoLayoutBtn);

  const fullscreenBtn = document.createElement('button');
  fullscreenBtn.textContent = '⛶ Fullscreen';
  btnGroup.appendChild(fullscreenBtn);

  const collapseBtn = document.createElement('button');
  collapseBtn.textContent = '▼ Collapse';
  btnGroup.appendChild(collapseBtn);

  toolbar.appendChild(btnGroup);

  const container = document.createElement('div');
  container.className = 'pipeline-graph-container';
  const graphId = 'visGraph_' + Date.now();
  container.id = graphId;

  wrap.appendChild(toolbar);
  wrap.appendChild(container);
  bubble.appendChild(wrap);

  // Legend
  const legend = document.createElement('div');
  legend.style.cssText = 'margin-top:8px;font-size:11px;color:var(--text-muted);line-height:1.8;';
  const legendTypes = [...new Set(activities.map(a => a.type))];
  legend.innerHTML = legendTypes.map(t => {
    const c = ACTIVITY_COLORS[t] || ACTIVITY_COLORS._default;
    return `<span style="display:inline-block;width:12px;height:12px;border-radius:3px;background:${c};vertical-align:middle;margin-right:3px;"></span>${t}`;
  }).join(' &nbsp; ');
  bubble.appendChild(legend);

  msgDiv.appendChild(label);
  msgDiv.appendChild(bubble);
  chatArea.appendChild(msgDiv);
  chatArea.scrollTop = chatArea.scrollHeight;

  // Render Vis.js network
  const network = new vis.Network(container, {
    nodes: new vis.DataSet(nodes),
    edges: new vis.DataSet(edges)
  }, {
    layout: { hierarchical: { direction: 'LR', sortMethod: 'directed', levelSeparation: 180, nodeSpacing: 100 } },
    physics: false,
    interaction: { hover: true, zoomView: true, dragView: true, dragNodes: true },
    edges: { smooth: { type: 'cubicBezier' } }
  });

  // Collapse / expand toggle
  let collapsed = false;
  collapseBtn.addEventListener('click', () => {
    collapsed = !collapsed;
    container.style.display = collapsed ? 'none' : 'block';
    collapseBtn.textContent = collapsed ? '▶ Expand' : '▼ Collapse';
  });

  // Fullscreen toggle
  let isFullscreen = false;
  const exitFullscreen = () => {
    if (!isFullscreen) return;
    isFullscreen = false;
    wrap.classList.remove('fullscreen');
    fullscreenBtn.textContent = '⛶ Fullscreen';
    network.redraw(); network.fit();
  };
  fullscreenBtn.addEventListener('click', () => {
    isFullscreen = !isFullscreen;
    wrap.classList.toggle('fullscreen', isFullscreen);
    fullscreenBtn.textContent = isFullscreen ? '✕ Exit' : '⛶ Fullscreen';
    network.redraw(); network.fit();
  });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') exitFullscreen(); });

  // Auto-layout (physics toggle)
  let physicsOn = false;
  autoLayoutBtn.addEventListener('click', () => {
    physicsOn = !physicsOn;
    network.setOptions({ physics: { enabled: physicsOn } });
    autoLayoutBtn.textContent = physicsOn ? '⏹ Stop layout' : '⚡ Auto-layout';
  });

  // Node click → popup with activity details
  network.on('click', params => {
    document.querySelectorAll('.pipeline-node-popup').forEach(p => p.remove());
    if (!params.nodes.length) return;
    const nodeId = params.nodes[0];
    const node = nodes.find(n => n.id === nodeId);
    if (!node || !node._act) return;
    const act = node._act;
    const popup = document.createElement('div');
    popup.className = 'pipeline-node-popup';
    const tp = act.typeProperties || {};
    let details = `<span class="close-popup">✕</span><h4>${act.name}</h4><p><b>Type:</b> ${act.type}</p>`;
    if (act.policy) details += `<p><b>Timeout:</b> ${act.policy.timeout || '—'} | <b>Retry:</b> ${act.policy.retry || 0}×</p>`;
    if (Object.keys(tp).length) details += `<pre>${JSON.stringify(tp, null, 2).slice(0, 600)}</pre>`;
    if (act.dependsOn && act.dependsOn.length) details += `<p><b>Depends on:</b> ${act.dependsOn.map(d => d.activity).join(', ')}</p>`;
    popup.innerHTML = details;
    popup.style.left = (params.event.center.x + container.offsetLeft + 10) + 'px';
    popup.style.top = (params.event.center.y + container.offsetTop - 20) + 'px';
    wrap.appendChild(popup);
    popup.querySelector('.close-popup').addEventListener('click', () => popup.remove());
  });

  chatHistory.push({ role: 'assistant', content: `[Pipeline visualization: ${displayName} — ${activities.length} activities]` });
  console.log(`[SKILL] Pipeline visualization rendered: ${displayName}, ${nodes.length} nodes, ${edges.length} edges`);
}

// ── Skill detection ────────────────────────────────────────────
function detectSkill(text) {
  const t = text.trim().toLowerCase();
  if (/^\/lineage\b/.test(t) || /генерирай\s+lineage\s+excel|generate\s+lineage|data\s+lineage\s+excel|линеадж\s+excel|lineage\s+excel/i.test(t))
    return 'lineage_excel';
  if (/^\/dictionary\b/.test(t) || /генерирай\s+data\s+dictionary|generate\s+dictionary|data\s+dictionary\s+excel/i.test(t))
    return 'data_dictionary';
  if (/^\/glossary\b/.test(t) || /генерирай\s+glossary|generate\s+glossary|business\s+glossary|бизнес\s+речник/i.test(t))
    return 'business_glossary';
  if (/^\/report\b/.test(t) || /направи\s+(ми\s+)?справка|custom\s+report|генерирай\s+справка|generate\s+report\s+excel|направи\s+report/i.test(t))
    return 'custom_report';
  if (/визуализирай\s+pipeline|visualize\s+pipeline|покажи\s+pipeline|нарисувай\s+pipeline|pipeline\s+diagram|pipeline\s+graph|^\/pipeline\b/i.test(t))
    return 'pipeline_viz';
  return null;
}

// ── Chat logic ─────────────────────────────────────────────────
async function sendMessage(text) {
  if (!text.trim()) return;

  // Remove welcome
  const welcome = chatArea.querySelector('.welcome');
  if (welcome) welcome.remove();

  // Add user bubble
  appendMessage('user', text);
  chatHistory.push({ role: 'user', content: text });

  // Trim history
  if (chatHistory.length > MAX_HISTORY) chatHistory = chatHistory.slice(-MAX_HISTORY);

  // ── Skill detection — intercept before sending to Claude ────
  const skill = detectSkill(text);
  if (skill) {
    console.log(`[SKILL] Detected: ${skill}`);
    try {
      if (skill === 'lineage_excel') {
        await generateLineageExcel();
      } else if (skill === 'data_dictionary') {
        await generateDictionaryExcel();
      } else if (skill === 'business_glossary') {
        await generateBusinessGlossary();
      } else if (skill === 'custom_report') {
        await generateCustomReportExcel(text);
      } else if (skill === 'pipeline_viz') {
        await visualizePipeline(text);
      }
    } catch (err) {
      appendMessage('assistant', '⚠️ Skill error: ' + (err && err.message ? err.message : String(err)));
      console.error('[SKILL] Error:', err);
    }
    return; // Don't send to Claude
  }

  // Show spinner
  const spinner = document.createElement('div');
  spinner.className = 'spinner-wrap';
  spinner.innerHTML = '<div class="spinner"></div> Thinking…';
  chatArea.appendChild(spinner);
  chatArea.scrollTop = chatArea.scrollHeight;

  // RAG context
  const context = await retrieveContext(text);
  let systemWithContext = SYSTEM_PROMPT;
  if (window._lastChainQuery) {
    systemWithContext += '\n\nIMPORTANT: The context contains a multi-layer lineage chain from Platinum (Warehouse) to Bronze (Landing). Trace and explain each step of the transformation pipeline, layer by layer. Show how the field flows through each layer with specific column names and transformations.';
  }
  systemWithContext += '\n\nCONTEXT:\n' + context;

  try {
    sendBtn.disabled = true;
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: chatHistory, system: systemWithContext })
    });

    const data = await resp.json();
    spinner.remove();

    if (data.error) {
      appendMessage('assistant', '⚠️ Error: ' + (data.error.message || data.error));
    } else {
      const reply = data.content ? data.content.map(c => c.text).join('') : 'No response';
      appendMessage('assistant', reply);
      chatHistory.push({ role: 'assistant', content: reply });
    }
  } catch (err) {
    spinner.remove();
    appendMessage('assistant', '⚠️ Failed to reach the API. Is the backend running?');
  } finally {
    sendBtn.disabled = false;
    userInput.focus();
  }
}

function appendMessage(role, content) {
  const div = document.createElement('div');
  div.className = `message ${role}`;
  const label = document.createElement('div');
  label.className = 'msg-label';
  label.textContent = role === 'user' ? 'You' : 'Assistant';
  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble';
  if (role === 'assistant') {
    bubble.innerHTML = marked.parse(content);
    // Render Mermaid diagrams: find <code class="language-mermaid"> blocks
    bubble.querySelectorAll('pre code.language-mermaid').forEach(codeEl => {
      const src = codeEl.textContent;
      const container = document.createElement('div');
      container.className = 'mermaid-container';
      const mermaidDiv = document.createElement('div');
      mermaidDiv.className = 'mermaid';
      mermaidDiv.textContent = src;
      container.appendChild(mermaidDiv);
      codeEl.closest('pre').replaceWith(container);
    });
    // Ask Mermaid to render the new nodes
    try { mermaid.run({ nodes: bubble.querySelectorAll('.mermaid') }); }
    catch (e) { console.warn('[Mermaid] render error:', e); }
  } else {
    bubble.textContent = content;
  }
  div.appendChild(label);
  div.appendChild(bubble);
  chatArea.appendChild(div);
  chatArea.scrollTop = chatArea.scrollHeight;
}

// ── Event listeners ────────────────────────────────────────────
sendBtn.addEventListener('click', () => { sendMessage(userInput.value); userInput.value = ''; userInput.style.height = 'auto'; });

userInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendBtn.click(); }
});

// Auto-resize textarea
userInput.addEventListener('input', () => {
  userInput.style.height = 'auto';
  userInput.style.height = Math.min(userInput.scrollHeight, 120) + 'px';
});

// Quick question buttons
document.querySelectorAll('.quick-btn').forEach(btn => {
  btn.addEventListener('click', () => { userInput.value = btn.dataset.q; sendBtn.click(); });
});

// Mobile sidebar toggle
menuToggle.addEventListener('click', () => sidebar.classList.toggle('open'));
chatArea.addEventListener('click', () => sidebar.classList.remove('open'));

// ── Prompts Drawer (push pattern) ───────────────────────────────
const promptsDrawer = document.getElementById('promptsDrawer');
const promptsDrawerBtn = document.getElementById('promptsDrawerBtn');
const drawerClose = document.getElementById('drawerClose');

function toggleDrawer(open) {
  const isOpen = open !== undefined ? open : !promptsDrawer.classList.contains('open');
  promptsDrawer.classList.toggle('open', isOpen);
}
promptsDrawerBtn.addEventListener('click', () => toggleDrawer(true));
drawerClose.addEventListener('click', () => toggleDrawer(false));

// Accordion — one section open at a time
document.querySelectorAll('.acc-header').forEach(header => {
  header.addEventListener('click', () => {
    const key = header.dataset.acc;
    const body = document.querySelector(`.acc-body[data-acc-body="${key}"]`);
    const isOpen = header.classList.contains('open');
    // Close all
    document.querySelectorAll('.acc-header').forEach(h => h.classList.remove('open'));
    document.querySelectorAll('.acc-body').forEach(b => b.classList.remove('open'));
    // Toggle clicked
    if (!isOpen) {
      header.classList.add('open');
      body.classList.add('open');
    }
  });
});

// "→ Изпрати" buttons — insert prompt into chat input
document.querySelectorAll('.prompt-send').forEach(btn => {
  btn.addEventListener('click', () => {
    const text = btn.parentElement.querySelector('.drawer-prompt-text').textContent;
    userInput.value = text;
    userInput.focus();
    userInput.style.height = 'auto';
    userInput.style.height = Math.min(userInput.scrollHeight, 120) + 'px';
    toggleDrawer(false);
  });
});

// ── Init ───────────────────────────────────────────────────────
loadData();
