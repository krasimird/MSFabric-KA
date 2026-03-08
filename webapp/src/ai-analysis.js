/**
 * AIAnalysis — Processes SQL queries through Claude for field-level lineage.
 * Uses KBCache for persistence. Reports progress via callbacks.
 */
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

Return ONLY valid JSON array. No markdown, no explanation. Example:
[
  {"target_field":"policy_id","data_type":"int","source_table":"landing_dbo_polici","source_column":"POL_ID","transformation_type":"direct_map","expression":"p.POL_ID","business_logic":"Direct mapping of policy identifier","join_key":null},
  {"target_field":"premium_bgn","data_type":"decimal","source_table":"landing_dbo_premii","source_column":"SUMA","transformation_type":"cast","expression":"CAST(pr.SUMA AS DECIMAL(18,2))","business_logic":"Premium amount cast to decimal","join_key":"POL_ID"}
]`;

class AIAnalysis {
  constructor(cache, apiEndpoint = '/api/chat') {
    this.cache = cache;
    this.apiEndpoint = apiEndpoint;
    this.onProgress = null; // callback(current, total, tableName)
    this.aborted = false;
  }

  /** Main entry: analyze all SQL queries from KB */
  async analyzeAll(KB) {
    const queries = (KB.metadata && KB.metadata.queries) || [];
    if (queries.length === 0) { console.warn('No queries to analyze'); return; }

    const fp = KBCache.fingerprint(KB);
    const already = await this.cache.isAnalyzed(fp);
    if (already) {
      console.log('Analysis already cached for fingerprint:', fp);
      if (this.onProgress) this.onProgress(queries.length, queries.length, '(cached)');
      return;
    }

    // Clear old data and mark new fingerprint
    await this.cache.clearAll();
    await this.cache.setFingerprint(fp, false);

    let completed = 0;
    const BATCH_SIZE = 3; // concurrent requests

    for (let i = 0; i < queries.length; i += BATCH_SIZE) {
      if (this.aborted) break;
      const batch = queries.slice(i, i + BATCH_SIZE);
      const promises = batch.map(q => this._analyzeQuery(q).catch(err => {
        console.error(`Analysis failed for ${q.target_table}:`, err);
        return null;
      }));
      await Promise.all(promises);
      completed += batch.length;
      if (this.onProgress) this.onProgress(Math.min(completed, queries.length), queries.length, batch[batch.length - 1]?.target_table || '');
      // Small delay between batches to avoid rate limiting
      if (i + BATCH_SIZE < queries.length) await this._sleep(500);
    }

    // Build table summaries from collected lineage
    await this._buildTableSummaries(KB);
    // Build execution chains
    await this._buildExecutionChains(KB);

    await this.cache.setFingerprint(fp, true);
    console.log('Analysis complete. Fingerprint:', fp);
  }

  async _analyzeQuery(query) {
    const sql = query.source_query || '';
    if (!sql || sql.length < 20) return;

    const targetTable = query.target_table || query.meta_table || 'unknown';
    const layer = query.layer || '';
    const mode = query.mode || '';

    const userMsg = `Analyze this ${layer} ${mode} SQL query for table "${targetTable}":\n\n${sql.slice(0, 15000)}`;

    const resp = await fetch(this.apiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [{ role: 'user', content: userMsg }],
        system: LINEAGE_SYSTEM_PROMPT,
        max_tokens: 8192
      })
    });

    if (!resp.ok) throw new Error(`API ${resp.status}`);
    const data = await resp.json();
    if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));

    const text = data.content ? data.content.map(c => c.text).join('') : '';
    const fields = this._parseLineageJSON(text);
    if (!fields || fields.length === 0) return;

    // Store each field as a lineage record
    for (const f of fields) {
      const id = `${targetTable}::${f.target_field}`;
      await this.cache.put('field_lineage', {
        id,
        target_table: targetTable,
        layer,
        mode,
        ...f,
        analyzed_at: new Date().toISOString()
      });
    }
  }

  _parseLineageJSON(text) {
    try {
      // Try to find JSON array in the response
      const start = text.indexOf('[');
      const end = text.lastIndexOf(']');
      if (start === -1 || end === -1) return null;
      return JSON.parse(text.slice(start, end + 1));
    } catch (e) {
      console.warn('Failed to parse lineage JSON:', e.message, text.slice(0, 200));
      return null;
    }
  }

  async _buildTableSummaries(KB) {
    const allLineage = await this.cache.getAll('field_lineage');
    const byTable = {};
    for (const r of allLineage) {
      if (!byTable[r.target_table]) byTable[r.target_table] = [];
      byTable[r.target_table].push(r);
    }
    for (const [table, fields] of Object.entries(byTable)) {
      const sourceTables = [...new Set(fields.map(f => f.source_table).filter(Boolean))];
      const transformTypes = [...new Set(fields.map(f => f.transformation_type).filter(Boolean))];
      await this.cache.put('table_summary', {
        id: table,
        layer: fields[0]?.layer || '',
        mode: fields[0]?.mode || '',
        field_count: fields.length,
        source_tables: sourceTables,
        transformation_types: transformTypes,
        fields: fields.map(f => f.target_field)
      });
    }
  }

  /** Build execution chains: Pipeline → Notebook → Query → Table */
  async _buildExecutionChains(KB) {
    if (!KB.workspaces) return;
    let chainId = 0;
    for (const ws of KB.workspaces) {
      for (const item of (ws.items || [])) {
        if (item.type !== 'DataPipeline' || !item.definition) continue;
        const def = typeof item.definition === 'string' ? item.definition : JSON.stringify(item.definition);
        // Find notebook references in pipeline activities
        const nbMatches = def.match(/Phase_\d+_\w+|GenDWH_\w+/g) || [];
        const notebooks = [...new Set(nbMatches)];
        // Find which tables this pipeline chain affects
        const queries = (KB.metadata && KB.metadata.queries) || [];
        const affectedTables = queries
          .filter(q => notebooks.some(nb => (q.source_query || '').includes(nb) || (q.target_table || '').includes('stg_') || (q.target_table || '').includes('dim_') || (q.target_table || '').includes('fact_')))
          .map(q => q.target_table)
          .filter(Boolean);

        await this.cache.put('execution_chain', {
          id: `chain_${chainId++}`,
          pipeline: item.name,
          workspace: ws.name,
          notebooks,
          target_tables: [...new Set(affectedTables)].slice(0, 50),
          activity_count: (def.match(/activity/gi) || []).length
        });
      }
    }
  }

  _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
  abort() { this.aborted = true; }
}

