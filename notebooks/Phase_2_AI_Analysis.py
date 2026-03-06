# Phase 2 — AI Analysis
# =======================
# Sends SQL queries to Claude for field-level lineage analysis.
# Reads from doc_metadata_queries (Phase 1 output).
# Writes to Delta table: doc_ai_lineage
# Caches results in ai_lineage_cache.json to avoid re-analyzing unchanged queries.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 2 ───
# Set force_rerun=True to re-analyze all queries even if cached
ai_results = phase_2_ai_analysis(force_rerun=False)

total_tables = len(ai_results)
total_fields = sum(len(v.get("fields", [])) for v in ai_results.values())
print(f"\n📋 Phase 2 result:")
print(f"   Tables analyzed: {total_tables}")
print(f"   Total fields:    {total_fields}")

