# Phase 1 — Metadata Extraction
# ===============================
# Extracts detailed metadata from all discovered Fabric items:
# pipelines, lakehouses, SQL queries, and bronze source tables.
# Writes to Delta tables: doc_pipeline_activities, doc_lakehouse_tables,
# doc_metadata_queries, doc_bronze_metadata.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 1 ───
meta = phase_1_metadata_extraction()

print(f"\n📋 Phase 1 result:")
print(f"   Pipeline activities: {len(meta['activities'])}")
print(f"   Lakehouse tables:    {len(meta['tables'])}")
print(f"   SQL queries:         {len(meta['queries'])}")
print(f"   Bronze sources:      {len(meta['bronze_meta'])}")

