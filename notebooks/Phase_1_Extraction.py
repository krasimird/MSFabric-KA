# Phase 1 — Metadata Extraction
# ===============================
# Extracts detailed metadata from all discovered Fabric items:
# pipelines, lakehouses, warehouses, notebooks, semantic models,
# reports, variable libraries, SQL queries, and bronze source tables.
# Writes to Delta tables: doc_pipeline_activities, doc_lakehouse_tables,
# doc_metadata_queries, doc_bronze_metadata, doc_warehouse_objects,
# doc_notebook_definitions, doc_semantic_models, doc_report_definitions,
# doc_variable_library.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 1 ───
meta = phase_1_metadata_extraction()

print(f"\n📋 Phase 1 result:")
print(f"   Pipeline activities:  {len(meta['activities'])}")
print(f"   Lakehouse tables:     {len(meta['tables'])}")
print(f"   SQL queries:          {len(meta['queries'])}")
print(f"   Bronze sources:       {len(meta['bronze_meta'])}")
print(f"   Warehouse objects:    {len(meta['warehouse_objects'])}")
print(f"   Notebooks:            {len(meta['notebooks'])}")
print(f"   Semantic model items: {len(meta['semantic_models'])}")
print(f"   Reports:              {len(meta['reports'])}")
print(f"   Variable library:     {len(meta['variable_library'])}")

