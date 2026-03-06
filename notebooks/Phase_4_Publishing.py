# Phase 4 — Publishing
# =====================
# Publishes documentation artefacts (HTML, JSONL) to the Lakehouse.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 4 ───
catalogue = phase_0_discovery()
catalogue = phase_1_profiling(catalogue)
catalogue = phase_2_ai_documentation(catalogue)
catalogue = phase_3_relationship_mapping(catalogue)
phase_4_publishing(catalogue)
log("Phase 4 complete — artefacts published")

