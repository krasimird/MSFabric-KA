# Phase 5 — Versioning
# =====================
# Snapshots the current documentation version for change tracking.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 5 ───
catalogue = phase_0_discovery()
catalogue = phase_1_profiling(catalogue)
catalogue = phase_2_ai_documentation(catalogue)
catalogue = phase_3_relationship_mapping(catalogue)
phase_4_publishing(catalogue)
phase_5_versioning(catalogue)
log("Phase 5 complete — version snapshot saved")

