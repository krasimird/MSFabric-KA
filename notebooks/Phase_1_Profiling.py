# Phase 1 — Profiling
# ====================
# Profiles each table/view — row counts, column stats, sample data.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 1 ───
catalogue = phase_0_discovery()
result = phase_1_profiling(catalogue)
log("Phase 1 complete — profiling finished")

