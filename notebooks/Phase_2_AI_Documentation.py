# Phase 2 — AI Documentation
# ============================
# Uses Claude to generate natural-language documentation for each object.

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 2 ───
catalogue = phase_0_discovery()
catalogue = phase_1_profiling(catalogue)
result = phase_2_ai_documentation(catalogue)
log("Phase 2 complete — AI documentation generated")

