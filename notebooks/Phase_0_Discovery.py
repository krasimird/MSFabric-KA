# Phase 0 — Discovery
# ====================
# Discovers warehouse objects (schemas, tables, views, procedures).

# CELL 0 ── Load Library ───
# %run is a Fabric magic that executes the library notebook in this context.
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 0 ───
result = phase_0_discovery()
log(f"Phase 0 complete — found {len(result.get('tables', []))} tables")

