# Phase 0 — Discovery
# ====================
# Discovers all Fabric workspaces and items accessible by this notebook's identity.
# Writes results to Delta table: doc_workspace_inventory

# CELL 0 ── Load Library ───
%run GenDWH_Documentation_LIB

# CELL 1 ── Run Phase 0 ───
inventory = phase_0_discovery()

ws_count = len(set(item["workspace_id"] for item in inventory))
print(f"\n📋 Phase 0 result: {len(inventory)} items across {ws_count} workspace(s)")
print(f"   Environment: {CONFIG.get('target_environment', 'N/A')}")

