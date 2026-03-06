# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  GenDWH KA — Universal Extraction Notebook                                ║
# ║  Fabric Notebook  |  InspirIT  |  v2.0  |  March 2026                     ║
# ║                                                                            ║
# ║  Extracts ALL metadata from the Fabric tenant into a single JSON file.     ║
# ║  No AI, no secrets — uses only the notebook's own Fabric identity.         ║
# ║  Output: gendwh_raw_export.json → OneLake Files                           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# CELL 0 ── Configuration ───

import requests, json, time, base64, os, hashlib
from datetime import datetime, timezone

CONFIG = {
    "fabric_api_base":      "https://api.fabric.microsoft.com/v1",
    "output_path":          "/lakehouse/default/Files/gendwh_raw_export.json",
    "admin_lakehouse_name": "GenDWH_Administration_LH",
    "max_retries":          3,
    "retry_delay":          2,
    "api_timeout":          60,
}

print("✓ Configuration loaded")
print(f"  Output: {CONFIG['output_path']}")

# CELL 1 ── API Helpers ───

def get_fabric_token():
    """Get Fabric REST API bearer token via notebook identity."""
    return mssparkutils.credentials.getToken("pbi")


def fabric_api_get(endpoint, token=None):
    """GET with auto-pagination. Returns list of items."""
    if token is None:
        token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{CONFIG['fabric_api_base']}{endpoint}"
    all_items = []
    while url:
        resp = requests.get(url, headers=headers, timeout=CONFIG["api_timeout"])
        resp.raise_for_status()
        data = resp.json()
        all_items.extend(data.get("value", []))
        url = data.get("continuationUri") or data.get("@odata.nextLink")
    return all_items


def fabric_api_post(endpoint, body=None, token=None):
    """POST to Fabric REST API. Returns parsed JSON or empty dict."""
    if token is None:
        token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{CONFIG['fabric_api_base']}{endpoint}"
    for attempt in range(CONFIG["max_retries"]):
        resp = requests.post(url, headers=headers, json=body or {}, timeout=CONFIG["api_timeout"])
        if resp.status_code == 429:
            wait = CONFIG["retry_delay"] * (attempt + 1)
            print(f"  ⏳ Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    return {}


def sha256_short(text):
    """Quick hash for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


print("✓ API helpers loaded")

# CELL 2 ── Discovery ───

def discover_workspaces(token):
    """List all workspaces and their items."""
    print("═" * 60)
    print("STEP 1 — DISCOVERY")
    print("═" * 60)

    workspaces_raw = fabric_api_get("/workspaces", token)
    print(f"  Found {len(workspaces_raw)} workspace(s)")

    workspaces = []
    for ws in workspaces_raw:
        ws_id = ws["id"]
        ws_name = ws.get("displayName", ws_id)
        print(f"  📁 {ws_name}")

        items = []
        try:
            items_raw = fabric_api_get(f"/workspaces/{ws_id}/items", token)
            for item in items_raw:
                items.append({
                    "id":          item.get("id", ""),
                    "type":        item.get("type", ""),
                    "displayName": item.get("displayName", ""),
                    "description": item.get("description", ""),
                })
            print(f"    {len(items)} items")
        except Exception as e:
            print(f"    ⚠ Error listing items: {e}")

        workspaces.append({
            "id":          ws_id,
            "displayName": ws_name,
            "description": ws.get("description", ""),
            "type":        ws.get("type", ""),
            "capacityId":  ws.get("capacityId", ""),
            "items":       items,
        })

    total_items = sum(len(ws["items"]) for ws in workspaces)
    print(f"\n  Total: {len(workspaces)} workspaces, {total_items} items")
    return workspaces


print("✓ Discovery function loaded")

# CELL 3 ── Definition Extraction ───

def extract_definitions(workspaces, token):
    """Call getDefinition for EVERY item. Items that don't support it get an error entry."""
    print("\n" + "═" * 60)
    print("STEP 2 — DEFINITION EXTRACTION")
    print("═" * 60)

    results = {}  # item_id -> definition parts or error dict
    ok = 0
    skipped = 0

    for ws in workspaces:
        ws_id = ws["id"]
        for item in ws["items"]:
            item_id = item["id"]
            item_type = item["type"]
            label = f"{item['displayName']} ({item_type})"
            try:
                resp = fabric_api_post(
                    f"/workspaces/{ws_id}/items/{item_id}/getDefinition",
                    token=token,
                )
                parts = resp.get("definition", {}).get("parts", [])
                decoded_parts = []
                for part in parts:
                    payload = part.get("payload", "")
                    try:
                        decoded = base64.b64decode(payload).decode("utf-8")
                    except Exception:
                        decoded = payload  # keep raw if decode fails
                    decoded_parts.append({
                        "path": part.get("path", ""),
                        "payloadType": part.get("payloadType", ""),
                        "payload": decoded,
                    })
                results[item_id] = decoded_parts
                ok += 1
                print(f"  ✓ {label}: {len(decoded_parts)} part(s)")
            except Exception as e:
                skipped += 1
                print(f"  ⚠ {label}: {e}")
                results[item_id] = {"error": str(e), "item_type": item_type}

    print(f"\n  Definitions: {ok} extracted, {skipped} unsupported/failed")
    return results


print("✓ Definition extraction function loaded")

# CELL 4 ── Schema Extraction ───

def extract_schemas(workspaces, token):
    """Extract table schemas from Lakehouses and Warehouses via Spark SQL."""
    print("\n" + "═" * 60)
    print("STEP 3 — SCHEMA EXTRACTION")
    print("═" * 60)

    schemas = {}  # item_id -> list of table dicts

    for ws in workspaces:
        for item in ws["items"]:
            item_id = item["id"]
            item_name = item["displayName"]
            item_type = item["type"]

            if item_type == "Lakehouse":
                print(f"  📦 Lakehouse: {item_name}")
                tables = _extract_lakehouse_schema(item_name)
                schemas[item_id] = tables
                print(f"    {len(tables)} table(s)")

            elif item_type == "Warehouse":
                print(f"  🏭 Warehouse: {item_name}")
                tables = _extract_warehouse_schema(item_name)
                schemas[item_id] = tables
                print(f"    {len(tables)} object(s)")

    print(f"\n  Schemas extracted for {len(schemas)} item(s)")
    return schemas


def _extract_lakehouse_schema(lakehouse_name):
    """Use Spark SQL to list tables and describe columns."""
    tables = []
    try:
        rows = spark.sql(f"SHOW TABLES IN {lakehouse_name}").collect()
        for row in rows:
            tbl_name = row["tableName"]
            tbl_info = {"table_name": tbl_name, "columns": [], "row_count": -1}
            try:
                cols = spark.sql(f"DESCRIBE TABLE {lakehouse_name}.{tbl_name}").collect()
                tbl_info["columns"] = [
                    {"name": c["col_name"], "type": c["data_type"]}
                    for c in cols if not c["col_name"].startswith("#")
                ]
                count_row = spark.sql(
                    f"SELECT COUNT(*) as cnt FROM {lakehouse_name}.{tbl_name}"
                ).first()
                tbl_info["row_count"] = count_row["cnt"]
            except Exception as e:
                tbl_info["error"] = str(e)
            tables.append(tbl_info)
    except Exception as e:
        print(f"    ⚠ Error: {e}")
    return tables


def _extract_warehouse_schema(warehouse_name):
    """Use INFORMATION_SCHEMA to list tables, views, and columns."""
    objects = []
    try:
        # Tables and views
        tv_rows = spark.sql(
            f"SELECT TABLE_NAME, TABLE_TYPE FROM {warehouse_name}.INFORMATION_SCHEMA.TABLES"
        ).collect()
        for tv in tv_rows:
            obj = {
                "object_name": tv["TABLE_NAME"],
                "object_type": tv["TABLE_TYPE"],
                "columns": [],
            }
            try:
                col_rows = spark.sql(
                    f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION "
                    f"FROM {warehouse_name}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_NAME = '{tv['TABLE_NAME']}' "
                    f"ORDER BY ORDINAL_POSITION"
                ).collect()
                obj["columns"] = [
                    {
                        "name": c["COLUMN_NAME"],
                        "type": c["DATA_TYPE"],
                        "nullable": c["IS_NULLABLE"],
                        "ordinal": c["ORDINAL_POSITION"],
                    }
                    for c in col_rows
                ]
            except Exception as e:
                obj["column_error"] = str(e)

            # View definitions
            if tv["TABLE_TYPE"] == "VIEW":
                try:
                    vd = spark.sql(
                        f"SELECT VIEW_DEFINITION FROM {warehouse_name}.INFORMATION_SCHEMA.VIEWS "
                        f"WHERE TABLE_NAME = '{tv['TABLE_NAME']}'"
                    ).first()
                    if vd:
                        obj["view_definition"] = vd["VIEW_DEFINITION"]
                except Exception:
                    pass

            objects.append(obj)
    except Exception as e:
        print(f"    ⚠ Error: {e}")
    return objects


print("✓ Schema extraction functions loaded")

# CELL 5 ── Metadata Query Extraction ───

def extract_metadata_queries(token):
    """Read SQL queries from gen_adm_* metadata tables in the admin lakehouse."""
    print("\n" + "═" * 60)
    print("STEP 4 — METADATA QUERY EXTRACTION")
    print("═" * 60)

    admin_lh = CONFIG["admin_lakehouse_name"]

    META_TABLES = [
        # (table_name, layer, mode, level)
        ("gen_adm_scd1_merge_silver_level_1_meta",    "Silver", "merge",     "L1"),
        ("gen_adm_scd1_merge_silver_level_2_meta",    "Silver", "merge",     "L2"),
        ("gen_adm_overwrite_silver_level_1_meta",     "Silver", "overwrite", "L1"),
        ("gen_adm_overwrite_silver_level_2_meta",     "Silver", "overwrite", "L2"),
        ("gen_adm_scd2_merge_gold_level_1_meta",      "Gold",   "merge",     "L1"),
        ("gen_adm_scd2_merge_gold_level_2_meta",      "Gold",   "merge",     "L2"),
        ("gen_adm_scd2_merge_gold_level_3_meta",      "Gold",   "merge",     "L3"),
        ("gen_adm_scd2_merge_gold_level_4_meta",      "Gold",   "merge",     "L4"),
        ("gen_adm_scd2_merge_gold_level_5_meta",      "Gold",   "merge",     "L5"),
        ("gen_adm_overwrite_gold_level_1_meta",       "Gold",   "overwrite", "L1"),
        ("gen_adm_overwrite_gold_level_2_meta",       "Gold",   "overwrite", "L2"),
    ]

    all_queries = []
    for meta_tbl, layer, mode, level in META_TABLES:
        try:
            rows = spark.sql(f"SELECT * FROM {admin_lh}.{meta_tbl}").collect()
            for row in rows:
                rd = row.asDict()
                source_query = rd.get("source_query", "") or ""
                all_queries.append({
                    "meta_table":    meta_tbl,
                    "target_table":  rd.get("target_table_name", "") or rd.get("target_table", "") or "",
                    "layer":         layer,
                    "mode":          mode,
                    "level":         level,
                    "source_query":  source_query,
                    "merge_key":     rd.get("merge_key", "") or rd.get("business_key", "") or "",
                    "query_hash":    sha256_short(source_query) if source_query else "",
                    "has_current":   str(rd.get("has_current", "true")),
                    "is_active":     str(rd.get("is_active", "true")),
                })
            print(f"  ✓ {meta_tbl}: {len(rows)} row(s)")
        except Exception as e:
            print(f"  ⚠ {meta_tbl}: {e}")

    # Bronze metadata
    bronze_meta = []
    try:
        rows = spark.sql(f"SELECT * FROM {admin_lh}.gen_adm_meta_bronze").collect()
        for row in rows:
            rd = row.asDict()
            bronze_meta.append({
                "source_schema":  rd.get("source_schema", ""),
                "source_table":   rd.get("source_table", ""),
                "source_columns": rd.get("source_columns", ""),
                "target_table":   rd.get("target_table", ""),
                "is_active":      str(rd.get("is_active", True)),
            })
        print(f"  ✓ gen_adm_meta_bronze: {len(rows)} row(s)")
    except Exception as e:
        print(f"  ⚠ gen_adm_meta_bronze: {e}")

    print(f"\n  Total queries: {len(all_queries)}, Bronze tables: {len(bronze_meta)}")
    return {"queries": all_queries, "bronze_meta": bronze_meta}


print("✓ Metadata query extraction function loaded")

# CELL 6 ── Export ───

def build_export(workspaces, definitions, schemas, metadata):
    """Assemble the full export dict and write to JSON."""
    print("\n" + "═" * 60)
    print("STEP 5 — EXPORT")
    print("═" * 60)

    export = {
        "_meta": {
            "version":      "2.0",
            "extracted_at":  datetime.now(timezone.utc).isoformat(),
            "notebook":     "GenDWH_KA_Extraction",
            "workspace_count": len(workspaces),
            "item_count":   sum(len(ws["items"]) for ws in workspaces),
        },
        "workspaces":   workspaces,
        "definitions":  definitions,
        "schemas":      schemas,
        "metadata":     metadata,
    }

    output_path = CONFIG["output_path"]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2, ensure_ascii=False, default=str)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  ✓ Written: {output_path}")
    print(f"  ✓ Size: {size_mb:.2f} MB")
    print(f"  ✓ Workspaces: {export['_meta']['workspace_count']}")
    print(f"  ✓ Items: {export['_meta']['item_count']}")
    print(f"  ✓ Definitions: {len(definitions)}")
    print(f"  ✓ Schemas: {len(schemas)}")
    print(f"  ✓ Metadata queries: {len(metadata.get('queries', []))}")
    print(f"  ✓ Bronze meta: {len(metadata.get('bronze_meta', []))}")

    return export


print("✓ Export function loaded")

# CELL 7 ── Main ───

print("\n" + "╔" + "═" * 58 + "╗")
print("║  GenDWH KA — Universal Extraction                        ║")
print("╚" + "═" * 58 + "╝")

start = time.time()

token = get_fabric_token()
workspaces  = discover_workspaces(token)
definitions = extract_definitions(workspaces, token)
schemas     = extract_schemas(workspaces, token)
metadata    = extract_metadata_queries(token)
export      = build_export(workspaces, definitions, schemas, metadata)

elapsed = time.time() - start
print(f"\n{'═' * 60}")
print(f"✓ Extraction complete in {elapsed:.1f}s")
print(f"  Output: {CONFIG['output_path']}")
print(f"{'═' * 60}")

