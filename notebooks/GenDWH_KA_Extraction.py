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
    "retry_delay":          1,
    "retry_max_delay":      5,
    "api_timeout":          60,
    "lro_poll_interval":    1,
    "lro_max_poll_interval": 3,
    "lro_timeout_seconds":  30,

    # ── Environment filter ────────────────────────────────────────────────────
    "target_environment":   "Dev",
    "environment_rules": {
        "Dev":  ["_WS_D", "_UWS_D", "_UWS"],
        "Test": ["_WS_T", "_UWS_T"],
        "Prod": ["_WS_P", "_UWS_P"],
    },
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
    """POST to Fabric REST API. Handles 202 long-running operations via polling."""
    if token is None:
        token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{CONFIG['fabric_api_base']}{endpoint}"

    for attempt in range(CONFIG["max_retries"]):
        resp = requests.post(url, headers=headers, json=body or {}, timeout=CONFIG["api_timeout"])
        if resp.status_code == 429:
            wait = min(CONFIG["retry_delay"] * (attempt + 1), CONFIG["retry_max_delay"])
            print(f"  ⏳ Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue

        # Long-running operation — poll until complete (strict timeout)
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            if location:
                return _poll_lro(location, token)
            return resp.json() if resp.content else {}

        resp.raise_for_status()
        return resp.json() if resp.content else {}
    return {}


def _poll_lro(url, token):
    """Poll a long-running operation URL with a strict time budget.

    Three-step LRO flow (per Microsoft docs):
      1. POST → 202 + Location header (operation URL)
      2. Poll GET /operations/{operationId} → wait for "Succeeded"
      3. GET /operations/{operationId}/result → actual definition content
    """
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.time() + CONFIG["lro_timeout_seconds"]
    wait = CONFIG["lro_poll_interval"]
    max_wait = CONFIG["lro_max_poll_interval"]

    while time.time() < deadline:
        time.sleep(wait)
        resp = requests.get(url, headers=headers, timeout=CONFIG["api_timeout"])

        if resp.status_code == 200:
            body = resp.json() if resp.content else {}
            status = body.get("status", "")

            # Step 3: if Succeeded, fetch the actual result from /result endpoint
            if status == "Succeeded":
                # Extract operationId from URL: .../operations/{operationId}
                op_id = url.rstrip("/").split("/")[-1]
                result_url = f"{CONFIG['fabric_api_base']}/operations/{op_id}/result"
                try:
                    result_resp = requests.get(result_url, headers=headers, timeout=CONFIG["api_timeout"])
                    if result_resp.status_code == 200 and result_resp.content:
                        return result_resp.json()
                except Exception:
                    pass
                # Fallback: return the poll body if /result fails
                return body

            # Terminal failure
            if status in ("Failed", "Cancelled"):
                return body

            # Still running — continue polling
            wait = min(int(resp.headers.get("Retry-After", wait)), max_wait)
            continue

        if resp.status_code == 202:
            wait = min(int(resp.headers.get("Retry-After", wait)), max_wait)
            continue
        resp.raise_for_status()

    raise TimeoutError(f"LRO timed out after {CONFIG['lro_timeout_seconds']}s")


def sha256_short(text):
    """Quick hash for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


print("✓ API helpers loaded")

# CELL 2 ── Discovery ───

def _matches_environment(ws_name):
    """Check if workspace name matches the target environment suffixes."""
    env = CONFIG["target_environment"]
    suffixes = CONFIG["environment_rules"].get(env, [])
    return any(ws_name.endswith(s) for s in suffixes)


def discover_workspaces(token):
    """List workspaces matching target_environment and their items."""
    print("═" * 60)
    print("STEP 1 — DISCOVERY")
    print("═" * 60)

    workspaces_raw = fabric_api_get("/workspaces", token)
    env = CONFIG["target_environment"]
    print(f"  Found {len(workspaces_raw)} workspace(s), filtering for '{env}'")

    workspaces = []
    skipped = 0
    for ws in workspaces_raw:
        ws_id = ws["id"]
        ws_name = ws.get("displayName", ws_id)

        if not _matches_environment(ws_name):
            skipped += 1
            continue

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
    print(f"\n  Total: {len(workspaces)} workspaces, {total_items} items (skipped {skipped} non-{env})")
    return workspaces


print("✓ Discovery function loaded")

# CELL 3 ── Definition Extraction ───

def extract_definitions(workspaces, token):
    """Call getDefinition for EVERY item, skipping types that return 400 on first attempt."""
    print("\n" + "═" * 60)
    print("STEP 2 — DEFINITION EXTRACTION")
    print("═" * 60)

    results = {}          # item_id -> definition parts or error dict
    unsupported = set()   # item types that returned 400 (not supported)
    # Per-type counters: {type: {"ok": N, "fail": N, "skip": N}}
    stats = {}

    def bump(item_type, key):
        stats.setdefault(item_type, {"ok": 0, "fail": 0, "skip": 0})
        stats[item_type][key] += 1

    # Build ordered work list so we can count remaining items per type
    work = []
    type_counts = {}
    for ws in workspaces:
        for item in ws["items"]:
            work.append((ws["id"], item))
            type_counts[item["type"]] = type_counts.get(item["type"], 0) + 1

    for ws_id, item in work:
        item_id = item["id"]
        item_type = item["type"]
        label = f"{item['displayName']} ({item_type})"

        # Skip types we already know don't support getDefinition
        if item_type in unsupported:
            bump(item_type, "skip")
            results[item_id] = {"error": "type unsupported", "item_type": item_type}
            continue

        try:
            resp = fabric_api_post(
                f"/workspaces/{ws_id}/items/{item_id}/getDefinition",
                token=token,
            )
            if not resp or not isinstance(resp, dict):
                bump(item_type, "fail")
                results[item_id] = {"error": "empty response", "item_type": item_type}
                continue

            definition = resp.get("definition")
            parts = (definition or {}).get("parts", []) if definition else []
            decoded_parts = []
            for part in parts:
                payload = part.get("payload", "")
                try:
                    decoded = base64.b64decode(payload).decode("utf-8")
                except Exception:
                    decoded = payload
                decoded_parts.append({
                    "path": part.get("path", ""),
                    "payloadType": part.get("payloadType", ""),
                    "payload": decoded,
                })

            if decoded_parts:
                results[item_id] = decoded_parts
                bump(item_type, "ok")
                print(f"  ✓ {label}: {len(decoded_parts)} part(s)")
            else:
                results[item_id] = {"raw_response": resp, "item_type": item_type}
                bump(item_type, "ok")
                print(f"  ✓ {label}: no parts, stored raw response")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                unsupported.add(item_type)
                remaining = sum(1 for _, i in work if i["type"] == item_type) - (
                    stats.get(item_type, {}).get("ok", 0)
                    + stats.get(item_type, {}).get("fail", 0)
                    + 1  # this one
                )
                bump(item_type, "fail")
                print(f"  ⚠ {label}: 400 — marking {item_type} unsupported, skipping {remaining} remaining")
            else:
                bump(item_type, "fail")
                print(f"  ⚠ {label}: {e}")
            results[item_id] = {"error": str(e), "item_type": item_type}

        except Exception as e:
            bump(item_type, "fail")
            print(f"  ⚠ {label}: {e}")
            results[item_id] = {"error": str(e), "item_type": item_type}

    # Summary table
    total_ok = sum(s["ok"] for s in stats.values())
    total_fail = sum(s["fail"] for s in stats.values())
    total_skip = sum(s["skip"] for s in stats.values())
    print(f"\n  {'Type':<25} {'OK':>5} {'Fail':>5} {'Skip':>5}")
    print(f"  {'-'*25} {'-'*5} {'-'*5} {'-'*5}")
    for t in sorted(stats):
        s = stats[t]
        print(f"  {t:<25} {s['ok']:>5} {s['fail']:>5} {s['skip']:>5}")
    print(f"  {'-'*25} {'-'*5} {'-'*5} {'-'*5}")
    print(f"  {'TOTAL':<25} {total_ok:>5} {total_fail:>5} {total_skip:>5}")
    return results


print("✓ Definition extraction function loaded")

# CELL 4 ── Schema Extraction ───

def extract_schemas(workspaces, token):
    """Extract table schemas from Lakehouses (Spark SQL) and Warehouses (JDBC)."""
    print("\n" + "═" * 60)
    print("STEP 3 — SCHEMA EXTRACTION")
    print("═" * 60)

    schemas = {}  # item_id -> schema dict
    extracted_names = set()  # track already-extracted lakehouse/warehouse names

    for ws in workspaces:
        ws_id = ws["id"]
        for item in ws["items"]:
            item_id = item["id"]
            item_name = item["displayName"]
            item_type = item["type"]

            if item_type == "Lakehouse":
                if item_name in extracted_names:
                    print(f"  ⏭ Lakehouse: {item_name} (already extracted, skipping)")
                    continue
                extracted_names.add(item_name)
                print(f"  📦 Lakehouse: {item_name}")
                tables = _extract_lakehouse_schema(item_name)
                schemas[item_id] = tables
                print(f"    {len(tables)} table(s)")

            elif item_type == "Warehouse":
                if item_name in extracted_names:
                    print(f"  ⏭ Warehouse: {item_name} (already extracted, skipping)")
                    continue
                extracted_names.add(item_name)
                print(f"  🏭 Warehouse: {item_name}")
                wh_schema = _extract_warehouse_schema(ws_id, item_id, item_name, token)
                schemas[item_id] = wh_schema

    print(f"\n  Schemas extracted for {len(schemas)} item(s), {len(extracted_names)} unique name(s)")
    return schemas


def _extract_lakehouse_schema(lakehouse_name):
    """Extract schema via SHOW TABLES + listColumns (fastest metadata-only approach)."""
    tables = []

    try:
        rows = spark.sql(f"SHOW TABLES IN {lakehouse_name}").collect()
    except Exception as e:
        print(f"    ⚠ Error listing tables: {e}")
        return []

    for row in rows:
        tbl_name = row["tableName"]
        tbl_info = {"table_name": tbl_name, "table_type": getattr(row, "isTemporary", False), "columns": []}
        try:
            cols = spark.catalog.listColumns(f"{lakehouse_name}.{tbl_name}")
            tbl_info["columns"] = [
                {"name": c.name, "dataType": c.dataType, "nullable": c.nullable}
                for c in cols
            ]
        except Exception as e:
            tbl_info["column_error"] = str(e)
        tables.append(tbl_info)

    return tables


def _extract_warehouse_schema(ws_id, wh_id, wh_name, token):
    """Extract Warehouse schema via JDBC (INFORMATION_SCHEMA)."""
    result = {"item_type": "Warehouse", "tables": [], "views": [], "procedures": []}
    try:
        # Step 1: Get connectionString from Warehouse REST API
        wh_token = get_fabric_token()
        headers = {"Authorization": f"Bearer {wh_token}"}
        resp = requests.get(
            f"{CONFIG['fabric_api_base']}/workspaces/{ws_id}/warehouses/{wh_id}",
            headers=headers, timeout=CONFIG["api_timeout"],
        )
        resp.raise_for_status()
        conn_string = resp.json().get("properties", {}).get("connectionString", "")
        if not conn_string:
            print(f"    ⚠ No connectionString found")
            return result

        # Step 2: Build JDBC URL
        jdbc_url = f"jdbc:sqlserver://{conn_string};database={wh_name};encrypt=true;trustServerCertificate=false"
        jdbc_token = get_fabric_token()

        def _jdbc_read(query):
            return (spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("query", query)
                    .option("accessToken", jdbc_token)
                    .load()
                    .collect())

        # Step 3a: Tables and columns
        try:
            col_rows = _jdbc_read(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
                "ORDINAL_POSITION, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS"
            )
            # Group columns by (schema, table)
            tbl_map = {}
            for r in col_rows:
                key = (r["TABLE_SCHEMA"], r["TABLE_NAME"])
                tbl_map.setdefault(key, []).append({
                    "name": r["COLUMN_NAME"],
                    "dataType": r["DATA_TYPE"],
                    "ordinal": r["ORDINAL_POSITION"],
                    "nullable": r["IS_NULLABLE"],
                })
            for (schema, name), cols in sorted(tbl_map.items()):
                cols.sort(key=lambda c: c["ordinal"])
                result["tables"].append({"schema": schema, "name": name, "columns": cols})
            print(f"    {len(col_rows)} column(s) in {len(tbl_map)} table(s)")
        except Exception as e:
            print(f"    ⚠ COLUMNS query failed: {e}")

        # Step 3b: Views
        try:
            view_rows = _jdbc_read(
                "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION "
                "FROM INFORMATION_SCHEMA.VIEWS"
            )
            for r in view_rows:
                result["views"].append({
                    "schema": r["TABLE_SCHEMA"],
                    "name": r["TABLE_NAME"],
                    "definition": r["VIEW_DEFINITION"],
                })
            print(f"    {len(view_rows)} view(s)")
        except Exception as e:
            print(f"    ⚠ VIEWS query failed: {e}")

        # Step 3c: Stored procedures
        try:
            proc_rows = _jdbc_read(
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION "
                "FROM INFORMATION_SCHEMA.ROUTINES"
            )
            for r in proc_rows:
                result["procedures"].append({
                    "schema": r["ROUTINE_SCHEMA"],
                    "name": r["ROUTINE_NAME"],
                    "type": r["ROUTINE_TYPE"],
                    "definition": r["ROUTINE_DEFINITION"],
                })
            print(f"    {len(proc_rows)} procedure(s)")
        except Exception as e:
            print(f"    ⚠ ROUTINES query failed: {e}")

    except Exception as e:
        print(f"    ⚠ Warehouse extraction failed: {e}")
        result["error"] = str(e)

    return result


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

