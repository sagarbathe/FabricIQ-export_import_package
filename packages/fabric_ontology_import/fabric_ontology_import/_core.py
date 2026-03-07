"""Core import logic — read definition, validate, rewrite bindings, create ontology."""

from __future__ import annotations

import json
import time
import base64
import requests
from collections import defaultdict

from ._helpers import fabric_headers, poll_lro, fs_exists, fs_read_text

API_BASE = "https://api.fabric.microsoft.com/v1"


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

def import_ontology(
    token: str,
    definition_path: str,
    target_workspace_id: str,
    new_ontology_name: str,
    *,
    description: str = "",
    target_lakehouse_names: list[str] | None = None,
    target_warehouse_names: list[str] | None = None,
    target_eventhouse_names: list[str] | None = None,
    target_semantic_model_names: list[str] | None = None,
    source_workspace_id: str | None = None,
    overwrite: bool = True,
    skip_binding_validation: bool = False,
    api_base: str = API_BASE,
    poll_interval: int = 10,
    timeout: int = 300,
    create_retries: int = 6,
    create_retry_interval: int = 20,
) -> dict:
    """
    Import a Fabric ontology from an exported definition JSON.

    Reads the definition, validates structure, rewrites data-source bindings
    to point at the configured target items, creates the ontology, applies
    bindings via ``updateDefinition``, and verifies the result.

    Parameters
    ----------
    token : str
        Fabric API access token.
    definition_path : str
        Path to ``*_definition.json`` (local or ``abfss://``).
    target_workspace_id : str
        Target workspace GUID.
    new_ontology_name : str
        Display name for the new ontology.
    description : str
        Optional ontology description.
    target_lakehouse_names : list[str]
        Lakehouse display name(s) in the target workspace for binding rewrite.
    target_warehouse_names : list[str]
        Warehouse display name(s).
    target_eventhouse_names : list[str]
        Eventhouse display name(s).
    target_semantic_model_names : list[str]
        Semantic model display name(s).
    source_workspace_id : str
        **Deprecated.** No longer needed — source item names are now embedded
        in the exported definition JSON.  Accepted but ignored.
    overwrite : bool
        Delete an existing ontology with the same name before creating.
    skip_binding_validation : bool
        Skip the check that target items/tables exist.
    api_base : str
        Fabric REST API base URL.
    poll_interval / timeout : int
        LRO polling parameters in seconds.
    create_retries / create_retry_interval : int
        Retry parameters for the create call (handles
        ``ItemDisplayNameNotAvailableYet``).

    Returns
    -------
    dict
        ``ontology_id``, ``name``, ``part_count``, ``rewrite_count``,
        ``verification``.
    """
    target_lakehouse_names = target_lakehouse_names or []
    target_warehouse_names = target_warehouse_names or []
    target_eventhouse_names = target_eventhouse_names or []
    target_semantic_model_names = target_semantic_model_names or []

    # ── 1. Validate target workspace ────────────────────────────────────────
    print(f"Validating target workspace {target_workspace_id} ...")
    ws_resp = requests.get(
        f"{api_base}/workspaces/{target_workspace_id}",
        headers=fabric_headers(token),
    )
    ws_resp.raise_for_status()
    print(f"  Workspace: {ws_resp.json().get('displayName')}")

    # ── 2. Read definition ──────────────────────────────────────────────────
    print(f"\nReading definition from: {definition_path}")
    if not fs_exists(definition_path):
        raise FileNotFoundError(f"Definition file not found: {definition_path}")

    content = fs_read_text(definition_path)
    definition = json.loads(content)
    parts = definition.get("definition", {}).get("parts", [])
    source_item_map = definition.get("_source_item_map", {})
    print(f"  Loaded {len(parts)} part(s)")
    if source_item_map:
        print(f"  Source-item map: {len(source_item_map)} item(s) embedded")
    else:
        print("  [INFO] No embedded source-item map (legacy export).")

    # ── 3. Validate structure ───────────────────────────────────────────────
    assert "definition" in definition, "Missing 'definition' key"
    assert "parts" in definition["definition"], "Missing 'parts' array"
    assert len(parts) > 0, "Definition has no parts"
    assert any(p["path"] == "definition.json" for p in parts), (
        "Missing required 'definition.json' part"
    )

    entity_types = [
        p for p in parts
        if p["path"].startswith("EntityTypes/") and p["path"].endswith("/definition.json")
    ]
    rel_types = [
        p for p in parts
        if p["path"].startswith("RelationshipTypes/") and p["path"].endswith("/definition.json")
    ]
    data_bindings = [p for p in parts if "/DataBindings/" in p["path"]]
    contextualizations = [p for p in parts if "/Contextualizations/" in p["path"]]

    print(f"\n{'=' * 50}")
    print(f" DEFINITION SUMMARY")
    print(f"{'=' * 50}")
    print(f" Total parts:         {len(parts)}")
    print(f" Entity Types:        {len(entity_types)}")
    print(f" Relationship Types:  {len(rel_types)}")
    print(f" Data Bindings:       {len(data_bindings)}")
    print(f" Contextualizations:  {len(contextualizations)}")
    print(f"{'=' * 50}")

    # Entity names
    for et in entity_types:
        try:
            dec = json.loads(base64.b64decode(et["payload"]).decode())
            name = dec.get("name", "?")
            props = len(dec.get("properties", []))
            ts = len(dec.get("timeseriesProperties", []))
            print(f"   {name}  ({props} props, {ts} timeseries)")
        except Exception:
            pass

    # ── 4. Extract binding details ──────────────────────────────────────────
    tables_by_type: dict[str, set[str]] = defaultdict(set)
    binding_details: list[dict] = []

    for part in parts:
        p = part["path"]
        if "/DataBindings/" not in p and "/Contextualizations/" not in p:
            continue

        part_type = "DataBinding" if "/DataBindings/" in p else "Contextualization"
        owner = _extract_owner(p)

        try:
            decoded = json.loads(base64.b64decode(part["payload"]).decode())
        except Exception:
            continue

        _, _, binfo = _find_binding_dict(decoded)
        if not binfo:
            binfo = {}

        src_type = binfo.get("sourceType", "Unknown")
        table_name = binfo.get("sourceTableName", "")

        if table_name:
            tables_by_type[src_type].add(table_name)

        binding_details.append({
            "owner": owner,
            "part_type": part_type,
            "source_type": src_type,
            "table_name": table_name or "(none)",
        })

    if binding_details:
        unique_tables: set[str] = set()
        for tbls in tables_by_type.values():
            unique_tables |= tbls

        print(
            f"\nFound {len(binding_details)} binding(s), "
            f"{len(unique_tables)} unique table(s):\n"
        )
        print(f" {'Owner':<35s} {'Type':<20s} {'Source':<18s} {'Table'}")
        print(f" {'─' * 35} {'─' * 20} {'─' * 18} {'─' * 25}")
        for d in binding_details:
            print(
                f" {d['owner']:<35s} {d['part_type']:<20s} "
                f"{d['source_type']:<18s} {d['table_name']}"
            )

    # ── 5. Validate bindings against target ─────────────────────────────────
    if not skip_binding_validation and tables_by_type:
        _validate_bindings(
            token, api_base, target_workspace_id, tables_by_type,
            target_lakehouse_names, target_warehouse_names,
            target_eventhouse_names, target_semantic_model_names,
        )
    elif skip_binding_validation:
        print("\n[INFO] Binding validation skipped.")

    # ── 6. Rewrite bindings ─────────────────────────────────────────────────
    rewrite_count = 0
    dropped_bindings: list[dict] = []
    target_items_by_type: dict = {}

    has_targets = any([
        target_lakehouse_names, target_warehouse_names,
        target_eventhouse_names, target_semantic_model_names,
    ])
    if has_targets:
        print(f"\nRewriting bindings to target items ...")
        target_items_by_type = _resolve_target_items(
            token, api_base, target_workspace_id,
            target_lakehouse_names, target_warehouse_names,
            target_eventhouse_names, target_semantic_model_names,
        )
        source_id_to_name = _map_source_to_target(
            parts, target_items_by_type, source_item_map,
        )
        parts, rewrite_count, dropped_bindings = _rewrite_bindings(
            parts, target_workspace_id, target_items_by_type,
            source_id_to_name, token, api_base,
        )
        print(f"  Rewritten: {rewrite_count} binding(s)")
    else:
        # No targets configured at all — strip every source binding so the
        # new ontology does not silently reference the source items.
        print(f"\n[INFO] No target data sources configured — stripping all source bindings ...")
        parts, dropped_bindings = _strip_all_bindings(parts)

    # Report unbound entities / relationships
    if dropped_bindings:
        _print_unbound_summary(dropped_bindings)

    # ── 7. Update .platform ─────────────────────────────────────────────────
    updated_parts: list[dict] = []
    for part in parts:
        new_part = dict(part)
        if part.get("path") == ".platform":
            try:
                platform = json.loads(base64.b64decode(part["payload"]).decode())
                if "metadata" in platform:
                    platform["metadata"]["displayName"] = new_ontology_name
                new_part["payload"] = base64.b64encode(
                    json.dumps(platform).encode()
                ).decode()
            except Exception:
                pass
        updated_parts.append(new_part)

    # ── 8. Delete existing if needed ────────────────────────────────────────
    existing_id = _find_ontology(token, api_base, target_workspace_id, new_ontology_name)
    if existing_id:
        if overwrite:
            print(
                f"\nDeleting existing ontology '{new_ontology_name}' "
                f"(ID: {existing_id}) ..."
            )
            del_resp = requests.delete(
                f"{api_base}/workspaces/{target_workspace_id}/ontologies/{existing_id}",
                headers=fabric_headers(token),
            )
            if del_resp.status_code == 200:
                print("  Deleted.")
            else:
                raise Exception(
                    f"Delete failed: {del_resp.status_code} – {del_resp.text}"
                )
            time.sleep(5)
        else:
            raise Exception(
                f"Ontology '{new_ontology_name}' already exists (ID: {existing_id}). "
                f"Set overwrite=True to replace it."
            )

    # ── 9. Create ontology (with retry for name-not-available) ──────────────
    create_url = f"{api_base}/workspaces/{target_workspace_id}/ontologies"
    body = {
        "displayName": new_ontology_name,
        "description": description,
        "definition": {"parts": updated_parts},
    }

    print(f"\nCreating ontology '{new_ontology_name}' ({len(updated_parts)} parts) ...")

    resp = None
    for attempt in range(1, create_retries + 1):
        resp = requests.post(create_url, headers=fabric_headers(token), json=body)
        if resp.status_code in (201, 202):
            break

        # Check if retriable
        is_retriable = False
        try:
            err = resp.json()
            is_retriable = err.get("isRetriable", False)
            err_code = err.get("errorCode", "")
            err_msg = err.get("message", resp.text)
        except Exception:
            err_code, err_msg = "", resp.text

        if is_retriable and attempt < create_retries:
            print(
                f"  [RETRY {attempt}/{create_retries}] {err_code}: {err_msg}\n"
                f"  Waiting {create_retry_interval}s ..."
            )
            time.sleep(create_retry_interval)
        else:
            break

    create_result: dict = {}
    if resp.status_code == 201:
        create_result = resp.json()
        print("  Created immediately.")
    elif resp.status_code == 202:
        location = resp.headers.get("Location")
        retry_after = int(resp.headers.get("Retry-After", poll_interval))
        print(f"  LRO started. Polling every {retry_after}s ...")

        elapsed = 0
        while elapsed < timeout:
            time.sleep(retry_after)
            elapsed += retry_after
            poll_resp = requests.get(location, headers=fabric_headers(token))
            status = poll_resp.json().get("status", "Unknown")
            print(f"  ... {status} ({elapsed}s)")

            if status == "Succeeded":
                new_id = _find_ontology(
                    token, api_base, target_workspace_id, new_ontology_name
                )
                if new_id:
                    ont_resp = requests.get(
                        f"{api_base}/workspaces/{target_workspace_id}/ontologies/{new_id}",
                        headers=fabric_headers(token),
                    )
                    create_result = (
                        ont_resp.json() if ont_resp.status_code == 200
                        else {"id": new_id}
                    )
                else:
                    create_result = {"status": "Succeeded"}
                break

            if status in ("Failed", "Cancelled"):
                raise Exception(
                    f"Create LRO {status}: "
                    f"{json.dumps(poll_resp.json(), indent=2)}"
                )
        else:
            raise TimeoutError(f"Create LRO timed out after {timeout}s")
    else:
        raise Exception(f"Create failed: {resp.status_code} – {resp.text}")

    new_ontology_id = create_result.get("id")
    if not new_ontology_id:
        new_ontology_id = _find_ontology(
            token, api_base, target_workspace_id, new_ontology_name
        )

    print(f"\n[OK] Ontology '{new_ontology_name}' created (ID: {new_ontology_id})")

    # ── 10. Apply rewritten bindings via updateDefinition ───────────────────
    if new_ontology_id and rewrite_count > 0:
        print("\nApplying rewritten bindings via updateDefinition ...")
        update_url = (
            f"{api_base}/workspaces/{target_workspace_id}"
            f"/ontologies/{new_ontology_id}/updateDefinition"
        )
        update_body = {"definition": {"parts": updated_parts}}
        update_resp = requests.post(
            update_url, headers=fabric_headers(token), json=update_body
        )

        if update_resp.status_code in (200, 201):
            print("  Bindings applied.")
        elif update_resp.status_code == 202:
            poll_lro(token, update_resp, poll_interval, timeout)
            print("  Bindings applied.")
        else:
            print(
                f"  [WARN] updateDefinition returned "
                f"{update_resp.status_code}: {update_resp.text}"
            )

    # ── 11. Verify ──────────────────────────────────────────────────────────
    verification = _verify_ontology(
        token, api_base, target_workspace_id, new_ontology_id,
        new_ontology_name, entity_types, rel_types, data_bindings,
        contextualizations, target_items_by_type, dropped_bindings,
        poll_interval, timeout,
    )

    print(f"\n[SUCCESS] Ontology '{new_ontology_name}' imported and verified!")

    return {
        "ontology_id": new_ontology_id,
        "name": new_ontology_name,
        "part_count": len(updated_parts),
        "rewrite_count": rewrite_count,
        "dropped_bindings": dropped_bindings,
        "verification": verification,
    }


# ═════════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═════════════════════════════════════════════════════════════════════════════

def _find_binding_dict(decoded_json: dict) -> tuple:
    """
    Recursively search for the dict containing ``sourceType`` +
    ``workspaceId``/``itemId``.

    Returns ``(parent_dict, key_name, binding_dict)`` or
    ``(None, None, None)``.
    """
    # Top-level first (Contextualizations use ``dataBindingTable`` at root)
    for key, val in decoded_json.items():
        if isinstance(val, dict):
            if "sourceType" in val and ("itemId" in val or "workspaceId" in val):
                return (decoded_json, key, val)
    # One level deeper (DataBindings use
    # ``dataBindingConfiguration.sourceTableProperties``)
    for key, val in decoded_json.items():
        if isinstance(val, dict):
            for k2, v2 in val.items():
                if isinstance(v2, dict):
                    if "sourceType" in v2 and (
                        "itemId" in v2 or "workspaceId" in v2
                    ):
                        return (val, k2, v2)
    return (None, None, None)


def _extract_owner(path: str) -> str:
    """Extract a human-readable owner label from a part path."""
    segs = path.split("/")
    if len(segs) >= 2 and segs[0] in ("EntityTypes", "RelationshipTypes"):
        return f"{segs[0].replace('Types', '')}/{segs[1]}"
    return "unknown"


def _find_ontology(
    token: str, api_base: str, workspace_id: str, name: str
) -> str | None:
    """Return the ID of an ontology with the given display name, or None."""
    url = f"{api_base}/workspaces/{workspace_id}/ontologies"
    resp = requests.get(url, headers=fabric_headers(token))
    if resp.status_code != 200:
        return None
    for item in resp.json().get("value", []):
        if item.get("displayName") == name:
            return item["id"]
    return None


def _resolve_target_items(
    token: str,
    api_base: str,
    workspace_id: str,
    lh_names: list[str],
    wh_names: list[str],
    eh_names: list[str],
    sm_names: list[str],
) -> dict:
    """Resolve configured target item display names to API item dicts."""
    TYPE_API = {
        "LakehouseTable": ("lakehouses", lh_names),
        "WarehouseTable": ("warehouses", wh_names),
        "KustoTable": ("eventhouses", eh_names),
        "SemanticModelTable": ("semanticModels", sm_names),
    }

    result: dict = {}  # { sourceType: { displayName: item_dict } }
    for src_type, (api_path, names) in TYPE_API.items():
        if not names:
            continue
        resp = requests.get(
            f"{api_base}/workspaces/{workspace_id}/{api_path}",
            headers=fabric_headers(token),
        )
        if resp.status_code != 200:
            print(f"  [WARN] Could not list {api_path}: {resp.status_code}")
            continue
        items = {
            i["displayName"]: i
            for i in resp.json().get("value", [])
            if i["displayName"] in names
        }
        if items:
            result[src_type] = items
            for n, it in items.items():
                print(f"  {src_type}: '{n}' -> ID {it['id']}")
    return result


def _map_source_to_target(
    parts: list[dict],
    target_items_by_type: dict,
    source_item_map: dict,
) -> dict:
    """
    Map ``(sourceType, sourceItemId)`` -> target item display name.

    Uses the ``_source_item_map`` embedded in the exported definition JSON
    to resolve source item IDs to display names — no API call to the
    source workspace is needed.
    """
    # Collect unique source item IDs from binding parts
    source_item_ids: dict[str, set[str]] = defaultdict(set)
    for part in parts:
        if "/DataBindings/" not in part["path"] and "/Contextualizations/" not in part["path"]:
            continue
        try:
            decoded = json.loads(base64.b64decode(part["payload"]).decode())
        except Exception:
            continue
        _, _, binfo = _find_binding_dict(decoded)
        if binfo:
            st = binfo.get("sourceType", "")
            iid = binfo.get("itemId", "")
            if st and iid:
                source_item_ids[st].add(iid)

    # Build mapping
    mapping: dict = {}
    for src_type, item_ids in source_item_ids.items():
        target_map = target_items_by_type.get(src_type, {})
        for item_id in item_ids:
            if len(target_map) == 1:
                # Single target → all source items map to it
                only_name = list(target_map.keys())[0]
                mapping[(src_type, item_id)] = only_name
                print(f"  Mapping {src_type} {item_id[:8]}... -> '{only_name}'")
            elif item_id in source_item_map:
                # Use the embedded source-item map from the export
                src_name = source_item_map[item_id].get("displayName", "")
                if src_name:
                    mapping[(src_type, item_id)] = src_name
                    print(f"  Resolved {src_type} {item_id[:8]}... -> '{src_name}' (from embedded map)")
                else:
                    print(
                        f"  [WARN] Embedded map entry for {item_id} has "
                        f"no displayName"
                    )
            else:
                print(
                    f"  [WARN] Cannot resolve {src_type} item {item_id[:8]}... "
                    f"— not in embedded source-item map. Re-export the "
                    f"ontology to include the map."
                )
    return mapping


def _rewrite_bindings(
    parts: list[dict],
    target_workspace_id: str,
    target_items_by_type: dict,
    source_id_to_name: dict,
    token: str,
    api_base: str,
) -> tuple[list[dict], int, list[dict]]:
    """
    Rewrite binding payloads to point to target items.

    Bindings that cannot be rewritten (no matching target configured) are
    **dropped** so the new ontology does not silently reference source items.

    Returns ``(parts, rewrite_count, dropped_bindings)``.
    """

    # For KustoTable, fetch queryServiceUri
    eventhouse_uris: dict[str, str] = {}
    if "KustoTable" in target_items_by_type:
        for name, item in target_items_by_type["KustoTable"].items():
            resp = requests.get(
                f"{api_base}/workspaces/{target_workspace_id}/eventhouses/{item['id']}",
                headers=fabric_headers(token),
            )
            if resp.status_code == 200:
                uri = resp.json().get("properties", {}).get("queryServiceUri", "")
                if uri:
                    eventhouse_uris[name] = uri

    rewrite_count = 0
    rewritten: list[dict] = []
    dropped: list[dict] = []

    for part in parts:
        p = part["path"]
        if "/DataBindings/" not in p and "/Contextualizations/" not in p:
            rewritten.append(part)
            continue

        try:
            decoded = json.loads(base64.b64decode(part["payload"]).decode())
        except Exception:
            rewritten.append(part)
            continue

        parent, bkey, binfo = _find_binding_dict(decoded)
        if not binfo:
            rewritten.append(part)
            continue

        src_type = binfo.get("sourceType", "")
        source_item_id = binfo.get("itemId", "")
        table_name = binfo.get("sourceTableName", "")
        target_map = target_items_by_type.get(src_type, {})

        if not target_map:
            # No target of this source type configured — drop the binding
            dropped.append({
                "path": p,
                "owner": _extract_owner(p),
                "part_type": "DataBinding" if "/DataBindings/" in p else "Contextualization",
                "source_type": src_type,
                "table_name": table_name or "(none)",
                "reason": f"No target configured for {src_type}",
            })
            continue

        target_name = source_id_to_name.get((src_type, source_item_id))
        target_item = target_map.get(target_name) if target_name else None

        if not target_item:
            # Could not resolve to a target item — drop the binding
            dropped.append({
                "path": p,
                "owner": _extract_owner(p),
                "part_type": "DataBinding" if "/DataBindings/" in p else "Contextualization",
                "source_type": src_type,
                "table_name": table_name or "(none)",
                "reason": f"No matching target item for source '{target_name or source_item_id}'",
            })
            continue

        # Rewrite IDs
        binfo["workspaceId"] = target_workspace_id
        binfo["itemId"] = target_item["id"]
        if src_type == "KustoTable" and target_name in eventhouse_uris:
            binfo["clusterUri"] = eventhouse_uris[target_name]
        parent[bkey] = binfo

        new_part = dict(part)
        new_part["payload"] = base64.b64encode(
            json.dumps(decoded).encode()
        ).decode()
        rewritten.append(new_part)
        rewrite_count += 1

    return rewritten, rewrite_count, dropped


def _strip_all_bindings(
    parts: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Remove every DataBinding / Contextualization part from the definition.

    Used when no target data sources are configured at all, to avoid the
    new ontology silently referencing the source workspace's items.

    Returns ``(filtered_parts, dropped_bindings)``.
    """
    filtered: list[dict] = []
    dropped: list[dict] = []

    for part in parts:
        p = part["path"]
        if "/DataBindings/" not in p and "/Contextualizations/" not in p:
            filtered.append(part)
            continue

        src_type = ""
        table_name = ""
        try:
            decoded = json.loads(base64.b64decode(part["payload"]).decode())
            _, _, binfo = _find_binding_dict(decoded)
            if binfo:
                src_type = binfo.get("sourceType", "")
                table_name = binfo.get("sourceTableName", "")
        except Exception:
            pass

        dropped.append({
            "path": p,
            "owner": _extract_owner(p),
            "part_type": "DataBinding" if "/DataBindings/" in p else "Contextualization",
            "source_type": src_type or "Unknown",
            "table_name": table_name or "(none)",
            "reason": "No target data sources configured",
        })

    return filtered, dropped


def _print_unbound_summary(dropped: list[dict]) -> None:
    """Print a clear, prominent summary of bindings that were removed."""
    print(f"\n{'!' * 60}")
    print(f" UNBOUND BINDINGS — ACTION REQUIRED")
    print(f"{'!' * 60}")
    print(
        f" The following {len(dropped)} binding(s) were REMOVED because no\n"
        f" matching target data source was configured. These entities /\n"
        f" relationships will have NO data binding in the new ontology.\n"
    )
    print(f" {'Owner':<35s} {'Type':<18s} {'Source':<18s} {'Table':<25s} Reason")
    print(f" {'─' * 35} {'─' * 18} {'─' * 18} {'─' * 25} {'─' * 30}")
    for d in dropped:
        print(
            f" {d['owner']:<35s} {d['part_type']:<18s} "
            f"{d['source_type']:<18s} {d['table_name']:<25s} {d['reason']}"
        )
    print(f"{'!' * 60}")
    print(
        f" To fix: configure the missing target item(s) in the import\n"
        f" notebook and re-run, or manually bind them in the Fabric UI.\n"
    )
    print(f"{'!' * 60}")


def _validate_bindings(
    token: str,
    api_base: str,
    workspace_id: str,
    tables_by_type: dict[str, set[str]],
    lh_names: list[str],
    wh_names: list[str],
    eh_names: list[str],
    sm_names: list[str],
) -> None:
    """Validate target items exist and contain required tables."""
    TYPE_CFG = {
        "LakehouseTable": (lh_names, "lakehouses", "Lakehouse", True),
        "WarehouseTable": (wh_names, "warehouses", "Warehouse", False),
        "KustoTable": (eh_names, "eventhouses", "Eventhouse", False),
        "SemanticModelTable": (sm_names, "semanticModels", "Semantic Model", False),
    }

    all_ok = True

    for src_type, expected_tables in tables_by_type.items():
        cfg = TYPE_CFG.get(src_type)
        if not cfg:
            print(f"\n[WARN] Unknown source type '{src_type}'")
            continue

        names, api_path, label, can_list = cfg
        if not names:
            print(f"\n[WARN] No target {label.lower()}(s) configured for {src_type}")
            continue

        resp = requests.get(
            f"{api_base}/workspaces/{workspace_id}/{api_path}",
            headers=fabric_headers(token),
        )
        if resp.status_code != 200:
            print(f"\n[WARN] Could not list {api_path}: {resp.status_code}")
            continue

        ws_items = {i["displayName"]: i for i in resp.json().get("value", [])}

        for name in names:
            item = ws_items.get(name)
            if not item:
                print(f"\n[FAIL] {label} '{name}' NOT found in target workspace!")
                all_ok = False
                continue

            print(f"\n[OK] {label} '{name}' (ID: {item['id']})")

            if can_list and expected_tables:
                tbl_resp = requests.get(
                    f"{api_base}/workspaces/{workspace_id}/{api_path}/{item['id']}/tables",
                    headers=fabric_headers(token),
                )
                if tbl_resp.status_code == 200:
                    available: set[str] = {
                        t["name"] for t in tbl_resp.json().get("data", [])
                    }
                    # Handle pagination
                    cont = tbl_resp.json().get("continuationUri")
                    while cont:
                        pr = requests.get(cont, headers=fabric_headers(token))
                        if pr.status_code == 200:
                            available.update(
                                t["name"] for t in pr.json().get("data", [])
                            )
                            cont = pr.json().get("continuationUri")
                        else:
                            break

                    missing = expected_tables - available
                    if missing:
                        print(
                            f"     MISSING tables: {', '.join(sorted(missing))}"
                        )
                        all_ok = False
                    else:
                        print(f"     All {len(expected_tables)} table(s) present")

    if not all_ok:
        raise Exception("Binding validation failed — see errors above.")


def _verify_ontology(
    token: str,
    api_base: str,
    workspace_id: str,
    ontology_id: str,
    name: str,
    src_entities: list,
    src_rels: list,
    src_bindings: list,
    src_ctx: list,
    target_items_by_type: dict,
    dropped_bindings: list[dict],
    poll_interval: int,
    timeout: int,
) -> dict:
    """Retrieve the new ontology definition and verify counts + bindings."""
    print(f"\nVerifying ontology '{name}' ...")

    # Count how many bindings / contextualizations were intentionally dropped
    dropped_db = sum(1 for d in dropped_bindings if d["part_type"] == "DataBinding")
    dropped_ctx = sum(1 for d in dropped_bindings if d["part_type"] == "Contextualization")

    verify_url = (
        f"{api_base}/workspaces/{workspace_id}"
        f"/ontologies/{ontology_id}/getDefinition"
    )
    resp = requests.post(verify_url, headers=fabric_headers(token))
    new_def = poll_lro(token, resp, poll_interval, timeout)

    new_parts = new_def.get("definition", {}).get("parts", [])
    new_entities = [
        p for p in new_parts
        if p["path"].startswith("EntityTypes/") and p["path"].endswith("/definition.json")
    ]
    new_rels = [
        p for p in new_parts
        if p["path"].startswith("RelationshipTypes/") and p["path"].endswith("/definition.json")
    ]
    new_bindings = [p for p in new_parts if "/DataBindings/" in p["path"]]
    new_ctx = [p for p in new_parts if "/Contextualizations/" in p["path"]]

    expected_bindings = len(src_bindings) - dropped_db
    expected_ctx = len(src_ctx) - dropped_ctx

    print(f"\n{'=' * 60}")
    print(f" VERIFICATION REPORT")
    print(f"{'=' * 60}")
    print(f" {'':30s} {'Source':>8s} {'Dropped':>8s}  {'Created':>8s}")
    print(f" {'-' * 30} {'-' * 8} {'-' * 8}  {'-' * 8}")
    print(f" {'Entity Types':30s} {len(src_entities):>8} {'':>8s}  {len(new_entities):>8}")
    print(f" {'Relationship Types':30s} {len(src_rels):>8} {'':>8s}  {len(new_rels):>8}")
    print(f" {'Data Bindings':30s} {len(src_bindings):>8} {dropped_db:>8}  {len(new_bindings):>8}")
    print(f" {'Contextualizations':30s} {len(src_ctx):>8} {dropped_ctx:>8}  {len(new_ctx):>8}")
    print(f"{'=' * 60}")

    # Check bindings point to target
    ok_count = 0
    wrong_count = 0
    for bp in new_bindings + new_ctx:
        try:
            bd = json.loads(base64.b64decode(bp["payload"]).decode())
            _, _, binfo = _find_binding_dict(bd)
            if binfo:
                ws = binfo.get("workspaceId", "")
                iid = binfo.get("itemId", "")
                st = binfo.get("sourceType", "")
                target_map = target_items_by_type.get(st, {})
                target_ids = {it["id"] for it in target_map.values()} if target_map else set()
                if iid in target_ids and ws == workspace_id:
                    ok_count += 1
                else:
                    wrong_count += 1
        except Exception:
            pass

    if wrong_count == 0 and ok_count > 0:
        print(f"\n[OK] All {ok_count} binding(s) point to target items.")
    elif wrong_count > 0:
        print(
            f"\n[WARN] {wrong_count} binding(s) still point to source! "
            f"{ok_count} correct."
        )
    elif dropped_bindings:
        print(f"\n[INFO] All source bindings were dropped (no targets configured).")
    else:
        print("\n[INFO] No bindings to verify.")

    all_match = (
        len(new_entities) == len(src_entities)
        and len(new_rels) == len(src_rels)
        and len(new_bindings) == expected_bindings
        and len(new_ctx) == expected_ctx
    )

    status = "PASSED" if all_match else "PARTIAL"
    print(f"\nVerification: {status}")

    return {
        "status": status,
        "bindings_ok": ok_count,
        "bindings_wrong": wrong_count,
        "bindings_dropped": len(dropped_bindings),
    }
