"""Core export logic — retrieve an ontology definition and save it to a lakehouse folder."""

import json
import base64
import os
import requests
from collections import defaultdict

from ._helpers import fabric_headers, poll_lro, fs_write_text, fs_mkdirs, _is_abfs

API_BASE = "https://api.fabric.microsoft.com/v1"


def export_ontology(
    token: str,
    workspace_id: str,
    ontology_id: str,
    ontology_name: str,
    output_path: str,
    *,
    api_base: str = API_BASE,
    poll_interval: int = 10,
    timeout: int = 300,
) -> dict:
    """
    Export a Fabric ontology definition and save to a lakehouse folder.

    Parameters
    ----------
    token : str
        Fabric API access token.
    workspace_id : str
        Source workspace GUID.
    ontology_id : str
        Ontology GUID to export.
    ontology_name : str
        Display name (used for output file naming).
    output_path : str
        Lakehouse Files folder path (local mount or ``abfss://`` URI).
    api_base : str
        Fabric REST API base URL.
    poll_interval / timeout : int
        LRO polling parameters in seconds.

    Returns
    -------
    dict
        ``definition_file``, ``decoded_folder``, ``part_count``, ``summary``,
        and the full ``definition`` payload.
    """

    # ── 1. Validate workspace & ontology ────────────────────────────────────
    print(f"Validating workspace {workspace_id} ...")
    ws_resp = requests.get(
        f"{api_base}/workspaces/{workspace_id}", headers=fabric_headers(token)
    )
    ws_resp.raise_for_status()
    print(f"  Workspace: {ws_resp.json().get('displayName')}")

    print(f"Validating ontology {ontology_id} ...")
    ont_resp = requests.get(
        f"{api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}",
        headers=fabric_headers(token),
    )
    ont_resp.raise_for_status()
    ont_info = ont_resp.json()
    print(f"  Ontology:  {ont_info.get('displayName')}")

    # ── 2. Retrieve definition (LRO) ───────────────────────────────────────
    print(f"\nRetrieving definition ...")
    get_url = f"{api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}/getDefinition"
    resp = requests.post(get_url, headers=fabric_headers(token))
    definition = poll_lro(token, resp, poll_interval, timeout)

    parts = definition.get("definition", {}).get("parts", [])
    print(f"  Retrieved {len(parts)} part(s)")

    # ── 3. Build source-item map ───────────────────────────────────────────
    # Scan bindings for unique (sourceType, itemId) pairs and resolve each
    # to a display name so the import side never needs access to this
    # workspace.
    source_item_map = _build_source_item_map(
        token, api_base, workspace_id, parts,
    )
    if source_item_map:
        definition["_source_item_map"] = source_item_map
        print(f"\n[OK] Embedded source-item map ({len(source_item_map)} item(s))")

    # ── 4. Save raw definition JSON ────────────────────────────────────────
    definition_file = f"{output_path}/{ontology_name}_definition.json"
    decoded_folder = f"{output_path}/{ontology_name}_decoded"

    fs_mkdirs(output_path)
    fs_write_text(definition_file, json.dumps(definition, indent=2))
    print(f"\n[OK] Raw definition saved: {definition_file}")

    # ── 5. Decode each part into a human-readable folder ───────────────────
    fs_mkdirs(decoded_folder)
    for part in parts:
        part_path = part.get("path", "unknown")
        payload = part.get("payload", "")
        payload_type = part.get("payloadType", "")

        if _is_abfs(decoded_folder):
            file_path = f"{decoded_folder}/{part_path}"
        else:
            file_path = os.path.join(decoded_folder, part_path)

        if payload_type == "InlineBase64" and payload:
            try:
                decoded = base64.b64decode(payload).decode("utf-8")
                try:
                    parsed = json.loads(decoded)
                    decoded = json.dumps(parsed, indent=2)
                except json.JSONDecodeError:
                    pass
                fs_write_text(file_path, decoded)
                print(f"  [decoded] {part_path}")
            except Exception as e:
                print(f"  [WARN] Could not decode {part_path}: {e}")
        else:
            fs_write_text(file_path, payload)
            print(f"  [raw] {part_path}")

    # ── 6. Summary ─────────────────────────────────────────────────────────
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

    summary = {
        "total_parts": len(parts),
        "entity_types": len(entity_types),
        "relationship_types": len(rel_types),
        "data_bindings": len(data_bindings),
        "contextualizations": len(contextualizations),
    }

    print(f"\n{'=' * 50}")
    print(f" EXPORT SUMMARY")
    print(f"{'=' * 50}")
    print(f" Ontology:            {ontology_name}")
    print(f" Total parts:         {summary['total_parts']}")
    print(f" Entity Types:        {summary['entity_types']}")
    print(f" Relationship Types:  {summary['relationship_types']}")
    print(f" Data Bindings:       {summary['data_bindings']}")
    print(f" Contextualizations:  {summary['contextualizations']}")
    print(f"{'=' * 50}")
    print(f" Definition file:     {definition_file}")
    print(f" Decoded folder:      {decoded_folder}")
    print(f"{'=' * 50}")

    return {
        "definition_file": definition_file,
        "decoded_folder": decoded_folder,
        "part_count": len(parts),
        "summary": summary,
        "definition": definition,
    }


def _find_binding_dict(decoded_json: dict) -> tuple:
    """
    Recursively search for the dict containing ``sourceType`` +
    ``workspaceId``/``itemId``.

    Returns ``(parent_dict, key_name, binding_dict)`` or
    ``(None, None, None)``.
    """
    for key, val in decoded_json.items():
        if isinstance(val, dict):
            if "sourceType" in val and ("itemId" in val or "workspaceId" in val):
                return (decoded_json, key, val)
    for key, val in decoded_json.items():
        if isinstance(val, dict):
            for k2, v2 in val.items():
                if isinstance(v2, dict):
                    if "sourceType" in v2 and (
                        "itemId" in v2 or "workspaceId" in v2
                    ):
                        return (val, k2, v2)
    return (None, None, None)


def _build_source_item_map(
    token: str,
    api_base: str,
    workspace_id: str,
    parts: list[dict],
) -> dict:
    """
    Scan binding parts for unique ``(sourceType, itemId)`` pairs and
    resolve each to a display name via the Fabric API.

    Returns a dict keyed by item-ID::

        {
            "<itemId>": {
                "displayName": "...",
                "sourceType": "LakehouseTable",
                "workspaceId": "..."
            },
            ...
        }
    """
    SOURCE_TYPE_API = {
        "LakehouseTable": "lakehouses",
        "WarehouseTable": "warehouses",
        "KustoTable": "eventhouses",
        "SemanticModelTable": "semanticModels",
    }

    # Collect unique (sourceType, workspaceId, itemId) triples
    items_to_resolve: dict[str, dict] = {}  # itemId -> {sourceType, workspaceId}
    for part in parts:
        p = part.get("path", "")
        if "/DataBindings/" not in p and "/Contextualizations/" not in p:
            continue
        try:
            decoded = json.loads(base64.b64decode(part["payload"]).decode())
        except Exception:
            continue
        _, _, binfo = _find_binding_dict(decoded)
        if not binfo:
            continue
        item_id = binfo.get("itemId", "")
        src_type = binfo.get("sourceType", "")
        ws_id = binfo.get("workspaceId", workspace_id)
        if item_id and src_type:
            items_to_resolve[item_id] = {
                "sourceType": src_type,
                "workspaceId": ws_id,
            }

    if not items_to_resolve:
        return {}

    # Resolve each item ID to its display name
    result: dict[str, dict] = {}
    for item_id, info in items_to_resolve.items():
        api_path = SOURCE_TYPE_API.get(info["sourceType"])
        if not api_path:
            continue
        ws = info["workspaceId"]
        try:
            resp = requests.get(
                f"{api_base}/workspaces/{ws}/{api_path}/{item_id}",
                headers=fabric_headers(token),
            )
            if resp.status_code == 200:
                name = resp.json().get("displayName", "")
                result[item_id] = {
                    "displayName": name,
                    "sourceType": info["sourceType"],
                    "workspaceId": ws,
                }
                print(f"  Resolved {info['sourceType']} {item_id[:8]}... -> '{name}'")
            else:
                print(
                    f"  [WARN] Could not resolve {info['sourceType']} "
                    f"item {item_id}: {resp.status_code}"
                )
        except Exception as exc:
            print(f"  [WARN] Error resolving {item_id}: {exc}")

    return result
