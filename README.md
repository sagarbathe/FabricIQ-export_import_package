# FabricIQ — Ontology Export & Import Package

Export and import [Microsoft Fabric](https://learn.microsoft.com/fabric/) **Ontology** definitions across workspaces and tenants using the Fabric REST API.

## Purpose

This repo is designed to **export an existing ontology and create a new ontology based on it**. It helps you:

- **Share ontologies** with other users — export from one workspace/tenant and import into another
- **Create a copy** of an existing ontology and make changes, instead of building from scratch
- **Work with your own data** — unlike other repos that create ontologies from sample datasets, this solution lets you use your own data to build and share ontologies

## Overview

| Notebook | Purpose |
|---|---|
| **`export_ontology.ipynb`** | Retrieve an ontology definition from a source workspace and save it to a lakehouse `Files` folder |
| **`import_ontology.ipynb`** | Read the exported definition, rewrite data-source bindings to target items, and create a new ontology in a target workspace |

### How it works

```
Source Workspace                          Target Workspace
┌─────────────────┐    definition.json    ┌─────────────────┐
│  ont_UBI01      │ ──── export ────────► │  lakehouse Files │
│  (ontology)     │                       │                  │
└─────────────────┘                       └────────┬─────────┘
                                                   │ import
                                                   ▼
                                          ┌─────────────────┐
                                          │  ont_UBI02      │
                                          │  (new ontology) │
                                          └─────────────────┘
```

1. **Export** calls `getDefinition` on the source ontology, embeds a **source-item map** (display names of every referenced data-source item), and writes `<name>_definition.json` plus a human-readable `<name>_decoded/` folder.
2. **Import** reads the definition JSON, rewrites binding payloads to point at the configured target items (lakehouses, warehouses, eventhouses), and creates the ontology via the Fabric API. Bindings that cannot be rewritten are **dropped** and clearly reported as unbound.

> **Cross-tenant support:** The source-item map is embedded at export time, so the import notebook **does not need access to the source workspace or tenant**.

---

## ⚠️ Prerequisites & Dependencies

### 1. Target tables must have the **same names** as source tables

The import rewrites `workspaceId` and `itemId` in each binding, but **`sourceTableName` is preserved as-is**. This means:

- **Lakehouse delta tables** in the target lakehouse must exist with the **exact same names** as in the source lakehouse.
- **Warehouse tables** in the target warehouse must match by name.
- **KQL tables** in the target eventhouse / KQL database must match by name.

> **If a table does not exist** in the target item with the expected name, the binding will fail at runtime even though the ontology is created successfully.

### 2. Target items must be pre-created

Before running the import notebook, ensure the target Fabric items exist in the target workspace:

| Source binding type | Target item to create | Configuration parameter |
|---|---|---|
| `LakehouseTable` | Lakehouse (with matching delta tables) | `TARGET_LAKEHOUSE_NAMES` |
| `WarehouseTable` | Warehouse (with matching tables) | `TARGET_WAREHOUSE_NAMES` |
| `KustoTable` | Eventhouse (with matching KQL tables) | `TARGET_EVENTHOUSE_NAMES` |

### 3. Unbound bindings are dropped, not silently kept

If no target item is configured for a source binding type (e.g., no eventhouse specified but the source ontology has KQL bindings), those bindings are **removed**. The import notebook prints a prominent **`UNBOUND BINDINGS — ACTION REQUIRED`** summary listing every dropped binding with its owner entity, source type, table name, and reason.

You can fix unbound entities by either:
- Adding the missing target item to the configuration and re-running the import
- Manually binding them in the Fabric UI after import

### 4. Multi-item mapping requires matching display names

When you configure **multiple** target items of the same type (e.g., two lakehouses), the import uses the **source item display name** (from the embedded source-item map) to match each binding to the correct target item. Ensure target item display names match the source names, or use a single target item to map all bindings.

---

## Repository Structure

```
├── README.md
├── export_ontology.ipynb                          # Fabric notebook — export workflow
├── import_ontology.ipynb                          # Fabric notebook — import workflow
└── dist/
    ├── fabric_ontology_export-1.0.0-py3-none-any.whl   # Pre-built export package
    └── fabric_ontology_import-1.0.0-py3-none-any.whl   # Pre-built import package
```

---

## Quick Start

### 1. Upload wheel files to your Fabric lakehouse

Download both `.whl` files from the `dist/` folder and upload them to a `Files` folder in your lakehouse (e.g., `Files/FabricIQ-export_import_package/`).

### 2. Export an ontology

1. Open `export_ontology.ipynb` in a Fabric notebook
2. Install the export wheel:
   ```python
   %pip install /lakehouse/default/Files/FabricIQ-export_import_package/fabric_ontology_export-1.0.0-py3-none-any.whl
   ```
3. Set `WORKSPACE_ID`, `ONTOLOGY_ID`, `ONTOLOGY_NAME`, and `OUTPUT_PATH`
4. Run all cells

**Output:**
- `<OUTPUT_PATH>/<ONTOLOGY_NAME>_definition.json` — feed this to the import notebook
- `<OUTPUT_PATH>/<ONTOLOGY_NAME>_decoded/` — human-readable decoded parts (for reference)

### 3. Import into a target workspace

1. Copy `*_definition.json` to the target lakehouse `Files` folder
2. Open `import_ontology.ipynb` in a Fabric notebook
3. Install the import wheel:
   ```python
   %pip install /lakehouse/default/Files/FabricIQ-export_import_package/fabric_ontology_import-1.0.0-py3-none-any.whl
   ```
4. Configure:
   - `DEFINITION_PATH` — path to the exported JSON
   - `TARGET_WORKSPACE_ID` — target workspace GUID
   - `NEW_ONTOLOGY_NAME` — display name for the new ontology
   - `TARGET_LAKEHOUSE_NAMES`, `TARGET_WAREHOUSE_NAMES`, `TARGET_EVENTHOUSE_NAMES` — target item names for binding rewrite
5. Run all cells

---

## Configuration Reference

### Export Notebook

| Parameter | Description |
|---|---|
| `WORKSPACE_ID` | Source workspace GUID |
| `ONTOLOGY_ID` | Source ontology GUID |
| `ONTOLOGY_NAME` | Display name (used for file naming) |
| `OUTPUT_PATH` | Lakehouse `Files` folder path (`abfss://` URI) |

### Import Notebook

| Parameter | Description |
|---|---|
| `DEFINITION_PATH` | Full path to `*_definition.json` (`abfss://` URI) |
| `TARGET_WORKSPACE_ID` | Target workspace GUID |
| `NEW_ONTOLOGY_NAME` | Display name for the new ontology |
| `DESCRIPTION` | Optional description |
| `TARGET_LAKEHOUSE_NAMES` | List of lakehouse names for binding rewrite |
| `TARGET_WAREHOUSE_NAMES` | List of warehouse names for binding rewrite |
| `TARGET_EVENTHOUSE_NAMES` | List of eventhouse names for binding rewrite |
| `OVERWRITE_IF_EXISTS` | Delete existing ontology with same name before creating |
| `SKIP_BINDING_VALIDATION` | Skip pre-check that target items/tables exist |

---

## Python Dependencies

| Dependency | Version | Notes |
|---|---|---|
| Python | ≥ 3.10 | Required |
| `requests` | ≥ 2.28 | HTTP client for Fabric REST API |
| `setuptools` | ≥ 68.0 | Build system |
| `notebookutils` | (Fabric built-in) | Used for ABFS file I/O and token retrieval — available automatically in Fabric notebooks |

---

## Supported Binding Types

| Fabric Item | Binding `sourceType` | Binding Rewrite | Table Name Match Required |
|---|---|---|---|
| Lakehouse | `LakehouseTable` | ✅ `workspaceId` + `itemId` | ✅ Delta table names must match |
| Warehouse | `WarehouseTable` | ✅ `workspaceId` + `itemId` | ✅ Table names must match |
| Eventhouse / KQL DB | `KustoTable` | ✅ `workspaceId` + `itemId` + `clusterUri` | ✅ KQL table names must match |

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'fabric_ontology_export'` | Wheel not installed or wrong filename | Run `%pip install` with the correct `.whl` path |
| `UNBOUND BINDINGS — ACTION REQUIRED` | No target item configured for a source binding type | Add the missing target item name to the configuration |
| `Binding validation failed` | Target item doesn't exist or is missing tables | Create the target item and ensure tables match source names |
| `notebookutils is required for ABFS paths` | Running outside a Fabric notebook | Use local file paths or run inside a Fabric notebook |
| `LRO timed out` | Large ontology or slow API response | Increase `timeout` parameter |

---

## License

This project is provided as-is for use with Microsoft Fabric.
