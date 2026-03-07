"""Shared helpers — ABFS-aware file I/O, HTTP headers, LRO polling."""

import json
import os
import time
import requests


# ── ABFS-aware file I/O ─────────────────────────────────────────────────────

def _is_abfs(path: str) -> bool:
    """Return True if the path is an ABFS URI (OneLake / ADLS Gen2)."""
    return path.startswith("abfss://") or path.startswith("abfs://")


def _get_notebookutils():
    """Import notebookutils if running inside a Fabric notebook."""
    try:
        import notebookutils
        return notebookutils
    except ImportError:
        return None


def fs_exists(path: str) -> bool:
    """Check whether a file or folder exists (local or ABFS)."""
    if _is_abfs(path):
        nu = _get_notebookutils()
        if not nu:
            raise RuntimeError("notebookutils is required for ABFS paths but is not available")
        try:
            nu.fs.ls(path)
            return True
        except Exception:
            return False
    return os.path.exists(path)


def fs_read_text(path: str) -> str:
    """Read a text file (local or ABFS)."""
    if _is_abfs(path):
        nu = _get_notebookutils()
        if not nu:
            raise RuntimeError("notebookutils is required for ABFS paths but is not available")
        return nu.fs.head(path, 100 * 1024 * 1024)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def fs_write_text(path: str, content: str) -> None:
    """Write text to a file (local or ABFS), creating parents as needed."""
    if _is_abfs(path):
        nu = _get_notebookutils()
        if not nu:
            raise RuntimeError("notebookutils is required for ABFS paths but is not available")
        nu.fs.put(path, content, True)
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


def fs_mkdirs(path: str) -> None:
    """Create directories (local or ABFS)."""
    if _is_abfs(path):
        nu = _get_notebookutils()
        if not nu:
            raise RuntimeError("notebookutils is required for ABFS paths but is not available")
        nu.fs.mkdirs(path)
    else:
        os.makedirs(path, exist_ok=True)


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def fabric_headers(token: str) -> dict:
    """Build HTTP headers for Fabric API calls."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def poll_lro(
    token: str,
    response: requests.Response,
    poll_interval: int = 10,
    timeout: int = 300,
) -> dict:
    """
    Handle the Fabric Long Running Operation (LRO) pattern.

    - 200/201 → return body directly.
    - 202     → poll the ``Location`` header until *Succeeded*, then GET ``/result``.
    """
    if response.status_code in (200, 201):
        return response.json()

    if response.status_code != 202:
        raise Exception(f"Unexpected status {response.status_code}: {response.text}")

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    retry_after = int(response.headers.get("Retry-After", poll_interval))

    print(f"  LRO started (Operation: {operation_id}). Polling every {retry_after}s ...")

    elapsed = 0
    while elapsed < timeout:
        time.sleep(retry_after)
        elapsed += retry_after

        poll_resp = requests.get(location, headers=fabric_headers(token))
        if poll_resp.status_code != 200:
            raise Exception(f"LRO poll failed: {poll_resp.status_code} – {poll_resp.text}")

        status = poll_resp.json().get("status", "Unknown")
        print(f"  ... {status} ({elapsed}s)")

        if status == "Succeeded":
            result_resp = requests.get(f"{location}/result", headers=fabric_headers(token))
            if result_resp.status_code == 200:
                return result_resp.json()
            # Some LROs (e.g. create) don't have /result — return the poll body
            return poll_resp.json()

        if status in ("Failed", "Cancelled"):
            raise Exception(f"LRO {status}: {json.dumps(poll_resp.json(), indent=2)}")

    raise TimeoutError(f"LRO timed out after {timeout}s")
