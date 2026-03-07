<#
.SYNOPSIS
    Build wheel (.whl) files for the Fabric Ontology Export and Import packages.

.DESCRIPTION
    Builds both packages into the dist/ folder.
    Upload the resulting .whl files to a Fabric lakehouse or environment
    and install them with:
        %pip install /lakehouse/default/Files/wheels/<whl-file>

.EXAMPLE
    .\build_wheels.ps1
#>

$ErrorActionPreference = "Stop"

$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$packagesDir = Join-Path $scriptDir "packages"
$outputDir   = Join-Path $scriptDir "dist"

# Ensure output directory exists
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

# Install build tool if needed
Write-Host "Ensuring 'build' package is installed ..."
pip install build --quiet

# ── Build export package ─────────────────────────────────────────────────
Write-Host "`n=== Building fabric-ontology-export ==="
Push-Location (Join-Path $packagesDir "fabric_ontology_export")
python -m build --wheel --outdir $outputDir
Pop-Location

# ── Build import package ─────────────────────────────────────────────────
Write-Host "`n=== Building fabric-ontology-import ==="
Push-Location (Join-Path $packagesDir "fabric_ontology_import")
python -m build --wheel --outdir $outputDir
Pop-Location

# ── Summary ──────────────────────────────────────────────────────────────
Write-Host "`n=== Wheel files ==="
Get-ChildItem $outputDir -Filter "*.whl" | ForEach-Object {
    Write-Host "  $($_.Name)  ($([math]::Round($_.Length / 1KB, 1)) KB)"
}
Write-Host "`nOutput folder: $outputDir"
Write-Host "Upload these .whl files to your Fabric lakehouse or environment."
