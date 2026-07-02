# Overview

The goal of this project is to bring together spending data from a variety of sources — banks, retailers, and other accounts — into a single, consistent Gold-layer dataset that can be pulled into Power BI for reporting. The intent is to be able to answer questions like *how much am I spending, on what, when, and broken out by category* across all of my accounts in one place, rather than having to look at each source individually.

To get there, the pipeline ingests raw source files (e.g., a bank's transaction export, a retailer's order history), validates and stages them, cleanses and models them into a dimensional (star) schema, and ultimately produces analytics-ready Gold-layer tables suitable for direct consumption by Power BI. Each stage is driven by configuration files rather than per-source code, so that adding a new data source — a new bank or retailer — is (in principle) a matter of adding configuration rows rather than writing new ingestion logic.

## Medallion Data Pipeline

A configuration-driven ETL pipeline written in Python that moves data through a **Bronze → Silver → Gold** (medallion) architecture, with schema validation, transformation, and dimensional modeling handled largely through CSV-based configuration rather than hard-coded logic.

> **Status: Work in progress.** This project is under active development. Core scaffolding (folder structure, configuration retrieval, logging) is in place, but several transformation and modeling functions are incomplete or being actively reworked. See [Project Status](#project-status) below for details.

### The three layers

| Layer | Purpose |
|---|---|
| **Bronze** | Raw, minimally-validated data as received from source. Files are checked for a matching column header configuration and either staged for Silver processing or routed to an Error folder. |
| **Silver** | Cleansed, typed, and de-duplicated data, transformed according to per-column rules (direct copy, expression, or dimension lookup) into dimension and fact tables. |
| **Gold** | Curated, analytics-ready dimensional model (e.g., Date dimension, and eventually spending/transaction facts joined to conformed dimensions like Brand, Category, and Seller) intended to be consumed directly by Power BI for spend reporting. |

## Repository Structure

```
.
├── LoadFileToBronze.py      # Entry point: ingest raw files into Bronze
├── LoadBronzeToSilver.py    # Entry point: transform Bronze data into Silver
├── LoadSilverToGold.py      # Entry point: populate Gold-layer dimensions/facts
├── Utilities.py             # Shared library: logging, config retrieval, file I/O, transformations
├── Configuration.File.csv                             # File-level config (one row per source file type)
├── Configuration.Column.csv                           # Column-level config (mapping/transformation rules)
├── Configuration.BrandCategoryProductServiceSeller.csv # Bridge/mapping table for source-value lookups
└── Log.txt                  # Pipe-delimited execution log (generated/appended at runtime)
```

At runtime, the scripts also expect (and will create, if missing) a data folder structure alongside the repo root:

```
<Root>/
├── Admin/                  # Configuration files, data dictionary, Log.txt
├── Documentation/          # Data dictionary and related docs
├── Bronze/
│   ├── Inbound/<Source>/   # Raw files land here, organized by source
│   ├── Archive/            # Successfully processed Bronze files
│   ├── Error/               # Files that failed validation
│   └── ToMap/               # Records awaiting manual mapping (e.g., unmapped Brand/Category/Seller values)
├── Silver/
│   ├── Inbound/<Source>/   # Staged, validated files awaiting Bronze→Silver transformation
│   ├── Dimension/          # Silver dimension tables
│   ├── Facts/               # Silver fact tables
│   └── Error/
└── Gold/
    ├── Inbound/
    ├── Dimensions/          # Gold dimension tables (e.g., Date)
    ├── Facts/
    └── Error/
```

# How It Works

## 1. `LoadFileToBronze.py`
Scans the `Bronze/Inbound/<Source>/` folders (or a single specified source folder), validates each file's column header against the configuration for that source, and either:
- Moves valid files to `Silver/Inbound/<Source>/`, or
- Renames and moves invalid files to `Bronze/Error/<Source>/` with the validation issue embedded in the filename.

## 2. `LoadBronzeToSilver.py`
For each configured source, reads staged files, applies cleansing (type coercion, expression-based derived columns) via `Utilities.CleanseData`, and models the result into Silver dimension and fact tables. Also ensures the Date dimension is populated for the past several years before any other processing runs, so downstream date lookups always have a target.

## 3. `LoadSilverToGold.py`
Populates Gold-layer dimensions from Silver data. Currently implements Date dimension population; additional dimension and fact loading (ultimately producing the spend-by-category-and-time facts that Power BI reports will be built on) is planned (see [Project Status](#project-status)).

## `Utilities.py`
Shared functionality used by all three entry-point scripts, including:
- **Logging** — every function call is logged with a unique `ExecutionGUID`, a `ParentExecutionGUID` linking it to its caller, and a full call stack, enabling end-to-end tracing of a single pipeline run through `Log.txt`.
- **Configuration retrieval** — loads and validates `Configuration.File.csv` and `Configuration.Column.csv` once per run, creating them from a built-in definition if they don't yet exist.
- **File/folder management** — folder creation, file moves, and column-header validation shared across all layers.
- **Transformation** — data cleansing and type conversion driven by the column-level configuration.

## Configuration-Driven Design

Rather than writing bespoke ingestion code per source, the pipeline is intended to be driven by a small set of configuration files:

- **`Configuration.File.csv`** — one row per source file type: expected delimiter, text qualifier, associated account, and a link to its column-level configuration (`ConfigurationFileID`).
- **`Configuration.Column.csv`** — one row per source column: how it maps to a Bronze/Silver column, its expected datatype, and its transformation rule (`Direct`, `Expression`, or `Lookup`) for moving from Bronze to Silver.
- **`Configuration.BrandCategoryProductServiceSeller.csv`** — a bridge table mapping raw source values to conformed dimension keys (Brand, Category, ProductService, Seller) per account.
- **`DataDictionary.csv`** *(generated under `Admin/`)* — the master definition of every entity (log schema, configuration schemas, Silver dimensions/facts) that `Utilities.SetGlobalVariables` reads at startup to build all other schema definitions.

The goal is that onboarding a new data source becomes primarily a configuration exercise: add a row to `Configuration.File.csv`, add its columns to `Configuration.Column.csv`, and drop files into the appropriate `Bronze/Inbound/<Source>/` folder.

## Logging

Every run produces structured log entries appended to `Admin/Log.txt` (pipe-delimited), including:

```
ExecutionGUID | ParentExecutionGUID | Begin | End | Severity | Caller | CallStack | Action | RowCount | Source | Target | Result | File | Parameters
```

`ExecutionGUID`/`ParentExecutionGUID` pairs let you reconstruct the full call tree for a single pipeline run — from the top-level `Main()` invocation down through every folder, file, and transformation step it touched — which is useful for debugging failed runs or auditing what happened to a specific source file.

## Project Status

This project is actively being developed and should be treated as **pre-production**.
