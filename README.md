# opensource-repo-analytics

GitHub repo analytics pipeline (Bronze / Silver / Gold) for open-source project health metrics.
Phase 2 covers **Raw -> Bronze -> Silver** on Databricks Free Edition (serverless).

Repos analysed: `psf/requests`, `pallets/flask`, `tiangolo/fastapi`, `scikit-learn/scikit-learn` (commits, issues, pull requests).

## Repository layout

```
notebooks/
  00_setup.ipynb              creates schemas (landing, bronze, silver, ops) and the raw volume
  00_logging.ipynb            creates ops.pipeline_execution_logs and the write_log() helper
  00_extract.ipynb            runs scripts/extract_raw.py inside Databricks (full / incremental download)
  01_config_schemas.ipynb     explicit StructType schemas + primary keys
  02_raw_to_bronze.ipynb      Raw -> Bronze (MERGE, quarantine, logging, parameters)
  03_bronze_to_silver.ipynb   Bronze -> Silver (casting, cleaning, PII hashing, MERGE, logging, parameters)
scripts/
  extract_raw.py              resumable GitHub extractor (full + incremental with watermark)
  extract_samples.py          Phase 1 sample generator
raw/                          Phase 1 sample files (full and incremental)
```

## Architecture

```
GitHub REST API
   -> extract_raw.py (00_extract)
   -> /Volumes/workspace/landing/raw/{full|incremental}/{batch_date}/{owner__repo}/{commits|issues|pulls}/page_NNNN.jsonl
   -> 02_raw_to_bronze  -> workspace.bronze.{commits, issues, pulls, quarantine}
   -> 03_bronze_to_silver -> workspace.silver.{commits, issues, pulls}
every run -> workspace.ops.pipeline_execution_logs
```

Raw data is not committed to Git (it lives in the Databricks volume). Small Phase 1 samples are in `raw/`.

Loads in this submission:
- Full load: 1 Oct 2025 to 1 Oct 2026, batch `full/2026-10-01`.
- Incremental load: records created or updated since 1 Oct 2026, batch `incremental/2026-10-10`.

## Data models

All dates in Bronze stay as ISO strings (as received). They are cast to TIMESTAMP in Silver.
Every Bronze and Silver table has `load_timestamp` (TIMESTAMP, time the row was loaded).

### Bronze

Common metadata columns on `bronze.commits`, `bronze.issues`, `bronze.pulls`:
`source_file STRING` (raw file the record came from), `batch_date STRING`, `load_type STRING` (full / incremental), `load_timestamp TIMESTAMP`.

**bronze.commits** - primary key `(repo_name, sha)`

| column | type |
|---|---|
| repo_name | STRING |
| entity_type | STRING |
| sha | STRING |
| commit | STRUCT<author: STRUCT<name, email, date: STRING>, committer: STRUCT<name, email, date: STRING>, message: STRING, comment_count: BIGINT> |
| author | STRUCT<login: STRING, id: BIGINT, type: STRING> |
| committer | STRUCT<login: STRING, id: BIGINT, type: STRING> |

**bronze.issues** (the issues endpoint also returns PR stubs) - primary key `(repo_name, id)`

| column | type |
|---|---|
| repo_name, entity_type | STRING |
| id, number | BIGINT |
| title, state | STRING |
| user | STRUCT<login: STRING, id: BIGINT, type: STRING> |
| labels | ARRAY<STRUCT<id: BIGINT, name: STRING>> |
| comments | BIGINT |
| created_at, updated_at, closed_at | STRING |
| pull_request | STRUCT<url: STRING> (non-null for PRs) |

**bronze.pulls** - primary key `(repo_name, id)`

| column | type |
|---|---|
| repo_name, entity_type | STRING |
| id, number | BIGINT |
| title, state | STRING |
| user | STRUCT<login: STRING, id: BIGINT, type: STRING> |
| created_at, updated_at, closed_at, merged_at | STRING |
| merged | BOOLEAN |
| comments, review_comments, commits, additions, deletions, changed_files | BIGINT |

**bronze.quarantine** - records that do not match the schema contract:
`entity STRING, batch_date STRING, load_type STRING, source_file STRING, raw_record STRING, load_timestamp TIMESTAMP`.

### Silver

Metadata columns on all Silver tables: `batch_date STRING`, `load_type STRING`, `load_timestamp TIMESTAMP`.

**silver.commits** - primary key `(repo_name, sha)`:
`repo_name STRING, sha STRING, commit_message STRING, author_email_hash STRING (SHA-256), committer_email_hash STRING (SHA-256), commit_date TIMESTAMP`.

**silver.issues** - primary key `(repo_name, id)`:
`repo_name STRING, id BIGINT, number BIGINT, title STRING, state STRING, user_login STRING, created_at_ts TIMESTAMP, closed_at_ts TIMESTAMP, updated_at_ts TIMESTAMP, time_to_close_hours DOUBLE, comments BIGINT`.

**silver.pulls** - primary key `(repo_name, id)`:
`repo_name STRING, id BIGINT, number BIGINT, title STRING, state STRING, user_login STRING, created_at_ts TIMESTAMP, closed_at_ts TIMESTAMP, merged_at_ts TIMESTAMP, time_to_merge_hours DOUBLE`.

PII: author and committer emails are hashed (SHA-256) in Silver.

### Operations

**ops.pipeline_execution_logs**: `log_id, layer (raw_to_bronze / bronze_to_silver), entity, parameter (load_type/batch_date/entity), load_type, start_time, end_time, status (SUCCESS / FAILURE), rows_inserted, rows_updated, error_message, load_timestamp`.

## How the requirements are met

- **Explicit schemas:** `01_config_schemas` defines `StructType`/`StructField` schemas; no `inferSchema` anywhere.
- **Idempotency:** both layers use `MERGE INTO` on the primary keys. Re-running the same batch shows `inserted=0 updated=N` and no duplicates.
- **Parameters / backfills:** `02_raw_to_bronze` and `03_bronze_to_silver` take notebook widgets `batch_date`, `load_type`, `entity` (and `raw_root` for Bronze). No hardcoded paths.
- **Schema drift:** Bronze reads in PERMISSIVE mode with a corrupt-record column. Rows whose types no longer match (e.g. an integer becomes a string) go to `bronze.quarantine` and the rest of the batch still loads. Extra source columns outside the contract are ignored without failing; add the field to `01_config_schemas` to capture it (the MERGE uses schema evolution). Bad records are caught at the Bronze boundary, so Silver only receives conforming rows.
- **Logging:** every entity in every run writes one row to `ops.pipeline_execution_logs`, including failures.

## Run guide

All notebooks live in the same Databricks folder. Run `00_setup` and `00_logging` once.

### Standard incremental load
1. In `00_extract`, set the `gh_token` widget and run:
   `run_extract("--mode", "incremental", "--out", "/Volumes/workspace/landing/raw")`
   (uses the saved watermark as `since`; output goes to `incremental/<today>`).
2. `02_raw_to_bronze` widgets: `batch_date = <today, yyyy-mm-dd>`, `load_type = incremental`, `entity = all`. Run all.
3. `03_bronze_to_silver` with the same widget values. Run all.

### Backfill an older period
Set `batch_date` to the folder you want to reprocess and `load_type` to match, for example `batch_date = 2026-10-01`, `load_type = full`, `entity = all`, then run `02_raw_to_bronze` followed by `03_bronze_to_silver`. Nothing is edited in the code, and the MERGE keeps the tables free of duplicates.

## Proof of execution

Notebook outputs are saved in `notebooks/`. Demo video: <add link here>.
