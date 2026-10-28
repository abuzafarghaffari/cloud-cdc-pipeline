# Cloud CDC pipeline

## Project and runtime

This repository processes customer CSV exports with Databricks Serverless,
PySpark, Auto Loader, Delta Lake, and Unity Catalog. `databricks.yml` defines
the `cloud-cdc-pipeline` bundle and a default development target, `dev`.
Job resources live in `resources/*.yml` and use environment version `2`.
Use the source and bundle definitions as the authority when this guide drifts.

The main workflow is:

```text
bronze.py -> cdc_stream.py -> data_quality.py -> gold.py
```

`resources/customer_pipeline_job.yml` defines the dependencies, timeouts,
and retries. Gold runs only after the data quality task succeeds.
The workflow has a maximum of one concurrent run.

## Source map

| File | Responsibility |
| --- | --- |
| `src/bronze.py` | Ingest headered CSVs with Auto Loader; append raw strings and ingestion metadata to Bronze. |
| `src/cdc_stream.py` | Process incremental Bronze microbatches, validate events, deduplicate, and merge persistent customer state with tombstones. |
| `src/data_quality.py` | Check state IDs, uniqueness, timestamps, tombstones, and current-view row counts; raise on failure. |
| `src/gold.py` | Read the current-customer view and overwrite tier aggregates. |
| `src/silver.py` | Earlier snapshot path: initialize Silver and Quarantine only when absent. Existing tables are skipped. |
| `src/cdc_merge.py` | Earlier batch path: process only `customers_batch_2_cdc.csv` from Bronze and merge into snapshot Silver with physical deletes. |
| `connection_test.py` | Remote Spark connectivity smoke check. |

Do not interchange snapshot Silver and streaming state. The main workflow uses
`customer_state_v2` and `silver_customers_current`, not `silver_customers`.

## Data contract and prerequisites

All table names below are qualified under `dev.customer_cdc`.

| Object | Producer or prerequisite |
| --- | --- |
| `bronze_customers` | Bronze ingestion appends records. |
| `customer_state_v2` | Must already exist as a Delta table; streaming CDC merges into it. |
| `silver_customers_current` | Must already exist as a view exposing nondeleted state; quality checks and Gold read it. |
| `gold_customer_tiers` | Gold overwrites `tier`, `total_customers`, `active_customers`, and `refreshed_at`. |
| `silver_customers`, `quarantine_customers` | Created by the earlier Silver snapshot script when absent. |

The repository does not currently include bootstrap DDL for the state table or
current view. Do not assume that deploying the bundle creates these objects.
The catalog, schema, volumes, and appropriate workspace permissions are also
prerequisites.

Input CSV columns, in schema order:
`customer_id`, `name`, `email`, `tier`, `status`, `updated_at`, `operation`.
Bronze keeps these as nullable strings and adds `ingestion_time` and
`source_file` from `_metadata.file_path`.

Streaming CDC converts customer IDs to integers and timestamps with
`yyyy-MM-dd HH:mm:ss` using tolerant parsing. IDs must be positive; timestamps
must parse; operations are `I`, `U`, or `D`. Inserts and updates require a name,
valid email, a tier in `Bronze/Silver/Gold/Platinum`, and a status in
`Active/Inactive`. Deletes require the ID, timestamp, and operation but may
omit customer attributes. Invalid streaming events fail the entire microbatch;
they are not written to Quarantine by this path.

The state table needs the cleaned customer columns, ingestion metadata, and
boolean `is_deleted`. Within a microbatch, newest event time wins; a delete
wins at equal event time, followed by ingestion time and source file.
The merge updates existing state only for a newer event or an equal-time
delete replacing a nondelete. Keep tombstones so stale events cannot restore
deleted customers. A newer insert or update can restore a deleted customer.

## Persistent storage

| Purpose | Path |
| --- | --- |
| Incoming CSVs | `/Volumes/dev/customer_cdc/landing/incoming/` |
| Auto Loader schema | `/Volumes/dev/customer_cdc/pipeline_state/bronze_schema/` |
| Bronze checkpoint | `/Volumes/dev/customer_cdc/pipeline_state/bronze_checkpoint/` |
| CDC checkpoint | `/Volumes/dev/customer_cdc/pipeline_state/cdc_v2_checkpoint` |

Both streaming jobs use `availableNow=True` and wait for completion; these
are bounded incremental runs. Preserve checkpoint locations during ordinary
fixes. A reset or replay changes processing history and needs an intentional
recovery plan. The standalone CDC job and main workflow share the same CDC
checkpoint; do not run those consumers concurrently.

## Development and validation

Use PowerShell from the repository root. This workspace has multiple profiles
matching its host; `abuzafarghaffari` was successfully used for validation.
Use an explicit profile and select the appropriate one in another environment.

Parse Python without importing Spark or running pipeline code:

```powershell
python -c "import ast; from pathlib import Path; paths = [Path('connection_test.py'), *Path('src').glob('*.py')]; [ast.parse(p.read_text(encoding='utf-8-sig'), filename=str(p)) for p in paths]; print('Python syntax OK')"
databricks bundle validate -t dev --profile abuzafarghaffari
```

When deployment and execution are within the requested task:

```powershell
databricks bundle deploy -t dev --profile abuzafarghaffari
databricks bundle run customer_pipeline_job -t dev --profile abuzafarghaffari
```

Standalone job keys are `connection_test_job`, `bronze_ingestion_job`,
`silver_transformation_job`, `customer_cdc_merge_job`,
`customer_cdc_stream_job`, and `customer_gold_job`. Use resource keys with
`bundle run`, not display names. A deployment uploads bundle changes;
running a job can change remote data and consume compute. Creating guidance
files does not itself authorize deployment or execution.

There is no automated test suite or dependency manifest in this repository.
Syntax checks and bundle validation do not demonstrate remote Spark success.
For CDC logic changes, verify insert, newer update, stale update, delete,
equal-time delete, late event after deletion, replay, and invalid-event
behavior in an appropriate test environment. Report which checks actually ran.
`data_quality.py --force-failure` intentionally raises after successful checks;
use it only for an explicitly requested failure-path exercise.

## Editing and troubleshooting

- Trace a reported failure from the job resource to the Python entrypoint and
  failing line before editing. Keep unrelated working-tree changes intact.
- Preserve the quality gate, event-time merge guards, delete semantics,
  append-only Bronze input, and checkpoint continuity unless the task changes
  those behaviors deliberately.
- For this project's Spark Connect runtime, use `.mode("error")` for
  create-or-fail writes. `.mode("errorifexists")` previously failed in client
  serialization. Do not replace it with overwrite to hide an error.
- Supply a schema through `.schema(customer_schema)`; `.option()` takes a
  key and value. DataFrame column additions use `.withColumn()`.
- In snapshot scripts, direct casts and `to_timestamp` can raise on malformed
  input under ANSI settings before validation runs; streaming CDC already
  uses tolerant parsing. Preserve this distinction when diagnosing failures.
- Keep credentials out of source. `.env`, `.databricks`, local incoming data,
  and virtual environments are ignored; do not copy their contents into docs.
- Do not describe the existing current-view count check as a full content
  equality check: it compares counts only.

The reusable project skill is `.claude/skills/cloud-cdc-pipeline/SKILL.md`.
