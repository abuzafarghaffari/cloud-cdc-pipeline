---
name: cloud-cdc-pipeline
description: Diagnose, modify, and validate this cloud-cdc-pipeline Databricks customer CDC project, including Auto Loader ingestion, Delta merges, quality gates, reporting, and bundle jobs. Use for work in this repository or its job logs.
---

# Work on the customer CDC pipeline

Locate the `cloud-cdc-pipeline` repository using the current workspace or the
user-provided path. Read its root `CLAUDE.md` for the architecture, data
contract, storage paths, prerequisites, job keys, and validation commands.
If the repository is not available, request its location before changing files.
Resolve project files relative to that repository, not the skill installation.

## Select the actual execution path

Read `databricks.yml` and the relevant `resources/*.yml` before diagnosing a
job. Follow the configured Python entrypoint:

- Main workflow: `bronze.py` -> `cdc_stream.py` -> `data_quality.py` -> `gold.py`.
- Earlier snapshot workflow: `silver.py` initializes snapshot tables;
  `cdc_merge.py` processes one named CDC file into snapshot Silver.

Inspect the current source; do not infer that all paths share validation,
target tables, delete behavior, or initialization logic. Streaming CDC needs
the preexisting state table and current view; bootstrap DDL is currently absent.

## Diagnose and change

For a pasted failure, read the complete traceback and match its workspace
file to the local entrypoint. Distinguish a client API error from a data,
authorization, missing-object, or orchestration failure. Inspect adjacent
calls for the same demonstrated defect and apply a focused correction.

Preserve event-time guards, delete precedence and tombstones in the streaming
path, and the Gold dependency on successful quality checks. Invalid streaming
events currently fail the batch; snapshot Quarantine is a separate behavior.
Do not reset checkpoints, recreate state, switch to overwrite, or weaken
validation as a shortcut to a successful run.

Known runtime corrections: use `.schema(customer_schema)` for the reader
schema, `.withColumn()` for column additions, and `.mode("error")` for
create-or-fail writes. This project's Spark Connect client has rejected the
`errorifexists` alias. Check current runtime evidence if a future failure
differs from these cases.

For CDC behavior changes, examine within-batch ordering and the merge
condition together. Consider replay, stale events after deletion, equal-time
delete precedence, and the treatment of incomplete delete payloads.

## Validate and report

Run Python syntax checks and `databricks bundle validate` using the appropriate
explicit profile from `CLAUDE.md`. These checks do not run Spark transformations.
For logic changes, exercise relevant observable CDC cases in an authorized test
environment; do not invent a local Spark setup or claim remote success from
static checks. Deploy or execute remote jobs only within the user's task scope.
Avoid concurrent standalone and orchestrated CDC runs sharing one checkpoint.

Report changed files, the cause and resulting behavior, checks completed,
and remaining runtime or prerequisite gaps. If the task is a local fix,
provide the specific deployment/run commands needed to apply it remotely.
