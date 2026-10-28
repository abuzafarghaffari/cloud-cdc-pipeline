# Problem Statement
Transactional customer and account records are exported periodically into cloud object storage (Azure Blob / AWS S3). The target warehouse needs continuous synchronization using Change Data Capture (CDC) without reprocessing the entire dataset on every run.

# Cloud Ingestion & CDC Pipeline using Databricks

## Project Overview

Designed and implemented an end-to-end, incremental Customer Change Data Capture (CDC) pipeline using **Databricks, PySpark, Delta Lake, Auto Loader, and Lakeflow Jobs**.

The pipeline ingests customer records from cloud storage, processes inserts, updates, and deletes, applies data quality checks, and generates curated Gold tables for reporting.

## Architecture

**Cloud Storage → Bronze (Auto Loader) → Silver (CDC MERGE) → Data Quality → Gold (Reporting)**

## Technology Stack

- **Processing:** Databricks Serverless, PySpark, Apache Spark
- **Storage:** Unity Catalog Volumes, Delta Lake
- **Ingestion:** Databricks Auto Loader, Structured Streaming
- **CDC:** Delta Lake MERGE INTO, `foreachBatch()`
- **Orchestration:** Databricks Lakeflow Jobs
- **Development:** Python, SQL, VS Code, Databricks CLI, Git

## Key Features

- **Incremental Ingestion:** Auto Loader processes newly arrived CSV files using persistent checkpoints.
- **Medallion Architecture:** Bronze preserves raw data, Silver maintains validated customer records, and Gold provides business aggregations.
- **CDC Processing:** Handles inserts, updates, and deletes using Delta Lake MERGE.
- **Late-Arriving Data:** Uses event timestamps to prevent stale updates from overwriting newer records.
- **Tombstone Management:** Preserves delete events to prevent deleted customers from being unintentionally restored.
- **Data Quality:** Validates customer IDs, timestamps, duplicate records, and CDC consistency.
- **Workflow Orchestration:** Executes Bronze → CDC → Data Quality → Gold with task dependencies.
- **Failure Handling:** Supports retries, timeouts, checkpoint recovery, and controlled failure testing.
- **Reporting:** Generates customer counts and active-customer metrics grouped by membership tier.

## Project Outcome

Built and successfully executed a multi-task Databricks Serverless data pipeline with incremental processing, idempotent CDC merges, data quality validation, and automated Gold reporting.

The solution demonstrates practical data engineering patterns for reliable cloud ingestion, Delta Lake transformations, and production-style workflow orchestration.