# Architecture and design notes

## Ingestion boundary

The original pipeline used a Databricks Job to ingest from a Ghost-hosted PostgreSQL source into Bronze. That source is being discontinued. The Airflow task therefore treats the Databricks Job ID as configuration and fails with an actionable message when credentials or the Job ID are missing. The repository does not claim that the old source remains available.

## Medallion layers

- **Bronze:** source-aligned tables expected in catalog `walmart`, schema `bronze`.
- **Silver technical (`silver_t`):** incremental models for six entities, using `updated_timestamp` and merge keys.
- **Silver business (`silver_b`):** wide order-item model `obt_b`, with `ref()` dependencies so dbt can build lineage.
- **Gold dimensions:** dimension-shaped views used as stable snapshot inputs.
- **Gold history:** dbt timestamp snapshots implement SCD Type 2 history.
- **Gold fact:** `fact_orders` built from the wide business model.

## Orchestration sequence

1. Trigger Databricks ingestion Job and poll until terminal state.
2. Run dbt source freshness.
3. Build Silver technical models.
4. Test Silver technical models.
5. Build and test Silver business model.
6. Build Gold dimension base views.
7. Run dbt snapshots.
8. Build Gold fact table.

The DAG is scheduled daily, disables historical catchup, limits concurrent runs to one, and retries tasks once.

## Design caveats to validate

1. Configure source freshness using a real ingestion timestamp and appropriate thresholds.
2. Validate the `obt_b` join grain. Joining employees to orders by store can multiply rows if multiple employees belong to a store.
3. Validate incremental cursor semantics for late-arriving records and equal timestamps.
4. Confirm all five snapshots compile and run against the selected Databricks adapter and workspace.
5. Confirm the Databricks Job completes Bronze ingestion before the downstream dbt tasks begin.
