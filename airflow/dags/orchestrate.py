"""Daily orchestration for the Walmart Databricks + dbt pipeline.

Configure DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_JOB_ID in airflow/.env.
The configured Databricks job must complete Bronze ingestion before dbt starts.
"""

from datetime import timedelta
import os
import time

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT = "/opt/dbt-venv/bin/dbt"
POLL_INTERVAL_SECONDS = 10
MAX_JOB_WAIT_SECONDS = 60 * 60


@dag(
    dag_id="walmart_daily_pipeline",
    description="Trigger Databricks ingestion and run dbt transformations and snapshots.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["walmart", "databricks", "dbt", "portfolio"],
)
def walmart_daily_pipeline():
    @task(task_id="trigger_databricks_ingestion")
    def trigger_databricks_ingestion():
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        job_id = os.getenv("DATABRICKS_JOB_ID")
        if not all([host, token, job_id]):
            raise ValueError(
                "Set DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_JOB_ID "
                "in airflow/.env before running this DAG."
            )

        client = WorkspaceClient(host=host, token=token)
        run = client.jobs.run_now(job_id=int(job_id))
        deadline = time.monotonic() + MAX_JOB_WAIT_SECONDS

        while time.monotonic() < deadline:
            current = client.jobs.get_run(run.run_id)
            state = current.state
            lifecycle = state.life_cycle_state if state else None
            result = state.result_state if state else None

            if lifecycle in {
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            }:
                if lifecycle == RunLifeCycleState.TERMINATED and result == RunResultState.SUCCESS:
                    return f"Databricks job run {run.run_id} completed successfully."
                raise RuntimeError(
                    f"Databricks job run {run.run_id} did not succeed: "
                    f"lifecycle={lifecycle}, result={result}"
                )
            time.sleep(POLL_INTERVAL_SECONDS)

        raise TimeoutError(
            f"Databricks job run {run.run_id} did not finish within "
            f"{MAX_JOB_WAIT_SECONDS} seconds."
        )

    source_freshness = BashOperator(
        task_id="source_freshness",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} source freshness",
    )
    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} run --select silver_t",
    )
    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} test --select silver_t",
    )
    silver_business = BashOperator(
        task_id="silver_business",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} run --select silver_b",
    )
    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} test --select silver_b",
    )
    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} run --select gold/dimensions",
    )
    gold_snapshots = BashOperator(
        task_id="gold_snapshots",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} snapshot",
    )
    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd=DBT_PROJECT_DIR,
        bash_command=f"{DBT} run --select gold/fact",
    )

    ingestion = trigger_databricks_ingestion()
    ingestion >> source_freshness >> silver_technical >> silver_technical_tests
    silver_technical_tests >> silver_business >> silver_business_tests
    silver_business_tests >> gold_dimensions >> gold_snapshots >> gold_facts


walmart_daily_pipeline()
