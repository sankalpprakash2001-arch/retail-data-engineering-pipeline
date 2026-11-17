
FROM apache/airflow:2.9.3

# Create a separate environment for dbt
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/dbt-venv \
    && chown -R airflow:root /opt/dbt-venv

USER airflow

# Install Databricks SDK in Airflow's environment
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    "databricks-sdk==0.49.0"

# Install dbt and its dependencies in the isolated environment
COPY requirements.txt /requirements-dbt.txt

RUN /opt/dbt-venv/bin/pip install --no-cache-dir \
    -r /requirements-dbt.txt
