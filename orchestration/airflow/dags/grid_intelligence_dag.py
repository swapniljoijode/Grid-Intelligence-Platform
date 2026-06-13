"""Grid Intelligence Platform — Airflow DAG skeleton (portable variant, T5-4).

Primary orchestration is Fabric Data Factory. This DAG is the portable
alternative that can run anywhere Airflow is available. Wire task bodies
in T5 once the ingestion and transform modules are built.
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="grid_intelligence_batch",
    description="Batch ingestion → silver → gold — portable Airflow variant",
    schedule="0 * * * *",      # hourly, aligned to EIA cadence
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["grid-intelligence", "batch"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_eia = EmptyOperator(task_id="ingest_eia")          # T1-1
    ingest_carbon = EmptyOperator(task_id="ingest_carbon")    # T1-2
    ingest_weather = EmptyOperator(task_id="ingest_weather")  # T1-3

    silver_transform = EmptyOperator(task_id="silver_transform")  # T3-1
    gold_dbt_build = EmptyOperator(task_id="gold_dbt_build")      # T3-3

    end = EmptyOperator(task_id="end")

    start >> [ingest_eia, ingest_carbon, ingest_weather] >> silver_transform >> gold_dbt_build >> end
