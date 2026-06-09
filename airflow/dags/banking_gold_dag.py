import os

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pendulum import datetime

from banking_config import BQ_LOCATION, DEFAULT_ARGS, ENV, PROJECT_ID


COMPOSER_BUCKET = os.environ["GCS_BUCKET"]
GOLD_SQL_PREFIX = "data/bigquery/gold"


def list_sql_files(bucket_name: str, prefix: str) -> list[dict[str, str]]:
    gcs = GCSHook()
    objects = gcs.list(bucket_name=bucket_name, prefix=prefix)
    sql_files = [
        {"file": object_name.rsplit("/", 1)[-1], "object_name": object_name}
        for object_name in objects
        if object_name.endswith(".sql")
    ]
    return sorted(sql_files, key=lambda item: item["file"])


def run_sql_file(bucket_name: str, object_name: str, layer: str) -> None:
    gcs = GCSHook()
    bq = BigQueryHook(use_legacy_sql=False, location=BQ_LOCATION)
    sql = gcs.download(bucket_name=bucket_name, object_name=object_name).decode("utf-8")
    bq.insert_job(
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
            },
            "labels": {
                "layer": layer,
                "domain": "banking",
                "env": ENV,
            },
        },
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
    )


with DAG(
    dag_id="banking_gold_dag",
    description="Silver to Gold transformations using BigQuery SQL",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "gold", "bigquery", "analytics"],
) as dag:
    @task
    def discover_sql_files() -> list[dict[str, str]]:
        sql_files = list_sql_files(COMPOSER_BUCKET, GOLD_SQL_PREFIX)
        if not sql_files:
            raise ValueError("No SQL files found for gold layer")
        return sql_files

    @task
    def execute_sql(sql_file: dict[str, str]) -> None:
        run_sql_file(COMPOSER_BUCKET, sql_file["object_name"], "gold")

    execute_sql.expand(sql_file=discover_sql_files())
