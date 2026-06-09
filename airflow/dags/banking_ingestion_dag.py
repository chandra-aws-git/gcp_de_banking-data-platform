import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from pendulum import datetime

from banking_config import (
    DEFAULT_ARGS,
    PROJECT_ID,
    REGION,
    RUNTIME_SERVICE_ACCOUNT,
    STAGING_LOCATION,
    TEMP_LOCATION,
    get_setting,
)


composer_bucket = os.environ["GCS_BUCKET"]
GCS_PYTHON_SCRIPT = f"gs://{composer_bucket}/data/dataflow/cloudsql_cdc_pipeline.py"
logging.info("GCS_PYTHON_SCRIPT = %s", GCS_PYTHON_SCRIPT)

INSTANCE_CONNECTION_NAME = get_setting(
    "banking_cloudsql_instance_connection_name",
    f"{PROJECT_ID}:{REGION}:mysql-instance",
)
DB_NAME = get_setting("banking_cloudsql_database", "banking_db")
DB_USER = get_setting("banking_cloudsql_user", "myuser")
DB_PASSWORD_SECRET = get_setting("banking_cloudsql_password_secret", "banking-cloudsql-password")
GCS_RAW_BASE = get_setting("banking_cdc_raw_base", "gs://banking-raw-dev-100/cloudsql/")


with DAG(
    dag_id="banking_ingestion_dag",
    description="Cloud SQL to GCS CDC using Apache Beam on Dataflow",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "cdc", "dataflow"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="cloudsql_cdc_dataflow_job",
        runner=BeamRunnerType.DataflowRunner,
        py_file=GCS_PYTHON_SCRIPT,
        py_interpreter="python3",
        py_system_site_packages=True,
        py_options=[],
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
            "save_main_session": True,
            "project_id": PROJECT_ID,
            "instance_connection_name": INSTANCE_CONNECTION_NAME,
            "db_name": DB_NAME,
            "db_user": DB_USER,
            "db_password_secret": DB_PASSWORD_SECRET,
            "gcs_raw_base": GCS_RAW_BASE,
            **(
                {"service_account_email": RUNTIME_SERVICE_ACCOUNT}
                if RUNTIME_SERVICE_ACCOUNT
                else {}
            ),
        },
        py_requirements=[],
        dataflow_config={
            "location": REGION,
            "job_name": "cloudsql-cdc-{{ ds_nodash }}",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_dataflow >> end
