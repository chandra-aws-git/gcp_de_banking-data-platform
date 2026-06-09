import logging
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from banking_config import DEFAULT_ARGS, ENV, PROJECT_ID, REGION, RUNTIME_SERVICE_ACCOUNT, TEMP_BUCKET


CLUSTER_NAME = "dev-dataproc-{{ ds_nodash }}"

composer_bucket = os.environ["GCS_BUCKET"]
PYSPARK_MAIN = f"gs://{composer_bucket}/data/dataproc/bronze_gcs_to_bq.py"
logging.info("PYSPARK_MAIN = %s", PYSPARK_MAIN)

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": 50,
        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": 100,
        },
    },
    "software_config": {
        "image_version": "2.1-debian11",
    },
    **(
        {
            "gce_cluster_config": {
                "service_account": RUNTIME_SERVICE_ACCOUNT,
                "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        }
        if RUNTIME_SERVICE_ACCOUNT
        else {}
    ),
    "lifecycle_config": {
        "auto_delete_ttl": {"seconds": 7200},
    },
}


with DAG(
    dag_id="banking_bronze_dag",
    description="Ephemeral Dataproc cluster for PySpark ingestion",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dataproc", "pyspark", "gcs", "bigquery"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_MAIN,
                "args": [
                    "--project-id", PROJECT_ID,
                    "--bq-temp-bucket", TEMP_BUCKET,
                    "--env", ENV,
                ],
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    create_cluster >> submit_pyspark >> delete_cluster
