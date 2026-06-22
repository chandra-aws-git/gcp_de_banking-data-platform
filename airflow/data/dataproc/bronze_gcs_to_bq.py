import argparse
import logging
import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime
import traceback

# =====================================================
# 2. GLOBAL CONFIGURATION
# =====================================================

# GCP Project
PROJECT_ID = "dev-banking-2026-499415"

# BigQuery datasets
BRONZE_DATASET = "banking_bronze"
METADATA_DATASET = "banking_metadata"

# Temporary GCS bucket for BigQuery connector
BQ_TEMP_BUCKET = "banking-temp-dev-bkt"

# Environment details
ENV = "DEV"
RUN_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")

# =====================================================
# 3. EXPLICIT SCHEMAS (NO SCHEMA DRIFT)
# =====================================================
# Each source table must have a predefined schema.
# Any new column in source must be added here explicitly.

SCHEMA_MAP = {
    "customers": StructType(
        [
            StructField("customer_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("bronze_load_ts", TimestampType(), True),
        ]
    ),
    "accounts": StructType(
        [
            StructField("account_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("account_type", StringType(), True),
            StructField("balance", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("status", StringType(), True),
            StructField("opened_date", DateType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("bronze_load_ts", TimestampType(), True),
        ]
    ),
    "transactions": StructType(
        [
            StructField("transaction_id", LongType(), True),
            StructField("account_id", LongType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_ts", TimestampType(), True),
            StructField("channel", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("bronze_load_ts", TimestampType(), True),
        ]
    ),
}

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(description="Load raw CDC parquet files into bronze BigQuery tables.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bq-temp-bucket", required=True)
    parser.add_argument("--env", default="dev")
    return parser.parse_args()


def write_audit_log(spark, audit_record, bq_temp_bucket):
    audit_df = spark.createDataFrame([audit_record], schema=AUDIT_SCHEMA)
    (
        audit_df.write.format("bigquery")
        .option("table", f"{METADATA_DATASET}.ingestion_audit_log")
        .option("temporaryGcsBucket", bq_temp_bucket)
        .mode("append")
        .save()
    )


def process_table(spark, row, run_id, bq_temp_bucket):
    source_table = row.source_table
    target_table = row.target_table
    primary_key = row.primary_key
    watermark_col = row.watermark_column
    source_path = row.source_path
    start_ts = datetime.now(timezone.utc)

    audit_record = {
        "run_id": run_id,
        "source_table": source_table,
        "target_table": target_table,
        "status": "STARTED",
        "records_read": 0,
        "records_written": 0,
        "start_ts": start_ts,
        "end_ts": None,
        "error_message": None,
    }

    try:
        logging.info("Starting Bronze ingestion for source_table=%s", source_table)
        if source_table not in SCHEMA_MAP:
            raise ValueError(f"Schema not defined for table: {source_table}")
        if not primary_key or not watermark_col:
            raise ValueError("Primary key or watermark column missing in metadata")

        df = spark.read.schema(SCHEMA_MAP[source_table]).parquet(source_path)
        records_read = df.count()
        audit_record["records_read"] = records_read
        if records_read == 0:
            raise ValueError(f"Source dataset is empty: {source_path}")

        window_spec = Window.partitionBy(primary_key).orderBy(col(watermark_col).desc())
        bronze_df = (
            df.withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
            .withColumn("bronze_load_ts", current_timestamp())
        )

        records_written = bronze_df.count()
        audit_record["records_written"] = records_written

        (
            bronze_df.write.format("bigquery")
            .option("table", f"{BRONZE_DATASET}.{target_table}")
            .option("temporaryGcsBucket", bq_temp_bucket)
            .mode("append")
            .save()
        )

        audit_record["status"] = "SUCCESS"
        audit_record["end_ts"] = datetime.now(timezone.utc)
        logging.info("Completed Bronze ingestion for source_table=%s", source_table)
        return True
    except Exception as exc:
        audit_record["status"] = "FAILED"
        audit_record["end_ts"] = datetime.now(timezone.utc)
        audit_record["error_message"] = str(exc)
        logging.error("Failed Bronze ingestion for source_table=%s\n%s", source_table, traceback.format_exc())
        return False
    finally:
        write_audit_log(spark, audit_record, bq_temp_bucket)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    spark = (
        SparkSession.builder.appName(f"banking-bronze-schema-safe-{args.env.lower()}")
        .config("parentProject", args.project_id)
        .getOrCreate()
    )

    metadata_df = (
        spark.read.format("bigquery")
        .option("table", f"{METADATA_DATASET}.table_ingestion_config")
        .option("temporaryGcsBucket", args.bq_temp_bucket)
        .load()
        .filter("is_active = true AND target_dataset = 'banking_bronze'")
    )

    failures = 0
    for row in metadata_df.toLocalIterator():
        if not process_table(spark, row, run_id, args.bq_temp_bucket):
            failures += 1

    if failures:
        raise RuntimeError(f"Bronze ingestion completed with {failures} failed table(s)")

    logging.info("Bronze ingestion job completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Bronze ingestion job failed")
        sys.exit(1)
