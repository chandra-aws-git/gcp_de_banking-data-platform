"""
Note : pip install cloud-sql-python-connector SQLAlchemy pymysql

PRODUCTION-GRADE CDC PIPELINE
--> Cloud SQL (MySQL) → GCS (Parquet) using Apache Beam (Dataflow)

Best Practices Implemented:
- Cloud SQL Python Connector (IAM-based, secure)
- SQLAlchemy connection pooling
- Incremental CDC using watermark (idempotent)
- Decimal-safe Parquet writes
- Structured logging (Cloud Logging compatible)
- Layered error handling
- Fail-fast on infra/schema issues
- Watermark update only after success
"""

# =====================================================
# IMPORTS
# =====================================================
import apache_beam as beam
import pyarrow as pa
import sqlalchemy
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.util import Reshuffle
from google.cloud import bigquery, secretmanager
from google.cloud.sql.connector import Connector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("cloudsql-cdc")

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-banking-2026-499415"
REGION = "us-central1"

BQ_DATASET = "banking_metadata"
BQ_TABLE = "cdc_config"

INSTANCE_NAME = "banking-db-mysql"
DB_NAME = "banking_db"
DB_USER = "myuser"
DB_PASSWORD = "Mypass@123"  # 👉 move to Secret Manager in prod

GCS_RAW_BASE = "gs://banking-raw-dev-bkt/cloudsql/"
TEMP_LOCATION = "gs://banking-temp-dev-bkt/temp/"
STAGING_LOCATION = "gs://banking-temp-dev-bkt/staging/"

INSTANCE_CONNECTION_NAME = f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}"

DATAFLOW_OPTIONS = {
    "runner": "DataflowRunner",
    "project": PROJECT_ID,
    "region": REGION,
    "temp_location": TEMP_LOCATION,
    "staging_location": STAGING_LOCATION,
    "save_main_session": True,
}

# =====================================================
# UTILS
# =====================================================
def log_exception(msg, exc):
    LOGGER.error("%s | %s\n%s", msg, exc, traceback.format_exc())


# =====================================================
# BIGQUERY METADATA
# =====================================================
def get_active_tables():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT table_name, watermark_column, last_success_ts
        FROM `{project_id}.{dataset}.{table}`
        WHERE is_active = TRUE
    """
    return list(client.query(query).result())


def update_watermark(project_id: str, dataset: str, table: str, table_name: str, ts):
    client = bigquery.Client(project=project_id)
    query = f"""
        UPDATE `{project_id}.{dataset}.{table}`
        SET last_success_ts = @last_success_ts
        WHERE table_name = @table_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("last_success_ts", "TIMESTAMP", ts),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
        ]
    )
    client.query(query, job_config=job_config).result()


def get_engine(instance_connection_name: str, db_name: str, db_user: str, db_password: str):
    connector = Connector()

    def getconn():
        return connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_password,
            db=db_name,
        )

    return sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )


def get_mysql_schema(engine, table_name: str):
    table_name = validate_identifier(table_name)
    fields = []

    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(f"DESCRIBE `{table_name}`"))
        for row in result:
            dtype = row.Type.lower()
            if "int" in dtype:
                pa_type = pa.int64()
            elif "decimal" in dtype:
                pa_type = pa.string()
            elif "float" in dtype or "double" in dtype:
                pa_type = pa.float64()
            elif "timestamp" in dtype or "datetime" in dtype:
                pa_type = pa.timestamp("us")
            elif "date" in dtype:
                pa_type = pa.date32()
            else:
                pa_type = pa.string()
            fields.append(pa.field(row.Field, pa_type))

    return pa.schema(fields)


def normalize_row(row):
    return {key: str(value) if isinstance(value, Decimal) else value for key, value in row.items()}


class ReadFromCloudSQL(beam.DoFn):
    def __init__(self, table, watermark_col, last_ts, engine_config):
        self.table = validate_identifier(table)
        self.watermark_col = validate_identifier(watermark_col)
        self.last_ts = last_ts
        self.engine_config = engine_config

    def setup(self):
        db_password = get_secret(
            self.engine_config["project_id"],
            self.engine_config["db_password_secret"],
        )
        self.engine = get_engine(
            self.engine_config["instance_connection_name"],
            self.engine_config["db_name"],
            self.engine_config["db_user"],
            db_password,
        )

    def process(self, element):
        if self.last_ts is None:
            query = sqlalchemy.text(f"SELECT * FROM `{self.table}`")
            params = {}
        else:
            query = sqlalchemy.text(
                f"""
                SELECT *
                FROM `{self.table}`
                WHERE `{self.watermark_col}` > :last_ts
                """
            )
            params = {"last_ts": self.last_ts}

        with self.engine.connect() as conn:
            for row in conn.execute(query, params):
                yield dict(row._mapping)


def run(argv=None):
    LOGGER.info("Starting CDC pipeline")
    run_ts = datetime.datetime.now(datetime.timezone.utc)

    pipeline_options = PipelineOptions(argv)
    options = pipeline_options.view_as(CloudSqlCdcOptions)

    engine_config = {
        "project_id": options.project_id,
        "instance_connection_name": options.instance_connection_name,
        "db_name": options.db_name,
        "db_user": options.db_user,
        "db_password_secret": options.db_password_secret,
    }
    engine = get_engine(
        options.instance_connection_name,
        options.db_name,
        options.db_user,
        get_secret(options.project_id, options.db_password_secret),
    )

    tables = get_active_tables(
        options.project_id,
        options.metadata_dataset,
        options.metadata_table,
    )

    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            for table_config in tables:
                schema = get_mysql_schema(engine, table_config.table_name)
                table_name = validate_identifier(table_config.table_name)

                (
                    pipeline
                    | f"Start-{table_name}" >> beam.Create([None])
                    | f"Read-{table_name}"
                    >> beam.ParDo(
                        ReadFromCloudSQL(
                            table_config.table_name,
                            table_config.watermark_column,
                            table_config.last_success_ts,
                            engine_config,
                        )
                    )
                    | f"Normalize-{table_name}" >> beam.Map(normalize_row)
                    | f"Reshuffle-{table_name}" >> Reshuffle()
                    | f"Write-{table_name}"
                    >> WriteToParquet(
                        file_path_prefix=(
                            f"{options.gcs_raw_base.rstrip('/')}/{table_name}/"
                            f"ingestion_date={run_ts:%Y-%m-%d}/part"
                        ),
                        schema=schema,
                        file_name_suffix=".parquet",
                    )
                )

        for table_config in tables:
            update_watermark(
                options.project_id,
                options.metadata_dataset,
                options.metadata_table,
                table_config.table_name,
                run_ts,
            )

        LOGGER.info("CDC pipeline completed successfully")
    except Exception as exc:
        LOGGER.error("CDC pipeline failed | %s\n%s", exc, traceback.format_exc())
        raise


if __name__ == "__main__":
    run()
