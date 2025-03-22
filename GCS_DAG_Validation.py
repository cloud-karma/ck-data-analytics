from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Constants
GCP_CONN_ID = "google_cloud_default"
BUCKET_NAME = "your-gcs-bucket"
SOURCE_OBJECT = "your-file.csv"  # You can use wildcard "*.csv"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_bq_dataset"
BQ_TABLE = "your_bq_table"

# Define default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Function to validate data in BigQuery
def validate_data():
    client = bigquery.Client()

    # 1. Row Count Check
    query = f"SELECT COUNT(*) AS row_count FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    result = client.query(query).to_dataframe()
    row_count = result["row_count"].iloc[0]

    if row_count == 0:
        log.error("Validation Failed: BigQuery table is empty!")
        raise ValueError("BigQuery table is empty after ingestion!")
    else:
        log.info(f"Row count validation passed: {row_count} rows loaded.")

    # 2. Null Value Check (optional: replace 'important_column' with actual column name)
    null_check_query = f"""
    SELECT COUNT(*) AS null_count
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE important_column IS NULL
    """
    null_result = client.query(null_check_query).to_dataframe()
    null_count = null_result["null_count"].iloc[0]

    if null_count > 0:
        log.warning(f"Found {null_count} null values in 'important_column'")
    else:
        log.info("No nulls found in 'important_column'.")

    log.info("✅ BigQuery data validation completed successfully.")
# Define DAG
with DAG(
    "gcs_to_bq_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # Adjust as needed
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    # Sensor to check if file exists in GCS
    check_gcs_file = GCSObjectExistenceSensor(
        task_id="check_gcs_file",
        bucket=BUCKET_NAME,
        object=SOURCE_OBJECT,
        google_cloud_conn_id=GCP_CONN_ID,
        timeout=60,
        poke_interval=10,
    )

    # Load data from GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[SOURCE_OBJECT],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        google_cloud_storage_conn_id=GCP_CONN_ID,
        create_disposition="CREATE_IF_NEEDED",
    )

    # Data validation
    validate_bq_data = PythonOperator(
        task_id="validate_bq_data",
        python_callable=validate_data,
    )

    end = DummyOperator(task_id="end")

    # Task dependencies
    start >> check_gcs_file >> load_to_bq >> validate_bq_data >> end
