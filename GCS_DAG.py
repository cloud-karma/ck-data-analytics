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
    query = f"SELECT COUNT(*) AS row_count FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    result = client.query(query).to_dataframe()
    row_count = result["row_count"].iloc[0]

    if row_count == 0:
        raise ValueError("BigQuery table is empty after ingestion!")
    print(f"Data validation passed: {row_count} rows in {BQ_TABLE}")

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
