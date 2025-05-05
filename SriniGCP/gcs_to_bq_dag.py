from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Set up default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False, ---
    "start_date": days_ago(1),
    "retries": 1,
}

GCP_PROJECT_ID = "your-gcp-project-id"
GCS_BUCKET = "your-gcs-bucket"
GCS_SOURCE_FILE = "data/input-file.csv"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"
DATAFLOW_TEMPLATE_PATH = "gs://your-bucket/templates/dataflow-template"

with DAG(
    dag_id="gcs_to_bq_via_dataflow",
    default_args=default_args,
    schedule_interval="@daily",  # Adjust schedule as needed
    catchup=False,
    tags=["gcp", "dataflow", "bigquery"],
) as dag:

    # Task 1: Run Dataflow Job
    run_dataflow_job = DataflowCreatePythonJobOperator(
        task_id="run_dataflow_job",
        py_file="gs://your-bucket/dataflow-script.py",
        job_name="dataflow-gcs-to-bq",
        project_id=GCP_PROJECT_ID,
        location="us-central1",
        options={
            "input": f"gs://{GCS_BUCKET}/{GCS_SOURCE_FILE}",
            "output": f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
        },
    )

    # Task 2: Direct GCS to BigQuery Load (Alternative Approach)
    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[GCS_SOURCE_FILE],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )

    # Define Task Dependencies
    run_dataflow_job >> gcs_to_bq  # Runs Dataflow job first, then loads data to BigQuery

