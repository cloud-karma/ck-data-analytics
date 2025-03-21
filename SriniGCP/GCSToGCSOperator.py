from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
import logging

# Logging function for traceability
def log_file_movement(**kwargs):
    files_moved = kwargs['ti'].xcom_pull(task_ids='move_files_to_processed')
    if files_moved:
        logging.info(f"Files moved to processed/: {files_moved}")
    else:
        logging.warning("No files moved.")

# Task to move files and log movements
move_files_to_processed = GCSToGCSOperator(
    task_id="move_files_to_processed",
    source_bucket=BUCKET_NAME,
    source_object=f"{RAW_FOLDER}*",
    destination_bucket=BUCKET_NAME,
    destination_object=PROCESSED_FOLDER,
    move_object=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
)

log_movement = PythonOperator(
    task_id="log_file_movement",
    python_callable=log_file_movement,
    provide_context=True,
)

move_files_to_processed >> log_movement  # Ensure logging runs after file movement
