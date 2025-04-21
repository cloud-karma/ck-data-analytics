from google.cloud import monitoring_v3, bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import time

# Set your GCP project details
PROJECT_ID = "your-project-id"
BQ_DATASET = "your_dataset_name"
BQ_TABLE = "your_table_name"
ALERT_THRESHOLD = 1000  # Change this threshold as needed
ALERT_EMAIL = "your-email@example.com"
BUCKET_PATH = 'gs://your-bucket-name/sample-data.csv'
BQ_TABLE_PATH = f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"

def get_bigquery_row_count():
    """Fetches the row count for the given BigQuery table."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    query_job = client.query(query)
    result = query_job.result()
    for row in result:
        return row.row_count
    return 0

def create_alert_policy():
    """Creates an alert policy in Cloud Monitoring if row count is below threshold."""
    client = monitoring_v3.AlertPolicyServiceClient()
    project_name = f"projects/{PROJECT_ID}"

    condition = monitoring_v3.AlertPolicy.Condition(
        display_name="BigQuery Row Count Alert",
        condition_threshold=monitoring_v3.Condition.ThresholdCondition(
            filter=f"metric.type=\"bigquery.googleapis.com/query/count\" resource.type=\"bigquery_table\"",
            comparison=monitoring_v3.ComparisonType.COMPARISON_LT,
            threshold_value=ALERT_THRESHOLD,
            duration={"seconds": 300},  # Trigger alert if below threshold for 5 minutes
            aggregations=[
                monitoring_v3.Aggregation(
                    alignment_period={"seconds": 300},
                    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
                )
            ],
        ),
    )

    notification_channel = monitoring_v3.AlertPolicy.NotificationChannelStrategy(
        notification_channel_names=[f"projects/{PROJECT_ID}/notificationChannels/{ALERT_EMAIL}"]
    )

    policy = monitoring_v3.AlertPolicy(
        display_name="BigQuery Data Quality Alert",
        conditions=[condition],
        notification_channels=[notification_channel],
        combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.AND,
        enabled=True,
    )

    client.create_alert_policy(name=project_name, alert_policy=policy)
    print("Alert policy created successfully!")

class FormatCSV(beam.DoFn):
    def process(self, element):
        fields = element.split(',')
        return [{'field1': fields[0], 'field2': fields[1]}]

def run_pipeline():
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.temp_location = 'gs://your-bucket-name/temp/'
    options.view_as(PipelineOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText(BUCKET_PATH)
            | 'Transform Data' >> beam.ParDo(FormatCSV())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                BQ_TABLE_PATH,
                schema='field1:STRING, field2:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

if __name__ == "__main__":
    run_pipeline()
    row_count = get_bigquery_row_count()
    print(f"Row count in table: {row_count}")
    
    if row_count < ALERT_THRESHOLD:
        print("Row count below threshold! Creating alert policy...")
        create_alert_policy()
    else:
        print("Row count is above threshold. No alert needed.")
