import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery


class FormatToDict(beam.DoFn):
    """Parses CSV lines into a dictionary format required by BigQuery."""
    def __init__(self, schema_fields):
        self.schema_fields = schema_fields

    def process(self, element, *args, **kwargs):
        print(f"🚀 Raw CSV Line: {element}")  # Debug: Print raw data line

        values = element.split(',')
        if len(values) == len(self.schema_fields):
            record = {self.schema_fields[i]: values[i] for i in range(len(values))}
            
            print(f"✅ Processed Record: {record}")  # Debug: Print the processed dictionary

            yield record


class DeduplicateRecords(beam.DoFn):
    """Deduplicates records by keeping the latest updated_at value per id."""
    def process(self, element):
        id_key, records = element  # Grouped data (id, list of records)
        latest_record = max(records, key=lambda x: x['updated_at'])  # Keep latest record
        yield latest_record  # Output only the latest record per id


def merge_data(project):
    """Executes a MERGE statement in BigQuery to update the target table."""
    client = bigquery.Client(project=project)

    query = """
    MERGE INTO `data-eng-303102.ck_test.target_table` AS target
    USING (
        -- Select only the latest record per ID
        SELECT id, name, age, salary, updated_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_num
            FROM `data-eng-303102.ck_test.staging_table`
        ) 
        WHERE row_num = 1
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET 
            target.name = source.name, 
            target.age = source.age, 
            target.salary = source.salary, 
            target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (id, name, age, salary, updated_at)
        VALUES (source.id, source.name, source.age, source.salary, source.updated_at);
    """

    query_job = client.query(query)
    query_job.result()
    print("✅ MERGE operation completed successfully.")


def run(argv=None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--region', required=True, help='GCP Region')
    parser.add_argument('--gcs_input', required=True, help='GCS input file path (gs://bucket/file.csv)')
    parser.add_argument('--bq_table', required=True, help='BigQuery table (project:dataset.table)')
    parser.add_argument('--temp_location', required=True, help='GCS temp location (gs://bucket/temp/)')
    
    args, pipeline_args = parser.parse_known_args(argv)

    # ✅ Define `pipeline_options` BEFORE using it
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        save_main_session=True  # ✅ Fix: Ensure Beam saves session variables
    )

    # Define BigQuery schema
    bq_schema = 'id:STRING,name:STRING,age:INTEGER,salary:FLOAT64,updated_at:TIMESTAMP'

    # Extract field names from schema
    schema_fields = [field.split(':')[0] for field in bq_schema.split(',')]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText(args.gcs_input, skip_header_lines=1)
            | 'FormatToDict' >> beam.ParDo(FormatToDict(schema_fields))
            | 'KeyByID' >> beam.Map(lambda record: (record['id'], record))  # Convert to key-value pairs
            | 'GroupRecords' >> beam.GroupByKey()  # Groups records by id
            | 'Deduplicate' >> beam.ParDo(DeduplicateRecords())  # Keeps only the latest record per id
            | 'WriteToBigQuery' >> WriteToBigQuery(
                args.bq_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # ✅ Keep historical data
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    merge_data("data-eng-303102")  # ✅ Automatically merge after pipeline execution