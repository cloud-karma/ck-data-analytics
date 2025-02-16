import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


class FormatToDict(beam.DoFn):
    """Parses CSV lines into a dictionary format required by BigQuery."""
    def __init__(self, schema_fields):
        self.schema_fields = schema_fields

    def process(self, element, *args, **kwargs):
        values = element.split(',')
        if len(values) == len(self.schema_fields):
            yield {self.schema_fields[i]: values[i] for i in range(len(values))}


def run(argv=None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--region', required=True, help='GCP Region')
    parser.add_argument('--gcs_input', required=True, help='GCS input file path (gs://bucket/file.csv)')
    parser.add_argument('--bq_table', required=True, help='BigQuery table (project:dataset.table)')
    parser.add_argument('--temp_location', required=True, help='GCS temp location (gs://bucket/temp/)')
    
    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        save_main_session=False
    )

    # Define BigQuery schema
    bq_schema = 'id:STRING,name:STRING,age:INTEGER,salary:FLOAT'

    # Extract field names from schema
    schema_fields = [field.split(':')[0] for field in bq_schema.split(',')]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText(args.gcs_input, skip_header_lines=1)
            | 'FormatToDict' >> beam.ParDo(FormatToDict(schema_fields))
            | 'WriteToBigQuery' >> WriteToBigQuery(
                args.bq_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
