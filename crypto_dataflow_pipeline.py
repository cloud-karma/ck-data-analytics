import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
import json
import logging
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub

# Define custom pipeline options
class CryptoPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_topic',
                            default='projects/your-project-id/topics/crypto-prices',
                            help='Input Pub/Sub topic')
        parser.add_argument('--output_table',
                            default='your-project-id:crypto_dataset.real_time_prices',
                            help='Output BigQuery table')

# Parse JSON messages from Pub/Sub
def parse_pubsub_message(message):
    try:
        data = json.loads(message.decode('utf-8'))
        return {
            'timestamp': data['timestamp'],  # Expected in ISO format or epoch
            'coin': data['coin'],
            'price_usd': float(data['price_usd']),
            'volume': float(data['volume'])
        }
    except (ValueError, KeyError) as e:
        logging.error(f"Failed to parse message: {message}, error: {e}")
        return None

# Format data for BigQuery
def format_for_bigquery(element):
    return {
        'timestamp': element['timestamp'],
        'coin': element['coin'],
        'avg_price_usd': element['avg_price_usd'],
        'total_volume': element['total_volume'],
        'window_start': element['window'].start.isoformat()
    }

# Define the streaming pipeline
def run():
    # Set up pipeline options
    options = CryptoPipelineOptions()
    options.view_as(beam.options.StandardOptions).streaming = True  # Enable streaming mode

    # Define the pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read from Pub/Sub
        pubsub_data = (pipeline 
                       | 'Read from Pub/Sub' >> ReadFromPubSub(topic=options.input_topic)
                       | 'Decode' >> beam.Map(lambda x: x.decode('utf-8')))

        # Parse JSON messages
        parsed_data = (pubsub_data 
                       | 'Parse JSON' >> beam.Map(parse_pubsub_message)
                       | 'Filter Invalid' >> beam.Filter(lambda x: x is not None))

        # Apply windowing (1-minute fixed windows)
        windowed_data = (parsed_data 
                         | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp']))
                         | 'Window' >> beam.WindowInto(window.FixedWindows(60)))  # 60 seconds = 1 minute

        # Aggregate data (average price and total volume per coin)
        aggregated_data = (windowed_data 
                           | 'Group by Coin' >> beam.GroupBy(lambda x: x['coin'])
                           | 'Compute Aggregates' >> beam.Map(
                               lambda g: {
                                   'coin': g[0],
                                   'avg_price_usd': sum(x['price_usd'] for x in g[1]) / len(g[1]),
                                   'total_volume': sum(x['volume'] for x in g[1]),
                                   'window': g[1][0]['timestamp']  # Use first timestamp as window reference
                               }))

        # Write to BigQuery
        aggregated_data | 'Write to BigQuery' >> WriteToBigQuery(
            table=options.output_table,
            schema='timestamp:TIMESTAMP,coin:STRING,avg_price_usd:FLOAT,total_volume:FLOAT,window_start:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()