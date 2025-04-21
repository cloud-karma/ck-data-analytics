import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
from datetime import datetime

class ParseMessage(beam.DoFn): --
    def process(self, element):
        row = json.loads(element)
        yield {
            'timestamp': row.get('timestamp'),
            'coin': row.get('coin'),
            'price_usd': float(row.get('price_usd', 0.0)),
            'volume': float(row.get('volume', 0.0))
        }

class AddWindowTimestamp(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        element['window_start'] = window.start.to_utc_datetime().isoformat()
        return [element]

def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic='projects/your-project/topics/your-topic')
            | 'Parse JSON' >> beam.ParDo(ParseMessage())
            | 'Window into 1-min' >> beam.WindowInto(beam.window.FixedWindows(60))
            | 'Group by coin' >> beam.CombinePerKey(lambda prices: {
                'average_price': sum(p['price_usd'] for p in prices) / len(prices),
                'volume': sum(p['volume'] for p in prices)
            })
            | 'Format Output' >> beam.ParDo(AddWindowTimestamp())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='your-project:dataset.real_time_prices',
                schema='window_start:TIMESTAMP,coin:STRING,average_price:FLOAT,volume:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
