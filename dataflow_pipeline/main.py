import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json

class CleanAndTransformFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            
            if record.get("quantity", 0) <= 0 or record.get("unit_price", 0.0) <= 0:
                return
            
            record["quantity"] = int(record["quantity"])
            record["unit_price"] = int(record["unit_price"])
            record["total_amount"] = int(record["total_amount"])
            
            if "timestamp" not in record:
                return
            
            record["payment_type"] = record["payment_type"].title()
            record["channel"] = record["channel"].capitalize()
            
            yield record
        except Exception as e:
            print(f"Error procesando mensaje: {e}")
            return

SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING"},
        {"name": "store_id", "type": "INTEGER"},
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "product_id", "type": "STRING"},
        {"name": "product_category", "type": "STRING"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price", "type": "FLOAT"},
        {"name": "total_amount", "type": "FLOAT"},
        {"name": "payment_type", "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "channel", "type": "STRING"}
    ]
}

def run():
    options = PipelineOptions(
        runner = 'DataflowRunner',
        project = 'PROJECT_ID', # retailstream-dev
        region = 'REGION',
        temp_location = 'gs://retailstream-dev-temp/temp/',
        streamin = True
    )
    
    p = beam.Pipeline(options=options)
    
    (
        p
        | 'Leer de PubSub' >> beam.io.ReadFromPubSub(topic = 'projects/PROJECT_ID/topics/sales-stream')
        | 'Limpiar y Transformar' >> beam.ParDo(CleanAndTransformFn())
        | 'Escribir en BigQuery' >> beam.io.WriteToBigQuery(
            table = 'retail_data.transactions',
            schema = SCHEMA,
            write_disposition = beam.io.BigueryDisposition.WRITE_APPEND,
            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )
    
    p.run()
    
if __name__ == '__main__':
    run()