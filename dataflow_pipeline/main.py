import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, AfterWatermark
import json
import logging



# Función personalizada para limpiar y validar mensajes
class CleanAndTransformFn(beam.DoFn):
    def process(self, element):
        try:
            # Decode del mensaje desde Pub/Sub y parseo del JSON
            record = json.loads(element.decode("utf-8"))

            # Validación de campos obligatorios
            required_fields = ["transaction_id", "store_id", "timestamp", "quantity", "unit_price", "total_amount"]
            for field in required_fields:
                if record.get(field) in [None, "", "null"]:
                    logging.warning(f"Campo faltante o nulo: {field}")
                    return  # No emitir este mensaje

            # Validación de valores numéricos razonables
            if int(record["quantity"]) <= 0 or float(record["unit_price"]) <= 0:
                logging.warning("Cantidad o precio inválido.")
                return

            # Limpieza y transformación
            record["quantity"] = int(record["quantity"])
            record["unit_price"] = float(record["unit_price"])
            record["total_amount"] = float(record["total_amount"])
            record["payment_type"] = record.get("payment_type", "").title()
            record["channel"] = record.get("channel", "").capitalize()

            # Emitimos el mensaje limpio
            yield record

        except Exception as e:
            logging.error(f"Error en limpieza: {e}")
            return  # Silenciosamente ignora si falla el parseo


# Definición del esquema de BigQuery
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
    # Configuración de opciones para Dataflow
    options = PipelineOptions(
        runner = 'DataflowRunner',
        project = 'PROJECT_ID', # retailstream-dev
        region = 'REGION',
        temp_location = 'gs://retailstream-dev-temp/temp/', # Bucket de GCS para archivos temporales
        streamin = True
    )
    
    # Definición del pipeline 
    with beam.Pipeline(options=options) as p:
        (
            p
            # Leer de Pub/Sub directamente desde el tópico
            | 'Leer de PubSub' >> beam.io.ReadFromPubSub(
                topic='projects/retailstream-dev/topics/sales-stream'
            )

            # Agrupar los datos en ventanas fijas de 1 minuto 
            | 'Ventana fija de 1 min' >> beam.WindowInto(
                FixedWindows(60),  # ventanas de 60 segundos
                trigger=AfterWatermark(late=AfterProcessingTime(60)),  # espera hasta watermark o 60s
                accumulation_mode=AccumulationMode.DISCARDING  # no acumula datos antiguos
            )

            # Limpiar y transformar
            | 'Limpiar y transformar' >> beam.ParDo(CleanAndTransformFn())

            # Escribir a BigQuery
            | 'Escribir en BigQuery' >> beam.io.WriteToBigQuery(
                table='retail_data.transactions',
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()