import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, AfterWatermark
import apache_beam.transforms.window as window
import json
import logging
from datetime import datetime

# Etiquetas para las salidas principales y de error
class OutputTags:
    VALID = 'valid'
    ERROR = 'error'

# Funcion para limpiar, transformar y enrutar los datos
class CleanAndTransformFn(beam.DoFn):
    def process(self, element):
        try:
            decoded = element.decode("utf-8")
            record = json.loads(decoded)

            # Validar campos obligatorios
            required_fields = ["transaction_id", "store_id", "timestamp", "quantity", "unit_price", "total_amount"]
            for field in required_fields:
                if record.get(field) in [None, "", "null"]:
                    yield beam.pvalue.TaggedOutput(OutputTags.ERROR, {
                        "error_type": "Dato incompleto",
                        "error_message": f"Falta el campo obligatorio: {field}",
                        "raw_message": decoded,
                        "error_timestamp": datetime.utcnow().isoformat() + "Z"
                    })
                    return

            # Validación de tipo y rango
            try:
                record["quantity"] = int(record["quantity"])
                record["unit_price"] = float(record["unit_price"])
                record["total_amount"] = float(record["total_amount"])
            except ValueError as e:
                yield beam.pvalue.TaggedOutput(OutputTags.ERROR, {
                    "error_type": "Tipo incorrecto",
                    "error_message": f"Error de conversión: {e}",
                    "raw_message": decoded,
                    "error_timestamp": datetime.utcnow().isoformat() + "Z"
                })
                return

            if record["quantity"] <= 0 or record["unit_price"] <= 0:
                yield beam.pvalue.TaggedOutput(OutputTags.ERROR, {
                    "error_type": "Fuera de rango",
                    "error_message": f"Cantidad o precio inválido: {record['quantity']} / {record['unit_price']}",
                    "raw_message": decoded,
                    "error_timestamp": datetime.utcnow().isoformat() + "Z"
                })
                return

            # Normalización
            record["payment_type"] = record.get("payment_type", "").title()
            record["channel"] = record.get("channel", "").capitalize()

            # Emitimos si es válido
            yield beam.pvalue.TaggedOutput(OutputTags.VALID, record)

        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput(OutputTags.ERROR, {
                "error_type": "JSON mal formado",
                "error_message": str(e),
                "raw_message": element.decode("utf-8", errors="ignore"),
                "error_timestamp": datetime.utcnow().isoformat() + "Z"
            })

        except Exception as e:
            yield beam.pvalue.TaggedOutput(OutputTags.ERROR, {
                "error_type": "Error desconocido",
                "error_message": str(e),
                "raw_message": element.decode("utf-8", errors="ignore"),
                "error_timestamp": datetime.utcnow().isoformat() + "Z"
            })

# Esquema para BigQuery: eventos validos
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

# Esquema para errores
ERROR_SCHEMA = {
    "fields": [
        {"name": "error_type", "type": "STRING"},
        {"name": "error_message", "type": "STRING"},
        {"name": "raw_message", "type": "STRING"},
        {"name": "error_timestamp", "type": "TIMESTAMP"}
    ]
}

# Esquema para resumen de ventas por tienda y ventana
SUMMARY_SCHEMA = {
    "fields": [
        {"name": "store_id", "type": "INTEGER"},
        {"name": "window_start", "type": "TIMESTAMP"},
        {"name": "window_end", "type": "TIMESTAMP"},
        {"name": "total_sales", "type": "FLOAT"},
        {"name": "transaction_count", "type": "INTEGER"}
    ]
}

# Calcular el total de ventas y cantidad de transacciones en una ventana de 1 minuto.
def format_summary(store_id, values, window):
    total_sales = sum(v['total_amount'] for v in values)
    transaction_count = len(values)
    return {
        "store_id": store_id,
        "window_start": window.start.to_utc_datetime().isoformat(),
        "window_end": window.end.to_utc_datetime().isoformat(),
        "total_sales": round(total_sales, 2),
        "transaction_count": transaction_count
    }

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='retailstream-dev',
        region='us-central1',
        temp_location='gs://retailstream-dev-temp/temp/',
        streaming=True
    )

    with beam.Pipeline(options=options) as p:
        # Leer y enrutar mensajes válidos y con error
        messages = (
            p
            | 'Leer de PubSub' >> beam.io.ReadFromPubSub(topic='projects/retailstream-dev/topics/sales-stream')
            | 'Procesar mensajes' >> beam.ParDo(CleanAndTransformFn()).with_outputs(OutputTags.VALID, OutputTags.ERROR, main=OutputTags.VALID)
        )

        # Eventos válidos → BigQuery (detalle)
        messages.valid | 'Guardar válidos en BQ' >> beam.io.WriteToBigQuery(
            table='retail_data.transactions',
            schema=SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Errores → BigQuery
        messages.error | 'Guardar errores en BQ' >> beam.io.WriteToBigQuery(
            table='retail_data.transactions_errors',
            schema=ERROR_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Agregación por tienda cada 1 min
        (
            messages.valid
            | 'Agrupar por tienda' >> beam.Map(lambda x: (x["store_id"], x))
            | 'Ventana de 1 minuto' >> beam.WindowInto(
                window.FixedWindows(60),
                trigger=AfterWatermark(late=AfterProcessingTime(60)),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | 'GroupByKey' >> beam.GroupByKey()
            | 'Formatear resumen' >> beam.MapTuple(format_summary)
            | 'Guardar resumen en BQ' >> beam.io.WriteToBigQuery(
                table='retail_data.sales_summary',
                schema=SUMMARY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
