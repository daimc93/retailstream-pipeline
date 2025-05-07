# RetailStream: Pipeline de Streaming en GCP para Ventas Retail

Este proyecto simula el procesamiento en tiempo real de datos de ventas de una cadena de retail utilizando servicios de Google Cloud Platform (GCP). 

## Objetivo

Procesar datos en formato JSON desde una base pública (Kaggle), simular la llegada en tiempo real mediante Pub/Sub, limpiar y transformar los datos en Dataflow y almacenarlos en BigQuery para análisis posteriores.

## Componentes

Pub/Sub: Canal de entrada para mensajes de ventas.

Dataflow (Apache Beam): Limpieza, validación y transformación.

BigQuery: Almacenamiento final para análisis.
   - Transacciones válidas (`transactions`)
   - Errores (`transactions_errors`)
   - Agregados por tienda y minuto (`sales_summary`)

GitHub Actions: CI/CD para despliegue del pipeline.

Entornos separados: Configuración para dev y prod.







