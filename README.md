# RetailStream: Pipeline de Streaming en GCP para Ventas Retail

Este proyecto simula el procesamiento en tiempo real de datos de ventas de una cadena de retail utilizando servicios de Google Cloud Platform (GCP). Fue desarrollado como un ejercicio práctico para aprender sobre CI/CD, manejo de entornos (dev/prod) y arquitectura moderna de datos en streaming.

---

## Objetivo

Procesar datos en formato JSON desde una base pública (Kaggle), simular la llegada en tiempo real mediante Pub/Sub, limpiar y transformar los datos en Dataflow y almacenarlos en BigQuery para análisis posteriores.

---

## Arquitectura

```plaintext
[Kaggle JSON Data] 
      │
[Publicador en Python] ──▶ [Pub/Sub Topic]
                                │
                                ▼
                         [Dataflow (Apache Beam)]
                                │
                                ▼
                         [BigQuery Dataset]

Componentes

    Pub/Sub: Canal de entrada para mensajes de ventas.

    Dataflow (Apache Beam): Limpieza, validación y transformación.

    BigQuery: Almacenamiento final para análisis.

    Cloud Build / GitHub Actions: CI/CD para despliegue del pipeline.

    Entornos separados: Configuración para dev y prod.
