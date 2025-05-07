#!/bin/bash
# Script para crear recursos GCP necesarios para el pipeline

PROJECT_ID=retailstream-dev
REGION=us-central1
TOPIC=sales-stream
BUCKET=retailstream-dev-temp
DATASET=retail_data

# Crear tópico Pub/Sub
gcloud pubsub topics create $TOPIC --project=$PROJECT_ID

# Crear bucket de almacenamiento
gsutil mb -l $REGION gs://$BUCKET/

# Crear dataset en BigQuery
bq --location=US mk --dataset $PROJECT_ID:$DATASET
