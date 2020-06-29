#!/usr/bin/env bash

# Set the Environment Variables
export PROJECT=$(gcloud config get-value core/project)
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT} --format='value(projectNumber)')

export GPS_BUCKET_NAME=fdt_gps_data
export BQ_BUCKET_NAME=fdt_bq_data
export SPARK_BUCKET=fdt_spark
export DATAPROC_BUCKET=fdt_dataproc
export SPARK_TEMP_BUCKET=fdt_spark_temp

export COMPOSER_REGION=us-east4
export COMPOSER_ZONE=us-east4-b
export COMPOSER_NAME=fdt-airflow

export DATAPROC_REGION=us-east1
export DATAPROC_ZONE=us-east1-b
export DATAPROC_NAME=fdt-cluster

export MACHINE_TYPE=n1-standard-1

export DATASET_NAME=fourcast_data_test
export GPS_TABLE_NAME=taxi_location
export BQ_TABLE_NAME=taxi_trips

export MULTI_REGION_LOCATION=US

export FUNCTION_NAME=gps-data-ariflow-trigger

gcloud config set composer/location ${COMPOSER_REGION}
gcloud config set dataproc/region ${DATAPROC_REGION}

# Replace environment variables in files
sed -i '' "s,%STORAGE_BUCKET%,${GPS_BUCKET_NAME},g" gps_data/dummy_data.py
sed -i '' "s,%PROJECT%,${PROJECT},g" gps_data/dummy_data.py

sed -i '' "s,%TEMP_BUCKET%,${SPARK_TEMP_BUCKET},g" gps_data/gps_spark.py
sed -i '' "s,%BQ_DATASET%,${DATASET_NAME},g" gps_data/gps_spark.py
sed -i '' "s,%GPS_TABLE%,${GPS_TABLE_NAME},g" gps_data/gps_spark.py

sed -i '' "s,%SPARK_BUCKET%,${SPARK_BUCKET},g" airflow/gps_dag.py
sed -i '' "s,%CLUSTER_NAME%,${DATAPROC_NAME},g" airflow/gps_dag.py
sed -i '' "s,%REGION%,${DATAPROC_REGION},g" airflow/gps_dag.py

sed -i '' "s,%PROJECT%,${PROJECT},g" airflow/bq_dag.py
sed -i '' "s,%DATASET%,${DATASET_NAME},g" airflow/bq_dag.py
sed -i '' "s,%TABLE%,${BQ_TABLE_NAME},g" airflow/bq_dag.py

sed -i '' "s,%PROJECT_ID%,${PROJECT},g" airflow/client_id.py
sed -i '' "s,%LOCATION%,${COMPOSER_REGION},g" airflow/client_id.py
sed -i '' "s,%ENVIRONMENT_NAME%,${COMPOSER_NAME},g" airflow/client_id.py

#Create Storage Buckets
gsutil mb -l ${MULTI_REGION_LOCATION} gs://${GPS_BUCKET_NAME}
gsutil mb -l ${MULTI_REGION_LOCATION} gs://${BQ_BUCKET_NAME}
gsutil mb -l ${MULTI_REGION_LOCATION} gs://${SPARK_BUCKET}
gsutil mb -l ${MULTI_REGION_LOCATION} gs://${DATAPROC_BUCKET}
gsutil mb -l ${MULTI_REGION_LOCATION} gs://${SPARK_TEMP_BUCKET}

gsutil cp gps_data/gps_spark.py gs://${SPARK_BUCKET}

#Create Dataset for taxi data
bq --location ${MULTI_REGION_LOCATION} mk \
--dataset \
${PROJECT}:${DATASET_NAME}

# Create Partitioned Table for Trip Data
bq --location=${MULTI_REGION_LOCATION} mk --table \
--schema bq_data/schema.json \
--time_partitioning_field date \
--time_partitioning_type DAY \
${PROJECT}:${DATASET_NAME}.${BQ_TABLE_NAME}

# Load the existing data into the partitioned table
# Query only fetches rows before the date to demonstrate the pipeline functioning on Airflow later.
bq --location=${MULTI_REGION_LOCATION} query \
--use_legacy_sql=false \
--destination_table ${DATASET_NAME}.${BQ_TABLE_NAME} \
--time_partitioning_type DAY \
--time_partitioning_field date \
--append_table \
'SELECT DATE(trip_start_timestamp) AS date, * FROM
`bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE pickup_latitude IS NOT NULL
AND dropoff_latitude IS NOT NULL
AND DATE(trip_start_timestamp) < DATE("2020-05-15")'

# Create Partitioned Table for Taxi Location
bq --location=${MULTI_REGION_LOCATION} mk --table \
--schema gps_data/schema.json \
--time_partitioning_type DAY \
--time_partitioning_field date \
${PROJECT}:${DATASET_NAME}.${GPS_TABLE_NAME}

# Create a Dataproc Cluster
gcloud dataproc clusters create ${DATAPROC_NAME} \
--region ${DATAPROC_REGION} \
--bucket ${DATAPROC_BUCKET} \
--master-machine-type ${MACHINE_TYPE} \
--num-masters 1 \
--worker-machine-type ${MACHINE_TYPE} \
--worker-boot-disk-size 20GB \
--worker-boot-disk-type pd-standard \
--num-workers 2 \
--zone ${DATAPROC_ZONE}


#Create Composer Environment
gcloud composer environments create ${COMPOSER_NAME} \
--location ${COMPOSER_REGION} \
--zone ${COMPOSER_ZONE} \
--machine-type ${MACHINE_TYPE} \
--disk-size 100 \
--python-version 3 \
--image-version composer-1.10.5-airflow-1.10.6

# Import the DAG for Trip Data
gcloud beta composer environments storage dags import \
--source airflow/bq_dag.py \
--environment ${COMPOSER_NAME} \
--location ${COMPOSER_REGION}

# Import the DAG for Taxi Location Data
gcloud beta composer environments storage dags import \
--source airflow/gps_dag.py \
--environment ${COMPOSER_NAME} \
--location ${COMPOSER_REGION}

# Prepare the Cloud Function with environment variables
export CLIENT_ID=$(python airflow/client_id.py)
export AIRFLOW_WEBSERVER=$(gcloud composer environments describe ${COMPOSER_NAME} --location ${COMPOSER_REGION} --format='value(config.airflowUri)')

sed -i '' "s,%CLIENT_ID%,${CLIENT_ID},g" cloud-functions/main.py
sed -i '' "s,%WEBSERVER%,${AIRFLOW_WEBSERVER},g" cloud-functions/main.py

# Create the function to trigger the Taxi Location Data DAG
gcloud functions deploy ${FUNCTION_NAME} \
--entry-point trigger_dag \
--memory 128MB \
--runtime python37 \
--source cloud-functions \
--trigger-resource=${GPS_BUCKET_NAME} \
--trigger-event=google.storage.object.finalize \
--region ${DATAPROC_REGION} \
--quiet

python gps_data/dummy_data.py