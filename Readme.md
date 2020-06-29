# Fourcast Implementation Challenge

This repository contains all the code required to deploy my challenge solution to GCP.

The deployment script is `create.sh`. There are variables at the top of the file that should be changed before execution, specifically the bucket names to ensure these are globally unique.

To fully deploy the infrastructure you need a service account in the top folder called `service-account.json` This is just used to upload the dummy data into Cloud Storage, so Storage Write access.

The Dataproc and Composer clusters are set to run in different regions because the initial quota for Compute IP addresses isn't high enough to run both in the same region.

The deployment uses the following components: 

### Cloud Composer
Airflow is used to manage the pipeline execution. Every 5 minutes the BigQuery source table is checked to see if it has the same number of rows as the destination table. If there are new rows, it runs a query to copy these to the destination table.

The GPS data job is triggered externally when a new file arrives, which runs a dataproc job to process the GPS data.

### Dataproc
Dataproc is used to modify the GPS data that arrives, removing duplicate rows and adding additional columns before writing it to the destination table in BigQuery.

### Cloud Functions
A Cloud Function is used as the trigger for the GPS data pipeline when a new file lands in the source storage bucket, this ensures that the pipeline only runs when there is work.

### Storage Buckets 
These are used to store the DAGs, Spark pipeline and arriving GPS data.

### BigQuery
There are two output tables from the pipelines, one is a partitioned version of the taxi_trips table, the other is a partitioned table containing the dump of the GPS data. 