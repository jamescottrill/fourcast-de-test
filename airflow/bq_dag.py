from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.macros import timedelta

STARTDATE = '20200501'

default_dag_args = {
    'dag_id': 'bq_etl',
    'start_date': STARTDATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'task_concurrency': 1,
    'depends_on_past': True
}

dag = DAG(
    dag_id='bq_etl',
    default_args=default_dag_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1
)

start = DummyOperator(task_id='start', dag=dag)

query_1 = """
    SELECT 
       CASE
        WHEN (
            (SELECT COUNT(*) AS count FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`) - 
            (SELECT COUNT(*) AS count FROM `%PROJECT%.%DATASET%.%TABLE%`) > 0 ) THEN 1
        ELSE 0
       END
"""

query_2 = """
WITH t1 AS (
    SELECT unique_key FROM `%PROJECT%.%DATASET%.%TABLE%`
)
SELECT DATE(trip_start_timestamp) as date, t2.* 
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` t2 
LEFT JOIN t1 ON t2.unique_key = t1.unique_key
WHERE t1.unique_key IS NULL
"""

check_op = BigQueryCheckOperator(
    sql=query_1,
    task_id='check_data',
    bigquery_conn_id='bigquery_default',
    use_legacy_sql=False,
    dag=dag
)


fetch_op = BigQueryOperator(
    task_id='fetch_data',
    sql=query_2,
    destination_dataset_table='%PROJECT%.%DATASET%.%TABLE%',
    delegate_to=False,
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
    use_legacy_sql=False
)


start >> check_op >> fetch_op
