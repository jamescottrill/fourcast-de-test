from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

STARTDATE = '20200601'

default_dag_args = {
    'dag_id': 'gps_data',
    'start_date': STARTDATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='gps_data',
    default_args=default_dag_args,
    schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)

main = DataProcPySparkOperator(
    task_id='main',
    main="gs://%SPARK_BUCKET%/gps_spark.py",
    arguments=['{{ dag_run.conf }}'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar'],
    cluster_name='%CLUSTER_NAME%',
    region='%REGION%',
    dag=dag

)
start >> main