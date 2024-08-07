import airflow

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


from dags.commons.ms import MSElasticsearch

ms_elasticsearch = MSElasticsearch()


dag = DAG(
    dag_id="cnv_covid19",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None
)


fetch_vaccination_data = PythonOperator(
    task_id="fetch_vaccination_data",
    python_callable=ms_elasticsearch.dag_get,
    dag=dag
)


notify = PythonOperator(
    task_id="notify_complete",
    python_callable=lambda: print("Finished execution!")
)

fetch_vaccination_data >> notify
