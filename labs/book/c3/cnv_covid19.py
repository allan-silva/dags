from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from labs.commons.ms import MSElasticsearch


ms_elasticsearch = MSElasticsearch(
    Variable.get("MS_COVID19_CNV_CLUSTER_URL"),
    Variable.get("MS_COVID19_CNV_CLUSTER_USER"),
    Variable.get("MS_COVID19_CNV_CLUSTER_PASS")
)


dag = DAG(
    dag_id="cnv_covid19",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None
)


fetch_vaccination_data = PythonOperator(
    task_id="fetch_vaccination_data",
    python_callable=lambda ts: ms_elasticsearch.get_daily(ts),
    dag=dag
)


notify = PythonOperator(
    task_id="notify_complete",
    python_callable=lambda: print("Finished execution!")
)

fetch_vaccination_data >> notify
