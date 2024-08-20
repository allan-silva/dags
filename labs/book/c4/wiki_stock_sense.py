import requests
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag = DAG(
    dag_id="wiki_stock_sense",
    start_date=pendulum.now().subtract(days=1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
)


def _get_data(wiki_url, output_path, logical_date):
    url = (
        f"{wiki_url}/{logical_date.year}/{logical_date.year}-{logical_date.month:0>2}"
        f"/pageviews-{logical_date.year}{logical_date.month:0>2}{logical_date.day:0>2}"
        f"-{logical_date.hour}0000.gz"
    )
    response = requests.get(url, stream=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "wiki_url": "https://dumps.wikimedia.org/other/pageviews",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)


extract_data = BashOperator(
    task_id="extract_data",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag,
)


def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)


write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="postgres-default",
    sql="postgres_query.sql",
    dag=dag,
)


get_data >> extract_data >> fetch_pageviews >> write_to_postgres
