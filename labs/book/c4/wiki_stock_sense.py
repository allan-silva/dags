import requests
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from labs.commons.fileutils import mkdir, join_path


dag = DAG(
    dag_id="wiki_stock_sense",
    start_date=pendulum.datetime(2020, 8, 11, 0, 0, 0),
    end_date=pendulum.datetime(2020, 8, 13, 0, 0, 0),
    schedule_interval="@hourly",
    template_searchpath="{{var.value.get('LOCAL_STORAGE')}}/ch4",
)


def _get_data(wiki_url, output_dir, output_file, logical_date, **_):
    url = (
        f"{wiki_url}/{logical_date.year}/{logical_date.year}-{logical_date.month:0>2}"
        f"/pageviews-{logical_date.year}{logical_date.month:0>2}{logical_date.day:0>2}"
        f"-{logical_date.hour:0>2}0000.gz"
    )
    response = requests.get(url, stream=True)

    response.raise_for_status()

    mkdir(output_dir)

    with open(join_path(output_dir, output_file), "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "wiki_url": "https://dumps.wikimedia.org/other/pageviews",
        "output_dir": "{{var.value.get('LOCAL_STORAGE')}}/ch4",
        "output_file": "wikipageviews{{logical_date.year}}{{logical_date.format('MM')}}{{logical_date.format('DD')}}-{{logical_date.format('HH')}}0000.gz"
    },
    dag=dag,
)


extract_data = BashOperator(
    task_id="extract_data",
    bash_command="gunzip --force {{var.value.get('LOCAL_STORAGE')}}/ch4/wikipageviews{{logical_date.year}}{{logical_date.format('MM')}}{{logical_date.format('DD')}}-{{logical_date.format('HH')}}0000.gz",
    dag=dag,
)


def _fetch_pageviews(page_views_file, output_file, pagenames, logical_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open(page_views_file, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open(output_file, "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{logical_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "page_views_file": "{{var.value.get('LOCAL_STORAGE')}}/ch4/wikipageviews{{logical_date.year}}{{logical_date.format('MM')}}{{logical_date.format('DD')}}-{{logical_date.format('HH')}}0000",
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
        "output_file": "{{var.value.get('LOCAL_STORAGE')}}/ch4/wikipageviews{{logical_date.year}}{{logical_date.format('MM')}}{{logical_date.format('DD')}}-{{logical_date.format('HH')}}0000.sql",
        },
    dag=dag,
)


write_to_postgres = SQLExecuteQueryOperator(
    task_id="write_to_postgres",
    postgres_conn_id="postgres-default",
    sql="{{var.value.get('LOCAL_STORAGE')}}/ch4/wikipageviews{{logical_date.year}}{{logical_date.format('MM')}}{{logical_date.format('DD')}}-{{logical_date.format('HH')}}0000.sql",
    dag=dag,
)


get_data >> extract_data >> fetch_pageviews >> write_to_postgres
