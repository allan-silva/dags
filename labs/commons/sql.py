import psycopg

from labs.commons.fileutils import read_line


def execute_sql_file(airflow_conn, sql_file):
    with psycopg.connect(
        user=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port,
        dbname=airflow_conn.schema,
        autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            for line in read_line(sql_file):
                cursor.execute(line)
