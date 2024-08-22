import pytest
import psycopg

from labs.commons.sql import execute_sql_file


@pytest.fixture
def pg_cursor(airflow_pg_conn):
    with psycopg.connect(
        host=airflow_pg_conn.host,
        user=airflow_pg_conn.login,
        password=airflow_pg_conn.password,
        port=airflow_pg_conn.port,
        dbname=airflow_pg_conn.schema,
    ) as conn:
        with conn.cursor() as cur:
            yield cur


@pytest.fixture
def table(pg_cursor):
    table_name = "execute_file"
    ddl = f"""
create table if not exists {table_name}(filename varchar(255));
"""
    pg_cursor.execute(ddl)
    yield table_name

    ddl = """
drop table {table_name};
"""


@pytest.fixture
def sql_file(table):
    file = "/tmp/sql_file.sql"
    with open(file, mode="w") as fh:
        fh.write(
            f"""
                 insert into {table}(filename) values('{PHRASE}');\n
                 """
        )
    return file


PHRASE = "A vida, o universo e tudo mais"


def test_execute_sql_file(airflow_pg_conn, pg_cursor, sql_file, table):
    execute_sql_file(airflow_pg_conn, sql_file)

    (filename,) = pg_cursor.execute(f"select filename from {table};").fetchone()
    assert PHRASE == filename
