import pytest

from airflow.models.connection import Connection


@pytest.fixture
def airflow_pg_conn():
    return Connection(
        conn_id="pg_integration_tests",
        login="labsu",
        password="labsu",
        host="192.168.60.30",
        port=5432,
        schema="intgration_tests",
    )
