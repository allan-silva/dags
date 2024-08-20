import pytest

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_wiki_stock_sense_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="wiki_stock_sense")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) > 0
