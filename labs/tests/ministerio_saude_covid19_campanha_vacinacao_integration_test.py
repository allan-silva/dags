import os
import pytest
import pendulum

from labs.commons.ms import MSElasticsearch


@pytest.fixture
def env_vars():
    os.environ["MS_COVID19_CNV_CLUSTER_URL"] = "https://imunizacao-es.saude.gov.br"
    os.environ["MS_COVID19_CNV_CLUSTER_USER"] = "imunizacao_public"
    os.environ["MS_COVID19_CNV_CLUSTER_PASS"] = "qlto5t&7r_@+#Tlstigi"


def test_get_daily(env_vars):
    '''Book practice helper, must me ignored.'''
    date = "2024-08-07T19:23:55.000+00:00"
    expected_string_start_date = "2024-08-07T00:00:00.000+00:00"
    expected_string_end_date = "2024-08-07T23:59:59.999+00:00"

    ms_elastic = MSElasticsearch()
    elastic_op = ms_elastic.get_daily(date)

    print(expected_string_start_date)
    assert expected_string_start_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["gte"]
    assert expected_string_end_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["lte"]
    assert expected_string_start_date == elastic_op.extra_data["start"]
    assert expected_string_end_date == elastic_op.extra_data["end"]


def test_get_interval(env_vars):
    date = pendulum.datetime(2024, 8, 7, 0, 0, 0)
    expected_string_start_date = "2024-08-07T00:00:00.000+00:00"
    expected_string_end_date = "2024-08-07T23:59:59.999+00:00"

    ms_elastic = MSElasticsearch()
    elastic_op = next(ms_elastic.get_interval(date))

    print(expected_string_start_date)
    assert expected_string_start_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["gte"]
    assert expected_string_end_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["lte"]
    assert expected_string_start_date == elastic_op.extra_data["start"]
    assert expected_string_end_date == elastic_op.extra_data["end"]
    
