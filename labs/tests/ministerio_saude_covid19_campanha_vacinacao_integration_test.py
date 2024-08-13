import os
import pytest
import pendulum

from labs.commons.ms import MSElasticsearch


@pytest.fixture
def env_vars():
    os.environ["MS_COVID19_CNV_CLUSTER_URL"] = "https://imunizacao-es.saude.gov.br"
    os.environ["MS_COVID19_CNV_CLUSTER_USER"] = "imunizacao_public"
    os.environ["MS_COVID19_CNV_CLUSTER_PASS"] = "qlto5t&7r_@+#Tlstigi"


def test_get_interval(env_vars):
    date_start = pendulum.datetime(2024, 8, 7, 0, 0, 0)
    date_end = pendulum.datetime(2024, 8, 8, 0, 0, 0)
    expected_string_start_date = "2024-08-07T00:00:00.000+00:00"
    expected_string_end_date = "2024-08-08T00:00:00.000+00:00"

    ms_elastic = MSElasticsearch()
    batch_size = 5
    elastic_op = next(ms_elastic.get_interval(date_start, date_end, batch_size=batch_size))

    print(expected_string_start_date)
    assert expected_string_start_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["gte"]
    assert expected_string_end_date == elastic_op.request_payload["query"]["range"]["@timestamp"]["lte"]
    assert expected_string_start_date == elastic_op.extra_data["start"]
    assert expected_string_end_date == elastic_op.extra_data["end"]
    assert len(elastic_op.response["hits"]["hits"]) == batch_size
    
