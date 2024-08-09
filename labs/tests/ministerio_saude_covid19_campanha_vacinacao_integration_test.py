import pendulum

from labs.commons.ms import MSElasticsearch


def test_get_daily():

    string_date = "2024-08-07T13:19:09.274542+00:00"
    expected_string_start_date = "2024-08-07T00:00:00.000+00:00"
    expected_string_end_date = "2024-08-07T23:59:59.999+00:00"

    ms_elastic = MSElasticsearch()
    payload, start_date, end_date = ms_elastic.get_daily(string_date)

    print(expected_string_start_date)
    assert expected_string_start_date == start_date
    assert expected_string_end_date == end_date
    assert expected_string_start_date == payload["query"]["range"]["@timestamp"]["gte"]
    assert expected_string_end_date == payload["query"]["range"]["@timestamp"]["lte"]
