import pendulum

from labs.commons.ms import MSElasticsearch

def test_get_daily():

    string_date = "2024-08-07T13:19:09.274542+00:00"
    expected_date = pendulum.parse(string_date)
    expected_start_date = expected_date.start_of("day")
    expected_end_date = expected_date.end_of("day")

    ms_elastic = MSElasticsearch()
    payload, start_date, end_date = ms_elastic.get_daily(string_date)

    assert expected_start_date == start_date
    assert expected_end_date == end_date
    assert expected_start_date == payload["query"]["range"]["@timestamp"]["gte"]
    assert expected_end_date == payload["query"]["range"]["@timestamp"]["lte"]
