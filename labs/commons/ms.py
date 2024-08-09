import pendulum

from labs.commons.dates import DEFAULT_TIME_ZONE, DEFAULT_FORMAT


class MSElasticsearch:
    def dag_get(self, ts):
        self.get_daily(ts)

    def get_daily(self, fetch_day):
        
        day = pendulum.parse(fetch_day, tz=DEFAULT_TIME_ZONE)
        start = day.start_of("day").format(DEFAULT_FORMAT)
        end = day.end_of("day").format(DEFAULT_FORMAT)

        payload = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lte": end,
                    }
                }
            },
            "from": 0,
            "size": 100,
        }

        print("*" * 100)
        print("Fetching data from MS Elasticsearch.")
        print(DEFAULT_TIME_ZONE)
        print(start)
        print(end)
        print(payload)
        print("*" * 100)

        return payload, start, end
