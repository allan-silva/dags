import pendulum


class MSElasticsearch:
    def dag_get(self, ts):
        self.get_daily(ts)

    def get_daily(self, fetch_day):
        
        day = pendulum.parse(fetch_day)
        start = day.start_of("day")
        end = day.end_of("day")

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
        print(start)
        print(end)
        print(payload)
        print("*" * 100)

        return payload, start, end
