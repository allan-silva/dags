class MSElasticsearch:
    def dag_get(self, ts):
        self.get_daily(ts)

    def get_daily(self, fetch_day):

        start = fetch_day.start_of("day")
        end = fetch_day.end_of("day")

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
