class MSElasticsearch:
    def dag_get(self, ts):
        self.get(ts)

    def get(self, fetch_day):
        print("*" * 100)
        print("Fetching data from MS Elasticsearch.")
        print(fetch_day)
        print("*" * 100)
