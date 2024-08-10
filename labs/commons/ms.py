import pendulum
import os
import json
import requests

from dataclasses import dataclass

from requests.auth import HTTPBasicAuth

from labs.commons.dates import DEFAULT_TIME_ZONE, DEFAULT_FORMAT


@dataclass
class ElasticOp:
    request_payload: str
    response: dict
    extra_data: dict


class MSElasticsearch:
    def __init__(self):
        self.cluster_url = os.getenv("MS_COVID19_CNV_CLUSTER_URL")
        self.index = "desc-imunizacao"
        self.search_api_endpoint = "_search"
        self.user = os.getenv("MS_COVID19_CNV_CLUSTER_USER")
        self.password = os.getenv("MS_COVID19_CNV_CLUSTER_PASS")

    @property
    def search_url(self):
        return f"{self.cluster_url}/{self.index}/{self.search_api_endpoint}"

    @property
    def auth(self):
        return HTTPBasicAuth(self.user, self.password)

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

        response = requests.get(self.search_url, auth=self.auth, json=payload)
        response.raise_for_status()

        return ElasticOp(payload, response.json(), {"start": start, "end": end})
