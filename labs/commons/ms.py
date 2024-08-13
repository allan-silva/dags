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
    def __init__(self, cluster_url=None, user=None, password=None):
        self.cluster_url = os.getenv("MS_COVID19_CNV_CLUSTER_URL", cluster_url)
        self.index = "desc-imunizacao"
        self.search_api_endpoint = "_search"
        self.user = os.getenv("MS_COVID19_CNV_CLUSTER_USER", user)
        self.password = os.getenv("MS_COVID19_CNV_CLUSTER_PASS", password)


    @property
    def search_url(self):
        return f"{self.cluster_url}/{self.index}/{self.search_api_endpoint}"


    @property
    def auth(self):
        return HTTPBasicAuth(self.user, self.password)


    def get_interval(self, start, end=None, skip=0, batch_size=10):
        start_interval = start.format(DEFAULT_FORMAT)
        end_interval = end if end else start.end_of("day")
        end_interval = end_interval.format(DEFAULT_FORMAT)
        payload = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_interval,
                        "lte": end_interval,
                    }
                }
            },
            "from": skip,
            "size": batch_size,
        }

        response = requests.get(self.search_url, auth=self.auth, json=payload)
        response.raise_for_status()

        print("*" * 100)
        print(f"Start: {start_interval} - End: {end_interval}")
        print(response.json())
        print("*" * 100)

        yield ElasticOp(payload, response.json(), {"start": start_interval, "end": end_interval})
