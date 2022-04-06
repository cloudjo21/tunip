import requests
import unittest

from elasticsearch import Elasticsearch
from pathlib import Path

from tunip.config import Config
from tunip.es_utils import (
    iterate_all_documents,
    search_query_ids,
    search_query_match
)
from tunip.logger import init_logging_handler_for_klass
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader


class EsUtilsTest(unittest.TestCase):

    def setUp(self):
        self.service_config = get_service_config()
        self.elastic_host = "192.168.10.215:9200"
        self.index = "kowiki_fulltext"
        self.logger = init_logging_handler_for_klass(klass=self.__class__)
        self.es = Elasticsearch(self.elastic_host)

    def test_init_elastic_client(self):
        es = Elasticsearch(
            hosts=self.service_config.elastic_host,
            http_auth=(
                self.service_config.elastic_username, self.service_config.elastic_password
            )
        )
        assert es is not None
        assert es.indices.exists('kowiki_fulltext')

    def test_iter_all_documents(self):

        for cnt, entry in enumerate(iterate_all_documents(self.es, self.index, self.logger)):

            assert entry is not None

            if cnt > 10:
                break

    def test_iter_all_documents_input_body(self):
        body = {
            "size": 250,
            "query": {
                "match": {
                    "title_origin": "드래곤"
                }
            }
        }

        for cnt, entry in enumerate(iterate_all_documents(self.es, self.index, self.logger, body=body)):

            assert entry is not None

            if cnt > 10:
                break

    def test_iter_all_documents_input_hit_value(self):

        for cnt, entry in enumerate(iterate_all_documents(self.es, self.index, self.logger, hit_value='_id')):

            assert entry is not None

            if cnt > 10:
                break

    def test_iter_all_documents_input_hit_value_all(self):

        for cnt, entry in enumerate(iterate_all_documents(self.es, self.index, self.logger, hit_value='all')):

            assert entry is not None

            if cnt > 10:
                break

    def test_search_by_fields(self):
        with requests.Session() as req_session:
            response = search_query_match(
                req_session,
                host=self.elastic_host.split(":")[0],
                port=self.elastic_host.split(":")[1],
                index="kowiki_fulltext_anchor",
                items={"_id": "936559"}
            )
            assert response["hits"]["hits"][0]['_source']

    def test_search_query_ids(self):
        with requests.Session() as req_session:
            response = search_query_ids(
                req_session,
                host=self.elastic_host.split(":")[0],
                port=self.elastic_host.split(":")[1],
                index="kowiki_fulltext_anchor",
                ids=["936559"]
            )
            assert len(response["hits"]["hits"]) > 0

    def tearDown(self):
        self.es.close()

if __name__ == "__main__":
    unittest.main()
