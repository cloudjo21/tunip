import requests
import unittest

from elasticsearch import Elasticsearch
from pathlib import Path

from tunip.config import Config
from tunip.es_utils import (
    init_elastic_client,
    iterate_all_documents,
    search_query_ids,
    search_query_match
)
from tunip.logger import init_logging_handler_for_klass
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader


class EsUtilsTest(unittest.TestCase):

    def setUp(self):
        self.logger = init_logging_handler_for_klass(klass=self.__class__)

        self.service_config = get_service_config()
        self.index = "news-2022-v1"
        
        try:
            self.use_https = self.service_config.elastic_host.index('https://') > -1
        except ValueError as ve:
            self.use_https = False

        if self.service_config.has_elastic_http_auth:
            self.http_auth = (
                self.service_config.elastic_username,
                self.service_config.elastic_password
            )
            self.es = Elasticsearch(
                hosts=self.service_config.elastic_host,
                http_auth=self.http_auth
            )
            self.user = self.service_config.elastic_username
            self.passwd = self.service_config.elastic_password
        else:
            self.es = Elasticsearch(
                hosts=self.service_config.elastic_host
            )
            self.user, self.passwd = None, None

        # self.elastic_host = self.service_config.elastic_host.replace("https://", "").replace("http://", "")
        self.elastic_host = self.service_config.elastic_host

    def test_init_elastic_client(self):
        es = init_elastic_client(self.service_config)
        print(self.service_config)
        assert es is not None
        assert es.indices.exists(self.index)

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
                    "title": "생수병"
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
                items={"_id": "936559"},
                use_https=self.use_https,
                user=self.user,
                passwd=self.passwd
            )
            assert response["hits"]["hits"][0]['_source']

    def test_search_query_ids(self):
        with requests.Session() as req_session:
            response = search_query_ids(
                req_session,
                host=self.elastic_host.split(":")[0],
                port=self.elastic_host.split(":")[1],
                index="kowiki_fulltext_anchor",
                ids=["936559"],
                use_https=self.use_https,
                user=self.user,
                passwd=self.passwd
            )
            assert len(response["hits"]["hits"]) > 0

    def tearDown(self):
        self.es.close()

if __name__ == "__main__":
    unittest.main()
