import unittest

from elasticsearch import Elasticsearch
from parameterized import parameterized

from tunip.es_utils import (
    add_or_update_alias,
    create_index,
    delete_alias,
    init_elastic_client,
    iterate_all_documents,
    search_filter_many_values,
)
from tunip.logger import init_logging_handler_for_klass
from tunip.service_config import get_service_config


class EsUtilsTest(unittest.TestCase):

    def setUp(self):
        self.logger = init_logging_handler_for_klass(klass=self.__class__)

        self.service_config = get_service_config()
        self.index = "user2item_request_vectors"
        
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

    @parameterized.expand([("user2item_request_vectors", "21603e64-dbb4-4c0e-9653-108f42cde42d")])
    def test_search_by_index_and_field(self, index_name, account_sid):
        assert self.es is not None
        assert self.es.indices.exists(index=index_name) 

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"account_sid": account_sid}}
                    ]
                }
            }
        }

        result = self.es.search(body=query, index=index_name)
        assert result

    @parameterized.expand([("item_vectors", "account_sid", ["21603e64-dbb4-4c0e-9653-108f42cde42d", "fc209aa9-7042-4633-9c37-a5afad4556a1"], ["next_vector"])])
    def test_search_filter_many_values(self, index_name: str, query_field_name: str, values: list, source_fields: list):
        result = search_filter_many_values(self.es, index_name, query_field_name, values, source_fields)
        assert result[0][source_fields[0]] is not None

    @parameterized.expand([("my-test-index-20200101-000000-000000", "my-test-index", {"properties": {"sid": {"type": "keyword"}}})])
    def test_put_alias(self, index_name: str, alias: str, mappings: dict):
        create_index(self.es, index_name, mappings)
        result = add_or_update_alias(self.es, index_name, alias)
        assert result is not None and result.body["acknowledged"] is True
        delete_alias(self.es, index_name, alias)

    def tearDown(self):
        self.es.close()

if __name__ == "__main__":
    unittest.main()
