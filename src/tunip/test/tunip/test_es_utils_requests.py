import requests
import unittest

from tunip.es_utils import (
    search_query_ids,
    search_query_match
)
from tunip.logger import init_logging_handler_for_klass
from tunip.service_config import get_service_config


class EsUtilsRequestsTest(unittest.TestCase):

    def setUp(self):
        self.logger = init_logging_handler_for_klass(klass=self.__class__)

        self.service_config = get_service_config()
        self.index = "user2item_request_vectors"
        
        try:
            self.use_https = self.service_config.elastic_host.index('https://') > -1
        except ValueError as ve:
            self.use_https = False

        if self.service_config.has_elastic_http_auth:
            self.user = self.service_config.elastic_username
            self.passwd = self.service_config.elastic_password
        else:
            self.user, self.passwd = None, None

        self.elastic_host = self.service_config.elastic_host

    def test_search_by_fields(self):
        print(self.elastic_host)
        with requests.Session() as req_session:
            response = search_query_match(
                req_session,
                # host=self.elastic_host.split(":")[0],
                host=self.service_config.elastic_host,
                port=(int)(self.elastic_host.split(":")[1]),
                index="user2item_request_vectors",
                items={"account_sid": "21603e64-dbb4-4c0e-9653-108f42cde42d"},
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
                index=self.index,
                ids=['21603e64-dbb4-4c0e-9653-108f42cde42d'],
                use_https=self.use_https,
                user=self.user,
                passwd=self.passwd
            )
            assert len(response["hits"]["hits"]) > 0


if __name__ == "__main__":
    unittest.main()
