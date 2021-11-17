from pathlib import Path
import unittest

from tunip.yaml_loader import YamlLoader
from tunip.service_config import get_service_config
from tunip.logger import init_logging_handler_for_klass
from tunip.es_utils import iter_search_scroll

from elasticsearch import Elasticsearch


class EsUtilsTest(unittest.TestCase):

    def setUp(self):
        self.service_config = get_service_config()
        self.elastic_host= "192.168.10.215:9200"
        self.index = "kowiki_fulltext"
        self.search_stat = {
            'scroll_id': None,
            'scroll_timeout': '2m',
            'scroll_size': 2000,
            'page_num': -1
        }
        self.logger = init_logging_handler_for_klass(klass=self.__class__)
        self.es_client = Elasticsearch(self.elastic_host)
        
        
    def test_iter_search_scroll(self):
        
        res_stat = iter_search_scroll(es=self.es_client, index=self.index, stat_dict=self.search_stat, logger=self.logger)
        while res_stat['has_next'] is True:
            res_stat = iter_search_scroll(
                es=self.es_client, 
                index=self.index,
                stat_dict=res_stat, 
                logger=self.logger
            )
            
            assert len(res_stat['hits'])==self.search_stat["scroll_size"]
            break

if __name__=="__main__":
    unittest.main()