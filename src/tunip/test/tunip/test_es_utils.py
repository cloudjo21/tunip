from pathlib import Path
import unittest

from tunip.yaml_loader import YamlLoader
from tunip.service_config import get_service_config
from tunip.logger import init_logging_handler_for_klass
from tunip.es_utils import iterate_all_documents

from elasticsearch import Elasticsearch


class EsUtilsTest(unittest.TestCase):

    def setUp(self):
        self.service_config = get_service_config()
        self.elastic_host= "192.168.10.215:9200"
        self.index = "kowiki_fulltext"
        self.logger = init_logging_handler_for_klass(klass=self.__class__)
        self.es_client = Elasticsearch(self.elastic_host)
        
        
    def test_iter_all_documents(self):
        
        for cnt, entry in enumerate(iterate_all_documents(self.es_client, self.index, self.logger)):
            
            assert entry is not None
            
            if cnt>10:
                break

if __name__=="__main__":
    unittest.main()