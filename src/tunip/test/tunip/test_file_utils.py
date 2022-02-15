import unittest
import urllib.parse

from pathlib import Path

import tunip.file_utils as fu

from tunip.config import Config
from tunip.constants import SPACE
from tunip.file_utils import HttpBasedWebHdfsFileHandler
from tunip.service_config import get_service_config


class FileUtilsTest(unittest.TestCase):

    def setUp(self):
        config = Config(
            Path(__file__).parent.parent.parent.parent.parent / "experiments" / "application.json"
        )
        self.hdfs_handler = fu.services.get("HDFS", config=config)
        self.local_handler = fu.services.get("LOCAL", config=config)

    def test_hdfs_handler_list(self):
        lake_dir_list = self.hdfs_handler.list_dir("/user/nauts/lake/tagged_entity/serp.small_span")
        assert len(lake_dir_list) > 0

    def test_local_handler_list(self):
        dirs = self.local_handler.list_dir(Path(__file__).parent.parent.parent.parent.parent)
        assert len(dirs) > 0

    def test_loads_pickle_keyword_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/wiki/wiki.small_span/20220121_114303_960817/entity_trie.pkl'
        assert self.hdfs_handler.exist(trie_path)
        if self.hdfs_handler.exist(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)
            assert '수호성인' in trie

    def test_loads_pickle_entity_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
        assert self.hdfs_handler.exist(trie_path)
        if self.hdfs_handler.exist(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)
            assert '히로시마 고속 교통'.replace(' ', SPACE) in trie

    def test_loads_and_dumps_pickle_entity_trie(self):
        trie_path = '/user/nauts/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
        assert self.hdfs_handler.exist(trie_path)
        if self.hdfs_handler.exist(trie_path):
            trie = self.hdfs_handler.loads_pickle(trie_path)

            new_trie_path = '/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360/entity_trie.pkl'
            self.hdfs_handler.mkdirs('/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360')
            self.hdfs_handler.dumps_pickle(new_trie_path, trie)
            new_trie = self.hdfs_handler.loads_pickle(new_trie_path)

            assert '히로시마 고속 교통'.replace(' ', SPACE) in new_trie

            self.hdfs_handler.client.delete('/user/nauts/test/warehouse/entity_trie/knowledge-entity/finance/20220121_114321_612360', recursive=True, skip_trash=True)

    def test_http_webhdfs(self):
        service_config = get_service_config(force_service_level='dev')
        model_name = urllib.parse.quote_plus('monologg/koelectra-small-v3-discriminator')
        config_path = f"/user/nauts/mart/plm/models/{urllib.parse.quote_plus(urllib.parse.quote_plus(model_name))}/config.json"

        webhdfs_handle = HttpBasedWebHdfsFileHandler(service_config.config)
        f = webhdfs_handle.open(config_path, mode='rb')
        assert f.content_length > 0
