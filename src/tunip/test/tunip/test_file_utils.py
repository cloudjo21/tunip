import unittest
from pathlib import Path

import tunip.file_utils as fu
from tunip.config import Config


class FileUtilsTest(unittest.TestCase):

    def setUp(self):
        config = Config(
            Path(__file__).parent.parent.parent.parent.parent / "experiments" / "application.json"
        )
        self.hdfs_handler = fu.services.get("HDFS", config=config)
        self.local_handler = fu.services.get("LOCAL", config=config)

    def test_hdfs_handler_list(self):
        lake_dir_list = self.hdfs_handler.list_dir("/user/nauts/lake/tagged_entity/span+serp")
        assert len(lake_dir_list) > 0

    def test_local_handler_list(self):
        dirs = self.local_handler.list_dir(Path(__file__).parent.parent.parent.parent.parent)
        assert len(dirs) > 0
