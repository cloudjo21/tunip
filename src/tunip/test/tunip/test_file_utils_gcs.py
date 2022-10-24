import unittest
import urllib.parse

from pathlib import Path

import tunip.file_utils as fu

from tunip.config import Config
from tunip.constants import SPACE
from tunip.file_utils import HttpBasedWebHdfsFileHandler
from tunip.service_config import get_service_config

class FileUtilsGcsTest(unittest.TestCase):

    def setUp(self):
        service_config = get_service_config()
        self.gcs_handler = fu.services.get("GCS", config=service_config.config)

    def test_gcs_handler_list(self):
        file_list = self.gcs_handler.list_dir('/user/')
        assert len(file_list) > 0

    def test_gcs_handler_write(self):
        try:
            self.gcs_handler.write('/user/ed/test/file.dat', 'Hello World!')
        except Exception as e:
            self.fail(f'test_gcs_handler_write is failed: {str(e)}')

    def test_gcs_handler_load(self):
        try:
            contents = self.gcs_handler.load('/user/ed/test/file.dat')
            decoded = contents.decode('utf-8')
            self.assertEqual(decoded, 'Hello World!')
        except Exception as e:
            self.fail(f'test_gcs_handler_load is failed: {str(e)}')
