import unittest
import logging

from tunip.file_utils import services as file_services
from tunip.path_utils import services as path_services
from tunip.path.warehouse import WarehouseEntityTrieDomainPath
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider


class SnapshotUtilsTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger()
        self.service_config = get_service_config()
        self.snapshot_path_provider = SnapshotPathProvider(self.service_config)

        # /user/nauts/warehouse/entity_trie/wiki/wiki_span
        self.test_path = WarehouseEntityTrieDomainPath(
            user_name='nauts',
            source_type='wiki',
            domain_name='wiki_span'
        )

    def test_latest_for_local_fs(self):
        f_handler = file_services.get('LOCAL', config=self.service_config.config)
        p_handler = path_services.get('LOCAL', config=self.service_config.config)
        real_test_path = p_handler.build(str(self.test_path))
        if f_handler.exist(real_test_path):
            self.snapshot_path_provider.latest(self.test_path, force_fs='LOCAL')
        else:
            self.logger.warning(f"NOT exist: {real_test_path}")

    def test_latest_for_hdfs_fs(self):
        f_handler = file_services.get('HDFS', config=self.service_config.config)
        p_handler = path_services.get('HDFS', config=self.service_config.config)
        real_test_path = p_handler.build(str(self.test_path))
        if f_handler.exist(real_test_path):
            self.snapshot_path_provider.latest(self.test_path, force_fs='HDFS')
        else:
            self.logger.warning(f"NOT exist: {real_test_path}")

    def test_get_latest_snapshot_dt(self):
        f_handler = file_services.get('HDFS', config=self.service_config.config)
        p_handler = path_services.get('HDFS', config=self.service_config.config)
        real_test_path = p_handler.build(str(self.test_path))
        if f_handler.exist(real_test_path):
            snapshot_dt = self.snapshot_path_provider.latest_snapshot_dt(self.test_path, force_fs='HDFS')
            print(snapshot_dt)
            self.assertTrue(snapshot_dt)
        else:
            self.logger.warning(f"NOT exist: {real_test_path}")