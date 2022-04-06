import hdfs
import unittest

from tunip.path.lake.serp import (
    LakeSerpQueryEntityDomainPath,
    LakeSerpQueryEntityDomainSnapshotPath,
    LakeSerpQueryKeywordDomainPath,
    LakeSerpQueryKeywordDomainSnapshotPath,
    LakeSerpQueryStatDomainPath,
    LakeSerpTextPairDomainPath,
    LakeSerpTextPairDomainSnapshotPath
)
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider


class SerpTest(unittest.TestCase):

    def setUp(self):
        self.config = get_service_config()
        self.user = self.config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_serp_query_entity_domain_path(self):
        serp_query_entity_domain_path = LakeSerpQueryEntityDomainPath(self.user, self.domain)
        serp_query_entity_snapshot_path = LakeSerpQueryEntityDomainSnapshotPath.from_parent(serp_query_entity_domain_path, self.snapshot)
        assert serp_query_entity_domain_path.has_snapshot() == True
        assert serp_query_entity_snapshot_path.has_snapshot() == False 
        
    def test_init_serp_text_pair_path_1(self):
        serp_query_entity_domain_path = LakeSerpQueryEntityDomainPath(self.user, self.domain)
        serp_query_entity_snapshot_path = LakeSerpQueryEntityDomainSnapshotPath.from_parent(serp_query_entity_domain_path, self.snapshot)
        assert serp_query_entity_domain_path.has_snapshot() == True
        assert serp_query_entity_snapshot_path.has_snapshot() == False 

    def test_init_serp_query_stat_path(self):
        snapshot_path = SnapshotPathProvider(self.config)
        domain_name = 'cosmetic'
        # stat_path = f'/user/nauts/lake/serp/query/stat/{domain_name}'
        stat_path = LakeSerpQueryStatDomainPath(
            user_name='nauts',
            entity_type='entity',
            domain_name=domain_name
        )

        # with self.assertRaises(hdfs.util.HdfsError):
        try:
            stat_snapshot_path = snapshot_path.latest(stat_path, force_fs='HDFS')
            assert stat_snapshot_path
        except hdfs.util.HdfsError as he:
            pass
        
    def test_init_serp_text_pair_path(self):
        snapshot_path = SnapshotPathProvider(self.config)
        domain_name = 'cosmetic'
        # pair_path = f'/user/nauts/lake/serp/text/pair/{domain_name}'
        pair_path = LakeSerpTextPairDomainPath(
            user_name='nauts',
            domain_name=domain_name
        )

        # with self.assertRaises(hdfs.util.HdfsError):
        try:
            pair_snapshot_path = snapshot_path.latest(pair_path, force_fs='HDFS')
            assert pair_snapshot_path
        except hdfs.util.HdfsError as he:
            pass
