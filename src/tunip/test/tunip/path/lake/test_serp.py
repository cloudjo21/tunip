import unittest

from tunip.path.lake.serp import (
    LakeSerpQueryEntityDomainPath,
    LakeSerpQueryEntityDomainSnapshotPath,
    LakeSerpQueryKeywordDomainPath,
    LakeSerpQueryKeywordDomainSnapshotPath,
)


class MetaTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_serp_query_entity_domain_path(self):
        serp_query_entity_domain_path = LakeSerpQueryEntityDomainPath(self.user, self.domain)
        serp_query_entity_snapshot_path = LakeSerpQueryEntityDomainSnapshotPath.from_parent(serp_query_entity_domain_path, self.snapshot)
        assert serp_query_entity_domain_path.has_snapshot() == True
        assert serp_query_entity_snapshot_path.has_snapshot() == False 
