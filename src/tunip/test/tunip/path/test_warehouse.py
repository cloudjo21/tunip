import unittest

from tunip.path.warehouse import (
    WarehouseEntitySetDomainPath,
    WarehouseEntitySetDomainSnapshotPath
)


class MartTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_meta_kws_domain_path(self):
        entity_domain_path = WarehouseEntitySetDomainPath(self.user, self.domain)
        entity_snapshot_path = WarehouseEntitySetDomainSnapshotPath.from_parent(entity_domain_path, self.snapshot)
        assert entity_domain_path.has_snapshot() == True
        assert entity_snapshot_path.has_snapshot() == False 
