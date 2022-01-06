import unittest

from tunip.service_config import get_service_config
from tunip.path.lake.kws import LakeKwsDomainPath, LakeKwsDomainSnapshotPath


class KwsTest(unittest.TestCase):
    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_kws_domain_path(self):
        kws_domain_path = LakeKwsDomainPath(self.user, self.domain)
        kws_snapshot_path = LakeKwsDomainSnapshotPath.from_parent(
            kws_domain_path, self.snapshot
        )
        assert kws_domain_path.has_snapshot() == True
        assert kws_snapshot_path.has_snapshot() == False
