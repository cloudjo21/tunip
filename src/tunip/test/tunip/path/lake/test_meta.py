import unittest

from tunip.path.lake.meta import (
    LakeMetaKwsDomainPath,
    LakeMetaKwsDomainSnapshotPath
)


class MetaTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_meta_kws_domain_path(self):
        meta_kws_domain_path = LakeMetaKwsDomainPath(self.user, self.domain)
        meta_kws_snapshot_path = LakeMetaKwsDomainSnapshotPath.from_parent(meta_kws_domain_path, self.snapshot)
        assert meta_kws_domain_path.has_snapshot() == True
        assert meta_kws_snapshot_path.has_snapshot() == False 
