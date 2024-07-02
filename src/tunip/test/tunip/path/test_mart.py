import unittest

from tunip.path.mart import (
    MartCorpusDomainPath,
    MartCorpusDomainSnapshotPath
)


class MartTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.task = "entity"
        self.snapshot = "19701231_000000_000000"

    def test_init_meta_kws_domain_path(self):
        corpus_domain_path = MartCorpusDomainPath(self.user, self,task, self.domain)
        corpus_snapshot_path = MartCorpusDomainSnapshotPath.from_parent(corpus_domain_path, self.snapshot)
        assert corpus_domain_path.has_snapshot() == True
        assert corpus_snapshot_path.has_snapshot() == False 
