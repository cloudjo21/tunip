import unittest

from tunip.service_config import get_service_config
from tunip.path.lake.knowledge import (
    LakeKnowledgeDomainPath,
    LakeKnowledgeDomainSnapshotPath,
)


class KnowledgeTest(unittest.TestCase):
    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "finance"
        self.snapshot = "19701231_000000_000000"

    def test_init_knowledge_domain_path(self):
        knowledge_domain_path = LakeKnowledgeDomainPath(self.user, self.domain)
        knowledge_snapshot_path = LakeKnowledgeDomainSnapshotPath.from_parent(
            knowledge_domain_path, self.snapshot
        )
        assert knowledge_domain_path.has_snapshot() == True
        assert knowledge_snapshot_path.has_snapshot() == False
