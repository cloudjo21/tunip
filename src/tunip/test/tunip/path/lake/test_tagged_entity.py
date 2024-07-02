import unittest

from tunip.path.lake.tagged_entity import (
    LakeTaggedEntityDomainPath,
    LakeTaggedEntityDomainSnapshotPath  
)

from tunip.service_config import get_service_config

class TaggedEntityTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"
        
    def test_init_tagged_entity_domain_path(self):
        tagged_entity_domain_path = LakeTaggedEntityDomainPath(self.user, self.domain)
        tagged_entity_snapshot_path = LakeTaggedEntityDomainSnapshotPath.from_parent(tagged_entity_domain_path, self.snapshot)
        assert tagged_entity_domain_path.has_snapshot() == True
        assert tagged_entity_snapshot_path.has_snapshot() == False 

