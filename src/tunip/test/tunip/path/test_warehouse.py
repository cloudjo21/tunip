import unittest

from tunip.service_config import get_service_config
from tunip.path.warehouse import (
    WarehouseEntitySetDomainPath,
    WarehouseEntitySetDomainSnapshotPath,

    WarehouseEntityTrieDomainPath,
    WarehouseEntityTrieDomainSnapshotPath,
    
    WarehouseSpanSetDomainPath,
    WarehouseSpanSetDomainSnapshotPath,
    
    WarehouseMentionSetDomainPath,
    WarehouseMentionSetDomainSnapshotPath,
    
    WarehouseQuoteSetDomainPath,
    WarehouseQuoteSetDomainSnapshotPath,
    
    WarehouseCleanSpanSetDomainPath,
    WarehouseCleanSpanSetDomainSnapshotPath,
    
    WarehouseCleanMentionSetDomainPath,
    WarehouseCleanMentionSetDomainSnapshotPath,
    
    WarehouseCleanQuoteSetDomainPath,
    WarehouseCleanQuoteSetDomainSnapshotPath,
    
    WarehouseAnchorSetDomainPath,
    WarehouseAnchorSetDomainSnapshotPath,
)

class WarehouseTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.source = "wiki"
        self.domain = "all"
        self.snapshot = "19701231_000000_000000"

    def test_init_entity_domain_path(self):
        entity_domain_path = WarehouseEntitySetDomainPath(self.user, self.source, self.domain)
        entity_snapshot_path = WarehouseEntitySetDomainSnapshotPath.from_parent(entity_domain_path, self.snapshot)
        assert entity_domain_path.has_snapshot() == True
        assert entity_snapshot_path.has_snapshot() == False 
    
    def test_init_entity_trie_domain_path(self):
        trie_domain_path = WarehouseEntityTrieDomainPath(self.user, self.source, self.domain)
        trie_snapshot_path = WarehouseEntityTrieDomainSnapshotPath.from_parent(trie_domain_path, self.snapshot)
        assert trie_domain_path.has_snapshot() == True
        assert trie_snapshot_path.has_snapshot() == False
    
    def test_init_span_domain_path(self):
        span_domain_path = WarehouseSpanSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseSpanSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 
        
    def test_init_mention_domain_path(self):
        span_domain_path = WarehouseMentionSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseMentionSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 
    
    def test_init_quote_domain_path(self):
        span_domain_path = WarehouseQuoteSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseQuoteSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 

    def test_init_clean_span_domain_path(self):
        span_domain_path = WarehouseCleanSpanSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseCleanSpanSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 
        
    def test_init_clean_mention_domain_path(self):
        span_domain_path = WarehouseCleanMentionSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseCleanMentionSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 
    
    def test_init_clean_quote_domain_path(self):
        span_domain_path = WarehouseCleanQuoteSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseCleanQuoteSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 

    def test_init_anchor_domain_path(self):
        span_domain_path = WarehouseAnchorSetDomainPath(self.user, self.source, self.domain)
        span_snapshot_path = WarehouseAnchorSetDomainSnapshotPath.from_parent(span_domain_path, self.snapshot)
        
        assert span_domain_path.has_snapshot() == True
        assert span_snapshot_path.has_snapshot() == False 
        
if __name__=="__main__":
    unittest.main()