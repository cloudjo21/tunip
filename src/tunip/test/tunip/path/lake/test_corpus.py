import unittest

from tunip.path.lake.corpus import (
    LakeCorpusSerpDomainPath,
    LakeCorpusSerpDomainSnapshotPath,
    LakeCorpusWikiDomainPath,
    LakeCorpusWikiDomainSnapshotPath
)


class CorpusTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.domain = "mybank"
        self.snapshot = "19701231_000000_000000"

    def test_init_corpus_serp_domain_path(self):
        corpus_serp_domain_path = LakeCorpusSerpDomainPath(self.user, self.domain)
        corpus_serp_snapshot_path = LakeCorpusSerpDomainSnapshotPath.from_parent(corpus_serp_domain_path, self.snapshot)
        assert corpus_serp_domain_path.has_snapshot() == True
        assert corpus_serp_snapshot_path.has_snapshot() == False 

    def test_init_corpus_wiki_domain_path(self):
        corpus_wiki_domain_path = LakeCorpusWikiDomainPath(self.user, self.domain)
        corpus_wiki_snapshot_path = LakeCorpusWikiDomainSnapshotPath.from_parent(corpus_serp_domain_path, self.snapshot)
        assert corpus_wiki_domain_path.has_snapshot() == True
        assert corpus_wiki_snapshot_path.has_snapshot() == False 
