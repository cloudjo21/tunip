import unittest

from tunip.service_config import get_service_config
from tunip.path.lake.document import (
    LakeDocumentPath,
    LakeDocumentSnapshotPath
)


class DocumentTest(unittest.TestCase):

    def setUp(self):
        config = get_service_config()
        self.user = config.username
        self.source = "wiki"
        self.snapshot = "19701231_000000_000000"

    def test_init_document_path(self):
        document_path = LakeDocumentPath(self.user, self.source)
        document_snapshot_path = LakeDocumentSnapshotPath.from_parent(document_path, self.snapshot)
        assert document_path.has_snapshot() == True
        assert document_snapshot_path.has_snapshot() == False 