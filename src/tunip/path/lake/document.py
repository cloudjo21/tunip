from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeDocumentPath(LakePath, Snapshot):
    def __init__(self, user_name, source_type):
        super(LakeDocumentPath, self).__init__(user_name)
        self.source_type = source_type

    def __repr__(self):
        return f"{super().__repr__()}/document/{self.source_type}"

    def has_snapshot(self):
        return True


class LakeDocumentSnapshotPath(LakeDocumentPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(LakeDocumentSnapshotPath, self).__init__(user_name, source_type)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeDocumentPath, snapshot_dt: str):
        return LakeDocumentSnapshotPath(
            parent.user_name, parent.source_type, snapshot_dt
        )
