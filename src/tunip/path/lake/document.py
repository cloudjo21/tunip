from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeDocumentPath(LakePath, Snapshot):
    def __init__(self, user_name, schema_type):
        super(LakeDocumentPath, self).__init__(user_name)
        self.schema_type = schema_type

    def __repr__(self):
        return f"{super().__repr__()}/document/{self.schema_type}"


class LakeDocumentSourcePath(LakeDocumentPath):
    def __init__(self, user_name, schema_type, source_type):
        super(LakeDocumentSourcePath, self).__init__(user_name, schema_type)
        self.source_type = source_type

    def __repr__(self):
        return f"{super().__repr__()}/{self.source_type}"

    def has_snapshot(self):
        return True


class LakeDocumentSnapshotPath(LakeDocumentSourcePath):
    def __init__(self, user_name, schema_type, source_type, snapshot_dt):
        super(LakeDocumentSnapshotPath, self).__init__(user_name, schema_type, source_type)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeDocumentSourcePath, snapshot_dt: str):
        return LakeDocumentSnapshotPath(
            parent.user_name, parent.schema_type, parent.source_type, snapshot_dt
        )
