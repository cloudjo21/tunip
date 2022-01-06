from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeKnowledgePath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeKnowledgePath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/knowledge"


class LakeKnowledgeDomainPath(LakeKnowledgePath):
    def __init__(self, user_name, domain_name):
        super(LakeKnowledgeDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeKnowledgeDomainSnapshotPath(LakeKnowledgeDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeKnowledgeDomainSnapshotPath, self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeKnowledgeDomainPath, snapshot_dt):
        return LakeKnowledgeDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )
