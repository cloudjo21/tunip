from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeKwsPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeKwsPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/kws"


class LakeKwsDomainPath(LakeKwsPath):
    def __init__(self, user_name, domain_name):
        super(LakeKwsDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeKwsDomainSnapshotPath(LakeKwsDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeKwsDomainSnapshotPath, self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeKwsDomainPath, snapshot_dt):
        return LakeKwsDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )
