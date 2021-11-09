from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeMetaPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeMetaPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/meta"


class LakeMetaKwsPath(LakeMetaPath):
    def __init__(self, user_name):
        super(LakeMetaKwsPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/kws"


class LakeMetaKwsDomainPath(LakeMetaKwsPath):
    def __init__(self, user_name, domain_name):
        super(LakeMetaKwsDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeMetaKwsDomainSnapshotPath(LakeMetaKwsDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeMetaKwsDomainSnapshotPath, self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeMetaKwsDomainPath, snapshot_dt: str):
        return LakeMetaKwsDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeMetaEntityStatPath(LakeMetaPath):
    def __init__(self, user_name):
        super(LakeMetaEntityStatPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/entity_stat"


class LakeMetaEntityStatDomainPath(LakeMetaEntityStatPath):
    def __init__(self, user_name, domain_name):
        super(LakeMetaEntityStatDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True