from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeSerpPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeSerpPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/serp"


class LakeSerpQueryEntityPath(LakeSerpPath):
    def __init__(self, user_name):
        super(LakeSerpQueryEntityPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/query/entity"


class LakeSerpQueryEntityDomainPath(LakeSerpQueryEntityPath):
    def __init__(self, user_name, domain_name):
        super(LakeSerpQueryEntityDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeSerpQueryEntityDomainSnapshotPath(LakeSerpQueryEntityDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeSerpQueryEntityDomainSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeSerpQueryEntityPath, snapshot_dt: str):
        return LakeSerpQueryEntityDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeSerpQueryKeywordPath(LakeSerpPath):
    def __init__(self, user_name):
        super(LakeSerpQueryKeywordPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/query/keyword"


class LakeSerpQueryKeywordDomainPath(LakeSerpQueryKeywordPath):
    def __init__(self, user_name, domain_name):
        super(LakeSerpQueryKeywordDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeSerpQueryKeywordDomainSnapshotPath(LakeSerpQueryKeywordDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeSerpQueryKeywordDomainSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeSerpQueryKeywordDomainPath, snapshot_dt: str):
        return LakeSerpQueryKeywordDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeSerpTextKeywordPath(LakeSerpPath):
    def __init__(self, user_name):
        super(LakeSerpTextKeywordPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/text/keyword"


class LakeSerpTextKeywordDomainPath(LakeSerpTextKeywordPath):
    def __init__(self, user_name, domain_name):
        super(LakeSerpTextKeywordDomainPath, self).__init__(
            user_name
        )
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeSerpTextKeywordDomainSnapshotPath(LakeSerpTextKeywordDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeSerpTextKeywordDomainSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeSerpTextKeywordDomainPath, snapshot_dt: str):
        return LakeSerpTextKeywordDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )
