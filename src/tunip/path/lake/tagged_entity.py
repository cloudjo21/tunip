from tunip.snapshot_utils import Snapshot

from . import LakePath

class LakeTaggedEntityPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeTaggedEntityPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/tagged_entity"

class LakeTaggedEntityDomainPath(LakeTaggedEntityPath):
    def __init__(self, user_name, domain_name):
        super(LakeTaggedEntityDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"
    
    def has_snapshot(self):
        return True
    
class LakeTaggedEntityDomainSnapshotPath(LakeTaggedEntityDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeTaggedEntityDomainSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeTaggedEntityDomainPath, snapshot_dt: str):
        return LakeTaggedEntityDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )
