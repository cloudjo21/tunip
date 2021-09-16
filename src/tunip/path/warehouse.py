from tunip.path_utils import UserPath
from tunip.snapshot_utils import Snapshot


class WarehousePath(UserPath, Snapshot):
    def __init__(self, user_name):
        super(WarehousePath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/warehouse"


class WarehouseEntitySetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseEntitySetPath, self).__init__(user_name)
        self.source_type = source_type

    def __repr__(self):
        return f"{super().__repr__()}/entity_set/{self.source_type}"


class WarehouseEntitySetDomainPath(WarehouseEntitySetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseEntitySetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseEntitySetDomainSnapshotPath(WarehouseEntitySetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseEntitySetDomainSnapshotPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseEntitySetDomainPath, snapshot_dt: str):
        return WarehouseEntitySetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )
