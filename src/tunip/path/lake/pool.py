# from pydantic import BaseModel

from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakePoolPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakePoolPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/pool"


class LakePoolDateTypedPath(LakePoolPath):
    def __init__(self, user_name, domain_name, schema_type, source_type, pool_date_type):
        super(LakePoolDateTypedPath, self).__init__(user_name)
        self.domain_name = domain_name
        self.schema_type = schema_type 
        self.source_type = source_type 
        self.pool_date_type = pool_date_type

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.schema_type}/{self.source_type}/{self.pool_date_type}"

    def has_snapshot(self):
        return True


class LakePoolDateTypedSnapshotPath(LakePoolDateTypedPath):
    def __init__(self, user_name, domain_name, schema_type, source_type, pool_date_type, snapshot_dt):
        super(LakePoolDateTypedSnapshotPath, self).__init__(user_name, domain_name, schema_type, source_type, pool_date_type)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False


class LakePoolDateTypedPartitionPath(LakePoolDateTypedSnapshotPath):
    def __init__(self, user_name, domain_name, schema_type, source_type, pool_date_type, snapshot_dt, partition_name):
        super(LakePoolDateTypedPartitionPath, self).__init__(user_name, domain_name, schema_type, source_type, pool_date_type, snapshot_dt)
        self.partition_name = partition_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.partition_name}"
