import datetime as dt

from pydantic import BaseModel

from tunip.path.lake import LakePath


class DomainBasedFeaturePath(BaseModel):
    lake_path: LakePath
    domain_name: str

    class Config:
        arbitrary_types_allowed = True
    
    def __repr__(self):
        return f"{self.lake_path}/{self.domain_name}"

class SchemaTypedFeaturePath(BaseModel):
    domain_path: DomainBasedFeaturePath
    schema_type: str
    phase_type: str
    class Config:
        arbitrary_types_allowed = True

    def __repr__(self):
        return f"{repr(self.domain_path)}/{self.schema_type}/{self.phase_type}"

class SubSchemaTypedFeaturePath(SchemaTypedFeaturePath):
    sub_schema_type: str

    def __repr__(self):
        return f"{repr(self.domain_path)}/{self.schema_type}/{self.sub_schema_type}/{self.phase_type}"

class SnapshotPath(BaseModel):
    snapshot_dt: str

class DailyPeriodicFeaturePath(SnapshotPath):
    schema_path: SchemaTypedFeaturePath
    end_date: dt.datetime
    period_days: int

    def __repr__(self):
        partition_name = f"{self.end_date.date()}-period-{self.period_days}d"
        return f"{repr(self.schema_path)}/{self.snapshot_dt}/{partition_name}"


class SubDailyPeriodicFeaturePath(DailyPeriodicFeaturePath):
    sub_partition_name: str

    def __repr__(self):
        return f"{super().__repr__()}/{self.sub_partition_name}"


class MonthlyPeriodicFeaturePath(SnapshotPath):
    schema_path: SchemaTypedFeaturePath
    end_date: dt.datetime
    period_months: int

    def __repr__(self):
        partition_name = f"{self.end_date.date()}-period-{self.period_months}m"
        return f"{repr(self.schema_path)}/{self.snapshot_dt}/{partition_name}"


class SchemaTypedSnapshotFeaturePath(SnapshotPath):
    schema_path: SchemaTypedFeaturePath

    def __repr__(self):
        return f"{repr(self.schema_path)}/{self.snapshot_dt}"

class SubSchemaTypedSnapshotFeaturePath(SnapshotPath):
    sub_schema_path: SubSchemaTypedFeaturePath

    def __repr__(self):
        return f"{repr(self.sub_schema_path)}/{self.snapshot_dt}"
