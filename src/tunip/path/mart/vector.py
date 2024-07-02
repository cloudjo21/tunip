from . import MartPath


class MartVectorIndexPath(MartPath):
    def __init__(self, user_name):
        super(MartVectorIndexPath, self).__init__(user_name)
    
    def __repr__(self):
        return f"{super().__repr__()}/vector_index"


class MartVectorIndexTaskNamePath(MartVectorIndexPath):
    def __init__(self, user_name, task_name):
        super(MartVectorIndexTaskNamePath, self).__init__(user_name)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.task_name}"


class MartVectorIndexDocumentPath(MartVectorIndexTaskNamePath):
    def __init__(self, user_name):
        super(MartVectorIndexDocumentPath, self).__init__(user_name, 'document')
    
    def __repr__(self):
        return f"{super().__repr__()}"


class MartVectorIndexDocumentSourcePath(MartVectorIndexDocumentPath):
    def __init__(self, user_name, source_type):
        super(MartVectorIndexDocumentSourcePath, self).__init__(user_name)
        self.source_type = source_type
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.source_type}"


class MartVectorIndexDocumentSourceIndexTypePath(MartVectorIndexDocumentSourcePath):
    def __init__(self, user_name, source_type, index_type):
        super(MartVectorIndexDocumentSourceIndexTypePath, self).__init__(user_name, source_type)
        self.index_type = index_type
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.index_type}"

    def has_snapshot(self):
        return True


class MartVectorIndexDocumentSourceIndexTypeSnapshotPath(MartVectorIndexDocumentSourceIndexTypePath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexDocumentSourceIndexTypeSnapshotPath, self).__init__(user_name, source_type, index_type)
        self.snapshot_dt = snapshot_dt
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False
    
    @classmethod
    def from_parent(cls, parent: MartVectorIndexDocumentSourceIndexTypePath, snapshot_dt: str):
        return MartVectorIndexDocumentSourceIndexTypeSnapshotPath(
            parent.user_name, parent.source_type, parent.index_type, snapshot_dt
        )


class MartVectorIndexDocumentDid2vidPath(MartVectorIndexDocumentSourceIndexTypeSnapshotPath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexDocumentDid2vidPath, self).__init__(
            user_name, source_type, index_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/did2vid"


class MartVectorIndexDocumentVid2didPath(MartVectorIndexDocumentSourceIndexTypeSnapshotPath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexDocumentVid2didPath, self).__init__(
            user_name, source_type, index_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/vid2did"


class MartVectorIndexEntityPath(MartVectorIndexTaskNamePath):
    def __init__(self, user_name):
        super(MartVectorIndexEntityPath, self).__init__(user_name, 'entity')
    
    def __repr__(self):
        return f"{super().__repr__()}"


class MartVectorIndexEntitySourcePath(MartVectorIndexEntityPath):
    def __init__(self, user_name, source_type):
        super(MartVectorIndexEntitySourcePath, self).__init__(user_name)
        self.source_type = source_type
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.source_type}"


class MartVectorIndexEntitySourceIndexTypePath(MartVectorIndexEntitySourcePath):
    def __init__(self, user_name, source_type, index_type):
        super(MartVectorIndexEntitySourceIndexTypePath, self).__init__(user_name, source_type)
        self.index_type = index_type
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.index_type}"

    def has_snapshot(self):
        return True


class MartVectorIndexEntitySourceIndexTypeSnapshotPath(MartVectorIndexEntitySourceIndexTypePath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexEntitySourceIndexTypeSnapshotPath, self).__init__(user_name, source_type, index_type)
        self.snapshot_dt = snapshot_dt
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False
    
    @classmethod
    def from_parent(cls, parent: MartVectorIndexEntitySourceIndexTypePath, snapshot_dt: str):
        return MartVectorIndexEntitySourceIndexTypeSnapshotPath(
            parent.user_name, parent.source_type, parent.index_type, snapshot_dt
        )


class MartVectorIndexEntityEid2vidPath(MartVectorIndexEntitySourceIndexTypeSnapshotPath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexEntityEid2vidPath, self).__init__(
            user_name, source_type, index_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/eid2vid"


class MartVectorIndexEntityVid2eidPath(MartVectorIndexEntitySourceIndexTypeSnapshotPath):
    def __init__(self, user_name, source_type, index_type, snapshot_dt):
        super(MartVectorIndexEntityVid2eidPath, self).__init__(
            user_name, source_type, index_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/vid2eid"

