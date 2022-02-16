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
        super(WarehouseEntitySetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseEntitySetDomainPath, snapshot_dt: str):
        return WarehouseEntitySetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )


class WarehouseEntityTriePath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseEntityTriePath, self).__init__(user_name)
        self.source_type = source_type

    def __repr__(self):
        return f"{super().__repr__()}/entity_trie/{self.source_type}"


class WarehouseEntityTrieDomainPath(WarehouseEntityTriePath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseEntityTrieDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseEntityTrieDomainSnapshotPath(WarehouseEntityTrieDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseEntityTrieDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseEntityTrieDomainPath, snapshot_dt: str):
        return WarehouseEntityTrieDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )


class WarehouseSpanSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseSpanSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/span_set/{self.source_type}"


class WarehouseSpanSetDomainPath(WarehouseSpanSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseSpanSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseSpanSetDomainSnapshotPath(WarehouseSpanSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseSpanSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseSpanSetDomainPath, snapshot_dt: str):
        return WarehouseSpanSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )

class WarehouseMentionSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseMentionSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/mention_set/{self.source_type}"


class WarehouseMentionSetDomainPath(WarehouseMentionSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseMentionSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseMentionSetDomainSnapshotPath(WarehouseMentionSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseMentionSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseMentionSetDomainPath, snapshot_dt: str):
        return WarehouseMentionSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )
        
class WarehouseQuoteSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseQuoteSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/quote_set/{self.source_type}"


class WarehouseQuoteSetDomainPath(WarehouseQuoteSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseQuoteSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseQuoteSetDomainSnapshotPath(WarehouseQuoteSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseQuoteSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseQuoteSetDomainPath, snapshot_dt: str):
        return WarehouseQuoteSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )

class WarehouseCleanSpanSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseCleanSpanSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/clean_span_set/{self.source_type}"


class WarehouseCleanSpanSetDomainPath(WarehouseCleanSpanSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseCleanSpanSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseCleanSpanSetDomainSnapshotPath(WarehouseCleanSpanSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseCleanSpanSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseCleanSpanSetDomainPath, snapshot_dt: str):
        return WarehouseCleanSpanSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )

class WarehouseCleanMentionSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseCleanMentionSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/clean_mention_set/{self.source_type}"


class WarehouseCleanMentionSetDomainPath(WarehouseCleanMentionSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseCleanMentionSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseCleanMentionSetDomainSnapshotPath(WarehouseCleanMentionSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseCleanMentionSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseCleanMentionSetDomainPath, snapshot_dt: str):
        return WarehouseCleanMentionSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )
        
class WarehouseCleanQuoteSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseCleanQuoteSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/clean_quote_set/{self.source_type}"


class WarehouseCleanQuoteSetDomainPath(WarehouseCleanQuoteSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseCleanQuoteSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseCleanQuoteSetDomainSnapshotPath(WarehouseCleanQuoteSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseCleanQuoteSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseCleanQuoteSetDomainPath, snapshot_dt: str):
        return WarehouseCleanQuoteSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )
        
class WarehouseAnchorSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseAnchorSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/anchor_set/{self.source_type}"


class WarehouseAnchorSetDomainPath(WarehouseAnchorSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseAnchorSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseAnchorSetDomainSnapshotPath(WarehouseAnchorSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseAnchorSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseAnchorSetDomainPath, snapshot_dt: str):
        return WarehouseAnchorSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )
        
class WarehouseAnchorInstanceSetPath(WarehousePath):
    def __init__(self, user_name, source_type):
        super(WarehouseAnchorInstanceSetPath, self).__init__(user_name)
        self.source_type = source_type
        
    def __repr__(self):
        return f"{super().__repr__()}/anchor_instance_set/{self.source_type}"


class WarehouseAnchorInstanceSetDomainPath(WarehouseAnchorInstanceSetPath):
    def __init__(self, user_name, source_type, domain_name):
        super(WarehouseAnchorInstanceSetDomainPath, self).__init__(user_name, source_type)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class WarehouseAnchorInstanceSetDomainSnapshotPath(WarehouseAnchorInstanceSetDomainPath):
    def __init__(self, user_name, source_type, domain_name, snapshot_dt):
        super(WarehouseAnchorInstanceSetDomainSnapshotPath, self).__init__(user_name, source_type, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseAnchorInstanceSetDomainPath, snapshot_dt: str):
        return WarehouseAnchorInstanceSetDomainSnapshotPath(
            parent.user_name, parent.source_type, parent.domain_name, snapshot_dt
        )


class WarehouseVectorsPath(WarehousePath):
    def __init__(self, user_name):
        super(WarehouseVectorsPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/vectors"


class WarehouseVectorsDocumentSourcePath(WarehouseVectorsPath):
    def __init__(self, user_name, source_type):
        super(WarehouseVectorsDocumentSourcePath, self).__init__(user_name)
        self.source_type = source_type
    
    def __repr__(self):
        return f"{super().__repr__()}/document/{self.source_type}"

    def has_snapshot(self):
        return True


class WarehouseVectorsDocumentSourceSnapshotPath(WarehouseVectorsDocumentSourcePath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsDocumentSourceSnapshotPath, self).__init__(
            user_name, source_type
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseVectorsDocumentSourcePath, snapshot_dt: str):
        return WarehouseVectorsDocumentSourceSnapshotPath(
            parent.user_name, parent.source_type, snapshot_dt
        )


class WarehouseVectorsDocumentSourceSnapshotArrowPath(WarehouseVectorsDocumentSourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsDocumentSourceSnapshotArrowPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/arrow"


class WarehouseVectorsDocumentSourceSnapshotDid2vidPath(WarehouseVectorsDocumentSourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsDocumentSourceSnapshotDid2vidPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/did2vid"


class WarehouseVectorsDocumentSourceSnapshotVid2didPath(WarehouseVectorsDocumentSourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsDocumentSourceSnapshotVid2didPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/vid2did"


class WarehouseVectorsEntitySourcePath(WarehouseVectorsPath):
    def __init__(self, user_name, source_type):
        super(WarehouseVectorsEntitySourcePath, self).__init__(user_name)
        self.source_type = source_type
    
    def __repr__(self):
        return f"{super().__repr__()}/entity/{self.source_type}"

    def has_snapshot(self):
        return True


class WarehouseVectorsEntitySourceSnapshotPath(WarehouseVectorsEntitySourcePath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsEntitySourceSnapshotPath, self).__init__(
            user_name, source_type
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: WarehouseVectorsEntitySourcePath, snapshot_dt: str):
        return WarehouseVectorsEntitySourceSnapshotPath(
            parent.user_name, parent.source_type, snapshot_dt
        )


class WarehouseVectorsEntitySourceSnapshotArrowPath(WarehouseVectorsEntitySourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsEntitySourceSnapshotArrowPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/arrow"


class WarehouseVectorsEntitySourceSnapshotEid2vidPath(WarehouseVectorsEntitySourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsEntitySourceSnapshotEid2vidPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/eid2vid"


class WarehouseVectorsEntitySourceSnapshotVid2eidPath(WarehouseVectorsEntitySourceSnapshotPath):
    def __init__(self, user_name, source_type, snapshot_dt):
        super(WarehouseVectorsEntitySourceSnapshotVid2eidPath, self).__init__(
            user_name, source_type, snapshot_dt
        )

    def __repr__(self):
        return f"{super().__repr__()}/vid2eid"
