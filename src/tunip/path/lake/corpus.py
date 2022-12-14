from tunip.snapshot_utils import Snapshot

from . import LakePath


class LakeCorpusPath(LakePath, Snapshot):
    def __init__(self, user_name):
        super(LakeCorpusPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/corpus"


class LakeCorpusSerpPath(LakeCorpusPath):
    def __init__(self, user_name):
        super(LakeCorpusSerpPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/serp"


class LakeCorpusSerpDomainPath(LakeCorpusSerpPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusSerpDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeCorpusSerpDomainSnapshotPath(LakeCorpusSerpDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusSerpDomainSnapshotPath,
              self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusSerpDomainPath, snapshot_dt: str):
        return LakeCorpusSerpDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusSerpDomainNuggetPath(LakeCorpusSerpPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusSerpDomainNuggetPath, self).__init__(
            user_name
        )
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}.nugget"

    def has_snapshot(self):
        return True


class LakeCorpusSerpDomainNuggetSnapshotPath(LakeCorpusSerpDomainNuggetPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusSerpDomainNuggetSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusSerpDomainNuggetPath, snapshot_dt: str):
        return LakeCorpusSerpDomainNuggetSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusWikiPath(LakeCorpusPath):
    def __init__(self, user_name):
        super(LakeCorpusWikiPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/wiki"


class LakeCorpusWikiDomainPath(LakeCorpusWikiPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusWikiDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeCorpusWikiDomainSnapshotPath(LakeCorpusWikiDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusWikiDomainSnapshotPath,
              self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusWikiDomainPath, snapshot_dt: str):
        return LakeCorpusWikiDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusWikiDomainNuggetPath(LakeCorpusWikiPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusWikiDomainNuggetPath, self).__init__(
            user_name
        )
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}.nugget"

    def has_snapshot(self):
        return True


class LakeCorpusWikiDomainNuggetSnapshotPath(LakeCorpusWikiDomainNuggetPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusWikiDomainNuggetSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusWikiDomainNuggetPath, snapshot_dt: str):
        return LakeCorpusWikiDomainNuggetSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusDoccanoPath(LakeCorpusPath):
    def __init__(self, user_name):
        super(LakeCorpusDoccanoPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/doccano"


class LakeCorpusDoccanoDomainPath(LakeCorpusDoccanoPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusDoccanoDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeCorpusDoccanoDomainSnapshotPath(LakeCorpusDoccanoDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusDoccanoDomainSnapshotPath,
              self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusDoccanoDomainPath, snapshot_dt: str):
        return LakeCorpusDoccanoDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusDoccanoDomainNuggetPath(LakeCorpusDoccanoPath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusDoccanoDomainNuggetPath, self).__init__(
            user_name
        )
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}.nugget"

    def has_snapshot(self):
        return True


class LakeCorpusDoccanoDomainNuggetSnapshotPath(LakeCorpusDoccanoDomainNuggetPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusDoccanoDomainNuggetSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusDoccanoDomainNuggetPath, snapshot_dt: str):
        return LakeCorpusDoccanoDomainNuggetSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusKnowledgePath(LakeCorpusPath):
    def __init__(self, user_name):
        super(LakeCorpusKnowledgePath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/knowledge"


class LakeCorpusKnowledgeDomainPath(LakeCorpusKnowledgePath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusKnowledgeDomainPath, self).__init__(user_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class LakeCorpusKnowledgeDomainSnapshotPath(LakeCorpusKnowledgeDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusKnowledgeDomainSnapshotPath,
              self).__init__(user_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusKnowledgeDomainPath, snapshot_dt: str):
        return LakeCorpusKnowledgeDomainSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )


class LakeCorpusKnowledgeDomainNuggetPath(LakeCorpusKnowledgePath):
    def __init__(self, user_name, domain_name):
        super(LakeCorpusKnowledgeDomainNuggetPath, self).__init__(
            user_name
        )
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}.nugget"

    def has_snapshot(self):
        return True


class LakeCorpusKnowledgeDomainNuggetSnapshotPath(LakeCorpusKnowledgeDomainNuggetPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeCorpusKnowledgeDomainNuggetSnapshotPath, self).__init__(
            user_name, domain_name
        )
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: LakeCorpusKnowledgeDomainNuggetPath, snapshot_dt: str):
        return LakeCorpusKnowledgeDomainNuggetSnapshotPath(
            parent.user_name, parent.domain_name, snapshot_dt
        )
