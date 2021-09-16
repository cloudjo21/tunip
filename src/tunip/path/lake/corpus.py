from . import LakePath


class LakeCorpusPath(LakePath):
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
        super(LakeCorpusSerpDomainSnapshotPath, self).__init__(user_name, domain_name)
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
        super(LakeCorpusWikiDomainSnapshotPath, self).__init__(user_name, domain_name)
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
