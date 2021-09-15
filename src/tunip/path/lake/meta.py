from .lake import LakePath

class LakeMetaPath(LakePath):
    def __init__(self, user_name):
        super(LakeMetaPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/query/keyword"


class LakeMetaKwsPath(LakeMetaPath):
    def __init__(self, user_name):
        super(LakeMetaKwsPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/kws"


class LakeMetaKwsDomainPath(LakeMetaPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeMetaKwsDomainPath, self).__init__(user_name)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"
