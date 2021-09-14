from . import LakePath


class LakeKwsPath(LakePath):
    def __init__(self, user_name):
        super(LakeKwsPath, self).__init__(user_name)
    
    def __repr__(self):
        return f"{super().__repr__()}/kws"


class LakeKwsDomainPath(LakeKwsPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(LakeKwsPath, self).__init__(user_name)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"
