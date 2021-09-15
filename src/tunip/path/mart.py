from tunip.path_utils import UserPath
from tunip.snapshot_utils import Snapshot


class MartPath(UserPath, Snapshot):

    def __init__(self, user_name):
        super(MartPath, self).__init__(user_name)
    
    def __repr__(self):
        return f"{super().__repr__()}/mart"


class MartCorpusPath(MartPath):

    def __init__(self, user_name):
        super(MartCorpusPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/corpus"


class MartCorpusTaskPath(MartCorpusPath):

    def __init__(self, user_name, task_name):
        super(MartCorpusTaskPath, self).__init__(user_name)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/{task_name}"


class MartCorpusDomainPath(MartCorpusTaskPath):

    def __init__(self, user_name, task_name, domain_name, snapshot_dt):
        super(MartCorpusDomainPath, self).__init__(user_name, task_name)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"
    
    def has_snapshot(self):
        return True
