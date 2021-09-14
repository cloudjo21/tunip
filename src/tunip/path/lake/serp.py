from . import LakePath


class LakeSerpPath(LakePath):
    def __init__(self, user_name):
        super(LakeSerpPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/serp"


class LakeSerpQueryEntityPath(LakeSerpPath):
    def __init__(self, user_name):
        super(LakeSerpEntityQueryPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/query/entity"


class LakeSerpQueryKeywordPath(LakeSerpPath):
    def __init__(self, user_name):
        super(LakeSerpKeywordQueryPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/query/keyword"
