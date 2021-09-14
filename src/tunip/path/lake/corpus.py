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


class LakeCorpusWikiPath(LakeCorpusPath):
    def __init__(self, user_name):
        super(LakeCorpusWikiPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/wiki"
