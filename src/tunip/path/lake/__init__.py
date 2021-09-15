from tunip.path_utils import UserPath

# from .corpus import *
# from .kws import *
# from .meta import *
# from .serp import *


class LakePath(UserPath):
    def __init__(self, user_name):
        super(LakePath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/lake"
