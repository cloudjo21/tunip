from tunip.path_utils import DomainPath, UserPath


class VectorSetTaskPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
        super(VectorSetTaskPath, self).__init__(
            user_name, domain_name, snapshot_dt)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/vector_set/{self.task_name}"


class VectorSetIndexPath(VectorSetTaskPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name, index_type):
        super(VectorSetIndexPath, self).__init__(
            user_name, domain_name, snapshot_dt, task_name)
        self.index_type = index_type

    def __repr__(self):
        return f"{super().__repr__()}/{self.index_type}"


class VectorSetVid2didPath(VectorSetTaskPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
        super(VectorSetVid2didPath, self).__init__(
            user_name, domain_name, snapshot_dt, task_name)

    def __repr__(self):
        return f"{super().__repr__()}/vid2did"


class VectorSetDid2vidPath(VectorSetTaskPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
        super(VectorSetDid2vidPath, self).__init__(
            user_name, domain_name, snapshot_dt, task_name)

    def __repr__(self):
        return f"{super().__repr__()}/did2vid"


class NotSupportNautsVecorSetPathException(Exception):
    pass


class NautsVecorSetPathFactory:

    @classmethod
    def create_vector_set_family(cls, user_name, domain_name, snapshot_dt, task_name=None, index_type=None):
        if index_type:
            return VectorSetIndexPath(user_name, domain_name, snapshot_dt, task_name, index_type)
        elif task_name:
            return VectorSetTaskPath(user_name, domain_name, snapshot_dt, task_name)
        else:
            raise NotSupportNautsVecorSetPathException()
