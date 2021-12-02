from tunip.path_utils import DomainPath


class VectorSetTaskPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
        super(VectorSetTaskPath, self).__init__(
            user_name, domain_name, snapshot_dt)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/vector_set/{self.task_name}"


class VectorSetModelPath(VectorSetTaskPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name, index_type):
        super(VectorSetModelPath, self).__init__(
            user_name, domain_name, snapshot_dt, task_name)
        self.index_type = index_type

    def __repr__(self):
        return f"{super().__repr__()}/{self.index_type}"


class NotSupportNautsVecorSetPathException(Exception):
    pass


class NautsVecorSetPathFactory:

    @classmethod
    def create_vector_set_family(cls, user_name, domain_name, snapshot_dt, task_name=None, index_type=None):
        if index_type:
            return VectorSetModelPath(user_name, domain_name, snapshot_dt, task_name, index_type)
        elif task_name:
            return VectorSetTaskPath(user_name, domain_name, snapshot_dt, task_name)
        else:
            raise NotSupportNautsVecorSetPathException()
