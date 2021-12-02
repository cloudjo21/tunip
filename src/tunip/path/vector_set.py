from tunip.path_utils import DomainPath


class VectorSetTaskPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
        super(VectorSetTaskPath, self).__init__(
            user_name, domain_name, snapshot_dt)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/model/{self.task_name}"


class VectorSetModelPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name, checkpoint):
        super(VectorSetModelPath, self).__init__(
            user_name, domain_name, snapshot_dt)
        self.checkpoint = checkpoint
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/model/{self.checkpoint}/{self.task_name}"


class NotSupportNautsPathException(Exception):
    pass


class NautsVecorSetPathFactory:

    @classmethod
    def create_training_family(cls, user_name, domain_name, snapshot_dt, task_name=None, checkpoint=None):
        if checkpoint:
            return VectorSetModelPath(user_name, domain_name, snapshot_dt, task_name, checkpoint)
        elif task_name:
            return VectorSetTaskPath(user_name, domain_name, snapshot_dt, task_name)
        else:
            raise NotSupportNautsPathException()

    @classmethod
    def create_evaluation_family(cls, user_name, domain_name, target_corpus_dt, snapshot_dt, checkpoint, task_name, condition_name):
        pass
