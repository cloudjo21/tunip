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
        return f"{super().__repr__()}/{self.task_name}"


class MartCorpusDomainPath(MartCorpusTaskPath):
    def __init__(self, user_name, task_name, domain_name):
        super(MartCorpusDomainPath, self).__init__(user_name, task_name)
        self.domain_name = domain_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}"

    def has_snapshot(self):
        return True


class MartCorpusDomainSnapshotPath(MartCorpusDomainPath):
    def __init__(self, user_name, task_name, domain_name, snapshot_dt):
        super(MartCorpusDomainSnapshotPath, self).__init__(user_name, task_name, domain_name)
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.snapshot_dt}"

    def has_snapshot(self):
        return False

    @classmethod
    def from_parent(cls, parent: MartCorpusDomainPath, snapshot_dt: str):
        return MartCorpusDomainSnapshotPath(
            parent.user_name, parent.task_name, parent.domain_name, snapshot_dt
        )


class MartPretrainedModelPath(MartPath):
    def __init__(self, user_name, model_name):
        super(MartPretrainedModelPath, self).__init__(user_name)
        self.model_name = model_name

    def __repr__(self):
        return f"{super().__repr__()}/plm/models/{self.model_name}"


class MartPretrainedModelRuntimePath(MartPretrainedModelPath):
    def __init__(self, user_name, model_name, runtime_type):
        super(MartPretrainedModelRuntimePath, self).__init__(user_name, model_name)
        self.runtime_type = runtime_type

    def __repr__(self):
        return f"{super().__repr__()}/{self.runtime_type}"


class MartTokenizersPath(MartPath):
    def __init__(self, user_name):
        super(MartTokenizersPath, self).__init__(user_name)
    
    def __repr__(self):
        return f"{super().__repr__()}/tokenizers"


class MartTokenizerPath(MartTokenizersPath):
    def __init__(self, user_name, tokenizer_name):
        super(MartTokenizerPath, self).__init__(user_name)
        self.tokenizer_name = tokenizer_name
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.tokenizer_name}"


class MartModelsPath(MartPath):
    def __init__(self, user_name):
        super(MartModelsPath, self).__init__(user_name)

    def __repr__(self):
        return f"{super().__repr__()}/models"


class MartTaskPath(MartModelsPath):
    def __init__(self, user_name, task_name):
        super(MartTaskPath, self).__init__(user_name)
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.task_name}"


class MartModelPath(MartTaskPath):
    def __init__(self, user_name, task_name, model_name):
        super(MartModelPath, self).__init__(user_name, task_name)
        self.model_name = model_name

    def __repr__(self):
        return f"{super().__repr__()}/{self.model_name}"
