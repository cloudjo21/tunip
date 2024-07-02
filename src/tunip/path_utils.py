from abc import ABC, abstractmethod
from glob import glob
from typing import Optional, TypeVar

from tunip.config import Config
from tunip.env import NAUTS_LOCAL_ROOT
from tunip.object_factory import ObjectFactory


def hdfs_path(hdfs_host, hdfs_port, hdfs_user):
    return f"hdfs://{hdfs_host}:{hdfs_port}/user/{hdfs_user}"


def web_hdfs_path(hdfs_host, web_hdfs_port, hdfs_user):
    return f"hdfs://{hdfs_host}:{web_hdfs_port}/user/{hdfs_user}"


def snapshot_path(path, domain_name, snapshot):
    return f"{path}/{domain_name}/{snapshot}"


def snapshot_hdfs_path(host, username, domain, snapshot, task):
    path = f"{host}/user/{username}/{domain}/{snapshot}/{task}"
    return path


def snapshot_dir_hdfs_path(host, username, domain, snapshot, task, dir):
    return f"{snapshot_path(host, username, domain, snapshot, task)}/{dir}"


def get_latest_path_for_prefix(dir_path, prefix):
    paths = sorted(glob(f"{dir_path}/{prefix}*"))
    assert len(paths) > 0
    return paths[-1]


def has_suffix_among(path, suffixes):
    for suffix in suffixes:
        if path[-len(suffix):] == suffix:
            return True
    return False


def default_local_user_dir(config: Config) -> Optional[str]:
    local_username = config.get("local.username") or None
    if local_username is not None:
        return f"/home/{local_username}/temp"
    return None


def get_service_repo_dir(config):
    return (config.get("service_repo_dir") or default_local_user_dir(config)) or NAUTS_LOCAL_ROOT


class NautsPath(ABC):

    @abstractmethod
    def __repr__(self):
        pass

class NotSupportNautsPathException(Exception):
    pass

class UserPath(NautsPath):
    def __init__(self, user_name):
        self.user_name = user_name
    
    def __repr__(self):
        return f"/user/{self.user_name}"


class DomainsPath(UserPath):
    def __init__(self, user_name):
        super(DomainsPath, self).__init__(user_name)
    
    def __repr__(self):
        return f"{super().__repr__()}/domains"


class DomainPath(DomainsPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(DomainPath, self).__init__(user_name)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}"


class TaskPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name):
       super(TaskPath, self).__init__(user_name, domain_name, snapshot_dt)
       self.task_name = task_name
    
    def __repr__(self):
        return f"{super().__repr__()}/model/{self.task_name}"


class ModelPath(DomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, task_name, checkpoint):
       super(ModelPath, self).__init__(user_name, domain_name, snapshot_dt)
       self.checkpoint = checkpoint
       self.task_name = task_name
    
    def __repr__(self):
        return f"{super().__repr__()}/model/{self.checkpoint}/{self.task_name}"


class EvalCorpusDomainPath(DomainsPath):
    def __init__(self, user_name, domain_name, snapshot_dt):
        super(EvalCorpusDomainPath, self).__init__(user_name)
        self.domain_name = domain_name
        self.snapshot_dt = snapshot_dt
    
    def __repr__(self):
        return f"{super().__repr__()}/{self.domain_name}/{self.snapshot_dt}/eval"


class EvalCorpusTaskPath(EvalCorpusDomainPath):
    def __init__(self, user_name, domain_name, snapshot_dt, checkpoint, task_name):
        super(EvalCorpusTaskPath, self).__init__(user_name, domain_name, snapshot_dt)
        self.checkpoint = checkpoint
        self.task_name = task_name

    def __repr__(self):
        return f"{super().__repr__()}/model/{self.checkpoint}/{self.task_name}"
        

class EvalCorpusConditionalPath(EvalCorpusTaskPath):
    def __init__(self, user_name, domain_name, snapshot_dt, checkpoint, task_name, condition_name, target_corpus_dt):
        super(EvalCorpusConditionalPath, self).__init__(user_name, domain_name, snapshot_dt, checkpoint, task_name)
        self.condition_name = condition_name
        self.target_corpus_dt = target_corpus_dt

    def __repr__(self):
        return f"{super().__repr__()}/{self.condition_name}/{self.target_corpus_dt}"


class NautsPathFactory:

    @classmethod
    def create_training_family(cls, user_name, domain_name, snapshot_dt, task_name=None, checkpoint=None):
        if checkpoint:
           return ModelPath(user_name, domain_name, snapshot_dt, task_name, checkpoint)
        elif task_name:
           return TaskPath(user_name, domain_name, snapshot_dt, task_name)
        elif snapshot_dt:
            return DomainPath(user_name, domain_name, snapshot_dt)
        elif user_name:
            return UserPath(user_name)
        else:
            raise NotSupportNautsPathException()

    @classmethod
    def create_evaluation_family(cls, user_name, domain_name, target_corpus_dt, snapshot_dt, checkpoint, task_name, condition_name):
        pass


class FileSystemPathProvider:
    pass

class HdfsUrlProvider(FileSystemPathProvider):
    def __init__(self, config):
        self.hdfs_hostname = config["hdfs.hostname"]
        self.hdfs_port = config.get("hdfs.namenode.http.port") or 50070
        self.hdfs_username = config["hdfs.username"]

        self.hdfs_host_root = f"http://{self.hdfs_hostname}:{self.hdfs_port}"
        self.hdfs_user_root = f"http://{self.hdfs_hostname}:{self.hdfs_port}/user/{self.hdfs_username}"

    def build(self, path):
        return path

    def build_http(self, path):
        url_path = f"{self.hdfs_host_root}/{path}"
        return url_path
    
    def build_by_user(self, path):
        """
        build path relative to hdfs username
        """
        url_path = f"{self.hdfs_user_root}/{path}"
        return url_path
    
    @property
    def user_root_path(self):
        return f"/user/{self.hdfs_username}"


class GcsUrlProvider(FileSystemPathProvider):
    def __init__(self, config):
        self.gcs_username = config.get("gcs.username") or "nauts"
        self.gcs_bucketname = config.get("gcs.bucketname")

    def build(self, path):
        file_path = f"{self.gcs_bucketname}{path}"
        return file_path


class LocalPathProvider(FileSystemPathProvider):
    def __init__(self, config):
        self.local_username = config.get("local.username") or "nauts"
    
    def build(self, path):
        file_path = f"{NAUTS_LOCAL_ROOT}/{path}"
        return file_path


T = TypeVar("T", LocalPathProvider, GcsUrlProvider, HdfsUrlProvider)

class PathProviderFactory(ObjectFactory):

    def get(self, service_id, **kwargs) -> T:
        return self.create(service_id, **kwargs)

    def __call__(self, service_config):
        return self.get(service_config.filesystem.upper(), config=service_config.config)


class LocalPathProviderBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = LocalPathProvider(config)
        return self._instance


class GcsUrlProviderBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = GcsUrlProvider(config)
        return self._instance


class HdfsUrlProviderBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = HdfsUrlProvider(config)
        return self._instance


services = PathProviderFactory()
services.register_builder("HDFS", HdfsUrlProviderBuilder())
services.register_builder("GCS", GcsUrlProviderBuilder())
services.register_builder("LOCAL", LocalPathProviderBuilder())
