import mmap
import os
import pickle
import urllib

from hdfs import InsecureClient
from pathlib import Path
from smart_open import open as sm_open
from typing import TypeVar

from tunip.object_factory import ObjectFactory
from tunip.path_utils import HdfsUrlProvider, LocalPathProvider


def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines


def filesize(filename):
    return os.stat(filename).st_size


class FileHandler:
    pass


class HdfsFileHandler(FileHandler):
    def __init__(self, config):
        self.hdfs_url_builder = HdfsUrlProvider(config)
        self.client = InsecureClient(self.hdfs_url_builder.hdfs_host_root)

        self.hdfs_hostname = config["hdfs.hostname"]
        self.webhdfs_port = config.get("hdfs.namenode.http.port") or 50070
        self.webhdfs_host_root = f"webhdfs://{self.hdfs_hostname}:{self.webhdfs_port}"
    
    def list_dir(self, path):
        path_url = urllib.parse.quote(path)
        return [f"{path}/{p}" for p in self.client.list(path_url, status=False)]
    
    def load(self, path, encoding="utf-8"):
        with self.client.read(path, encoding=encoding) as f:
            contents = f.read()
        return contents
    
    def load_by_user(self, path):
        """
        :param: path    the path which has the prefix as user root hdfs path(/user/[HDFS_USER_NAME])
                        e.g., 'path' from /user/[HDFS_USER_NAME]/'path'
        """
        file_path = self.hdfs_url_builder.build(path)
        with self.client.read(file_path, encoding="utf-8") as f:
            contents = f.read()
        return contents
    
    def load_pickle(self, path):
        file_path = self.hdfs_url_builder.build(path)
        with self.client.read(file_path) as reader:
            bt_contents = reader.read()
            contents = pickle.load(bt_contents)
        return contents

    def loads_pickle(self, path):
        file_path = self.hdfs_url_builder.build(path)
        with self.client.read(file_path) as f:
            contents = f.read()
        pkl_obj = pickle.loads(contents)
        return pkl_obj

    def dumps_pickle(self, path, obj):
        file_path = self.hdfs_url_builder.build(path)
        contents = pickle.dumps(obj)
        self.client.write(path, data=contents)
    
    def mkdirs(self, path):
        self.client.makedirs(path)

    def write(self, path, contents, encoding='utf-8', append=False):
        self.client.write(path, data=contents, encoding=encoding, append=append)

    def exist(self, path):
        return self.client.status(path, strict=False)

    def open(self, path):
        f = sm_open(f"{self.webhdfs_host_root}/{path}")
        return f
    

class LocalFileHandler(FileHandler):
    def __init__(self, config):
        self.local_path_builder = LocalPathProvider(config)
    
    def list_dir(self, path):
        return [str(p.absolute()) for p in Path(path).glob("*")]
    
    def load(self, path, encoding="utf-8"):
        file_path = self.local_path_builder.build(path)
        with open(file_path) as f:
            contents = f.read()
        return contents
    
    def load_pickle(self, path):
        file_path = self.local_path_builder.build(path)
        with open(file_path, mode="rb") as f:
            contents = pickle.load(f)
        return contents

    def loads_pickle(self, path):
        file_path = self.local_path_builder.build(path)
        with open(file_path, mode='rb') as f:
            contents = f.read()
        pkl_contents = pickle.loads(contents)
        return pkl_contents

    def dumps_pickle(self, path, obj):
        file_path = self.local_url_builder.build(path)
        contents = pickle.dumps(obj)
        with open(file_path, mode="w") as f:
            f.write(contents)

    def mkdirs(self, path):
        dir_path = self.local_path_builder.build(path)
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def write(self, path, contents, append=False):
        file_path = self.local_path_builder.build(path)
        mode="a+" if append else "w"
        with open(file_path, mode=mode, encoding="utf-8") as f:
            f.write(contents)
    
    def save_pickle(self, path: str, contents: object):
        file_path = self.local_path_builder.build(path)
        with open(file_path, "wb") as f:
             pickle.dump(contents, f)

    def exist(self, path):
        return os.path.exists(path)



T = TypeVar("T", LocalFileHandler, HdfsFileHandler)


class FileHandlerFactory(ObjectFactory):

    def get(self, service_id, **kwargs) -> T:
        return self.create(service_id, **kwargs)


class LocalFileHandlerBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = LocalFileHandler(config)
        return self._instance


class HdfsFileHandlerBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = HdfsFileHandler(config)
        return self._instance


services = FileHandlerFactory()
services.register_builder("HDFS", HdfsFileHandlerBuilder())
services.register_builder("LOCAL", LocalFileHandlerBuilder())
