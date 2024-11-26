import json
import mmap
import os
import pickle
import pyarrow
import shutil
import urllib

from hdfs import InsecureClient
from pathlib import Path
from pyarrow import fs as arrow_fs
from pyarrow.fs import FileType as arrow_FileType
from smart_open import open as sm_open
from typing import TypeVar

from tunip.constants import SAFE_SYMBOLS_FOR_HTTP
from tunip.env import NAUTS_HOME
from tunip.object_factory import ObjectFactory
from tunip.path_utils import HdfsUrlProvider, GcsUrlProvider, LocalPathProvider


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

        try:
            self.pa_fs = arrow_fs.HadoopFileSystem(
                host=config["hdfs.hostname"],
                port=int(config["hdfs.port"])
            )
        except OSError:
            print(f"service_config.filesystem: {config['fs']}")
            self.pa_fs = None

    def copy_file(self, source, target):
        self.pa_fs.copy_file(source, target)
    
    def copy_files(self, source, target):
        arrow_fs.copy_files(source, target, source_filesystem=self.pa_fs, destination_filesystem=self.pa_fs)

    def list_dir(self, path, prepend_prefix=True):
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
        self.client.write(file_path, data=contents)
    
    def mkdirs(self, path):
        self.client.makedirs(path)

    def write(self, path, contents, encoding='utf-8', append=False):
        self.client.write(path, data=contents, encoding=encoding, append=append)

    def exists(self, path):
        return self.client.status(path, strict=False)

    def open(self, path, mode='r'):
        f = sm_open(f"{self.webhdfs_host_root}/{path}", mode=mode)
        return f

    def remove_dir(self, path, use_nauts_path=True):
        if use_nauts_path is True:
            dir_path = self.hdfs_url_builder.build(path)
        else:
            dir_path = path
        self.client.delete(dir_path, recursive=True)


class HttpBasedWebHdfsFileHandler(HdfsFileHandler):

    def __init__(self, service_config):
        super(HttpBasedWebHdfsFileHandler, self).__init__(service_config.config)
        self.webhdfs_http_host_root = f"http://{self.hdfs_hostname}:{self.webhdfs_port}/webhdfs/v1"
        self.service_config = service_config
        self.local_fh = services.get("LOCAL", config=service_config.config)

    def open(self, path, mode='r'):
        path_parts = Path(f"{self.webhdfs_http_host_root}/{path}").parts
        http_path = 'http://' + '/'.join(path_parts[1:]) + '?op=OPEN&noredirect=True'
        f = sm_open(urllib.parse.quote_plus(http_path, SAFE_SYMBOLS_FOR_HTTP), mode='rb')
        res_json = json.loads(f.response.text)
        if f.response.text:
            redirected_path = urllib.parse.quote_plus(res_json["Location"], SAFE_SYMBOLS_FOR_HTTP)
            redirected_file = sm_open(redirected_path, 'rb')
            return redirected_file
        else:
            return f

    def download(self, hdfs_path, overwrite=False, read_mode='r', write_mode='w'):
        status = self.client.status(urllib.parse.quote_plus(hdfs_path, SAFE_SYMBOLS_FOR_HTTP))
        if status['type'] == 'FILE':
            self.download_file(
                hdfs_path,
                hdfs_path,
                overwrite,
                read_mode,
                write_mode
            )
            return

        trails = self.client.walk(
                # FROM '/user/nauts/mart/plm/models/monologg%2Fkoelectra-small-v3-discriminator'
                # TO '/user/nauts/mart/plm/models/monologg%252Fkoelectra-small-v3-discriminator'
                urllib.parse.quote_plus(hdfs_path, SAFE_SYMBOLS_FOR_HTTP)
            )
        walked = next(trails, None)
        if not walked:
            raise Exception(f'INVALID hdfs path: {hdfs_path}')
        current_path, child_dirs, child_files = walked
        current_path = urllib.parse.unquote(current_path)

        for child_f in child_files:
            self.download_file(
                f"{current_path}/{child_f}",
                f"{current_path}/{child_f}",
                overwrite,
                read_mode,
                write_mode
            )
        for child_d in child_dirs:
            self.local_fh.mkdirs(f"{current_path}/{child_d}")

            self.download(
                f"{current_path}/{child_d}",
                overwrite,
                read_mode,
                write_mode
            )

    def download_file(self, hdfs_path, local_path, overwrite=False, read_mode='r', write_mode='w') -> str:
        f = self.open(urllib.parse.quote_plus(hdfs_path, SAFE_SYMBOLS_FOR_HTTP), mode=read_mode)

        local_path = Path(local_path)
        if local_path.is_dir() and local_path.exists():
            self.local_fh.mkdirs(local_path)
        else:
            self.local_fh.mkdirs(local_path.parent)

        if 'b' in write_mode:
            self.local_fh.write_binary(local_path, f.response.content)
        else:
            self.local_fh.write(local_path, f.response.text)
        return local_path
        # NOT WORKING for slash path encoded %2F
        # downloaded_path = self.client.download(hdfs_path=hdfs_path, local_path=local_path, overwrite=overwrite)
        # return downloaded_path


class GcsFileHandler(FileHandler):
    def __init__(self, config):

        from gcsfs import GCSFileSystem
        from tunip.google_cloud_utils import StorageDriver

        self.protocol = config['gcs.protocol']
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(Path(NAUTS_HOME) / 'resources' / f"{config.get('gcs.project_id')}.json")
        self.gcs_url_builder = GcsUrlProvider(config)
        self.pa_fs = arrow_fs.GcsFileSystem(default_bucket_location=self.gcs_url_builder.gcs_bucketname)
        self.storage_driver = StorageDriver(config)
        self.fs = GCSFileSystem(project=config.get("gcs.project_id"))
        self.protocol = config.get("gcs.protocol")

    def copy_file(self, source, target):
        self.pa_fs.copy_file(source, target)

    def copy_files(self, source, target):
        source = self.gcs_url_builder.build(source)
        target = self.gcs_url_builder.build(target)
        arrow_fs.copy_files(source, target, source_filesystem=self.pa_fs, destination_filesystem=self.pa_fs)

    def list_dir(self, path, prepend_prefix=True):
        url = self.gcs_url_builder.build(path)

        paths = []
        # :List[pyarrow.fs.FileInfo]
        for file_info in self.pa_fs.get_file_info(arrow_fs.FileSelector(url, recursive=False)):
        # for path in self.fs.ls(url):
            if file_info.type == 3:
            # if self.fs.isdir(path):
                paths.append(f"{self.protocol}{file_info.path}" if prepend_prefix else file_info.path)
        return paths

    def list_files(self, path, prepend_prefix=True):
        url = self.gcs_url_builder.build(path)
        paths = []
        # :List[pyarrow.fs.FileInfo]
        for file_info in self.pa_fs.get_file_info(arrow_fs.FileSelector(url, recursive=False)):
        # for path in self.fs.ls(url):
            if file_info.type == 2:
            # if self.fs.isdir(path):
                paths.append(f"{self.protocol}{file_info.path}" if prepend_prefix else file_info.path)
        return paths

    def load(self, path, encoding='utf-8'):
        url = self.gcs_url_builder.build(path)
        contents = None
        with self.pa_fs.open_input_stream(url) as stream:
            contents = stream.readall()
        return contents
    
    def load_pickle(self, path):
        file_path = self.gcs_url_builder.build(path)
        with self.pa_fs.open_input_stream(file_path) as reader:
            contents = pickle.load(reader)
        return contents

    def loads_pickle(self, path):
        file_path = self.gcs_url_builder.build(path)
        with self.pa_fs.open_input_stream(file_path) as reader:
            contents = reader.read()
        pkl_obj = pickle.loads(contents)
        return pkl_obj

    def dumps_pickle(self, path, obj):
        file_path = self.gcs_url_builder.build(path)
        contents = pickle.dumps(obj)
        with self.pa_fs.open_output_stream(file_path) as writer:
            writer.write(contents)

    def mkdirs(self, path):
        file_path = self.gcs_url_builder.build(path)
        file_info: pyarrow.fs.FileInfo = self.pa_fs.get_file_info(file_path)
        if file_info.type == pyarrow.fs.FileType.NotFound:
            self.pa_fs.create_dir(file_path)

    def write(self, path, contents, encoding='utf-8', append=False):
        file_path = self.gcs_url_builder.build(path)
        with self.pa_fs.open_output_stream(file_path) as writer:
            writer.write(contents.encode(encoding))

    def write_binary(self, path, contents):
        self.storage_driver.write_blob_and_parent(path, contents)

    def exists(self, path):
        file_path = self.gcs_url_builder.build(path)
        file_info = self.pa_fs.get_file_info(file_path)
        return (file_info.type == arrow_FileType.File or file_info.type == arrow_FileType.Directory)

    def download(self, path):
        self.storage_driver.download(path)
    
    def transfer_from_local(self, source_local_dirpath, dest_dirpath):
        self.storage_driver.recursive_copy_from_local(source_local_dirpath, self.gcs_url_builder.gcs_bucketname, dest_dirpath)

    def transfer_to_local(self, source_dirpath, dest_local_dirpath):
        """ download files of source path into the local path of destination """
        self.storage_driver.recursive_copy_to_local(source_dirpath, self.gcs_url_builder.gcs_bucketname, dest_local_dirpath)

    def copy_file_to_local(self, source_blob_filepath, dest_local_filepath):
        self.storage_driver.copy_file_to_local(source_blob_filepath, self.gcs_url_builder.gcs_bucketname, dest_local_filepath)

    def remove_dir(self, path, use_nauts_path=True):
        if use_nauts_path is True:
            dir_path = self.gcs_url_builder.build(path)
        else:
            protocol_index = path.index(self.protocol)
            if protocol_index == 0:
                dir_path = path[protocol_index + len(self.protocol):]
            else:
                dir_path = path
        self.pa_fs.delete_dir(dir_path)


class LocalFileHandler(FileHandler):
    def __init__(self, config):
        self.local_path_builder = LocalPathProvider(config)

    def copy_file(self, source, target):
        source_path = self.local_path_builder.build(source)
        target_path = self.local_path_builder.build(target)
        shutil.copyfile(source_path, target_path)
    
    def copy_files(self, source, target):
        arrow_fs.copy_files(source, target)

    def list_dir(self, path, prepend_prefix=True):
        if prepend_prefix is True:
            dir_path = self.local_path_builder.build(path)
        else:
            dir_path = path
        return [str(p.absolute()) for p in Path(dir_path).glob("*")]
    
    def load(self, path, encoding="utf-8"):
        file_path = self.local_path_builder.build(path)
        with open(file_path) as f:
            contents = f.read()
        return contents
    
    def load_binary(self, path, mode='rb'):
        file_path = self.local_path_builder.build(path)
        with open(file_path, mode=mode) as f:
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
        file_path = self.local_path_builder.build(path)
        contents = pickle.dumps(obj)
        with open(file_path, mode="wb") as f:
            f.write(contents)

    def mkdirs(self, path):
        dir_path = self.local_path_builder.build(path)
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def write(self, path, contents, encoding="utf-8", append=False):
        file_path = self.local_path_builder.build(path)
        mode="a+" if append else "w"
        with open(file_path, mode=mode, encoding=encoding) as f:
            f.write(contents)
            
    def write_binary(self, path, contents):
        file_path = self.local_path_builder.build(path)
        with open(file_path, mode='wb') as f:
            f.write(contents)
    
    def save_pickle(self, path: str, contents: object):
        file_path = self.local_path_builder.build(path)
        with open(file_path, "wb") as f:
             pickle.dump(contents, f)

    def exists(self, path):
        file_path = self.local_path_builder.build(path)
        return os.path.exists(file_path)

    def open(self, path, mode='r'):
        file_path = self.local_path_builder.build(path)
        f = sm_open(file_path, mode=mode)
        return f
    
    def remove_dir(self, path, use_nauts_path=True):
        if use_nauts_path:
            path = self.local_path_builder.build(path)
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(path)


T = TypeVar("T", LocalFileHandler, GcsFileHandler, HdfsFileHandler)


class FileHandlerFactory(ObjectFactory):

    def get(self, service_id, **kwargs) -> T:
        return self.create(service_id, **kwargs)

    def __call__(self, service_config):
        return self.get(service_config.filesystem.upper(), config=service_config.config)


class LocalFileHandlerBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = LocalFileHandler(config)
        return self._instance


class GcsFileHandlerBuilder:
    def __init__(self):
        self._instance = None

    def __call__(self, config):
        if not self._instance:
            self._instance = GcsFileHandler(config)
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
services.register_builder("GCS", GcsFileHandlerBuilder())
services.register_builder("LOCAL", LocalFileHandlerBuilder())
