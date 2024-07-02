import os
from pyarrow import fs as arrow_fs
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account

from tunip import file_utils
from tunip.path_utils import TaskPath, DomainPath, GcsUrlProvider, LocalPathProvider
from tunip.env import NAUTS_HOME
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader
from tunip.path.mart import MartPretrainedModelPath


class ModelLoader:
    def __init__(self, domain_name, model_name, snapshot_dt, task_name):
        self.service_config = get_service_config()

        self.task_name = task_name
        self.domain_path = DomainPath(user_name = self.service_config.username, 
                                      domain_name = domain_name,
                                      snapshot_dt = snapshot_dt)
        self.config_path = str(self.domain_path) + "/model/config.json"
        self.model_path = str(self.domain_path) + "/model/" + self.task_name

        self.vocab_path = str(MartPretrainedModelPath(self.service_config.username, model_name)) + "/vocab"
        self.file_loader = file_utils.services.get(self.service_config.filesystem.upper(), config=self.service_config.config)
        self.local_path_builder = LocalPathProvider(self.service_config.config)
        

    def _write_binary(self, load_path, save_path, f=""):
        with open(f"{load_path}/{f}".rstrip('/'), 'rb') as cont:
            contents = cont.read()
            self.file_loader.write_binary(path=f"{save_path}/{f}".rstrip('/'), contents=contents)

    def upload(self):
        local_config_path = self.local_path_builder.build(self.config_path)
        local_model_path = self.local_path_builder.build(self.model_path)
        local_vocab_path = self.local_path_builder.build(self.vocab_path)

        
        self.file_loader.mkdirs(self.model_path)

        model_files = os.listdir(local_model_path)
        for f in model_files:
            if os.path.isdir(f"{local_model_path}/{f}"):
                for _f in os.listdir(f"{local_model_path}/{f}"):
                    self._write_binary(f"{local_model_path}/{f}", f"{self.model_path}/{f}", _f)
            else:
                self._write_binary(local_model_path, self.model_path, f)

        self._write_binary(local_config_path, self.config_path)

        vocab_files = os.listdir(local_vocab_path)
        for f in vocab_files:
            self._write_binary(local_vocab_path, self.vocab_path, f)