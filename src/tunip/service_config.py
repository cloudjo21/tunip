import getpass
import os
import socket
import toml

from pathlib import Path
from pydantic import BaseModel
from typing import Optional

from tunip.config import Config
from tunip.constants import ELASTICSEARCH_ORIGIN
from tunip.env import NAUTS_LOCAL_ROOT, NAUTS_HOME


class GetServiceConfigException(Exception):
    pass


class ServiceConfigException(Exception):
    pass


def get_service_config(servers_path=None, dev_user=None, force_service_level=None):
    if not force_service_level:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        servers_config = (
            toml.load(
                Path(NAUTS_HOME)
                / "resources"
                / "servers.toml"
                # Path(__file__).parent.parent.parent / "resources" / "servers.toml"
            )
            if servers_path is None
            else servers_path
        )
        my_server = list(
            [
                s[0] for s in filter(
                    lambda server: server[1]["ip"] == local_ip,
                    servers_config["servers"].items(),
                )
            ]
        )
        if not my_server:
            # get service level including server_type=docker or else dev
            my_server = next(
                filter(
                    lambda s: "server_type" in s[1] and s[1]["server_type"] == "docker",
                    servers_config["servers"].items(),
                ),
                ["dev", None],
            )
    else:
        my_server = [force_service_level]

    if my_server[0] == "dev":
        user = getpass.getuser() if dev_user is None else dev_user
        config_path = (
            Path(NAUTS_HOME) / "experiments" / user / "resources" / "application.json"
        )
        resource_path = Path(NAUTS_HOME) / "experiments" / user / "resources"
    elif my_server[0] == "stage":
        config_path = Path(NAUTS_HOME) / "stage" / "resources" / "application.json"
        resource_path = Path(NAUTS_HOME) / "stage" / "resources"
    elif my_server[0] == "prod":
        config_path = Path(NAUTS_HOME) / "resources" / "application.json"
        resource_path = Path(NAUTS_HOME) / "resources"
    else:
        raise GetServiceConfigException()

    return ServiceLevelConfig(config=Config(config_path), resource_path=str(resource_path), service_level=my_server[0], available_service_levels=my_server)


class DbConfig(BaseModel):
    user: str
    password: str
    port: int
    database: str

    def get_url(self, protocol, host="127.0.0.1"):
        return f'{protocol}://{self.user}:{self.password}@{host}:{self.port}/{self.database}'


class ServiceLevelConfig:
    def __init__(self, config: Config, resource_path: Optional[str]=None, service_level: Optional[str]=None, available_service_levels: Optional[list]=None):
        self.config = config
        self.resource_path = resource_path
        self.service_level = service_level
        self.available_service_levels = available_service_levels

    @property
    def filesystem(self):
        return self.config.get("fs") or "hdfs"

    @property
    def has_local_fs(self):
        return self.filesystem.upper() == "LOCAL"

    @property
    def has_hdfs_fs(self):
        return self.filesystem.upper() == "HDFS"

    @property
    def has_gcs_fs(self):
        return self.filesystem.upper() == "GCS"
    
    @property
    def has_dfs(self):
        return self.has_hdfs_fs or self.has_gcs_fs

    @property
    def username(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            username = self.config.get("hdfs.username")
        elif fs_type == 'gcs':
            username = self.config.get("gcs.username")
        elif fs_type == "local":
            username = self.config.get("local.username")
        else:
            raise ServiceConfigException(
                "NO MATCHED FILE SYSTEM: fs of application.json"
            )
        return username
    
    @property
    def service_username(self):
        return getpass.getuser()

    @property
    def hdfs_prefix(self):
        hdfs_prefix_path = f"{self.config['hdfs.protocol']}{self.config['hdfs.hostname']}:{self.config['hdfs.port']}"
        return hdfs_prefix_path

    @property
    def local_prefix(self):
        return NAUTS_LOCAL_ROOT

    @property
    def gcs_prefix(self):
        gcs_prefix_path = f"{self.config['gcs.protocol']}{self.config['gcs.bucketname']}"
        return gcs_prefix_path

    @property
    def filesystem_prefix(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            fs_prefix = self.hdfs_prefix
        elif fs_type == "gcs":
            fs_prefix = self.gcs_prefix
        elif fs_type == "local":
            fs_prefix = self.local_prefix
        else:
            raise ServiceConfigException(
                "NO MATCHED FILE SYSTEM: fs of application.json"
            )
        return fs_prefix

    @property
    def filesystem_scheme(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            fs_prefix = self.hdfs_prefix
        elif fs_type == "gcs":
            fs_prefix = self.gcs_prefix
        elif fs_type == "local":
            fs_prefix = f"file://{self.local_prefix}"
        else:
            raise ServiceConfigException(
                "NO MATCHED FILE SYSTEM: fs of application.json"
            )
        return fs_prefix

    @property
    def spark_master(self):
        return self.config.get("spark.master")

    @property
    def elastic_host(self):
        return self.config.get("elastic.host")

    @property
    def elastic_username(self):
        return self.config.get("elastic.username")

    @property
    def elastic_password(self):
        return self.config.get("elastic.password")

    @property
    def has_elastic_http_auth(self):
        return (
            self.config.get("elastic.username") is not None
            and self.config.get("elastic.password") is not None
        )
    
    @property
    def elastic_product(self):
        return self.config.get("elastic.product") or ELASTICSEARCH_ORIGIN
    
    @property
    def db_config(self):
        return DbConfig(
            user=self.config.get("db.user"),
            password=self.config.get("db.password"),
            port=self.config.get("db.port"),
            database=self.config.get("db.database")
        )

    @property
    def deploy_root_path(self):
        my_hostname = os.environ.get('CONTAINER_NAME') or socket.gethostname()
        deploy_root_path = f"/user/{self.service_username}/{my_hostname}"
        return deploy_root_path

    @property
    def api_key_name(self):
        return self.config.get("api_key.name")

    @property
    def api_key_token(self):
        return self.config.get("api_key.token")

    @property
    def gcp_project_id(self):
        return self.config.get("gcp.project_id") or None

    @property
    def gcs_project_id(self):
        return self.config.get("gcs.project_id") or None

class ServiceModule:
    def __init__(self, service_config: ServiceLevelConfig):
        self.service_config = service_config
