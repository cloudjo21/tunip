import getpass
import os
import socket
import toml

from pathlib import Path
from pydantic import BaseModel
from typing import Optional

from tunip.config import Config
from tunip.constants import ELASTICSEARCH_ORIGIN
from tunip.env import DEV, NAUTS_LOCAL_ROOT, NAUTS_HOME, STAGE, PROD


class GetResourcePathException(Exception):
    pass


class GetServiceConfigException(Exception):
    pass


class ServiceConfigException(Exception):
    pass


def get_service_config(servers_path=None, dev_user=None, force_service_level=None, home_dir=NAUTS_HOME):
    if force_service_level:
        my_service_levels = [force_service_level]
        if servers_path:
            server_info = servers_config["servers"].get(force_service_level)
            if server_info:
                dev_user = server_info.get("local_username", dev_user)
                my_service_levels = [force_service_level]
            else:
                raise GetServiceConfigException(f"Invalid service level: {force_service_level}")
    else:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        servers_config = (
            toml.load(
                Path(home_dir) / "resources" / "servers.toml"
                if servers_path is None
                else servers_path
            )
        )
        my_service_levels = list(
            [
                s[0] for s in filter(
                    lambda server: server[1]["ip"] == local_ip,
                    servers_config["servers"].items(),
                )
            ]
        )
        my_service_usernames = list(
            [
                s[1]["local_username"] for s in filter(
                    lambda server: server[1]["local_username"],
                    servers_config["servers"].items(),
                )
            ]
        ) or None
        dev_user = my_service_usernames[0] if my_service_usernames else dev_user
        if not my_service_levels:
            # get service level including server_type=docker or else dev
            my_service_levels = next(
                filter(
                    lambda s: "server_type" in s[1] and s[1]["server_type"] == "docker",
                    servers_config["servers"].items(),
                ),
                ["dev", None],
            )

    my_service_level = my_service_levels[0]
    resource_path = get_resource_path(my_service_level, home_dir, dev_user)
    config_path = (
        Path(resource_path) / "application.json"
    )

    return ServiceLevelConfig(config=Config(config_path), resource_path=str(resource_path), service_level=my_service_level, available_service_levels=my_service_levels, home_dir=home_dir)


def get_resource_path(env="dev", home_dir=NAUTS_HOME, dev_user=None) -> str:
    if env == DEV:
        user = os.environ.get("NAUTS_DEV_USER", getpass.getuser()) if dev_user is None else dev_user
        resource_path = Path(home_dir) / "experiments" / user / "resources"
    elif env == STAGE:
        resource_path = Path(home_dir) / STAGE / "resources"
    elif env == PROD:
        resource_path = Path(home_dir) / "resources"
    else:
        raise GetResourcePathException()
    return str(resource_path)


class DbConfig(BaseModel):
    user: str
    password: str
    port: int
    database: str

    def get_url(self, protocol, host="127.0.0.1"):
        return f'{protocol}://{self.user}:{self.password}@{host}:{self.port}/{self.database}'


class ServiceLevelConfig:
    def __init__(self, config: Config, home_dir: str, resource_path: Optional[str]=None, service_level: Optional[str]=None, available_service_levels: Optional[list]=None):
        self.config = config
        self.resource_path = resource_path
        self.service_level = service_level
        self.available_service_levels = available_service_levels
        self.home_dir = home_dir

    @property
    def csp(self):
        return self.config.get("csp") or None

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
    def has_s3_fs(self):
        return self.filesystem.upper() == "S3"
    
    @property
    def has_dfs(self):
        return self.has_hdfs_fs or self.has_s3_fs or self.has_gcs_fs

    @property
    def username(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            username = self.config.get("hdfs.username")
        elif fs_type == 'gcs':
            username = self.config.get("gcs.username")
        elif fs_type == 's3':
            username = self.config.get("s3.username")
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
    def s3_prefix(self):
        s3_prefix_path = f"{self.config['s3.protocol']}{self.config['s3.bucketname']}"
        return s3_prefix_path

    @property
    def filesystem_prefix(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            fs_prefix = self.hdfs_prefix
        elif fs_type == "gcs":
            fs_prefix = self.gcs_prefix
        elif fs_type == "s3":
            fs_prefix = self.s3_prefix
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
        elif fs_type == "s3":
            fs_prefix = self.s3_prefix
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

    def access_config(self, key) -> Optional[str]:
        return self.config.get(key) or None

class ServiceModule:
    def __init__(self, service_config: ServiceLevelConfig):
        self.service_config = service_config
