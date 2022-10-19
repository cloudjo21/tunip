import getpass
import os
import socket
import toml

from pathlib import Path

from tunip.config import Config
from tunip.env import NAUTS_LOCAL_ROOT, NAUTS_HOME


class GetServiceConfigException(Exception):
    pass


class ServiceConfigException(Exception):
    pass


def get_service_config(servers_path=None, force_service_level=None):
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
        my_server = next(
            filter(
                lambda server: server[1]["ip"] == local_ip,
                servers_config["servers"].items(),
            ),
            None,
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
        user = getpass.getuser()
        config_path = (
            Path(NAUTS_HOME) / "experiments" / user / "resources" / "application.json"
        )
    elif my_server[0] == "stage":
        config_path = Path(NAUTS_HOME) / "resources" / "application.json"
    else:
        raise GetServiceConfigException()

    return ServiceLevelConfig(Config(config_path))


class ServiceLevelConfig:
    def __init__(self, config: Config):
        self.config = config

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
    def username(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            username = self.config.get("hdfs.username")
        elif fs_type == "local":
            username = self.config.get("local.username")
        elif fs_type == 'gcs':
            username = self.config.get("gcs.username")
        else:
            raise ServiceConfigException(
                "NO MATCHED FILE SYSTEM: fs of application.json"
            )
        return username

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
        return self.config.get("elastic.product") or "opensearch"

    @property
    def deploy_root_path(self):
        my_hostname = os.environ.get('CONTAINER_NAME') or socket.gethostname()
        dfs_username = self.config.get('hdfs.username') or self.config.get('gcs.username')
        if 'CONTAINER_NAME' not in os.environ:
            deploy_root_path = f"{NAUTS_LOCAL_ROOT}/user/{dfs_username}/{my_hostname}"
        else:
            deploy_root_path = f"/user/{dfs_username}/{my_hostname}"
        return deploy_root_path
