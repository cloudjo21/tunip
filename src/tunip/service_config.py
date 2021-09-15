import getpass
import socket
import toml

from pathlib import Path

from tunip.config import Config
from tunip.env import NAUTS_LOCAL_ROOT, NAUTS_HOME


class GetServiceConfigException(Exception):
    pass


class ServiceConfigException(Exception):
    pass


def get_service_config(servers_path=None):
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
        )
    )
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
    def username(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            username = self.config.get("hdfs.username")
        elif fs_type == "local":
            username = self.config.get("local.username")
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
    def filesystem_prefix(self):
        fs_type = self.filesystem
        if fs_type == "hdfs":
            fs_prefix = self.hdfs_prefix
        elif fs_type == "local":
            fs_prefix = self.local_prefix
        else:
            raise ServiceConfigException(
                "NO MATCHED FILE SYSTEM: fs of application.json"
            )
        return fs_prefix