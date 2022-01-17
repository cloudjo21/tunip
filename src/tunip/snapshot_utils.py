import pathlib
import re

from abc import ABC
from datetime import datetime
from typing import Optional

from tunip.env import NAUTS_LOCAL_ROOT
from tunip.path_utils import NautsPath
from tunip.service_config import ServiceLevelConfig

import tunip.file_utils as file_utils


def snapshot_now():
    snapshot = datetime.now().strftime(r"%Y%m%d_%H%M%S_%f")
    return snapshot


def snapshot2datetime(snapshot):
    dt = datetime.strptime(snapshot, r"%Y%m%d_%H%M%S_%f")
    return dt


# snapshot = snapshot_now()
# dt = snapshot2datetime(snapshot)
# print(snapshot)
# print(dt)


class Snapshot(ABC):
    def has_snapshot(self):
        return False


class NotSupportSnapshotException(Exception):
    pass


class NoSnapshotPathException(Exception):
    pass


class SnapshotPathProvider:
    def __init__(self, service_config: ServiceLevelConfig):
        self.config = service_config
        self.regex_snapshot_dt = re.compile(r'[0-9]{8,}_[0-9]{6}_[0-9]{6}')

    def provide(self, nauts_path: NautsPath, force_fs: Optional[str]=None) -> Optional[list]:
        snapshot_paths = None
        filesystem_type = self.config.filesystem.upper() if not force_fs else force_fs
        file_handler = file_utils.services.get(
            filesystem_type, config=self.config.config
        )
        if isinstance(nauts_path, Snapshot) and nauts_path.has_snapshot():
            real_path = repr(nauts_path)
            if filesystem_type == 'LOCAL':
                real_path = f"{NAUTS_LOCAL_ROOT}{real_path}"
            snapshot_paths = file_handler.list_dir(real_path)
        else:
            raise NotSupportSnapshotException()
        return snapshot_paths

    def latest(self, nauts_path: NautsPath, force_fs: Optional[str]=None) -> Optional[str]:
        paths = self.provide(nauts_path, force_fs)
        if paths:
            return paths[-1]
        else:
            raise NoSnapshotPathException()

    def latest_snapshot_dt(self, nauts_path, force_fs):
        latest_path = pathlib.Path(self.latest(nauts_path, force_fs))
        if self.regex_snapshot_dt.match(latest_path.parts[-1]):
            return latest_path.parts[-1]
        else:
            raise NoSnapshotPathException()
