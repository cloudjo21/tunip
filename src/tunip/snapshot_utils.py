import pathlib
import pendulum
import re

from abc import ABC
from datetime import datetime
from typing import Optional, Union

from tunip.env import NAUTS_LOCAL_ROOT
from tunip.path_utils import NautsPath
from tunip.service_config import ServiceLevelConfig

from tunip.constants import TIME_ZONE
import tunip.file_utils as file_utils


REGEX_SNAPSHOT_DT = r"[0-9]{8,}_[0-9]{6}_[0-9]{6}"


def snapshot_now(timezone=TIME_ZONE):
    snapshot = datetime.now().astimezone(pendulum.timezone(timezone)).strftime(r"%Y%m%d_%H%M%S_%f")
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
        self.regex_snapshot_dt = re.compile(REGEX_SNAPSHOT_DT)

    def provide(self, nauts_path: Union[NautsPath, str], force_fs: Optional[str] = None, return_only_nauts_path: bool=False) -> Optional[list]:
        snapshot_paths = None
        filesystem_type = self.config.filesystem.upper() if not force_fs else force_fs
        file_handler = file_utils.services.get(filesystem_type, config=self.config.config)
        if isinstance(nauts_path, Snapshot) and nauts_path.has_snapshot():
            real_path = repr(nauts_path)
            snapshot_paths = file_handler.list_dir(real_path)
        elif isinstance(nauts_path, str):
            snapshot_paths = file_handler.list_dir(nauts_path)
        else:
            raise NotSupportSnapshotException()
        if return_only_nauts_path:
            return [p.split(self.config.filesystem_prefix)[-1] for p in snapshot_paths]
        else:
            return snapshot_paths

    def latest(self, nauts_path: Union[NautsPath, str], force_fs: Optional[str] = None, return_only_nauts_path: bool=False) -> Optional[str]:
        paths = self.provide(nauts_path, force_fs, return_only_nauts_path)
        if paths:
            return paths[-1]
        else:
            raise NoSnapshotPathException()

    def latest_snapshot_dt(self, nauts_path: Union[NautsPath, str], force_fs: Optional[str]=None, return_only_nauts_path: bool=False):
        latest_path = pathlib.Path(self.latest(nauts_path, force_fs, return_only_nauts_path))
        if self.regex_snapshot_dt.match(latest_path.parts[-1]):
            return latest_path.parts[-1]
        else:
            raise NoSnapshotPathException()


class SnapshotCleaner:
    def __init__(self, service_config: ServiceLevelConfig, paths_left=3):
        self.config = service_config
        self.regex_snapshot_dt = re.compile(REGEX_SNAPSHOT_DT)
        self.paths_left = paths_left

    def clean(self, root_path: str, force_fs=None):
        if force_fs is None:
            file_service = file_utils.services(self.config)
        else:
            file_service = file_utils.services.get(force_fs.upper(), config=self.config.config)
        if file_service.exists(root_path):
            snapshot_paths = []
            for snapshot_path in file_service.list_dir(root_path):
                if self.regex_snapshot_dt.match(pathlib.Path(snapshot_path).parts[-1]):
                    snapshot_paths.append(snapshot_path)

            for snapshot_path in sorted(snapshot_paths)[:-self.paths_left]:
                file_service.remove_dir(snapshot_path, use_nauts_path=False)
