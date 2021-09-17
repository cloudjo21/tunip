from abc import ABC
from datetime import datetime
from typing import Optional

from tunip.path_utils import NautsPath
from tunip.service_config import ServiceLevelConfig

import tunip.file_utils


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

    def provide(self, nauts_path: NautsPath) -> Optional[list]:
        snapshot_paths = None
        file_handler = file_utils.services.get(self.config.filesystem.upper())
        if isinstance(nauts_path, Snapshot) and nauts_path.has_snapshot():
            snapshot_paths = file_handler.list_dir(repr(nauts_path))
        else:
            raise NotSupportSnapshotException()
        return snapshot_paths

    def lastest(self, nauts_path: NautsPath) -> Optional[str]:
        paths = self.provide(nauts_path)
        if paths:
            return paths[-1]
        else:
            raise NoSnapshotPathException()
