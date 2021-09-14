from abc import ABC
from datetime import datetime


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
