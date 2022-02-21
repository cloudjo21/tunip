from tunip.path import NotSupportedVectorTaskName
from . import (
    MartVectorIndexDocumentSourceIndexTypeSnapshotPath,
    MartVectorIndexEntitySourceIndexTypeSnapshotPath,
    MartVectorIndexDocumentDid2vidPath,
    MartVectorIndexEntityEid2vidPath,
    MartVectorIndexDocumentVid2didPath,
    MartVectorIndexEntityVid2eidPath
)


class VectorIndexPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, index_type, snapshot_dt):
        if task_name == 'document':
            return MartVectorIndexDocumentSourceIndexTypeSnapshotPath(user_name, source_type, index_type, snapshot_dt)
        elif task_name == 'entity':
            return MartVectorIndexEntitySourceIndexTypeSnapshotPath(user_name, source_type, index_type, snapshot_dt)

        raise NotSupportedVectorTaskName(f"{task_name}")


class VectorsItem2VectorPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, snapshot_dt):
        if task_name == 'document':
            path = MartVectorIndexDocumentDid2vidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        elif task_name == 'entity':
            path = MartVectorIndexEntityEid2vidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        else:
            raise NotSupportedVectorTaskName(f"NOT SUPPORTED task_name: {task_name}")
        return path


class VectorsVector2ItemPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, snapshot_dt):
        if task_name == 'document':
            path = MartVectorIndexDocumentVid2didPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        elif task_name == 'entity':
            path = MartVectorIndexEntityVid2eidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        else:
            raise NotSupportedVectorTaskName(f"NOT SUPPORTED task_name: {task_name}")
        return path

