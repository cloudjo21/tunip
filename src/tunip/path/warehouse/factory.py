from tunip.path import NotSupportedVectorTaskName
from . import (
    WarehouseVectorsDocumentSourceSnapshotDid2vidPath,
    WarehouseVectorsEntitySourceSnapshotEid2vidPath,
    WarehouseVectorsDocumentSourceSnapshotVid2didPath,
    WarehouseVectorsEntitySourceSnapshotVid2eidPath,
    WarehouseVectorsDocumentSourceSnapshotArrowPath,
    WarehouseVectorsEntitySourceSnapshotArrowPath
)


class VectorsItem2VectorPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, snapshot_dt, path_service=None, return_filepath=False):
        if task_name == 'document':
            path = WarehouseVectorsDocumentSourceSnapshotDid2vidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        elif task_name == 'entity':
            path = WarehouseVectorsEntitySourceSnapshotEid2vidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        else:
            raise NotSupportedVectorTaskName(f"NOT SUPPORTED task_name: {task_name}")

        if path_service:
            return path_service.build(str(path))
        else:
            return path


class VectorsVector2ItemPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, snapshot_dt, path_service=None, return_filepath=False):
        if task_name == 'document':
            path = WarehouseVectorsDocumentSourceSnapshotVid2didPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        elif task_name == 'entity':
            path = WarehouseVectorsEntitySourceSnapshotVid2eidPath(
                user_name=user_name,
                source_type=source_type,
                snapshot_dt=snapshot_dt
            )
        else:
            raise NotSupportedVectorTaskName(f"NOT SUPPORTED task_name: {task_name}")

        if path_service:
            return path_service.build(str(path))
        else:
            return path


class VectorsArrowPathFactory:

    @classmethod
    def create(cls, task_name, user_name, source_type, snapshot_dt):
        if task_name == 'document':
            return WarehouseVectorsDocumentSourceSnapshotArrowPath(user_name, source_type, snapshot_dt)
        elif task_name == 'entity':
            return WarehouseVectorsEntitySourceSnapshotArrowPath(user_name, source_type, snapshot_dt)

        raise NotSupportedVectorTaskName(f"{task_name}")
