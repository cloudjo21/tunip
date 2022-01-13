import unittest

from tunip.path.lake.corpus import (
    LakeCorpusSerpDomainPath,
    LakeCorpusSerpDomainSnapshotPath
)
from tunip.path.warehouse import (
    WarehouseEntityTrieDomainPath,
    WarehouseEntityTrieDomainSnapshotPath
)
from tunip.path.path_getter import get_lake_path_by, get_warehouse_path_by
from tunip.service_config import get_service_config


class PathGetterTest(unittest.TestCase):
    
    def setUp(self):
        service_config = get_service_config(force_service_level='dev')
        self.user_name = service_config.username


    def test_get_lake_path_by(self):
        path_type = 'corpus_serp'
        domain_name = 'lg_display.nugget'

        lake_path = get_lake_path_by(
            path_type=path_type,
            user_name=self.user_name,
            domain_name=domain_name
        )
        assert type(lake_path) == LakeCorpusSerpDomainPath


    def test_get_lake_path_with_snapshot_dt(self):
        path_type = 'corpus_serp'
        domain_name = 'lg_display.nugget'
        snapshot_dt = '20211230_154048_990901'

        lake_path = get_lake_path_by(
            path_type=path_type,
            user_name=self.user_name,
            domain_name=domain_name,
            snapshot_dt=snapshot_dt
        )
        assert type(lake_path) == LakeCorpusSerpDomainSnapshotPath


    def test_get_warehouse_path_by(self):
        path_type = 'entity_trie'
        source_type = 'wiki'
        domain_name = 'wiki_span'

        warehouse_path = get_warehouse_path_by(
            path_type=path_type,
            user_name=self.user_name,
            source_type=source_type,
            domain_name=domain_name,
        )
        assert type(warehouse_path) == WarehouseEntityTrieDomainPath


    def test_get_warehouse_path_with_snapshot_dt(self):
        path_type = 'entity_trie'
        source_type = 'wiki'
        domain_name = 'wiki_span'
        snapshot_dt = '20220112_163953_476555'

        warehouse_path = get_warehouse_path_by(
            path_type=path_type,
            user_name=self.user_name,
            source_type=source_type,
            domain_name=domain_name,
            snapshot_dt=snapshot_dt
        )
        assert type(warehouse_path) == WarehouseEntityTrieDomainSnapshotPath
