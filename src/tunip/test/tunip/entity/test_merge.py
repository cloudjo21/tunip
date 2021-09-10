import unittest

from tunip.entity.entities import MetaSourcedEntity, TaggedEntity
from tunip.entity.entity_sets import MetaSourcedEntitySet
from tunip.entity.merge import MetaSourcedEntitySetMerger
from tunip.entity.meta_source import (
    MetaSource,
    RawKwsMetaSourceValue,
    WikiMetaSourceValue,
)


class MergeTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_merge_entities_set(self):
        a_meta_source = MetaSource(
            {"KWD": RawKwsMetaSourceValue(domain="kb", column_name="BRAND")}
        )
        b_meta_source_alias_False = MetaSource(
            {"WIKI": WikiMetaSourceValue(head_entity="KB국민은행", alias=False)}
        )
        b_meta_source = MetaSource(
            {"WIKI": WikiMetaSourceValue(head_entity="KB국민은행", alias=True)}
        )

        a_ent_set = MetaSourcedEntitySet(
            entities=[
                MetaSourcedEntity(
                    TaggedEntity(lexical="국민은행", tag="COMPANY", domain="kb"), a_meta_source
                ),
                MetaSourcedEntity(
                    TaggedEntity(lexical="kb국민은행", tag="COMPANY", domain="kb"), a_meta_source
                ),
            ]
        )

        b_ent_set = MetaSourcedEntitySet(
            entities=[
                MetaSourcedEntity(
                    TaggedEntity(lexical="kb국민은행", tag="COMPANY", domain="kb"),
                    b_meta_source_alias_False,
                ),
                MetaSourcedEntity(
                    TaggedEntity(lexical="국민은행", tag="COMPANY", domain="kb"), b_meta_source
                ),
            ]
        )
        entities = MetaSourcedEntitySetMerger.merge(a_ent_set, b_ent_set)
        merged = MetaSourcedEntitySet(entities=entities)
        print(merged)
        assert merged.entities[0].source.contains_key(RawKwsMetaSourceValue.TYPE_)
        assert merged.entities[0].source.contains_key(RawKwsMetaSourceValue.TYPE_)
        assert merged.entities[1].source.contains_key(WikiMetaSourceValue.TYPE_)
        assert merged.entities[1].source.contains_key(WikiMetaSourceValue.TYPE_)
