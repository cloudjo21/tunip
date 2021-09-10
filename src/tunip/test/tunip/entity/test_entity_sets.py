import unittest

from tunip.entity.entities import (
    MetaSourcedEntity,
    TaggedEntity
)
from tunip.entity.entity_sets import MetaSourcedEntitySet
from tunip.entity.meta_source import (
    MetaSource,
    RawKwsMetaSourceValue,
    WikiMetaSourceValue
)


class EntitySetsTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_init_entities_set(self):
        tagged_entity = TaggedEntity(lexical="국민은행", tag="COMPANY", domain="kb")
        assert tagged_entity.tag == "COMPANY"

        kwd_meta_value = RawKwsMetaSourceValue(domain="kb", column_name="BRAND")
        wiki_meta_value = WikiMetaSourceValue(head_entity="KB국민은행", alias=True)
        meta_source = MetaSource({"KWD": kwd_meta_value, "WIKI": wiki_meta_value})

        meta_entity = MetaSourcedEntity(entity=tagged_entity, source=meta_source)
        entity_set = MetaSourcedEntitySet(entities=[meta_entity])

        assert entity_set.entities[0].source["KWD"].domain == "kb"
        assert entity_set.entities[0].source["WIKI"].head_entity == "KB국민은행"
