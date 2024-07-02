import unittest
import pathlib

from tunip.entity.entities import (
    MetaSourcedEntity,
    TaggedEntity
)
from tunip.entity.entity_sets import MetaSourcedEntitySet
from tunip.entity.meta_source import (
    MetaSource,
    WikiMetaSourceValue
)


class EntityDistinctTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_entity_distinct(self):

        lexical_list = ["볼보 자동차", "기아", "기아"]
        
        entities = []

        for lexical in lexical_list:
            tagged_entity = TaggedEntity(
                lexical=lexical,
                tag="BRAND",
                domain="ev",
            )
            wiki_meta_value = WikiMetaSourceValue(
                head_entity=lexical,
                alias=False,
                has_es_wiki_search_result=True
            )
            meta_source = MetaSource({"WIKI": wiki_meta_value})
            meta_entity = MetaSourcedEntity(
                entity=tagged_entity, source=meta_source
            )
            entities.append(meta_entity)
        
        print("[중복 제거 전 메타 소스 엔티티 셋]")
        for ent in entities:
            print(ent)
        
        print()
        entities = MetaSourcedEntitySet(entities= entities)
        print("[중복 제거 후 메타 소스 엔티티 셋]")
        ent_cnt = 0
        for ent in entities:
            ent_cnt += 1
            print(ent)

        assert ent_cnt == 2

if __name__ == "__main__":
   unittest.main()
        

