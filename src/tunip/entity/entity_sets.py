from abc import ABC
from operator import attrgetter
import pyspark
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StructType,
    StructField,
    StringType
)
from typing import List, Optional

from .entities import (
    TaggedEntity,
    MetaSourcedEntity
)
from .meta_source import (
    MetaSource,
    WikiMetaSourceValue
)

class EntitySet(ABC):
    pass


class MetaSourcedEntitySet:
    def __init__(self, entities: List[MetaSourcedEntity]):
        self.entities = entities
        self.distinct()
    
    def __iter__(self):
        return iter(self.entities)
    
    def __str__(self):
        str_entities = [str(ent) for ent in self.entities]
        return ", ".join(str_entities)

    def search(self, lexical: str) -> Optional[MetaSourcedEntity]:
        filter(lambda a, lexical: a.entity.lexical == lexical, self.entities)
        entity: Optional[MetaSourcedEntity(entity, source)] = next(filter(lambda a, lexical: a.entity.lexical == lexical, self.entities), None)
        return entity
    
    def sort(self):
        self.entities = sorted(self.entities, key=attrgetter("lexical"))
        
    def distinct(self):
        # initialize
        self.sort()
        dis_entities = [self.entities[0]]
        for ent in self.entities:
            unique = True
            for d_ent in dis_entities:
                if ent.__eq__(d_ent):
                    unique = False
                    break
            if unique:
                dis_entities.append(ent)

        self.entities = dis_entities
        
    @classmethod
    def from_dataframe_for_wiki(cls, dataframe: pyspark.sql.DataFrame) -> List[MetaSourcedEntity]:
        entity_set_df = dataframe.toPandas()
        entities = []

        for row in range(len(entity_set_df)):
            entity = entity_set_df["entity"][row]
            source = entity_set_df["source"][row]
            tagged_entity = TaggedEntity(
                lexical=entity.lexical,
                tag=entity.tag,
                domain=entity.domain,
            )
            wiki_meta_value = WikiMetaSourceValue(
                head_entity=source.values["WIKI"].head_entity,
                alias=source.values["WIKI"].alias,
                has_es_wiki_search_result=source.values["WIKI"].has_es_wiki_search_result
            )
            meta_source = MetaSource({"WIKI": wiki_meta_value})
            meta_entity = MetaSourcedEntity(
                entity=tagged_entity, source=meta_source
            )
            entities.append(meta_entity)

        return entities       


entity_set_schema = StructType([
    StructField("entity", 
        StructType([
            StructField("lexical", StringType()),
            StructField("tag", StringType()),
            StructField("domain", StringType())
        ])
    ),
    StructField("source",
        StructType([
            StructField("values",
                StructType([
                    StructField("KWD",
                        StructType([
                            StructField("domain", StringType()),
                            StructField("column_name", StringType())
                        ]),
                        True
                    ),
                    StructField("WIKI",
                        StructType([
                            StructField("head_entity", StringType()),
                            StructField("alias", BooleanType()),
                            StructField("has_es_wiki_search_result", BooleanType()),
                            StructField("categories", ArrayType(StringType(), True), True)
                        ]),
                        True
                    ),
                    StructField("DOCCANO",
                        StructType([
                            StructField("domain", StringType())
                        ]),
                        True
                    )
                ])
            )
        ])
    )
])