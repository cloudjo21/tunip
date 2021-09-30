from abc import ABC
from operator import attrgetter
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StructType,
    StructField,
    StringType
)
from typing import List, Optional

from .entities import MetaSourcedEntity


class EntitySet(ABC):
    pass


class MetaSourcedEntitySet:
    def __init__(self, entities: List[MetaSourcedEntity]):
        self.entities = entities
    
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
                    )
                ])
            )
        ])
    )
])