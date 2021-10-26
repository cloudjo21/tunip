from abc import ABC
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class MetaSourceValue(ABC):
    pass


@dataclass
class RawKwsMetaSourceValue(MetaSourceValue):
    TYPE_ = "KWD"

    domain: str
    column_name: str


@dataclass
class WikiMetaSourceValue(MetaSourceValue):
    TYPE_ = "WIKI"

    head_entity: str
    alias: bool
    has_es_wiki_search_result: bool
    categories: list = field(default_factory=lambda: [])

@dataclass
class DoccanoMetaSourceValue(MetaSourceValue):
    TYPE_ = "DOCCANO"

    domain: str

@dataclass
class MetaSource:
    values: Dict[str, MetaSourceValue]
    
    def __getitem__(self, key: str):
        return self.values.get(key)

    def __setitem__(self, key: str, value: MetaSourceValue):
        self.values[key] = value

    def __str__(self):
        str_val = []
        for key, value in self.items():
            str_val.append(f"{key}:{value}") 
        
        return ",".join(str_val)

    def items(self):
        return self.values.items()
    
    def contains_key(self, source_type):
        return source_type in self.values
