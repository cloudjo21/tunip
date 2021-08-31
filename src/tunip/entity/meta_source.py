from abc import ABC
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class MetaSourceValue(ABC):
    pass


@dataclass
class RawKwsMetaSourceValue(MetaSourceValue):
    domain: str
    column_name: str


@dataclass
class WikiMetaSourceValue(MetaSourceValue):
    head_entity: str
    alias: bool
    categories: list = field(default_factory=lambda: [])


class MetaSource:
    def __init__(self, values: Dict[str, MetaSourceValue]):
        self.values = values
    
    def __getitem__(self, key: str):
        return self.values.get(key)
