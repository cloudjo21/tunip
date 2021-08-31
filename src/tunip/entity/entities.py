from abc import ABC
from dataclasses import dataclass

from .meta_source import MetaSource


class Entity(ABC):
    pass


@dataclass
class TaggedEntity:
    lexical: str
    tag: str


@dataclass
class MetaSourcedEntity:
    entity: TaggedEntity
    source: MetaSource
