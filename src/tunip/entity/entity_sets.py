from abc import ABC
from typing import List

from .entities import MetaSourcedEntity


class EntitySet(ABC):
    pass


class MetaSourcedEntitySet:
    def __init__(self, entities: List[MetaSourcedEntity]):
        self.entities = entities
