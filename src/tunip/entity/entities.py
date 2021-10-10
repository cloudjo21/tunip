from abc import ABC
from dataclasses import dataclass

from .meta_source import MetaSource

from tunip.hash_utils import hash_func

class NotEqualEntityLexcialException(Exception):
    pass


class Entity(ABC):
    pass


@dataclass
class TaggedEntity:
    lexical: str
    tag: str
    domain: str

    def __str__(self):
        return f"{self.lexical}/{self.tag}/{self.domain}"


@dataclass
class MetaSourcedEntity:
    entity: TaggedEntity
    source: MetaSource

    def __hash__(self):
        text = f"{self.entity}|{self.source}"
        return hash_func(text)

    def __eq__(self,other):
        return other.__hash__() == self.__hash__()

    def __getitem__(self, key):
        if key == "lexical":
            return entity.lexical
        elif key == "tag":
            return entity.tag
        elif key == "domain":
            return entity.domain
        else:
            raise NotImplementedError("Not supported key for MetaSourcedEntity")

    def __str__(self):
        str_entity = str(self.entity)
        str_source = str(self.source)

        return f"{str_entity}|{str_source}"

    def update_meta(self, other):
        if self.entity.lexical != other.lexical:
            raise NotEqualEntityLexcialException(f"CANNOT update meta source for {self.entity.lexical} != {entity.lexical}")

        source: MetaSource = other.source
        for key, value in source.items():
            self.source[key] = value

    @property
    def lexical(self):
        return self.entity.lexical