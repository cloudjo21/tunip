import yaml

from abc import abstractmethod
from typing import TypeVar


T = TypeVar('T')


class YamlLoader:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file

    def load(self):
        with open(self.yaml_file, 'r') as file:
            cfg = yaml.load(file, Loader=yaml.Loader)

        return cfg

    @abstractmethod
    def parse(self) -> T:
        pass
