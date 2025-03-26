from omegaconf import OmegaConf as oc

from abc import abstractmethod
from typing import TypeVar


T = TypeVar('T')


class YamlLoader:
  def __init__(self, yaml_file):
    self.yaml_file = yaml_file

  def load(self):
    return oc.load(self.yaml_file)

  @staticmethod
  def create(payload):
    return oc.create(payload)

  @abstractmethod
  def parse(self) -> T:
    pass
