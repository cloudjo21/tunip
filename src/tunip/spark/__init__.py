from abc import ABC, abstractmethod

from tunip.service_config import ServiceLevelConfig


class SparkConfigLoader(ABC):

    def __init__(self, service_config: ServiceLevelConfig):
        self.service_config = service_config

    @abstractmethod
    def hadoop_config(self) -> dict:
        pass

    @abstractmethod
    def spark_config(self) -> dict:
        pass