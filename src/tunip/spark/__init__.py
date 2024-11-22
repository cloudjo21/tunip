from abc import ABC, abstractmethod, classmethod

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


class DummyConfigLoader(SparkConfigLoader):

    def __init__(self, service_config: ServiceLevelConfig):
        super(DummyConfigLoader, self).__init__(service_config)

    def hadoop_config(self) -> dict:
        return dict()

    def spark_config(self) -> dict:
        return dict()


class SparkConfigLoaderFactory:

    @classmethod
    def create(cls, service_config: ServiceLevelConfig) -> SparkConfigLoader:
        cloud_service_provider = service_config.csp
        if cloud_service_provider == "aws":
            config_loader = AWSConfigLoader(service_config)
        elif cloud_service_provider == "gcp":
            config_loader = GCPConfigLoader(service_config)
        else:
            config_loader = DummyConfigLoader(service_config)
        return config_loader

